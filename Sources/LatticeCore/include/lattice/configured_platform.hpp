#pragma once
#ifdef __cplusplus
#include <lattice/network.hpp>
#include <lattice/platform_retirement.hpp>

namespace lattice::detail {
class configured_platform_bridge;
struct configured_platform_access;
}
namespace lattice {
// SDK construction custody only. This value never grants source, TLS, storage,
// or recovery authority. Copies refer to ONE native bridge and ONE adapter
// construction claim; copying it cannot authorize a second physical adapter.
class configured_transport_registration {
    friend struct detail::configured_platform_access;
    std::shared_ptr<detail::configured_platform_bridge> bridge_;
    explicit configured_transport_registration(
        std::shared_ptr<detail::configured_platform_bridge>) noexcept;
public:
    configured_transport_registration() noexcept = default;
    bool valid() const noexcept;
    bool matches(const platform_retirement_receipt&) const noexcept;
};

using configured_retirement_request_fn_ptr = bool (*)(void*, const void*);
// Arguments: owned builder, borrowed platform_transport_callbacks endpoint,
// borrowed std::string dial URL. Endpoint and URL pointers last only the call.
using configured_tls_verify_fn_ptr = int32_t (*)(void*, const void*, const void*);

// Consume exactly ONE owned builder-context retain on EVERY path, including
// invalid receipt, missing function, duplicate registration and allocation
// failure. A non-null destroy is a caller precondition: it must release that
// retain and must not throw. Registration
// precedes platform client/session/group allocation. The SDK holds its actual
// construction Use before registration, through late attachment or failure.
//
// Each transport/TLS call admits and retains an exact native bridge use BEFORE
// it reads builder_context or calls foreign code. The separately admitted
// once-only request remains callable after ordinary bridge admission closes.
// Its second argument points to a borrowed platform_retirement_receipt for the
// call only; the SDK copies that value before retaining it. Returning true is
// request acceptance, NEVER cleanup completion. False/throw quarantines the
// attempt. Actual adapter completion uses receipt.complete_adapter_cleanup.
//
// Registered context survives an unstarted/failed adapter until real cleanup
// and off-callback native collection. An invalid result authorizes no platform
// allocation. No callback/capture destruction occurs under registry leaves.
configured_transport_registration register_configured_system_tls_platform_transport(
    const platform_retirement_receipt& receipt,
    void* builder_context,
    owned_platform_sync_transport::connect_fn_ptr connect,
    owned_platform_sync_transport::disconnect_fn_ptr disconnect,
    owned_platform_sync_transport::send_fn_ptr send,
    owned_platform_sync_transport::destroy_fn_ptr destroy,
    configured_tls_verify_fn_ptr verify,
    configured_retirement_request_fn_ptr request) noexcept;

// Atomically consume this registration's single construction claim. Copies and
// concurrent calls cannot create multiple adapters. Null does not prove adapter
// cleanup: a registered builder remains charged until its actual request and
// completion. The resulting transport preserves the existing system-TLS gate.
sync_transport* make_configured_system_tls_platform_sync_transport(
    const configured_transport_registration&) noexcept;

// Separate opt-in capability; the existing network_factory vtable and legacy
// factory callbacks remain unchanged. The configured owner pins BOTH the base
// factory and this capability before the first attempt. Unsupported factories
// keep single-use semantics and cannot silently gain automatic replacement.
class configured_platform_factory {
public:
    virtual ~configured_platform_factory() = default;
    virtual std::unique_ptr<sync_transport> create_configured_sync_transport(
        std::shared_ptr<scheduler> owner_scheduler,
        const platform_retirement_receipt&) = 0;
};

// C function pointers are Swift-safe on supported platforms. The second pointer
// borrows the actual std::shared_ptr<scheduler>; the third borrows the exact
// platform_retirement_receipt. Neither pointer may escape the synchronous call.
// Swift need not inspect the scheduler; it copies the receipt value. Native
// capacity and synchronous factory custody are already reserved before entry.
using create_configured_transport_fn_ptr = sync_transport* (*)(
    void* factory_context, const void* owner_scheduler, const void* receipt);
using configured_factory_publication_fn_ptr = void (*)(void*, bool);

// Register one object implementing BOTH old and typed paths. Consumes the owned
// factory context on failure as well as success (when destroy is non-null).
// False is explicit registration failure, never fallback to an untyped path.
// If supplied, publication is called EXACTLY ONCE with the result, outside the
// factory mutex, BEFORE destroying any displaced or failed supplied context.
// publication_context is borrowed for this synchronous call. The callback must
// not throw; this is a trusted foreign-function contract. An SDK registration
// gate publishes ready/failed and wakes its waiters here, before arbitrary
// destructor reentry. A missing/duplicate/inconsistent publication observation
// is a registration error in the SDK, never permission for a legacy fallback.
bool register_configured_generic_network_factory(
    void* factory_context,
    generic_network_factory::create_http_fn_ptr http,
    generic_network_factory::create_transport_fn_ptr legacy_transport,
    create_configured_transport_fn_ptr configured_transport,
    void (*destroy)(void*) = nullptr,
    configured_factory_publication_fn_ptr publication = nullptr,
    void* publication_context = nullptr) noexcept;
}
#endif
