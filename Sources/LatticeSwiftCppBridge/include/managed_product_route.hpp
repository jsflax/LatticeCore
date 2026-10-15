#pragma once
#include <bridging.hpp>
#include <unistd.h>
#include "../../LatticeCore/src/ordinary_managed_ipc_endpoint.hpp"
#include "../../LatticeCore/src/ordinary_managed_wss_owner.hpp"
#include "../../LatticeCore/src/ordinary_managed_product_route.hpp"
#include "../../LatticeCore/src/ordinary_managed_receiver_wss.hpp"

namespace lattice {
class swift_close_result;
struct cxx_error;
namespace detail { class LatticeCache; }
class swift_managed_product_credentials final {
    friend class swift_lattice_ref;
    friend struct swift_configuration;
    friend class swift_managed_product_route;
    friend class swift_owned_receiver_preservation;
    friend class swift_managed_receiver_request;
    std::shared_ptr<detail::ordinary_managed_product_credentials> value_;
public:
    swift_managed_product_credentials()=default; // invalid, grants nothing
    static swift_managed_product_credentials create(const std::string& endpoint,const std::string& token) noexcept {
        try{swift_managed_product_credentials result;
            result.value_=std::shared_ptr<detail::ordinary_managed_product_credentials>(
                new detail::ordinary_managed_product_credentials(endpoint,token),
                [](detail::ordinary_managed_product_credentials* value){
                    // The inherited principal mutex may have been held at
                    // fork. Do not run its destructor in the foreign child.
                    if(value->creator_==static_cast<std::uint64_t>(::getpid()))delete value;
                });return result;
        }catch(...){return {};}
    }
    bool valid()const noexcept{return value_&&value_->current();}
    void revoke()const noexcept{if(value_)value_->revoke();}
};
class swift_managed_wss_owner final {
    friend class swift_managed_product_route;
    friend class swift_managed_receiver_request;
#ifndef __EMSCRIPTEN__
    std::shared_ptr<detail::ordinary_managed_wss_handle> value_;
    explicit swift_managed_wss_owner(std::shared_ptr<detail::ordinary_managed_wss_handle> value):value_(std::move(value)){}
#endif
public:
    swift_managed_wss_owner()=default;
    bool valid()const noexcept{
#ifndef __EMSCRIPTEN__
        return bool(value_);
#else
        return false;
#endif
    }
    void stop()const noexcept{
#ifndef __EMSCRIPTEN__
        if(value_)value_->stop();
#endif
    }
    bool settled()const noexcept{
#ifndef __EMSCRIPTEN__
        return value_&&value_->settled();
#else
        return false;
#endif
    }
    swift_close_result close()const noexcept;
};
// Opaque actual enrolled primary/paired socket owner. No fd/profile/JSON input.
class swift_managed_peer_outcome final {
    friend class swift_managed_peer_owner;
    bool cleaned_=false,failed_=true,cleanup_failed_=false;
    std::int32_t code_=static_cast<std::int32_t>(detail::ordinary_ipc_code::admission_refused);
public:
    bool cleanup_complete()const noexcept{return cleaned_;}
    bool failed()const noexcept{return failed_;}
    bool cleanup_failed()const noexcept{return cleanup_failed_;}
    std::int32_t code()const noexcept{return code_;}
};
class swift_managed_peer_owner final {
    friend class swift_lattice_ref;
    friend class swift_managed_product_route;
    std::shared_ptr<detail::ordinary_managed_ipc_endpoint> value_;
    explicit swift_managed_peer_owner(std::shared_ptr<detail::ordinary_managed_ipc_endpoint> value):value_(std::move(value)){}
public:
    swift_managed_peer_owner()=default;
    bool valid()const noexcept{return bool(value_);}
    bool failed()const noexcept {if(!value_)return true;const auto facts=value_->observe();return facts.stopping||!facts.first_failure.succeeded();}
    void stop()const noexcept{if(value_)value_->request_stop();}
    swift_managed_peer_outcome close()const noexcept{
        swift_managed_peer_outcome result;if(!value_)return result;
        result.cleaned_=value_->retire(std::chrono::steady_clock::now()+std::chrono::seconds(2));
        const auto facts=value_->observe();result.code_=static_cast<std::int32_t>(facts.first_failure.code);
        result.failed_=!facts.first_failure.succeeded();result.cleanup_failed_=bool(facts.cleanup_error);return result;
    }
};
class swift_managed_product_route final {
    friend class swift_managed_product_request;
    std::shared_ptr<const detail::ordinary_managed_ipc_local_route> value_;
    explicit swift_managed_product_route(std::shared_ptr<const detail::ordinary_managed_ipc_local_route> v):value_(std::move(v)){}
public:
    swift_managed_product_route()=default; // invalid, grants nothing
    bool current()const noexcept{return value_&&detail::ordinary_managed_ipc_access::route_current(*value_);}
    swift_managed_wss_owner start_wss(const swift_managed_product_credentials& credentials,const std::string& token)const noexcept {
#ifndef __EMSCRIPTEN__
        try{if(value_)return swift_managed_wss_owner(detail::ordinary_managed_ipc_access::start_wss(*value_,credentials.value_,token));}
        catch(...){}
#endif
        return {};
    }

    bool bind_peer(const swift_managed_peer_owner& peer)const noexcept {
        try{if(!value_||!peer.value_)return false;
            detail::ordinary_managed_ipc_access::bind_peer_route(peer.value_,value_,std::chrono::steady_clock::now()+std::chrono::seconds(2));return true;
        }catch(...){return false;}
    }
    std::string principal()const noexcept {
        // Revocation can race the Swift property read. Never let a refusal
        // exception cross the imported C++/Swift boundary.
        try{return value_?detail::ordinary_managed_ipc_access::principal(*value_):std::string{};}
        catch(...){return {};}
    }
};
class swift_managed_product_request final {
    friend class swift_lattice_ref;
    std::shared_ptr<detail::ordinary_managed_product_state> value_;
    explicit swift_managed_product_request(std::shared_ptr<detail::ordinary_managed_product_state> value):value_(std::move(value)){}
public:
    swift_managed_product_request()=default;
    bool valid()const noexcept{return detail::ordinary_managed_ipc_access::current(value_);}
    std::uint64_t transport_cookie()const noexcept SWIFT_NAME(transportCookie()) {return detail::ordinary_managed_ipc_access::cookie(value_);}
    void cancel()const noexcept{detail::ordinary_managed_ipc_access::cancel(value_);}
    // There is no API accepting a response/principal/membership. This succeeds
    // only after the fixed SDK's private transport completion was observed.
    swift_managed_product_route publish()const noexcept {
        try{return swift_managed_product_route(detail::ordinary_managed_ipc_access::publish(value_));}
        catch(...){cancel();return {};}
    }
};

// Closed passive directory-selection result from the actual request accessor.
// Unavailable, personal and group are distinct; copied observations cannot start
// a receiver or publish directory/membership/TLS/install/receipt authority.
class swift_managed_receiver_directory_selection final {
    friend class swift_managed_receiver_request;
    enum class role : std::uint8_t { unavailable,personal,group };
    role role_=role::unavailable;
    std::string group_,nonce_;
public:
    swift_managed_receiver_directory_selection()=default;
    bool available()const noexcept SWIFT_NAME(isAvailable()){return role_!=role::unavailable;}
    bool personal()const noexcept SWIFT_NAME(isPersonal()){return role_==role::personal;}
    bool group()const noexcept SWIFT_NAME(isGroup()){return role_==role::group;}
    std::string group_identifier()const noexcept SWIFT_NAME(groupIdentifier()){
        try{return group_;}catch(...){return {};}
    }
    std::string admission_nonce()const noexcept SWIFT_NAME(admissionNonce()){
        try{return nonce_;}catch(...){return {};}
    }
};
// Opaque closed restart receiver handoff, never a path/profile/receipt DTO.
class swift_managed_receiver_request final {
    friend class swift_owned_receiver_preservation;
    std::shared_ptr<const detail::ordinary_receiver_wss_request> value_;
    explicit swift_managed_receiver_request(std::shared_ptr<const detail::ordinary_receiver_wss_request> value):value_(std::move(value)){}
public:
    swift_managed_receiver_request()=default;
    bool valid()const noexcept{return value_&&value_->current();}
    swift_managed_receiver_directory_selection directory_selection()const noexcept SWIFT_NAME(directorySelection()){
#ifndef __EMSCRIPTEN__
        try{
            swift_managed_receiver_directory_selection result;
            // This may throw on retired request OR string-copy allocation. No
            // role is published until that actual operation and current recheck
            // both succeed; a catch returns unavailable and cancels admission.
            result.group_=detail::ordinary_receiver_wss_access::requested_group(value_);
            result.nonce_=detail::ordinary_receiver_wss_access::admission_nonce(value_);
            if(!valid()){cancel();return {};}
            result.role_=result.group_.empty()?swift_managed_receiver_directory_selection::role::personal:
                swift_managed_receiver_directory_selection::role::group;
            return result;
        }catch(...){cancel();}
#endif
        return {};
    }
    std::string requested_group()const noexcept SWIFT_NAME(requestedGroup()){
#ifndef __EMSCRIPTEN__
        try{return detail::ordinary_receiver_wss_access::requested_group(value_);}catch(...){}
#endif
        return {};
    }
    std::uint64_t transport_cookie()const noexcept SWIFT_NAME(transportCookie()){
#ifndef __EMSCRIPTEN__
        return detail::ordinary_receiver_wss_access::cookie(value_);
#else
        return 0;
#endif
    }
    void cancel()const noexcept{
#ifndef __EMSCRIPTEN__
        detail::ordinary_receiver_wss_access::cancel(value_);
#endif
    }
    swift_managed_wss_owner start(const swift_managed_product_credentials& credentials,const std::string& token)const noexcept{
#ifndef __EMSCRIPTEN__
        try{return swift_managed_wss_owner(detail::ordinary_receiver_wss_access::start(value_,credentials.value_,token));}catch(...){cancel();}
#endif
        return {};
    }
};
class swift_owned_receiver_preservation final {
    friend class swift_lattice_ref;
    friend class swift_managed_group_receiver;
    std::shared_ptr<const detail::ordinary_managed_receiver_preservation> value_;
    explicit swift_owned_receiver_preservation(std::shared_ptr<const detail::ordinary_managed_receiver_preservation> value):value_(std::move(value)){}
public:
    swift_owned_receiver_preservation()=default;
    bool valid()const noexcept{return bool(value_);}
    swift_managed_receiver_request begin_receiver_request(const swift_managed_product_credentials& credentials)const noexcept
        SWIFT_NAME(beginReceiverRequest(credentials:)){
#ifndef __EMSCRIPTEN__
        try{return swift_managed_receiver_request(detail::ordinary_receiver_wss_access::begin(value_,credentials.value_));}catch(...){}
#endif
        return {};
    }
};
// Opaque original preparation custody. Empty/copy-only instances grant nothing.
// The actual unpublished owner, first failure and preservation stay in the
// process-bound native registry even if a Swift wrapper is dropped.
class swift_managed_group_receiver final {
    friend class swift_lattice_ref;
    friend class detail::LatticeCache;
    struct state;
    std::shared_ptr<state> value_;
public:
    swift_managed_group_receiver()=default;
    // Storage observation only. Success supplies the SAME privately produced
    // capability to its existing request API; no DTO/path reconstruction.
    bool preservation(swift_owned_receiver_preservation& held,cxx_error&) const noexcept
        SWIFT_NAME(preservation(held:error:));
};
}
