#include "configured_platform.hpp"
#include "configured_retirement.hpp"
#include "network_factory_state.hpp"
#include <stdexcept>

namespace lattice::detail {
configured_platform_bridge::configured_platform_bridge(platform_retirement_receipt receipt,
    std::shared_ptr<configured_attempt_custody> custody,void* context,
    owned_platform_sync_transport::connect_fn_ptr connect,
    owned_platform_sync_transport::disconnect_fn_ptr disconnect,
    owned_platform_sync_transport::send_fn_ptr send,
    owned_platform_sync_transport::destroy_fn_ptr destroy,
    configured_tls_verify_fn_ptr verify,configured_retirement_request_fn_ptr request)
    :receipt_(std::move(receipt)),custody_(std::move(custody)),context_(context),
     connect_(connect),disconnect_(disconnect),send_(send),destroy_(destroy),verify_(verify),request_(request){}
configured_platform_bridge::~configured_platform_bridge(){
    // Before binding, no platform resource is permitted. After binding the
    // registry retains this object until collect clears its foreign context.
    if(context_&&destroy_)destroy_(context_);
}
bool configured_platform_bridge::valid()const noexcept {
    {std::lock_guard<std::mutex> lock(mutex_);if(collected_)return false;}
    return receipt_.valid();
}
bool configured_platform_bridge::matches(const platform_retirement_receipt& value)const noexcept{return receipt_.matches(value)&&valid();}
bool configured_platform_bridge::claim()noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    if(claimed_||closing_||collected_)return false;
    claimed_=true;return true;
}
void* configured_platform_bridge::admitted_context()const {
    std::lock_guard<std::mutex> lock(mutex_);
    if(closing_||collected_)throw std::runtime_error("configured platform attempt is retiring");
    return context_;
}
void configured_platform_bridge::connect(const void* url,const void* headers,const void* callbacks){
    auto use=custody_->admit(configured_attempt_custody::kind::command);
    if(!use)throw std::runtime_error("configured platform connect admission closed");
    custody_->before_foreign(configured_bridge_operation::connect);
    connect_(admitted_context(),url,headers,callbacks);
}
void configured_platform_bridge::disconnect(){
    auto use=custody_->admit(configured_attempt_custody::kind::command);
    if(!use)return; // Exact retirement request owns cleanup after the fence.
    custody_->before_foreign(configured_bridge_operation::disconnect);
    void* context=nullptr;
    {std::lock_guard<std::mutex> lock(mutex_);if(closing_||collected_)return;context=context_;}
    disconnect_(context);
}
void configured_platform_bridge::send(const void* message,const void* callbacks){
    auto use=custody_->admit(configured_attempt_custody::kind::command);
    if(!use)throw std::runtime_error("configured platform send admission closed");
    custody_->before_foreign(configured_bridge_operation::send);
    send_(admitted_context(),message,callbacks);
}
int32_t configured_platform_bridge::verify(const void* callbacks,const void* url)noexcept {
    try {
        auto use=custody_->admit(configured_attempt_custody::kind::command);
        if(!use)return 0;
        custody_->before_foreign(configured_bridge_operation::verify);
        return verify_(admitted_context(),callbacks,url);
    }catch(...){return 0;}
}
void configured_platform_bridge::request(platform_retirement_receipt receipt){
    if(!receipt_.matches(receipt))throw std::logic_error("configured platform retirement receipt differs");
    void* context=nullptr;
    {std::lock_guard<std::mutex> lock(mutex_);if(collected_)throw std::logic_error("configured platform already collected");closing_=true;context=context_;}
    custody_->close();
    // This once-only control call is counted by registry.notifying, not by
    // ordinary admission which was just closed. Collection waits its return.
    if(!request_(context,&receipt))throw std::runtime_error("configured platform retirement request rejected");
}
bool configured_platform_bridge::collect()noexcept {
    const auto counts=custody_->snapshot();
    if(counts.commands||counts.payloads||counts.workers||counts.first_error)return false;
    void* context=nullptr;
    {std::lock_guard<std::mutex> lock(mutex_);if(collected_)return true;if(!closing_)return false;collected_=true;context=std::exchange(context_,nullptr);}
    if(context&&destroy_)destroy_(context);
    return true;
}
namespace {
using held_bridge=std::shared_ptr<configured_platform_bridge>;
void connect(void* p,const void* u,const void* h,const void* c){(*static_cast<held_bridge*>(p))->connect(u,h,c);}
void disconnect(void* p){(*static_cast<held_bridge*>(p))->disconnect();}
void send(void* p,const void* m,const void* c){(*static_cast<held_bridge*>(p))->send(m,c);}
int32_t verify(void* p,const void* c,const void* u){return (*static_cast<held_bridge*>(p))->verify(c,u);}
void release(void* p){delete static_cast<held_bridge*>(p);}
class configured_generic_factory final : public generic_network_factory,public configured_platform_factory {
    void* context_;
    create_configured_transport_fn_ptr create_;
public:
    configured_generic_factory(void* context,generic_network_factory::create_http_fn_ptr http,
        generic_network_factory::create_transport_fn_ptr legacy,create_configured_transport_fn_ptr create,void(*destroy)(void*))
        :generic_network_factory(context,http,legacy,destroy),context_(context),create_(create){}
    std::unique_ptr<sync_transport> create_configured_sync_transport(std::shared_ptr<scheduler> owner,const platform_retirement_receipt& receipt)override{
        return std::unique_ptr<sync_transport>(create_(context_,&owner,&receipt));
    }
};
}
}
namespace lattice {
configured_transport_registration::configured_transport_registration(std::shared_ptr<detail::configured_platform_bridge> bridge)noexcept:bridge_(std::move(bridge)){}
bool configured_transport_registration::valid()const noexcept{return bridge_&&bridge_->valid();}
bool configured_transport_registration::matches(const platform_retirement_receipt& receipt)const noexcept{return bridge_&&bridge_->matches(receipt);}
configured_transport_registration register_configured_system_tls_platform_transport(
    const platform_retirement_receipt& receipt,void* context,
    owned_platform_sync_transport::connect_fn_ptr connect,
    owned_platform_sync_transport::disconnect_fn_ptr disconnect,
    owned_platform_sync_transport::send_fn_ptr send,
    owned_platform_sync_transport::destroy_fn_ptr destroy,
    configured_tls_verify_fn_ptr verify,configured_retirement_request_fn_ptr request)noexcept {
    std::shared_ptr<detail::configured_platform_bridge> bridge;
    try {
        const auto registry=detail::configured_platform_access::registry(receipt);
        const auto custody=registry?registry->attempt_custody(receipt):nullptr;
        if(!custody||!context||!connect||!disconnect||!send||!destroy||!verify||!request)
            throw std::invalid_argument("configured platform requires issued construction custody and owned callbacks");
        bridge=std::make_shared<detail::configured_platform_bridge>(receipt,custody,context,connect,disconnect,send,destroy,verify,request);
        if(!registry->bind_platform_bridge(receipt,bridge,[bridge](platform_retirement_receipt r){bridge->request(std::move(r));}))return {};
        return detail::configured_platform_access::registration(std::move(bridge));
    }catch(...){if(!bridge&&destroy)destroy(context);return {};}
}
sync_transport* make_configured_system_tls_platform_sync_transport(const configured_transport_registration& registration)noexcept {
    using namespace detail;
    auto bridge=configured_platform_access::bridge(registration);
    if(!bridge||!bridge->claim())return nullptr;
    try {
        auto transport=std::make_unique<held_bridge>(bridge);
        auto verification=std::make_unique<held_bridge>(std::move(bridge));
        // Existing helper consumes both native holder allocations on every path.
        return make_system_tls_platform_sync_transport(transport.release(),connect,disconnect,send,release,
            verification.release(),verify,release);
    }catch(...){return nullptr;}
}
bool register_configured_generic_network_factory(void* context,
    generic_network_factory::create_http_fn_ptr http,generic_network_factory::create_transport_fn_ptr legacy,
    create_configured_transport_fn_ptr configured,void(*destroy)(void*),
    configured_factory_publication_fn_ptr publication,void* publication_context)noexcept {
    std::shared_ptr<detail::configured_generic_factory> factory;
    std::shared_ptr<network_factory> displaced;
    try {
        if(!configured)throw std::invalid_argument("configured transport factory callback required");
        factory=std::make_shared<detail::configured_generic_factory>(context,http,legacy,configured,destroy);
        // Keep this local owner until after publication even if state-slot
        // allocation or locking throws. Failure cannot run its destructor
        // before the registration gate has observed failure.
        displaced=detail::exchange_network_factory(factory);
    }catch(...){
        if(publication)publication(publication_context,false);
        if(!factory&&destroy)destroy(context);
        factory.reset();
        return false;
    }
    if(publication)publication(publication_context,true);
    displaced.reset();factory.reset();
    return true;
}
}
