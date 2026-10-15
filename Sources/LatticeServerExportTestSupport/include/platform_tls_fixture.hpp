#pragma once
#include <lattice/network.hpp>

namespace lattice {
// Observation-only test product. It accepts the ACTUAL SDK native transport;
// no verifier/descriptor/grant setters and no install authority are exposed.
class platform_tls_test_driver {
    struct state {
        std::mutex mutex;int64_t opens=0,errors=0;platform_transport_callbacks endpoint;
        std::unique_ptr<sync_transport> transport;
    };
    std::shared_ptr<state> state_;
public:
    explicit platform_tls_test_driver(sync_transport* transport):state_(std::make_shared<state>()){state_->transport.reset(transport);}
    void connect(const std::string& url) {
        auto* transport=dynamic_cast<owned_platform_sync_transport*>(state_->transport.get());if(!transport)return;
        const auto weak=std::weak_ptr<state>(state_);
        transport->connect_with_attempt_handlers(url,{},[weak](const platform_transport_callbacks& endpoint){if(auto s=weak.lock()){
            platform_transport_callbacks old;{std::lock_guard lock(s->mutex);++s->opens;std::swap(old,s->endpoint);s->endpoint=endpoint;}}},
            [](const platform_transport_callbacks&,const transport_message&){},
            [weak](const platform_transport_callbacks&,const std::string&){if(auto s=weak.lock()){std::lock_guard lock(s->mutex);++s->errors;}},
            [](const platform_transport_callbacks&,int,const std::string&){});
    }
    int64_t opens()const{std::lock_guard lock(state_->mutex);return state_->opens;}
    int64_t errors()const{std::lock_guard lock(state_->mutex);return state_->errors;}
    bool system_tls()const{std::lock_guard lock(state_->mutex);return state_->endpoint.current_system_tls_for_owner();}
    void disconnect(){if(state_->transport)state_->transport->disconnect();}
    void close(){state_->transport.reset();platform_transport_callbacks old;{std::lock_guard lock(state_->mutex);std::swap(old,state_->endpoint);}}
};
}
