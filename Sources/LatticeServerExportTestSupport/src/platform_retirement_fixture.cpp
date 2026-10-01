#include "platform_retirement_fixture.hpp"
#include "../../LatticeCore/src/configured_retirement.hpp"
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <utility>

namespace lattice {
struct platform_retirement_test_driver::state : std::enable_shared_from_this<state> {
    using registry=detail::configured_retirement_registry;
    std::shared_ptr<registry> registry_owner=registry::instance();
    std::optional<registry::reservation> owner;
    platform_retirement_receipt receipt;
    std::shared_ptr<sync_transport> transport;
    mutable std::mutex mutex;
    std::condition_variable changed;
    platform_retirement_fixture_facts observed;
    bool collecting=false,hold_armed=false;
    int32_t native_error=0;

    state(){owner.emplace(registry_owner->reserve());receipt=owner->begin_attempt();}
    struct call_use {
        std::shared_ptr<state> owner;
        std::shared_ptr<sync_transport> transport;
        ~call_use(){
            // Drop this actual owning borrow before exposing a zero count.
            transport.reset();
            if(owner){std::lock_guard lock(owner->mutex);--owner->observed.bridge_uses;}
        }
    };
    template<class Function> bool invoke(Function&& function,bool retire=false) {
        call_use use;
        {
            std::lock_guard lock(mutex);
            if(observed.closing||collecting||!transport)return false;
            if(retire)observed.closing=true;
            ++observed.bridge_uses;use.owner=shared_from_this();use.transport=transport;
        }
        try {function(*use.transport);return true;}
        catch(...){std::lock_guard lock(mutex);if(!native_error)native_error=101;return false;}
    }
    struct callback_use {
        std::shared_ptr<state> owner;
        explicit callback_use(std::shared_ptr<state> value):owner(std::move(value)){
            std::lock_guard lock(owner->mutex);++owner->observed.callback_uses;
        }
        ~callback_use(){std::lock_guard lock(owner->mutex);--owner->observed.callback_uses;}
    };
    void message() {
        std::unique_lock lock(mutex);++observed.messages;
        if(!hold_armed)return;
        hold_armed=false;observed.hold_entered=true;changed.notify_all();
        const auto until=std::chrono::steady_clock::now()+std::chrono::seconds(10);
        if(!changed.wait_until(lock,until,[&]{return observed.hold_released;})) {
            observed.safety_release=true;
            if(!native_error)native_error=102;
        }
    }
};
platform_retirement_test_driver::platform_retirement_test_driver() noexcept {
    try {state_=std::make_shared<state>();}catch(...){}
}
platform_retirement_receipt platform_retirement_test_driver::issued_receipt() const noexcept {
    return state_?state_->receipt:platform_retirement_receipt{};
}
bool platform_retirement_test_driver::install(sync_transport* raw) const noexcept {
    // shared_ptr consumes raw even if allocation of its control block throws.
    std::shared_ptr<sync_transport> transport;
    try {transport.reset(raw);}catch(...){return false;}
    if(!state_||!transport)return false;
    const auto s=state_;const std::weak_ptr<state> weak=s;
    try {
        transport->set_on_open([weak]{if(auto s=weak.lock()){state::callback_use use(s);std::lock_guard lock(s->mutex);++s->observed.opens;}});
        transport->set_on_message([weak](const transport_message&){if(auto s=weak.lock()){state::callback_use use(s);s->message();}});
        transport->set_on_error([weak](const std::string&){if(auto s=weak.lock()){state::callback_use use(s);std::lock_guard lock(s->mutex);++s->observed.errors;}});
        transport->set_on_close([weak](int,const std::string&){if(auto s=weak.lock()){state::callback_use use(s);std::lock_guard lock(s->mutex);++s->observed.closes;}});
        {
            std::lock_guard lock(s->mutex);
            if(s->transport||s->observed.closing||s->collecting)return false;
            if(!s->registry_owner->retain_transport(s->receipt,transport))return false;
            s->transport=std::move(transport);
        }
        return true;
    }catch(...){return false;}
}
bool platform_retirement_test_driver::bind_request(void* userdata,void(*request)(void*,const void*),void(*release)(void*)) const noexcept {
    struct receiver {
        void* userdata;void(*request)(void*,const void*);void(*release)(void*);
        receiver(void* u,void(*r)(void*,const void*),void(*d)(void*)):userdata(u),request(r),release(d){}
        receiver(const receiver&)=delete;receiver& operator=(const receiver&)=delete;
        ~receiver(){if(release)release(userdata);}
    };
    bool consumed=false;
    try {
        auto unique=std::make_unique<receiver>(userdata,request,release);
        consumed=true;
        std::shared_ptr<receiver> retained(std::move(unique));
        if(!state_||!userdata||!request||!release)return false;
        return state_->registry_owner->bind_request(state_->receipt,[retained](platform_retirement_receipt value){
            retained->request(retained->userdata,&value);
        });
    }catch(...){if(!consumed&&release)release(userdata);return false;}
}
bool platform_retirement_test_driver::connect(const std::string& url) const noexcept {
    return state_&&state_->invoke([&](sync_transport& t){t.connect(url);});
}
bool platform_retirement_test_driver::send_text(const std::string& text) const noexcept {
    return state_&&state_->invoke([&](sync_transport& t){t.send(transport_message::from_string(text));});
}
bool platform_retirement_test_driver::disconnect() const noexcept {
    return state_&&state_->invoke([](sync_transport& t){t.disconnect();});
}
bool platform_retirement_test_driver::request_retirement() const noexcept {
    if(!state_)return false;
    const auto s=state_;
    // Keep the call/transport borrow through actual once-only request delivery.
    bool accepted=false;
    const bool invoked=s->invoke([&](sync_transport& t){
        t.disconnect();accepted=s->registry_owner->request_retirement(s->receipt);
    },true);
    return invoked&&accepted;
}
bool platform_retirement_test_driver::collect_if_settled() const noexcept {
    if(!state_)return false;
    const auto s=state_;const auto before=s->registry_owner->snapshot(s->receipt);
    if(!before.valid||!before.requested||!before.adapter_complete||before.first_error)return false;
    std::shared_ptr<sync_transport> released;int32_t native_error=0;
    {
        std::lock_guard lock(s->mutex);
        if(!s->observed.closing||s->collecting||s->observed.collected||s->observed.bridge_uses||s->observed.callback_uses)return false;
        s->collecting=true;native_error=s->native_error;released=std::move(s->transport);
    }
    released.reset();
    const bool settled=before.native_complete||s->registry_owner->complete_native_cleanup(s->receipt,native_error);
    const bool collected=settled&&s->registry_owner->collect_completed(s->receipt);
    if(collected)s->owner.reset();
    {std::lock_guard lock(s->mutex);s->collecting=false;s->observed.collected=collected;}
    return collected;
}
platform_retirement_fixture_facts platform_retirement_test_driver::facts() const noexcept {
    if(!state_)return {};
    const auto s=state_;platform_retirement_fixture_facts value;
    {std::lock_guard lock(s->mutex);value=s->observed;}
    const auto actual=s->registry_owner->snapshot(s->receipt);
    value.issued=actual.valid;value.requested=actual.requested;value.adapter_complete=actual.adapter_complete;
    value.native_complete=actual.native_complete;value.quarantined=actual.quarantined;value.first_error=actual.first_error;
    value.charged_owners=s->registry_owner->charged_owners();return value;
}
bool platform_retirement_test_driver::arm_message_hold() const noexcept {
    if(!state_)return false;
    std::lock_guard lock(state_->mutex);
    if(state_->observed.closing||state_->hold_armed||state_->observed.hold_entered)return false;
    state_->hold_armed=true;return true;
}
void platform_retirement_test_driver::release_message_hold() const noexcept {
    if(!state_)return;
    {std::lock_guard lock(state_->mutex);state_->observed.hold_released=true;}
    state_->changed.notify_all();
}
}
