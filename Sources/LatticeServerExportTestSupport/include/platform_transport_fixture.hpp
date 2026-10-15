#pragma once
#include <lattice/network.hpp>

namespace lattice {
// Test-only owner of an ACTUAL platform transport returned by the SDK factory.
// No recovery authority. Calls to connect/disconnect/send/close are serialized
// by the fixture's test; foreign platform callbacks touch only bounded stats.
class platform_transport_test_driver {
    struct state {
        std::mutex mutex;
        int64_t opens = 0, messages = 0, errors = 0, closes = 0, oversized = 0;
        std::string last;
        std::unique_ptr<sync_transport> transport;
    };
    std::shared_ptr<state> state_;
public:
    explicit platform_transport_test_driver(sync_transport* transport) : state_(std::make_shared<state>()) {
        state_->transport.reset(transport);
        if (!transport) return;
        const auto weak = std::weak_ptr<state>(state_);
        transport->set_on_open([weak] { if (auto state = weak.lock()) { std::lock_guard<std::mutex> lock(state->mutex); ++state->opens; } });
        transport->set_on_message([weak](const transport_message& message) {
            if (auto state = weak.lock()) {
                std::lock_guard<std::mutex> lock(state->mutex);
                ++state->messages;
                if (message.data.size() > 256) { ++state->oversized; return; }
                state->last = message.as_string();
            }
        });
        transport->set_on_error([weak](const std::string&) { if (auto state = weak.lock()) { std::lock_guard<std::mutex> lock(state->mutex); ++state->errors; } });
        transport->set_on_close([weak](int, const std::string&) { if (auto state = weak.lock()) { std::lock_guard<std::mutex> lock(state->mutex); ++state->closes; } });
    }
    void connect(const std::string& url) { if (state_->transport) state_->transport->connect(url); }
    void disconnect() { if (state_->transport) state_->transport->disconnect(); }
    void send_text(const std::string& value) { if (state_->transport) state_->transport->send(transport_message::from_string(value)); }
    void close() { state_->transport.reset(); }
    int64_t opens() const { std::lock_guard<std::mutex> lock(state_->mutex); return state_->opens; }
    int64_t messages() const { std::lock_guard<std::mutex> lock(state_->mutex); return state_->messages; }
    int64_t errors() const { std::lock_guard<std::mutex> lock(state_->mutex); return state_->errors; }
    int64_t closes() const { std::lock_guard<std::mutex> lock(state_->mutex); return state_->closes; }
    int64_t oversized() const { std::lock_guard<std::mutex> lock(state_->mutex); return state_->oversized; }
    std::string last_text() const { std::lock_guard<std::mutex> lock(state_->mutex); return state_->last; }
};
}
