#pragma once
#include "canonical_writer_adapter.hpp"
#include <atomic>
#include <chrono>
#include <mutex>

namespace lattice { class swift_lattice_ref; }
namespace lattice::detail {
// Payload-free shared stop state. Only the real setup can authorize it. Close
// and kicks may stop it from any thread without entering SQL or waiting.
class authenticated_session_fence {
    friend class authenticated_relay_setup;
    friend class authenticated_relay_operation;
    std::atomic<bool> stopped_{false},authorized_{false};
    std::atomic<int64_t> deadline_{0};
    mutable std::mutex mutex_;
    size_t active_=0;
    authenticated_session_fence()=default;
public:
    static int64_t now() noexcept;
    bool live()const noexcept;
    bool stopped()const noexcept;
    bool drained()const noexcept;
    void stop()noexcept;
};
// Payload-free counted handoff. A result retains it through SDK publication;
// destruction settles exactly one admission without running SQL or callbacks.
class authenticated_relay_operation {
    friend class authenticated_relay_setup;
    std::shared_ptr<authenticated_session_fence> fence_;
    explicit authenticated_relay_operation(std::shared_ptr<authenticated_session_fence>);
public:
    ~authenticated_relay_operation();
    bool publishable()const noexcept;
};
struct authenticated_relay_result {
    // 1 completed with actual IDs/bookkeeping, 2 retired. Exceptions retain
    // uncertainty; the bridge reports 4 rather than claiming effect absence.
    int32_t status=0;
    std::vector<std::string> applied;
    std::shared_ptr<authenticated_relay_operation> operation;
};
struct authenticated_mounted_source;
class authenticated_relay_setup {
    friend class ::lattice::swift_lattice_ref;
    struct state;
    std::shared_ptr<state> state_;
    explicit authenticated_relay_setup(std::shared_ptr<state>);
    // Bridge-private creation retains the actual ref impl_. It creates an
    // UNAUTHORIZED setup, never an admission from caller claims.
    static std::shared_ptr<authenticated_relay_setup> open(std::shared_ptr<lattice_db>,
        const std::string& source_policy,const std::string& connection,
        void*,int32_t(*)(void*),void(*)(void*));
public:
    authenticated_relay_setup(const authenticated_relay_setup&)=delete;
    ~authenticated_relay_setup();
    std::shared_ptr<authenticated_session_fence> stop_token()const noexcept;
    std::string descriptor()const;
    bool finish_authorization(const std::string& exact_application_outcome);
    authenticated_relay_result receive(const std::string&);
    void close()noexcept;
    static constexpr bool receiver_source_authority=false;
    static constexpr bool automatic_recovery=false;
};
}
