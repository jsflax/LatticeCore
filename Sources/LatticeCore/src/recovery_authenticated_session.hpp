#pragma once
#include "canonical_writer_adapter.hpp"
#include <atomic>
#include <chrono>
#include <mutex>

namespace lattice { class swift_lattice_ref; }
namespace lattice::detail {
namespace authenticated_ready_maintenance_test_observation {
// Passive scheduling probe only. It cannot create a setup/admission, supply
// lifecycle facts, change time or bypass the actual owned transaction.
struct probe {const lattice_db* owner=nullptr;std::function<void(const char*)> observed;};
std::shared_ptr<const probe> exchange(std::shared_ptr<const probe>);
}
struct authenticated_ready_budget;
struct authenticated_ready_fence;
// Payload-free source-wide charge. This is capacity, never authority.
class authenticated_ready_charge {
    friend class authenticated_session_fence;
    friend class authenticated_relay_setup;
    std::shared_ptr<authenticated_ready_budget> budget_;
    uint64_t input_=0,charged_=0;
    std::atomic<bool> consumed_{false};
    authenticated_ready_charge()=default;
public:
    ~authenticated_ready_charge();
};
// Payload-free shared stop state. Only the real setup can authorize it. Close
// and kicks may stop it from any thread without entering SQL or waiting.
class authenticated_session_fence {
    friend class authenticated_relay_setup;
    friend class authenticated_relay_operation;
    std::atomic<bool> stopped_{false},authorized_{false};
    std::atomic<int64_t> deadline_{0};
    mutable std::mutex mutex_;
    size_t active_=0;
    std::shared_ptr<authenticated_ready_budget> ready_budget_;
    std::shared_ptr<instance_guard> owner_guard_;
    std::shared_ptr<const std::atomic<bool>> active_guard_;
    authenticated_session_fence()=default;
public:
    static int64_t now() noexcept;
    bool live()const noexcept;
    bool stopped()const noexcept;
    bool drained()const noexcept;
    void stop()noexcept;
    std::shared_ptr<authenticated_ready_charge> reserve_ready(uint64_t input_bytes)const;
};
// Payload-free counted handoff. A result retains it through SDK publication;
// destruction settles exactly one admission without running SQL or callbacks.
class authenticated_relay_operation {
    friend class authenticated_relay_setup;
    std::shared_ptr<authenticated_session_fence> fence_;
    std::shared_ptr<authenticated_ready_charge> charge_;
    std::shared_ptr<authenticated_ready_fence> ready_;
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
struct authenticated_ready_result {
    // 0 not a control, 1 typed control response, 2 retired. Exceptions retain
    // uncertainty; no control result enters ordinary ACK/NACK/fan-out.
    int32_t status=0;
    std::string wire;
    std::shared_ptr<authenticated_relay_operation> operation;
    std::string request_id;
};
struct authenticated_lifecycle_adoption_result {
    bool pending_quiescence=false;
    canonical_ready_adoption_result adoption;
};
struct authenticated_mounted_source;
class authenticated_relay_setup {
    friend class ::lattice::swift_lattice_ref;
    friend struct authenticated_ready_test_access;
    static thread_local const std::function<void()>* ready_before_owned_test_hook_;
    static thread_local const std::function<void()>* admin_before_open_test_hook_;
    // Passive friend-only observation; cannot inject work or skip validation.
    static thread_local uint64_t* setup_registry_entries_test_counter_;
    struct source_file_administration;
    static std::unique_ptr<source_file_administration> open_administrative_file(const std::string&,
        recovery_owner_schema,int64_t,int);
    struct state;
    std::shared_ptr<state> state_;
    explicit authenticated_relay_setup(std::shared_ptr<state>);
    // Bridge-private creation retains the actual ref impl_. It creates an
    // UNAUTHORIZED setup, never an admission from caller claims.
    static std::shared_ptr<authenticated_relay_setup> open(std::shared_ptr<lattice_db>,
        const std::string& source_policy,const std::string& connection,
        void*,int32_t(*)(void*),void(*)(void*));
    static std::shared_ptr<authenticated_relay_setup> open_automatic(std::shared_ptr<lattice_db>,
        const std::string&,const std::string&,void*,int32_t(*)(void*),int32_t(*)(void*),void(*)(void*),bool&);
    static std::shared_ptr<authenticated_relay_setup> open_impl(std::shared_ptr<lattice_db>,
        const std::string&,const std::string&,void*,int32_t(*)(void*),void(*)(void*),int32_t(*)(void*),bool*);
    // Explicit administration of the real resolved mount owner, never an
    // issuer. False is only pre-SQL quiescence contention; exceptions do not
    // imply durable absence and ordinary open never invokes this transition.
    static bool migrate_receipt_coverage_file(const std::string& path, recovery_owner_schema catalog,
        int64_t schema_version,int busy_timeout_ms,const std::string& prior,const std::string& next);
    static authenticated_lifecycle_adoption_result adopt_lifecycle_file(const std::string&,recovery_owner_schema,
        int64_t,int,const std::string& prior,const std::string& next);
    static bool migrate_receipt_coverage(std::shared_ptr<lattice_db>,
        const std::string& prior_mount_policy,const std::string& next_mount_policy);
public:
    authenticated_relay_setup(const authenticated_relay_setup&)=delete;
    ~authenticated_relay_setup();
    std::shared_ptr<authenticated_session_fence> stop_token()const noexcept;
    std::string descriptor()const;
    bool finish_authorization(const std::string& exact_application_outcome);
    authenticated_relay_result receive(const std::string&);
    authenticated_ready_result ready(const std::string&,const std::shared_ptr<authenticated_ready_charge>&);
    void close()noexcept;
    static constexpr bool receiver_source_authority=false;
    static constexpr bool automatic_recovery=false;
};
}
