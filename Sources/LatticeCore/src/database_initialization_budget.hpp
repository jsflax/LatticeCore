#pragma once
#include "observer_delivery_store.hpp"
#include <chrono>
#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>

namespace lattice { class database; }
namespace lattice::detail {
class observer_delivery_startup;
class observer_delivery_open_startup;
class database_sql_request;
class database_initialization_episode;
class recovery_obligation_bootstrap_reader;
class recovery_obligation_bootstrap_statement;
class observer_delivery_initialization_sql;
class store_physical_api;
struct database_initialization_budget_test_access;

// One finite, pre-admitted construction liability. There is no SQL permission,
// Native owner, user callback, path lookup or refund-on-seal in this object.
// The same actual open-original retains it through its real API/VFS/Native
// return; the startup context can keep it longer for retained catalog copies.
class database_initialization_budget final {
    friend class observer_delivery_startup;
    friend class observer_delivery_domain;
    friend class observer_delivery_open_startup;
    friend class database_sql_request;
    friend class recovery_obligation_bootstrap_reader;
    friend class recovery_obligation_bootstrap_statement;
    friend class database_initialization_episode;
    friend class store_physical_api;
    friend class ::lattice::database;
    friend struct database_initialization_budget_test_access;
public:
    struct limits {
        std::uint64_t statements=0,workspace_bytes=0,columns=0,rows=0;
    };
private:
    struct inspection {
        const void* identity=nullptr;
        limits maximum{};
        std::uint64_t statements=0,workspace=0;
        std::chrono::steady_clock::time_point deadline{};
        bool failed=false;
        // Actual owned-wrapper returns; no assertion that Native was entered,
        // a transaction ended, a statement was consumed or credit refunded.
        int first_api_rc=0,cleanup_api_rc=0;
        const void* account_identity=nullptr; // DATA identity only; no admission grant
        const void* record_identity=nullptr;
        std::uint64_t record_bytes=0,bootstrap_bytes=0,bootstrap_requests=0;
        observer_delivery_domain::snapshot domain_snapshot{};
    };
    inspection inspect_for_test_()const noexcept;
    // Fixed DATA account, independent of physical/SQL binding. Its real state
    // address, immutable end/limits and monotone counters never change when a
    // wrapper records an open. It owns no Store, client, lease, owner or Open.
    // Current create still issues it only AFTER physical validation. Moving the
    // actual issuer earlier requires the separate trusted-factory/type work;
    // merely having this private DATA type grants no earlier admission.
    class allocation_account final {
        friend class database_initialization_budget;
        friend struct database_initialization_budget_test_access;
        const std::uint64_t creator_;
        const std::thread::id builder_;
        const limits maximum_;
        const std::chrono::steady_clock::time_point end_;
        std::atomic<std::uint64_t> statements_{0},workspace_{0};
        std::atomic<bool> failed_{false};
        std::atomic<int> first_api_rc_{0},cleanup_api_rc_{0};
        std::atomic<std::uint64_t> bootstrap_bytes_{0},bootstrap_requests_{0};
        allocation_account(std::uint64_t,std::thread::id,limits,
            std::chrono::steady_clock::time_point,std::uint64_t initial_workspace)noexcept;
        bool current_()const noexcept;
        void require_current_()const;
        void claim_workspace_(std::uint64_t);
        void claim_statements_(std::uint64_t);
        void fail_()noexcept;
        void note_api_error_(int,bool cleanup)noexcept;
        allocation_account(const allocation_account&)=delete;
        allocation_account& operator=(const allocation_account&)=delete;
    };

    struct state;
    struct admission_record;
    template<class T> class bootstrap_allocator;
    static std::shared_ptr<observer_delivery_domain::lease_state> make_pending_(
        observer_delivery_domain::initialization_limits,
        std::chrono::steady_clock::time_point,
        const std::shared_ptr<observer_delivery_data_domain>&,
        const std::shared_ptr<observer_delivery_domain::client_state>&,bool& transferred);
    static std::shared_ptr<database_initialization_budget> from_pending_(
        std::shared_ptr<observer_delivery_domain::lease_state>);
    // Called ONLY after the actual global managed-return queue releases both
    // Native/API stacks. The fixed record owns the account/credit together.
    void delete_after_return_()noexcept;
    const std::uint64_t creator_;
    state* state_=nullptr;
    admission_record* record_=nullptr;
    // Intrusive last-owner DELETE obligation, allocated with this fixed
    // shell before lease admission. It is not an open/SQL/physical grant.
    database_initialization_budget* returned_next_=nullptr;
    bool returned_delete_=false;
    database_initialization_budget(std::uint64_t,state*,admission_record*)noexcept;
    static void destroy(database_initialization_budget*)noexcept;
    static std::shared_ptr<database_initialization_budget> create(database& first_actual_open,
        const observer_delivery_store&,const observer_delivery_store::client&,limits,
        std::chrono::steady_clock::time_point original_end);
    // Called once per genuine open token, off all Native/store leaves. This
    // binds a typed DATA hold to that SAME physical-open original, never an
    // original found by current sqlite3* address or a policy-token conversion.
    void bind_open_(database&,const observer_delivery_open_startup&,
        const std::shared_ptr<database_initialization_budget>& actual_self);
    bool current_()const noexcept;
    bool usable_()const noexcept;
    bool bound_for_(const database&,const observer_delivery_open_startup&)const noexcept;
    void require_current_()const;
    void claim_workspace_(std::uint64_t);
    void claim_statements_(std::uint64_t);
    void fail_()noexcept;
    void note_api_error_(int,bool cleanup)noexcept;
    limits limits_()const;
    std::chrono::steady_clock::time_point deadline_()const;
public:
    ~database_initialization_budget()noexcept;
    database_initialization_budget(const database_initialization_budget&)=delete;
    database_initialization_budget& operator=(const database_initialization_budget&)=delete;
};
// A private receipt for the genuine raw/schema BEGIN only. It never becomes a
// Core managed transaction original or a Native owner. Only the actual channel
// publishes it after successful BEGIN and observes its real autocommit ending.
class database_initialization_episode final {
    friend class ::lattice::database;
    friend class database_sql_request;
    friend class recovery_obligation_bootstrap_reader;
    friend class recovery_obligation_bootstrap_statement;
    friend struct database_initialization_budget_test_access;
    const std::uint64_t creator_;
    database* const owner_; // comparison only, valid only while private owner builds
    std::shared_ptr<const observer_delivery_open_startup> open_;
    std::shared_ptr<database_initialization_budget> budget_;
    database_initialization_budget::limits maximum_{};
    const void* begin_=nullptr,*commit_=nullptr,*rollback_=nullptr;
    std::atomic<bool> begin_claimed_{false},commit_claimed_{false},rollback_claimed_{false};
    std::atomic<bool> live_{false},ended_{false};
    // Denial-only revision of privately issued may-change ATTEMPTS. It is
    // neither schema-mutation proof, an ending nor authority for any SQL.
    std::atomic<std::uint64_t> schema_revision_{0};
    std::atomic<std::uint64_t> cleanup_workspace_{0};
    std::uint64_t cleanup_maximum_=0;
    std::atomic<int> first_api_error_{0},cleanup_api_error_{0};
    database_initialization_episode(std::uint64_t,database*)noexcept;
    static void destroy(database_initialization_episode*)noexcept;
    static std::shared_ptr<database_initialization_episode> prepare_(database&,
        const observer_delivery_initialization_sql&,const observer_delivery_initialization_sql&,
        const observer_delivery_initialization_sql&);
    void claim_request_(database&,const observer_delivery_initialization_sql&,std::uint64_t,bool);
    void claim_cleanup_workspace_(std::uint64_t);
    bool original_for_(const database&,const observer_delivery_initialization_sql&)const noexcept;
    bool cleanup_for_(const database&,const observer_delivery_initialization_sql&)const noexcept;
};
}
