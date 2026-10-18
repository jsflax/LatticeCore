#pragma once
#include "observer_delivery_store.hpp"
#include "observer_delivery_meter.hpp"
#include "observer_delivery_workspace.hpp"
#include <atomic>
#include <array>
#include <functional>
#include <memory>
#include <vector>

namespace lattice { class database; class lattice_db; }
namespace lattice::detail {
class observer_delivery_connection;
class database_sql_request;
struct database_sql_read_admission_test_access;
class observer_delivery_sql_runtime;
class observer_delivery_plan;
class observer_delivery_begin;
class observer_delivery_write;
struct observer_delivery_sql_test_access;
// DATA only: no writer/original/checkpoint, callback or Native retirement pin.
// Actual database issue/end/capture sites are its only settlement producers.
class observer_delivery_original {
    struct carried_work;
    struct batch_target;
    struct prepared_batch;
    const uint64_t creator_;
    const observer_delivery_store::lease credit_;
    const observer_delivery_meter_budget budget_;
    const std::array<unsigned char,32> token_;
    const std::weak_ptr<const void> input_identity_;
    const bool explicit_envelope_;
    // Monotonic through actual same-original lifetime. Recovery/rollback-to
    // never refunds storage still retained by admitted plans/callback tails.
    const std::shared_ptr<observer_delivery_workspace> workspace_;
    mutable std::atomic<bool> attempted_{false},unissued_{false},began_{false},ended_{false},captured_{false};
    mutable std::atomic<bool> claimed_{false};
    mutable std::atomic<bool> owner_storage_ended_{false};
    observer_delivery_original(uint64_t,observer_delivery_store::lease,observer_delivery_meter_budget,std::weak_ptr<const void>,bool,
        std::shared_ptr<observer_delivery_workspace>);
    static void destroy(const observer_delivery_original*)noexcept;
    static std::shared_ptr<const observer_delivery_original> create(observer_delivery_store::lease,observer_delivery_meter_budget,std::weak_ptr<const void>,bool,
        std::shared_ptr<observer_delivery_workspace>);
    void claim_workspace(uint64_t)const;
    void claim_read_workspace(uint64_t)const;
    void claim_begin_workspace(uint64_t)const;
    uint64_t remaining_workspace()const;
    void attempting()const noexcept;
    void unissued()const noexcept;
    void began()const noexcept;
    void ended()const noexcept;
    void captured()const noexcept;
    void owner_storage_ended()const noexcept;
    friend class lattice::database;
    friend class lattice::lattice_db;
    friend class observer_delivery_connection;
    friend class observer_delivery_sql_runtime;
    friend class observer_delivery_plan;
    friend class observer_delivery_write;
    friend class observer_delivery_update;
    friend class database_sql_request;
    friend struct database_sql_read_admission_test_access;
    static std::shared_ptr<const observer_delivery_original> offered(database&);
    // Denial-only identity observation; never consumes offered() or issues credit.
    static std::shared_ptr<const observer_delivery_original> observe_offer(database&);
public:
    observer_delivery_original(const observer_delivery_original&)=delete;
    observer_delivery_original& operator=(const observer_delivery_original&)=delete;
    // This private production entry carries an ALREADY admitted physical-store
    // lease into one genuine Core BEGIN. It grants no raw SQL authority and
    // never waits. The eventual connection factory is responsible for calling
    // acquire/prepare before entering ANY Native/store/registry leaf.
    class begin_offer {
        const uint64_t creator_;
        database* const database_;
        const std::shared_ptr<const observer_delivery_original> original_;
        begin_offer* const previous_;
        static thread_local begin_offer* current_;
        friend class observer_delivery_original;
        begin_offer(database&,observer_delivery_store::lease,observer_delivery_meter_budget,std::weak_ptr<const void>,bool=false,
            std::shared_ptr<observer_delivery_workspace> = {});
        friend class observer_delivery_begin;
        friend struct observer_delivery_sql_test_access;
    public:
        begin_offer(database&,observer_delivery_store::lease,observer_delivery_meter_budget);
        begin_offer(database&,const observer_delivery_plan&);
        ~begin_offer()noexcept;
        begin_offer(const begin_offer&)=delete;
        begin_offer& operator=(const begin_offer&)=delete;
    };
    // A known capture owns this wrapped callable. Its invocation AND final
    // target destruction preserve the same credit; callback entry/return is
    // not a refund. No new SQL, retry, callback policy or authority is created.
    class carrier {
        std::shared_ptr<carried_work> owned_;
        std::function<void()> ready_;
        carrier(std::shared_ptr<carried_work>,std::function<void()>&&);
        friend class observer_delivery_original;
    public:
        carrier(carrier&&)noexcept=default;
        carrier(const carrier&)=delete;
        carrier& operator=(const carrier&)=delete;
        void adopt(std::function<void()>&&);
        std::function<void()> finish();
    };
    // Prepare BEFORE a Native capture detaches its batch. No allocation may
    // follow that detach merely to attach credit. Adoption/finish only move
    // the already-owned callable into this preallocated heap custody.
    static carrier prepare(std::shared_ptr<const observer_delivery_original>);
    static std::function<void()> carry(std::shared_ptr<const observer_delivery_original>,std::function<void()>&&);
    using batch_preparation=std::function<std::function<void()>(std::vector<std::function<void()>>&&)>;
    // Allocates wrapper custody BEFORE opaque callback copies. The returned
    // one-use adapter only moves the original callback vector into that heap.
    // Legacy delivery (no actual admitted original) returns an empty adapter.
    static batch_preparation prepare_current_batch();
    static std::shared_ptr<const observer_delivery_original> current()noexcept;
    // Closed deleter helper; false means retain the existing heap custody.
    static bool retire_target(const std::shared_ptr<const observer_delivery_original>&,std::function<void()>&)noexcept;
};
}
