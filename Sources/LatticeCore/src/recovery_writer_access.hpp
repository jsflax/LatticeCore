#pragma once
#include <lattice/lattice.hpp>
#include <exception>
#include <functional>
#include <memory>

namespace lattice::detail {
enum class recovery_install_state { refused, rolled_back, committed, unsettled, ownership_lost };
// Internal controller/automatic-export no-effect outcome. Never a transaction
// settlement and is never set after notification reservation or BEGIN.
enum class recovery_install_deferred { none, connection_mutex, engine_read, store_gate, core_write };
struct recovery_install_result {
    recovery_install_deferred deferred = recovery_install_deferred::none;
    recovery_install_state state = recovery_install_state::refused;
    std::exception_ptr primary_error, cleanup_error, postcommit_error;
    // An ordinary successor may have committed inside a contract-violating
    // body. Its off-lock drain is not this failed install's postcommit tail.
    std::exception_ptr notification_error;
    // An observed physical COMMIT before validated owner finalization is not
    // a completed install. False does not assert rollback or durable absence.
    bool unexpected_commit_observed = false;
};
struct recovery_channel_reset_error : db_error {
    std::exception_ptr primary_error, cleanup_error;
    recovery_channel_reset_error(std::exception_ptr primary,std::exception_ptr cleanup)
        :db_error("channel reset cleanup failed; writer fenced until explicit rollback"),
         primary_error(std::move(primary)),cleanup_error(std::move(cleanup)) {}
};
struct recovery_channel_reset_notification_error : db_error {
    std::exception_ptr primary_error, notification_error;
    recovery_channel_reset_notification_error(std::exception_ptr primary, std::exception_ptr notification)
        : db_error("channel reset failed; ordinary successor notification also failed"),
          primary_error(std::move(primary)), notification_error(std::move(notification)) {}
};
struct legacy_sync_write_error : db_error {
    std::exception_ptr primary_error, cleanup_error;
    legacy_sync_write_error(std::exception_ptr primary,std::exception_ptr cleanup)
        :db_error("legacy sync cleanup failed; writer fenced until explicit rollback"),
         primary_error(std::move(primary)),cleanup_error(std::move(cleanup)) {}
};
namespace legacy_sync_write_test_hooks {
extern thread_local void (*after_writer_capture)();
extern thread_local void (*after_write_admission)();
}
struct recovery_install_test_access;
struct recovery_install_admission_test_access;
class recovery_local_producer_adapter;
struct ordinary_owned_transaction_test_access;
class ordinary_history_final_install_guard;
class recovery_obligation_bootstrap_reader;
namespace recovery_channel_reset_test_hooks {
// Private bounded deterministic rendezvous; production leaves both null.
extern thread_local void (*after_writer_capture)();
extern thread_local void (*after_write_admission)();
}
// active_writer borrows either an explicit Core-owned transaction or the exact
// private install frame below. It never creates ownership from autocommit=false.
// Its returned pointer is valid only within the caller's admitted transaction.
struct recovery_writer_access {
    // One controller's real no-effect retry episode. This is denial-only;
    // it holds no transaction, source grant, ticket or store-write gate.
    class controller_admission_intent {
        // Only fixed cell metadata/strong-pointer moves occur under this leaf;
        // never SQL, callbacks, another mutex acquisition or final destruction.
        std::mutex mutex_;
        std::shared_ptr<database> writer_;
        database::topology_wait_scope waiting_;
    public:
        class turn {
            controller_admission_intent& value_;
            turn* previous_;
            std::unique_lock<std::mutex> capture_;
            const std::shared_ptr<database> writer_;
            database::topology_wait_scope::bypass bypass_;
            static thread_local turn* current_;
            friend class controller_admission_intent;
        public:
            explicit turn(controller_admission_intent& value) : value_(value),previous_(current_),capture_(value.mutex_),
                writer_(value.writer_),bypass_(value.waiting_) { capture_.unlock();current_=this; }
            ~turn() noexcept { current_=previous_; }
            turn(const turn&)=delete;turn& operator=(const turn&)=delete;
        };
        struct retired_custody {
            std::shared_ptr<database> writer;
            std::shared_ptr<database_retirement_state> retirement;
            void reset() noexcept { retirement.reset();writer.reset(); } // caller is off leaf
        };
        void begin(const std::shared_ptr<lattice_db>&,std::chrono::steady_clock::time_point);
        // Metadata-only retirement may run under the coordinator leaf. Move
        // strong custody out; its caller destroys that returned owner off leaf.
        retired_custody retire() noexcept {
            std::lock_guard lock(mutex_);waiting_.release();
            return {std::move(writer_),waiting_.take_retired_original()};
        }
        void release() noexcept { auto retired=retire(); }
        controller_admission_intent()=default;
        controller_admission_intent(const controller_admission_intent&)=delete;
        controller_admission_intent& operator=(const controller_admission_intent&)=delete;
    };
    static database* active_writer(lattice_db& owner);
    // Internal engine SQL only: borrow the exact already-owned writer without
    // registering the public raw-handle escape. No ownership is synthesized.
    static sqlite3* active_handle(lattice_db& owner, database& expected_writer);
    // Common reset/remove path: preserve an exact caller-owned WRITE via a
    // savepoint, or retain/admit a new maintenance transaction before fencing.
    static void reset_channel(lattice_db&, const std::string&, bool retire);
    // Private legacy bookkeeping unit. The owner overload retains the actual
    // physical writer; the database overload borrows its caller-held lifetime.
    // Existing caller-owned explicit transactions (including raw deferred/
    // READ turns) retain their legacy commit/rollback responsibility. The
    // durable absence read pins their main snapshot; stale upgrades refuse.
    // No public ownership, install frame, or producer authority is created.
    // Trusted bodies/hooks must not settle caller turns; read-only COMMIT need
    // not fire the engine commit hook, so it cannot prove successor detection.
    // Standalone databases must not install external transaction callbacks;
    // their hooks are never replaced. Errors in caller turns simply propagate.
    // Only helper-owned turns get whole cleanup. Requires the engine's exact
    // commit-attempt marker before following COMMIT into any successor.
    static void legacy_sync_write(lattice_db&, const std::function<void(database&)>&);
    static void legacy_sync_write(database&, const std::function<void(database&)>&);
    // Private installer only. The actual owning shared_ptr is retained through
    // all callbacks and unwind. The body must not settle/replace the transaction,
    // replace hooks, or return a live SQLite statement. A premature COMMIT is
    // vetoed by the retained engine hook; consumed frames cannot follow a
    // successor. No hook-replacement guarantee and no borrowed overload.
    static recovery_install_result install(std::shared_ptr<lattice_db> owner,
                                           const std::function<void(database&)>& body);
private:
    // Synchronous ordinary engine entry only. The public lattice_db& caller
    // retains the parent through this entire call, including observer delivery.
    // The common engine strongly retains the exact physical writer. No alias
    // shared_ptr, asynchronous parent escape or borrowed recovery API is added.
    static recovery_install_result ordinary_owned_write(lattice_db&,
        const std::function<void(database&)>&, const std::function<void()>& after_unlock = {},
        const std::function<void()>& after_writer_capture = {});
    static recovery_install_result ordinary_owned_history_write(lattice_db&,
        const std::function<void(database&)>&,const ordinary_history_final_install_guard&);
    struct install_owner;
    static recovery_install_result install_owned_impl(install_owner,
        const std::function<void(database&)>&, const std::function<void()>& after_unlock,
        const std::function<void()>& after_writer_capture, bool* initial_admission_busy, bool controller_try,
        const ordinary_history_final_install_guard* final_guard = nullptr,
        recovery_obligation_bootstrap_reader* initialization = nullptr);
    static void legacy_sync_write_impl(database&, lattice_db*, const std::function<void(database&)>&);
    struct legacy_frame;
    static thread_local legacy_frame* legacy_current_;
    struct frame;
    static thread_local frame* current_;
    // A previously admitted caller reset may finish after logical close. This
    // separate frame grants only receive-guard access, never install suppression
    // or general ownership of a callback's successor transaction.
    struct channel_reset_frame;
    static thread_local channel_reset_frame* reset_current_;
    static bool active_channel_reset_for(const lattice_db&,const database&) noexcept;
    // Nonwriting trigger phase check. The frame already owns this physical
    // transaction; no SQLite call or fabricated public owner state is needed.
    static bool active_install_for(const lattice_db*, sqlite3*) noexcept;
    static recovery_install_result install_impl(std::shared_ptr<lattice_db>,
        const std::function<void(database&)>&, const std::function<void()>& after_unlock,
        const std::function<void()>& after_writer_capture = {}, bool* initial_admission_busy = nullptr, bool controller_try = false,
        recovery_obligation_bootstrap_reader* initialization = nullptr);
    static void deliver(lattice_db&, const lattice_db::recovery_commit_batch&);
    friend struct recovery_install_test_access;
    friend struct recovery_install_admission_test_access;
    friend struct recovery_controller_admission_test_access;
    friend class recovery_local_producer_adapter;
    friend class recovery_export_adapter;
    friend class recovery_continuous_producer;
    friend class canonical_writer_adapter;
    friend struct receive_delivery_guard_access;
    friend struct ordinary_owned_transaction_test_access;
    friend class ordinary_export_access; // Closed ordinary storage bodies only; same owning engine.
};
} // namespace lattice::detail
