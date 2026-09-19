#pragma once
#include <lattice/lattice.hpp>
#include <exception>
#include <functional>
#include <memory>

namespace lattice::detail {
enum class recovery_install_state { refused, rolled_back, committed, unsettled };
struct recovery_install_result {
    recovery_install_state state = recovery_install_state::refused;
    std::exception_ptr primary_error, cleanup_error, postcommit_error;
};
struct recovery_channel_reset_error : db_error {
    std::exception_ptr primary_error, cleanup_error;
    recovery_channel_reset_error(std::exception_ptr primary,std::exception_ptr cleanup)
        :db_error("channel reset cleanup failed; writer fenced until explicit rollback"),
         primary_error(std::move(primary)),cleanup_error(std::move(cleanup)) {}
};
struct recovery_install_test_access;
struct recovery_install_admission_test_access;
class recovery_local_producer_adapter;
namespace recovery_channel_reset_test_hooks {
// Private bounded deterministic rendezvous; production leaves both null.
extern thread_local void (*after_writer_capture)();
extern thread_local void (*after_write_admission)();
}
// active_writer borrows either an explicit Core-owned transaction or the exact
// private install frame below. It never creates ownership from autocommit=false.
// Its returned pointer is valid only within the caller's admitted transaction.
struct recovery_writer_access {
    static database* active_writer(lattice_db& owner);
    // Internal engine SQL only: borrow the exact already-owned writer without
    // registering the public raw-handle escape. No ownership is synthesized.
    static sqlite3* active_handle(lattice_db& owner, database& expected_writer);
    // Common reset/remove path: preserve an exact caller-owned WRITE via a
    // savepoint, or retain/admit a new maintenance transaction before fencing.
    static void reset_channel(lattice_db&, const std::string&, bool retire);
    // Private installer only. The actual owning shared_ptr is retained through
    // all callbacks and unwind. The body must not settle/replace the transaction,
    // replace hooks, or return a live SQLite statement. No borrowed overload.
    static recovery_install_result install(std::shared_ptr<lattice_db> owner,
                                           const std::function<void(database&)>& body);
private:
    struct frame;
    static thread_local frame* current_;
    // Nonwriting trigger phase check. The frame already owns this physical
    // transaction; no SQLite call or fabricated public owner state is needed.
    static bool active_install_for(const lattice_db*, sqlite3*) noexcept;
    static recovery_install_result install_impl(std::shared_ptr<lattice_db>,
        const std::function<void(database&)>&, const std::function<void()>& after_unlock,
        const std::function<void()>& after_writer_capture = {});
    static void deliver(lattice_db&, const lattice_db::recovery_commit_batch&);
    friend struct recovery_install_test_access;
    friend struct recovery_install_admission_test_access;
    friend class recovery_local_producer_adapter;
};
} // namespace lattice::detail
