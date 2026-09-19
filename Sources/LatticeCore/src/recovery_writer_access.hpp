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
struct recovery_install_test_access;
struct recovery_install_admission_test_access;
// active_writer borrows either an explicit Core-owned transaction or the exact
// private install frame below. It never creates ownership from autocommit=false.
// Its returned pointer is valid only within the caller's admitted transaction.
struct recovery_writer_access {
    static database* active_writer(lattice_db& owner);
    // Private installer only. The actual owning shared_ptr is retained through
    // all callbacks and unwind. The body must not settle/replace the transaction,
    // replace hooks, or return a live SQLite statement. No borrowed overload.
    static recovery_install_result install(std::shared_ptr<lattice_db> owner,
                                           const std::function<void(database&)>& body);
private:
    struct frame;
    static thread_local frame* current_;
    static recovery_install_result install_impl(std::shared_ptr<lattice_db>,
        const std::function<void(database&)>&, const std::function<void()>& after_unlock,
        const std::function<void()>& after_writer_capture = {});
    static void deliver(lattice_db&, const lattice_db::recovery_commit_batch&);
    friend struct recovery_install_test_access;
    friend struct recovery_install_admission_test_access;
};
} // namespace lattice::detail
