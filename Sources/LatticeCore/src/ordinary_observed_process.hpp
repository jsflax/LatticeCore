#pragma once
#include "ordinary_owned_launch.hpp"
#include <array>
#include <memory>
#include <vector>
#include <optional>

// Private dedicated-upgrade-supervisor primitive. A kernel-observed exit of
// one non-child is not descendant coverage, a launch fence, store adoption,
// configured recovery permission, or authority derived from a caller's PID.
namespace lattice::detail::ordinary_launch {
struct observed_exit {
    std::int64_t pid = 0;
    std::array<std::uint8_t, 32> audit_token{};
    std::uint64_t executable_device = 0, executable_inode = 0;
};

// A current kernel observation, never an exit/descendant completion grant.
struct observed_incarnation {
    std::int64_t pid = 0;
    std::array<std::uint8_t, 32> audit_token{};
    std::uint64_t executable_device = 0, executable_inode = 0;
};
struct observed_open_vnode {
    std::uint64_t device = 0, inode = 0;
    std::uint32_t descriptor = 0, flags = 0;
    bool close_on_exec = false;
};
struct observed_process_files {
    std::int64_t pid = 0, parent_pid = 0;
    std::array<std::uint8_t, 32> audit_token{};
    std::vector<observed_open_vnode> files;
};

class observed_process {
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit observed_process(std::unique_ptr<implementation>);
public:
    // Caller independently owns the supported-host/old-writer selection and
    // retained approved executable FD. This checks actual mapped-vnode identity,
    // task-name audit token and a kernel-bound process knote. It never signals
    // on attach, opens a store, or grants authority from executable pathname.
    // Darwin private API absence/permission denial refuses; no PID fallback.
    static observed_process attach(std::int64_t pid, int retained_executable, deadline);
    // Snapshot the exact held incarnation before persisting an expected-role
    // journal. This is explicitly not observed_exit.
    observed_incarnation incarnation(deadline) const;
    // Fresh kernel inspection includes the exited list. Empty means actual
    // SZOMB or ESRCH from that wider lookup, never permission denial or an
    // ordinary live-list miss during file-table teardown.
    // PID-keyed FD inspections are bracketed by exact task-token checks.
    static std::optional<observed_process_files> inspect_files(std::int64_t, deadline);
    // Resume a persisted expectation only by re-observing the real kernel.
    // Empty means the old incarnation is terminal/absent (actual SZOMB, the
    // wider lookup ESRCH, or a different currently live audit token).
    // A present exact incarnation receives a new real knote. No decoded field
    // alone reports completion, and caller still owes complete cohort closure.
    static std::optional<observed_process> reobserve(const observed_incarnation&, int retained_executable, deadline);
    ~observed_process();
    observed_process(observed_process&&) noexcept;
    observed_process& operator=(observed_process&&) noexcept;
    observed_process(const observed_process&) = delete;
    observed_process& operator=(const observed_process&) = delete;
    // Same creating process/thread only. Signals exactly the captured audit
    // token, never a revalidated numeric PID. SIGTERM acceptance is not exit.
    void request_termination(deadline);
    // Requires an actual NOTE_EXIT of the retained knote. Fork/exec evidence,
    // deadline, unknown identity, or missing observation never become success.
    observed_exit wait_for_exit(deadline);
};
namespace observed_test_hooks {
// Nil-default actual-boundary rendezvous. Cannot supply an identity, OS result,
// knote, exit or permission; all real checks still run after the callback.
enum class boundary { before_registration, after_registration };
using rendezvous = void (*)(boundary);
inline thread_local rendezvous observe = nullptr;
}
} // namespace lattice::detail::ordinary_launch
