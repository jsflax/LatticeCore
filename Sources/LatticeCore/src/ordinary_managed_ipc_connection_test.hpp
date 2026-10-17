#pragma once
#include <cstddef>

namespace lattice::detail {
// Private, fixed actual-resource teardown probe. It creates no product role,
// connection wrapper, profile, directory acceptance or endpoint admission.
// Caller cannot provide an fd, callback, joined bit or completion receipt.
struct ordinary_managed_ipc_connection_probe final {
    bool descriptor_open_while_worker_held=false;
    bool release_refused_while_worker_held=false;
    bool expired_join_refused=false;
    bool capture_destroyed_before_join_return=false;
    bool real_worker_joined=false;
    bool primary_failure_preserved=false;
    bool descriptor_closed_after_join=false;
    bool both_records_released=false;
    bool capacity_held_by_collected_shell=false;
    bool capacity_released_after_last_shell=false;
};
ordinary_managed_ipc_connection_probe exercise_managed_ipc_unpublished_cleanup(bool failing_worker);
}
