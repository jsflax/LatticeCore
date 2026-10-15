#pragma once
#include <cstddef>
#include <condition_variable>
#include <cstdlib>
#include <thread>
namespace lattice::detail {
// Fixed passive codec exercise only. No fd/profile/role/receipt/endpoint or
// database write is returned. This is not positive peer authority evidence.
struct ordinary_managed_peer_codec_probe {
    std::size_t input_bytes=0,frames=0,largest_frame=0;
    bool exact=false,wrong_nonce_refused=false,unknown_kind_refused=false;
    bool oversized_total_refused=false,trailing_scalar_refused=false;
};
ordinary_managed_peer_codec_probe exercise_managed_peer_codec();
// Mechanical deadline/ownership observation from a real owned SQL turn and
// actual joined thread. It has no managed profile, endpoint, peer, product
// role, request or receipt; it cannot be used as positive peer qualification.
struct ordinary_managed_peer_deadline_probe {
    bool external_entered=false,held_before_sql=false,held_after_sql=false;
    bool caller_upper_preserved=false,external_returned=false,worker_joined=false;
    bool actual_commit_clean=false,actual_rollback_observed=false;
    bool held_after_failed_turn=false,released_after_success=false;
    bool deadline_failed=false;
    std::size_t denied_commits=0;
};
}
