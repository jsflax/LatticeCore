#pragma once
#include "ordinary_managed_history.hpp"
#include <array>

namespace lattice::detail {
// Actual receiver-issued transport association. These fixed observations do
// not authorize a principal, source, partition, import, ACK or repair.
struct ordinary_managed_cohort_link_facts final {
    ordinary_launch::process_identity local, peer, supervisor;
    ordinary_installation::file_identity generation, cohort, local_image, peer_image;
    std::array<std::uint8_t,32> local_image_digest{}, peer_image_digest{};
    std::array<std::uint8_t,16> nonce{};
    bool local_is_daemon=false; // observed fixed native entry, never caller input
};
// Passive actual close facts, never worker/profile/product authority.
// Uncertain close must not retry a potentially recycled descriptor number.
enum class ordinary_managed_cohort_link_settlement_state { pending, in_progress, clean, uncertain, inherited };
struct ordinary_managed_cohort_link_settlement final {
    ordinary_managed_cohort_link_settlement_state state=ordinary_managed_cohort_link_settlement_state::pending;
    int native_error=0;
    bool deadline_met=false;
};
class ordinary_managed_cohort_link final {
    friend class ordinary_managed_child_admission;
    friend class ordinary_managed_ipc_access;
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit ordinary_managed_cohort_link(std::unique_ptr<implementation>);
    void validate(ordinary_launch::deadline)const;
    const ordinary_managed_cohort_link_facts& facts(ordinary_launch::deadline)const;
    // The actual connection owner must retain this object throughout every
    // reader/writer/callback and until its issuer-owned join record settles.
    // This borrow cannot be detached into another raw-fd owner.
    int descriptor(ordinary_launch::deadline)const;
    // Cancellation only. Never closes/recycles the descriptor or claims that
    // any worker, capture, child or cohort has actually settled.
    void request_stop()noexcept;
    // Only the fixed native connection collector may call this, after its
    // actual counted operations/terminal uses/captures and worker joins settle.
    // No caller Boolean asserts those facts. Close is attempted exactly once;
    // errors stay uncertain and charged even if the fd number was consumed.
    // A late actual close0 is recorded clean with deadline_met=false; it never
    // changes the operation's primary failure or claims a hard OS time bound.
    ordinary_managed_cohort_link_settlement settle_descriptor(ordinary_launch::deadline)noexcept;
public:
    ~ordinary_managed_cohort_link();
    ordinary_managed_cohort_link(const ordinary_managed_cohort_link&)=delete;
    ordinary_managed_cohort_link& operator=(const ordinary_managed_cohort_link&)=delete;
};
}
