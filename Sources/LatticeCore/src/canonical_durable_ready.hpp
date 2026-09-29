#pragma once
#include "canonical_range_package.hpp"
#include "canonical_source_capture.hpp"
#include "recovery_writer_access.hpp"
#include <array>
#include <chrono>

namespace lattice::detail {
namespace canonical_ready_test_observation {
// Passive fixed-storage observations only. No callback, SQL, allocation, wait,
// clock override or authority. A fixture owns/resets this same-thread slot.
// Times measure real elapsed work and never participate in lease decisions.
enum class point { entered,prepared,captured,assembled,publish_requested,publish_body,publication_settled,audit_begin,audit_end,finished,count };
struct observation {
    std::chrono::steady_clock::time_point origin=std::chrono::steady_clock::now();
    std::array<uint64_t,static_cast<size_t>(point::count)> visits{},first_us{},last_us{};
    uint64_t audited_frames=0;
    int preparation=-1,publication=-1;
    bool preparation_error=false,publication_error=false,capture_error=false,lease_available=false;
};
extern thread_local observation* current;
}
namespace canonical_ready_read_test_observation {
// Same-thread fixed-storage measurements only. Values never grant admission,
// skip validation, change a deadline or invoke a fixture callback.
enum class point { entered,owned_requested,body_begin,body_end,settled,finished,count };
struct observation {
    std::chrono::steady_clock::time_point origin=std::chrono::steady_clock::now();
    std::array<uint64_t,static_cast<size_t>(point::count)> visits{},first_us{},last_us{};
    uint64_t index=0,full_audits=0,audited_frames=0,audited_bytes=0,positive_receipt_lookups=0;
    uint64_t addressed_frames=0,addressed_bytes=0;
    int64_t deadline_ms=-1,clock_before_ms=-1,clock_after_ms=-1,clock_settled_ms=-1;
    int settlement=-1;
    bool primary_error=false,cleanup_error=false,postcommit_error=false,notification_error=false;
};
extern thread_local observation* current;
}
class canonical_writer_adapter;
struct canonical_ready_profile {
    // Exact private source spelling, not authenticated issuer authority.
    std::string authority;
    int64_t transfers, bindings, charged_bytes, transfer_bytes;
    canonical_range::package_limits package;
    sync_recovery::canonical_capture_limits capture;
    // Explicit lifecycle-v1 opt-in only. No default, wall-clock inference or
    // reinterpretation of an old persisted policy/lease deadline.
    std::optional<int64_t> orphan_resume_grace_ms;
};
// Private administrative outcome. The record proves a policy transition only,
// never historical request publication, receipt absence, a lease or installation.
enum class canonical_ready_adoption_disposition { applied, verified_existing };
struct canonical_ready_adoption_result {
    recovery_install_result settlement;
    std::optional<std::string> record;
    // Describes this owned operation only. verified_existing reports its own
    // no-change audit COMMIT, never the settlement of an earlier invocation.
    std::optional<canonical_ready_adoption_disposition> disposition;
};
// Durable lookup identity only. Possession cannot read a frame, grant a lease,
// acknowledge installation, settle a receipt or authorize a peer.
class canonical_ready_identity {
    friend class canonical_writer_adapter;
    std::string binding_, request_digest_;
    int64_t sequence_;
    canonical_ready_identity(std::string binding,int64_t sequence,std::string digest)
        :binding_(std::move(binding)),request_digest_(std::move(digest)),sequence_(sequence){}
public:
    canonical_ready_identity(const canonical_ready_identity&)=default;
    canonical_ready_identity& operator=(const canonical_ready_identity&)=default;
};
class canonical_ready_lease {
    friend class canonical_writer_adapter;
    std::weak_ptr<void> session_,context_;
    canonical_ready_identity identity_;
    int64_t incarnation_,sequence_,deadline_;
    uint64_t route_;
    canonical_ready_lease(std::weak_ptr<void> session,std::weak_ptr<void> context,canonical_ready_identity identity,
        int64_t incarnation,int64_t sequence,int64_t deadline,uint64_t route)
        :session_(std::move(session)),context_(std::move(context)),identity_(std::move(identity)),
         incarnation_(incarnation),sequence_(sequence),deadline_(deadline),route_(route){}
public:
    canonical_ready_lease(const canonical_ready_lease&)=default;
    canonical_ready_lease& operator=(const canonical_ready_lease&)=default;
};
struct canonical_ready_info {
    canonical_ready_identity identity;
    std::string namespace_id,replica_id;
    canonical_range::attempt logical;
    bool ready=false,orphan=false;
    int64_t protected_base=0;
    uint64_t frames=0,wire_bytes=0;
    std::optional<canonical_range::manifest> manifest;
};
struct canonical_ready_result {
    recovery_install_result preparation,publication;
    std::exception_ptr capture_error;
    bool requires_full_request=false;
    // A known committed preparation is discoverable even after capture or
    // publication fails. Neither presence nor absence asserts remote state.
    std::optional<canonical_ready_info> transfer;
    std::optional<canonical_ready_lease> lease;
};
struct canonical_ready_resume_result {
    recovery_install_result settlement;
    std::optional<canonical_ready_info> transfer;
    std::optional<canonical_ready_lease> lease;
};
struct canonical_ready_inspection {
    recovery_install_result settlement;
    std::vector<canonical_ready_info> transfers;
};
enum class canonical_ready_lifecycle_state { available,terminal,unstarted };
struct canonical_ready_lifecycle_result {
    recovery_install_result settlement;
    // A capsule disposition only, never original receipt/installation evidence.
    std::optional<canonical_ready_lifecycle_state> disposition;
    int64_t binding_high_water=0;
};
struct canonical_ready_maintenance_result {
    recovery_install_result settlement;
    // Present only after known COMMIT. A scheduling hint, never admission.
    // nullopt next_delay means the audited source has no remaining capsules.
    bool observed=false;
    std::optional<int64_t> next_delay_ms;
};
struct canonical_ready_frame_result {
    recovery_install_result settlement;
    std::optional<std::string> frame;
};
} // namespace lattice::detail
