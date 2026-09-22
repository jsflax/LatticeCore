#pragma once
#include "canonical_range_package.hpp"
#include "canonical_source_capture.hpp"
#include "recovery_writer_access.hpp"

namespace lattice::detail {
class canonical_writer_adapter;
struct canonical_ready_profile {
    // Exact private source spelling, not authenticated issuer authority.
    std::string authority;
    int64_t transfers, bindings, charged_bytes, transfer_bytes;
    canonical_range::package_limits package;
    sync_recovery::canonical_capture_limits capture;
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
struct canonical_ready_frame_result {
    recovery_install_result settlement;
    std::optional<std::string> frame;
};
} // namespace lattice::detail
