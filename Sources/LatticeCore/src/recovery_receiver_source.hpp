#pragma once
#include "recovery_receipt_coverage.hpp"
#include <lattice/network.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

namespace lattice { class lattice_db; class synchronizer_base; }
namespace lattice::detail {
class sync_callback_lifetime;
class receiver_upload_view;
class recovery_export_adapter;
class recovery_export_route;
class recovery_receiver_controller;
class recovery_receiver_route;
bool reserved_recovery_source_frame(std::string_view);
struct receiver_source_test_access;
// No public constructor or authority conversion. Only the actual synchronizer
// can start a describe on its owned, system-TLS verified physical attempt.
// The opted physical controller consumes exact current source custody.
// The binding alone cannot grant UNSENT custody or installation.
class receiver_source_binding : public std::enable_shared_from_this<receiver_source_binding> {
    friend class ::lattice::synchronizer_base;
    friend struct receiver_source_test_access;
    friend class receiver_upload_view;
    friend class recovery_receiver_controller;
    friend class recovery_receiver_route;
    friend class recovery_reconciliation_descriptor;
    friend class recovery_unknown_reconciliation;
    struct policy;
    struct record;
    // Unforgeable current-record custody. Only the actual opted controller
    // consumes it; stored descriptions/digests cannot reconstruct a view.
    struct recovery_view {
        std::shared_ptr<const record> value;
    };
    std::shared_ptr<const policy> policy_;
    std::weak_ptr<lattice_db> owner_;
    std::weak_ptr<sync_callback_lifetime> lifetime_;
    mutable std::mutex mutex_;
    std::shared_ptr<const record> current_;
    uint64_t upload_revision_=0;
    bool upload_pending_=false;
    std::string upload_failure_;
    receiver_source_binding(const std::shared_ptr<lattice_db>&,
        const std::shared_ptr<sync_callback_lifetime>&,const std::string&,const std::string&);
    std::string dial_url()const;
    void opened(const platform_transport_callbacks&,uint64_t,owned_platform_sync_transport&);
    bool receive(const platform_transport_callbacks&,uint64_t,const transport_message&);
    void invalidate(const std::shared_ptr<const record>&);
    bool live(const std::shared_ptr<const record>&)const;
    bool described()const;
    void request_upload();
    std::shared_ptr<const receiver_upload_view> capture_upload(uint64_t);
    bool upload_pending(uint64_t)const;
    void finish_upload(const std::shared_ptr<const receiver_upload_view>&,const std::string& failure={});
    std::optional<recovery_view> recovery_current()const;
    bool recovery_live(const recovery_view&)const;
    bool recovery_expired()const;
    bool recovery_matches(const recovery_view&,const platform_transport_callbacks&,uint64_t)const;
    std::string recovery_description(const recovery_view&)const;
    uint64_t recovery_lifecycle(const recovery_view&)const;
    int64_t recovery_remaining(const recovery_view&)const;
    bool recovery_send(const recovery_view&,owned_platform_sync_transport&,const transport_message&)const;
public:
    ~receiver_source_binding();
    receiver_source_binding(const receiver_source_binding&)=delete;
    static constexpr bool ready_authority=false;
    static constexpr bool install_authority=false;
    static constexpr bool automatic_recovery=false;
};
// Constructible only after this owner's exact successful describe comparison.
// Numeric limits constrain a frame; they never confer table/operation authority.
class receiver_upload_view {
    friend class recovery_unknown_reconciliation;
    friend class receiver_source_binding;
    friend class recovery_export_adapter;
    friend class recovery_export_route;
    std::weak_ptr<receiver_source_binding> binding_;
    std::shared_ptr<const receiver_source_binding::record> record_;
    uint64_t revision_=0;
    size_t entries_=0,wire_=0,scalar_=0,nodes_=0,depth_=0,deletes_=0;
    std::optional<recovery_receipt_binding> receipt_binding_;
    std::string schema_digest_;
    receiver_upload_view()=default;
    bool current()const;
    bool fits(const std::string&,size_t,size_t,std::string&)const;
    bool send(owned_platform_sync_transport&,const transport_message&)const;
};
}
