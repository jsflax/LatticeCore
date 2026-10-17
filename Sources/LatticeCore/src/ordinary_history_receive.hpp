#pragma once
#include "ordinary_export.hpp"
#include "ordinary_resource_pool.hpp"
#include <lattice/network.hpp>
#include <lattice/ordinary_history.hpp>
#include <atomic>
#include <optional>
#include <variant>
#include <string_view>

namespace lattice::detail {
class sync_callback_lifetime;
class ordinary_managed_wss_binding;
// Decoded, bounded observations only. The actual session issues an operation
// after its current native TLS endpoint/lifecycle check. None of these fields
// grant authority to bind a source or commit the eventual model replacement.
struct ordinary_history_descriptor {
    ordinary_export_frame projection; // models/columns/types only; no rows
    std::string stage,header,content_digest,header_digest;
    uint64_t rows=0,known=0,unknown=0,pages=0;
};
struct ordinary_history_row {
    size_t table=0;
    std::vector<column_value_t> values;
};
struct ordinary_history_known {
    audit_log_entry entry;
    ordinary_audit_original_descriptor original;
};
struct ordinary_history_unknown {
    // Same twelve observed AuditLog cells as the source. TEXT bytes and
    // nonfinite REAL bits remain observations, never known original fields.
    std::vector<column_value_t> audit;
};
using ordinary_history_record=std::variant<ordinary_history_row,ordinary_history_known,ordinary_history_unknown>;
struct ordinary_history_decoded_page {
    uint64_t index=0;
    std::string previous,full_digest,raw;
    std::vector<ordinary_history_record> records; // at most64
};
struct ordinary_history_progress {
    uint64_t next_page=0,rows=0,known=0,unknown=0;
    std::string previous;
    int64_t last_table=-1;
    bool complete=false;
};

ordinary_history_descriptor decode_ordinary_history_descriptor(std::string_view,const ordinary_history_limits&);
ordinary_history_decoded_page decode_ordinary_history_page(std::string_view,const ordinary_history_descriptor&,const ordinary_history_progress&,const ordinary_history_limits&);

// Source-private passive observations. Only the current authenticated session
// correlates them; they cannot create old receipt, rebind, RESULT or ACK authority.
struct ordinary_history_lineage_predecessor {
    std::string scope,source,schema_digest,device,inode;
    int64_t epoch=0,issued_frontier=0;
};
struct ordinary_history_lineage_current {
    std::string scope,source,schema_digest,device,inode,stage,header_digest,content_digest;
    int64_t epoch=0,cut=0;
};
struct ordinary_history_lineage_relation {
    std::string request,profile_identifier,principal,server_channel;
    std::optional<std::string> group;
    std::optional<ordinary_history_lineage_predecessor> mounted_predecessor,selected_predecessor;
    ordinary_history_lineage_current current;
};

// One resource reservation spans all queued page commands for this physical
// session. A single pending command is admitted at a time; historyProgress
// after its actual durable COMMIT is the backpressure handshake. Progress is
// not an original ACK or a final canonical receipt. On reconnect a new native
// attempt must authenticate again before resuming the existing passive stage.
class ordinary_history_receive_state final {
    friend class ordinary_receiver_session;
    friend class ordinary_export_access;
    friend class ordinary_history_receive_operation;
    friend class ordinary_history_final_install_guard;
    ordinary_resource_reservation<true> charge_;
    const ordinary_history_limits limits_;
    ordinary_history_descriptor descriptor_;
    ordinary_history_progress progress_;
    // Retained only after actual current request/TLS correlation, never a grant.
    std::optional<ordinary_history_lineage_relation> lineage_relation_;
    std::string channel_;
    platform_transport_callbacks endpoint_;
    uint64_t lifecycle_=0;
    std::weak_ptr<sync_callback_lifetime> lifetime_;
    std::shared_ptr<ordinary_managed_wss_binding> managed_binding_;
    std::chrono::steady_clock::time_point episode_end_=std::chrono::steady_clock::time_point::max();
    std::shared_ptr<const ordinary_managed_receiver_preservation> restart_;
    bool current()const;
    std::atomic<bool> command_pending_{false};
    std::atomic<bool> failed_{false};
    ordinary_history_receive_state(ordinary_resource_reservation<true>&& charge,
        ordinary_history_limits limits,std::string channel)
        :charge_(std::move(charge)),limits_(limits),channel_(std::move(channel)){}
};
class ordinary_history_receive_operation final {
    friend class ordinary_receiver_session;
    friend class ordinary_export_access;
    // Resource owner is declared first and released after every queued input.
    std::shared_ptr<ordinary_history_receive_state> state_;
    std::optional<ordinary_history_decoded_page> page_; // absence = descriptor
    std::atomic<bool> consumed_{false};
    explicit ordinary_history_receive_operation(std::shared_ptr<ordinary_history_receive_state> state)
        :state_(std::move(state)){}
public:
    ~ordinary_history_receive_operation(){if(!consumed_.load(std::memory_order_acquire)){state_->failed_.store(true,std::memory_order_release);state_->command_pending_.store(false,std::memory_order_release);}}
};
}
