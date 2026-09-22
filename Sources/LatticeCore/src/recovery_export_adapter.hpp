#pragma once
#include "recovery_local_producer.hpp"
#include "recovery_producer_continuity.hpp"
#include "sync_callback_lifetime.hpp"
#include <lattice/sync.hpp>

namespace lattice::detail {
struct recovery_export_limits {
    size_t entries=1000, field_bytes=1048576, raw_bytes=4194304, wire_bytes=8388608;
    // Inactive qualification containment: each raw candidate stream must
    // exhaust within this cap. Large legitimate backlogs refuse explicitly.
    size_t coverage_candidates=4096;
};
namespace recovery_export_test_hooks {
// Finite source-owned rendezvous; production leaves these null. The first is
// inside the owned claim transaction; the second is after known COMMIT.
extern thread_local void (*before_claim_commit)();
extern thread_local void (*after_claim_commit)();
// Private fault seam after one contribution's claim postimage was captured.
// Null in production; permits deterministic cross-contribution fault tests.
extern thread_local void (*after_contribution_claim)(size_t);
}
class recovery_export_adapter;
class recovery_export_route;
class recovery_server_export_endpoint;
class recovery_server_export_page;
class receiver_upload_view;
class committed_export_frame {
    friend class recovery_export_adapter;
    friend class recovery_export_route;
    friend class recovery_server_export_page;
    std::shared_ptr<lattice_db> owner_;
    std::shared_ptr<recovery_continuous_work> continuous_work_;
    std::shared_ptr<const receiver_upload_view> upload_view_;
    std::vector<recovery_obligation_export_ticket> claims_;
    std::vector<recovery_local_export_scope> scopes_;
    recovery_obligation_producer_discovery_limits limits_{};
    std::vector<audit_log_entry> entries_;
    transport_message message_;
    uint64_t physical_generation_=0;
    bool consumed_=false;
    committed_export_frame()=default;
public:
    committed_export_frame(committed_export_frame&&) noexcept;
    committed_export_frame& operator=(committed_export_frame&&) noexcept;
    committed_export_frame(const committed_export_frame&)=delete;
    const std::vector<audit_log_entry>& entries()const noexcept{return entries_;}
    // Exact last selected local PK, not a source frontier or delivery receipt.
    // A moved-from/empty permit has no cursor. Only successful preparation
    // publishes a nonempty frame; the caller advances after its own handoff.
    std::optional<int64_t> last_audit_id()const noexcept {
        if(consumed_||entries_.empty())return std::nullopt;
        return entries_.back().id;
    }
};
// Retains the exact physical transport through a reentrant handoff. The gate
// never runs SQLite, callbacks, sends or teardown while holding its leaf lock.
class recovery_export_route {
    std::mutex mutex_;
    std::shared_ptr<sync_transport> transport_;
    std::shared_ptr<sync_callback_lifetime> lifetime_;
#ifndef __EMSCRIPTEN__
    std::optional<sync_retirement_lane::reservation> retirement_;
#endif
    uint64_t generation_=0;
    bool open_=false, retired_=false, protected_=false;
public:
    explicit recovery_export_route(std::shared_ptr<sync_transport>,std::shared_ptr<sync_callback_lifetime>);
    void prepare_protected(uint64_t generation);
    bool retire_protected(std::thread = {}) noexcept;
    void publish(uint64_t generation,bool open) noexcept;
    void retire() noexcept;
    bool current(uint64_t generation) noexcept;
    // A final owned durable-mode recheck precedes physical handoff admission.
    // A freeze after admission may coexist with completion of this already
    // claimed send. This is not a network drain or source receipt.
    bool handoff(committed_export_frame);
};
struct recovery_export_preparation {
    bool protected_store=false;
    std::optional<committed_export_frame> frame;
    // A bounded refusal about the first pending local PK; never an empty page,
    // skipped original, receipt, or authorization decision.
    std::string blocked_original;
};
class recovery_export_adapter {
    friend class ::lattice::synchronizer_base;
    static std::optional<recovery_export_preparation> prepare_for_route(std::shared_ptr<lattice_db>,
        const std::shared_ptr<recovery_continuous_route>&,const std::string&,uint64_t,size_t,
        const std::vector<int64_t>&,bool,bool*,std::shared_ptr<const receiver_upload_view> = {});
    friend class recovery_export_route;
    friend class recovery_server_export_endpoint;
    friend class recovery_server_export_page;
    static void validate_server_limits(const recovery_export_limits&);
    static void revalidate_claimed_frame(const committed_export_frame&);
    static recovery_export_preparation prepare(std::shared_ptr<lattice_db>,
        const std::string&,uint64_t,size_t,const std::vector<int64_t>&,bool,
        const recovery_export_limits&,std::optional<int64_t> history_after,bool* discovery_busy=nullptr,
        bool retained_delete_page=false,std::shared_ptr<recovery_continuous_work> = {},
        std::shared_ptr<const receiver_upload_view> = {});
public:
    // These methods require genuine retained owner custody. No public caller
    // assertion or supplied frame can create a committed permit.
    static bool protected_store(std::shared_ptr<lattice_db>);
    // Nullopt is exclusively the first no-effect mutex probe being busy.
    // Every other classifier/preparation failure still throws unchanged.
    static std::optional<bool> try_protected_store(std::shared_ptr<lattice_db>);
    static std::optional<recovery_export_preparation> try_prepare_pending(std::shared_ptr<lattice_db>,
        const std::string& sync_id,uint64_t physical_generation,size_t maximum_entries,
        const std::vector<int64_t>& in_flight,bool filtered,const recovery_export_limits& = {});
    static recovery_export_preparation prepare_pending(std::shared_ptr<lattice_db>,
        const std::string& sync_id,uint64_t physical_generation,size_t maximum_entries,
        const std::vector<int64_t>& in_flight,bool filtered,const recovery_export_limits& = {});
    // Inactive, resolved-PK history selection: no pending/ACK/filter/floor
    // exclusions. Every selected row must have a genuine generated stamp and
    // current open obligation, or the entire page refuses. A protected result
    // with no frame means an empty sampled view, never frontier authority. No mount
    // authorization is implied; a future SDK route must authorize every row.
    static recovery_export_preparation prepare_history_page(std::shared_ptr<lattice_db>,
        uint64_t physical_generation,int64_t after_audit_id,size_t maximum_entries,
        const recovery_export_limits& = {});
    // Inactive retained-original continuation. Same bounded ordered PK page and
    // genuine generated/open-obligation checks as history preparation. Only
    // this entry may omit unavailable UPDATE NoHistory fields, and only when
    // this page contains a later generated DELETE of the exact target and the
    // same owned view proves final absence. All selected originals are claimed
    // together. A page ending before DELETE refuses; no unbounded lookahead,
    // skipped original, UNSENT/receipt inference or controller activation.
    static recovery_export_preparation prepare_retained_page(std::shared_ptr<lattice_db>,
        uint64_t physical_generation,int64_t after_audit_id,size_t maximum_entries,
        const recovery_export_limits& = {});
    // Compatibility bookkeeping for currently sent IDs only. Leaves every
    // journal obligation open/pinned; never calls canonical acknowledge.
    static void acknowledge_legacy(std::shared_ptr<lattice_db>,const std::string&,const std::vector<std::string>&);
    static void require_committed(const recovery_install_result&);
};
} // namespace lattice::detail
