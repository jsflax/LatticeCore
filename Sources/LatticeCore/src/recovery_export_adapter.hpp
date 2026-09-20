#pragma once
#include "recovery_local_producer.hpp"
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
}
class recovery_export_adapter;
class recovery_export_route;
class committed_export_frame {
    friend class recovery_export_adapter;
    friend class recovery_export_route;
    std::shared_ptr<lattice_db> owner_;
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
};
class recovery_export_adapter {
public:
    // These methods require genuine retained owner custody. No public caller
    // assertion or supplied frame can create a committed permit.
    static bool protected_store(std::shared_ptr<lattice_db>);
    static recovery_export_preparation prepare_pending(std::shared_ptr<lattice_db>,
        const std::string& sync_id,uint64_t physical_generation,size_t maximum_entries,
        const std::vector<int64_t>& in_flight,bool filtered,const recovery_export_limits& = {});
    // Compatibility bookkeeping for currently sent IDs only. Leaves every
    // journal obligation open/pinned; never calls canonical acknowledge.
    static void acknowledge_legacy(std::shared_ptr<lattice_db>,const std::string&,const std::vector<std::string>&);
    static void require_committed(const recovery_install_result&);
};
} // namespace lattice::detail
