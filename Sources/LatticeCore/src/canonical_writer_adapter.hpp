#pragma once
#include "canonical_change_store.hpp"
#include "canonical_source_capture.hpp"
#include "canonical_transfer_retention.hpp"
#include "canonical_durable_ready.hpp"
#include "lattice/sync.hpp"
#include "lattice/recovery_schema.hpp"
#include <memory>
#include <string>
#include <vector>

namespace lattice::detail {
class canonical_upstream_delivery;
class canonical_namespace_admission;
class authenticated_relay_setup;
class authenticated_session_fence;
struct canonical_source_session_test_access;
struct canonical_retention_test_access;
namespace canonical_upstream_test_hooks {
// Private failure injection only. Restricts an already admitted authorizer
// action; never installs/replaces a raw hook. The callback must not run SQL,
// allocate, throw, or escape a pointer. Tests scope/reset the TLS slot.
struct authorizer_fault {
    sqlite3* connection;
    int (*restrict_action)(int,const char*,const char*,const char*) noexcept;
};
extern thread_local const authorizer_fault* fault;
}
namespace canonical_retention_test_hooks {
// Restriction-only faults; neither a clock override nor writable authority.
extern thread_local const canonical_upstream_test_hooks::authorizer_fault* fault;
}
// Private fixed source qualification, not serving/epoch authority. The legacy
// attachment borrows its owner. Every upstream call separately retains the
// actual owner and exact physical writer through its synchronous callback tail.
// Exclusive callback/DDL custody remains required; no transport is activated.
struct canonical_writer_profile {
    canonical_store_binding binding;
    canonical_store_limits limits;
    std::vector<std::string> models;
    bool upstream_requested = false; // legacy attach still refuses this request
};
struct canonical_namespaced_writer_profile {
    canonical_writer_profile writer;
    canonical_namespace_profile namespaces;
};
// Opaque actual-owner admission. The named fixture issuer below remains
// qualification-only. The private real relay setup separately binds its
// registered-peer application outcome and live physical session; neither
// path establishes the receiver's trusted platform TLS/source authority.
// Copies retain the actual physical owner but cannot survive its retirement.
class canonical_namespace_admission {
    friend class canonical_writer_adapter;
    friend class canonical_upstream_delivery;
    std::shared_ptr<lattice_db> owner_;
    std::shared_ptr<database> writer_;
    std::shared_ptr<void> context_;
    uint64_t revision_=0;
    canonical_namespace_entry namespace_;
    std::string replica_;
    std::shared_ptr<authenticated_session_fence> authenticated_;
    canonical_namespace_admission() = default;
public:
    canonical_namespace_admission(const canonical_namespace_admission&) = default;
    canonical_namespace_admission& operator=(const canonical_namespace_admission&) = default;
};
struct canonical_upstream_limits {
    size_t entries, field_bytes, delivery_bytes; // explicit finite caller budgets
};
class canonical_writer_adapter {
    friend class authenticated_relay_setup;
    friend struct authenticated_relay_catalog_test_access;
    friend void require_canonical_relation(database&, const std::string&);
    friend class canonical_upstream_delivery;
    struct context;
    struct retention_session;
    struct retention_frame;
    std::shared_ptr<retention_session> retention_;
    static bool matches_connection(const database&, sqlite3*) noexcept;
    std::shared_ptr<database> writer_;
    std::shared_ptr<context> context_;
    // Default-null, friend-only deterministic scheduling probe. It runs before
    // BEGIN, confers no admission, and cannot skip the owned-write validation.
    static thread_local const std::function<void(lattice_db&)>* namespace_before_write_test_hook_;
    explicit canonical_writer_adapter(lattice_db&, const canonical_writer_profile&,
                                      const canonical_upstream_limits* = nullptr,
                                      const canonical_retention_limits* = nullptr,
                                      const canonical_namespace_profile* = nullptr,
                                      const canonical_ready_profile* = nullptr);
    static void validate_namespace_admission(const std::shared_ptr<lattice_db>&,
        const std::shared_ptr<database>&, const std::shared_ptr<context>&,
        const canonical_namespace_admission&);
    canonical_namespace_admission admit_authenticated_session(std::shared_ptr<lattice_db>,
        const std::string&,const std::string&,std::shared_ptr<authenticated_session_fence>);
    std::string authenticated_descriptor_digest()const;
    static const recovery_owner_schema& authenticated_catalog(const lattice_db&) noexcept;
    static std::shared_ptr<canonical_writer_adapter> open_authenticated_source(std::shared_ptr<lattice_db>,
        const canonical_namespaced_writer_profile&,canonical_upstream_limits,canonical_retention_limits,
        const canonical_ready_profile&,bool);
    std::vector<std::string> apply_upstream_impl(std::shared_ptr<lattice_db>,
        const std::vector<audit_log_entry>&, const std::optional<std::string>&,
        const canonical_namespace_admission*);
    void prepare_retention(lattice_db&, const canonical_retention_limits&);
    void enroll_retention(lattice_db&, bool);
    static void verify_retention(database&, const context&, const retention_session&);
    static recovery_install_result retention_owned(std::shared_ptr<lattice_db>,
        std::shared_ptr<database>, std::shared_ptr<context>, std::shared_ptr<retention_session>,
        int, const std::function<void(database&,retention_session&)>&);
    friend struct canonical_source_session_test_access;
    friend struct canonical_retention_test_access;
    friend struct canonical_namespace_test_access;
    friend struct canonical_ready_test_access;
    static sync_recovery::owned_canonical_capture capture_reserved_session(std::shared_ptr<lattice_db>,
        std::shared_ptr<database>,std::shared_ptr<context>,std::shared_ptr<retention_session>,
        const canonical_retention_ticket&,const std::vector<sync_recovery::canonical_capture_request>&,
        const sync_recovery::canonical_capture_limits&,const std::function<void(size_t,uint64_t)>&,
        const canonical_namespace_admission*);
    static sync_recovery::owned_canonical_capture capture_recovery_session(std::shared_ptr<lattice_db>,
        std::shared_ptr<database>,std::shared_ptr<context>,const canonical_store_binding&,std::optional<int64_t>,
        const std::vector<sync_recovery::canonical_capture_request>&,const sync_recovery::canonical_capture_limits&,
        const std::function<void(size_t,uint64_t)>&,const std::function<void()>&,const std::function<void()>&,
        const std::function<void(uint64_t)>&,const canonical_namespace_admission*);
    void enroll_ready(lattice_db&,bool);
    static void verify_ready_retention(database&,const context&,const retention_session&);
    static void expire_ready_rows(database&,const context&,retention_session&);
    static canonical_ready_info ready_info(database&,const context&,const retention_session&,const std::string&);
    canonical_ready_result prepare_ready_impl(std::shared_ptr<lattice_db>,const canonical_namespace_admission&,
        const canonical_range::attempt&,const canonical_range::request&,int64_t,uint64_t,
        const std::function<void()>&,const std::function<void(size_t,uint64_t)>&);
    sync_recovery::owned_canonical_capture capture_reserved_impl(std::shared_ptr<lattice_db>,
        const canonical_retention_ticket&,const std::vector<sync_recovery::canonical_capture_request>&,
        const sync_recovery::canonical_capture_limits&,const std::function<void(size_t,uint64_t)>&,
        const canonical_namespace_admission* = nullptr);
    sync_recovery::owned_canonical_capture capture_recovery_impl(std::shared_ptr<lattice_db>,
        const canonical_store_binding&, std::optional<int64_t>,
        const std::vector<sync_recovery::canonical_capture_request>&,
        const sync_recovery::canonical_capture_limits&,
        const std::function<void(size_t,uint64_t)>&,
        const std::function<void()>&, const std::function<void()>&,
        const std::function<void(uint64_t)>& = {},
        const canonical_namespace_admission* = nullptr);

public:
    // Distinct fresh receipt-v2/retention-v3 profile, or exact v3 reopen only.
    // The actual writer must already use WAL and synchronous FULL or EXTRA;
    // this API never changes ordinary owner/SDK durability configuration.
    // No implicit v2 adoption, authenticated issuer or serving capability.
    static std::unique_ptr<canonical_writer_adapter> attach_ready_for_qualification(
        std::shared_ptr<lattice_db>,const canonical_namespaced_writer_profile&,
        canonical_upstream_limits,canonical_retention_limits,const canonical_ready_profile&);
    canonical_ready_result prepare_ready_owned(std::shared_ptr<lattice_db>,const canonical_namespace_admission&,
        const canonical_range::attempt&,const canonical_range::request&,int64_t duration_ms,uint64_t route_generation);
    // Equivalent logical identity only. Immutable manifest lease bytes remain
    // unchanged; receiver/controller binding of this new physical lease is a
    // separate, still-unimplemented admission contract.
    canonical_ready_resume_result resume_ready_owned(std::shared_ptr<lattice_db>,const canonical_namespace_admission&,
        const canonical_range::attempt&,const canonical_range::request&,int64_t duration_ms,uint64_t route_generation);
    canonical_ready_frame_result read_ready_frame_owned(std::shared_ptr<lattice_db>,const canonical_namespace_admission&,
        const canonical_ready_lease&,uint64_t index);
    // Source-owner inspection/disposal only. No remote cancellation, quiescence,
    // installation, ACK or receipt settlement is implied by abandonment/expiry.
    canonical_ready_inspection inspect_ready_owned(std::shared_ptr<lattice_db>);
    recovery_install_result abandon_ready_owned(std::shared_ptr<lattice_db>,const canonical_ready_identity&);
    recovery_install_result expire_ready_owned(std::shared_ptr<lattice_db>);
    static std::unique_ptr<canonical_writer_adapter> attach(lattice_db&, const canonical_writer_profile&);
    // Inactive qualification only. Requires upstream_requested and an idle,
    // retained owner with no configured sync/IPC. No borrowed upstream route.
    static std::unique_ptr<canonical_writer_adapter> attach_upstream_for_qualification(
        std::shared_ptr<lattice_db>, const canonical_writer_profile&, canonical_upstream_limits);
    // Explicit profile v2 enrollment; refuses upgrading a legacy attached
    // canonical store. One file-WAL session holds bound directory custody.
    // No public callback, caller transaction or caller-supplied protected base.
    static std::unique_ptr<canonical_writer_adapter> attach_retention_for_qualification(
        std::shared_ptr<lattice_db>, const canonical_writer_profile&, canonical_retention_limits);
    // Same actual source combines canonical imported-entry settlement and
    // committed tail reservations. Both existing admissions remain required;
    // this does not authenticate a peer, issue negative receipts, persist a
    // transfer, advertise READY or activate a network route.
    static std::unique_ptr<canonical_writer_adapter> attach_retained_upstream_for_qualification(
        std::shared_ptr<lattice_db>, const canonical_writer_profile&,
        canonical_upstream_limits, canonical_retention_limits);
    // New receipt v2 + existing retention v2 only: never adopts a v1 ledger.
    static std::unique_ptr<canonical_writer_adapter> attach_namespaced_upstream_for_qualification(
        std::shared_ptr<lattice_db>, const canonical_namespaced_writer_profile&,
        canonical_upstream_limits, canonical_retention_limits);
    canonical_namespace_admission admit_namespace_for_qualification(std::shared_ptr<lattice_db>,
        const std::string& namespace_id, const std::string& replica_id);
    std::vector<std::string> apply_upstream_namespaced_owned(std::shared_ptr<lattice_db>,
        const canonical_namespace_admission&, const std::vector<audit_log_entry>&,
        const std::optional<std::string>& receiving_channel = std::nullopt);
    sync_recovery::owned_canonical_capture capture_reserved_namespaced_owned(std::shared_ptr<lattice_db>,
        const canonical_namespace_admission&, const canonical_retention_ticket&,
        const std::vector<sync_recovery::canonical_capture_request>&,
        const sync_recovery::canonical_capture_limits&);
    canonical_retention_result reserve_recovery_owned(std::shared_ptr<lattice_db>,
        std::optional<int64_t> base, int64_t duration_ms);
    recovery_install_result release_recovery_owned(std::shared_ptr<lattice_db>, const canonical_retention_ticket&);
    recovery_install_result expire_recovery_owned(std::shared_ptr<lattice_db>);
    recovery_install_result prune_recovery_owned(std::shared_ptr<lattice_db>, int64_t floor);
    // Caller keeps the returned reservation through later bounded preparation;
    // failure retains it until explicit release or source-owned expiry. No
    // read custody survives return/throw; no READY/spool proof is manufactured.
    sync_recovery::owned_canonical_capture capture_reserved_owned(std::shared_ptr<lattice_db>,
        const canonical_retention_ticket&,
        const std::vector<sync_recovery::canonical_capture_request>&,
        const sync_recovery::canonical_capture_limits&);
    std::vector<std::string> apply_upstream_owned(std::shared_ptr<lattice_db>,
        const std::vector<audit_log_entry>&, const std::optional<std::string>& receiving_channel = std::nullopt);
    // Private, synchronous, file-WAL-only source qualification. Scope and
    // descriptor come from this admitted adapter, never a caller table vector.
    // Null base explicitly requests full; retired numeric base returns a
    // requires_full_request result with no capture. No frozen Q is rewritten.
    // Actual owner/writer/context are retained through the final decision.
    // Retirement/replacement observed before that decision refuses; later
    // close does not retroactively invalidate these UNSEALED facts. No serving,
    // authentication, negative receipt, spool, lease or transport authority.
    sync_recovery::owned_canonical_capture capture_recovery_owned(std::shared_ptr<lattice_db>,
        const canonical_store_binding& declared_binding, std::optional<int64_t> declared_base,
        const std::vector<sync_recovery::canonical_capture_request>&,
        const sync_recovery::canonical_capture_limits&);
    ~canonical_writer_adapter();
    canonical_writer_adapter(const canonical_writer_adapter&) = delete;
    canonical_writer_adapter& operator=(const canonical_writer_adapter&) = delete;
    static constexpr bool serving_capability = false;
    static std::string uuid_key(const std::string&); // comparison only; never rewrites persisted spelling
};

// Private stack capability. Only the adapter constructs a delivery; only the
// owned apply loop constructs an entry after chunk admission and SAVEPOINT.
// Neither an active SQLite transaction nor this TLS pointer proves ownership.
class canonical_upstream_delivery {
    friend class canonical_writer_adapter;
    friend struct canonical_writer_adapter::context;
    std::shared_ptr<lattice_db> owner_;
    std::shared_ptr<database> writer_;
    std::shared_ptr<canonical_writer_adapter::context> context_;
    uint64_t revision_;
    std::optional<canonical_namespace_admission> namespace_admission_;
    const audit_log_entry* entry_ = nullptr;
    std::string original_, target_;
    bool finalizing_ = false;
    canonical_upstream_delivery* previous_ = nullptr;
    static thread_local canonical_upstream_delivery* current_;
    canonical_upstream_delivery(std::shared_ptr<lattice_db>, std::shared_ptr<database>,
        std::shared_ptr<canonical_writer_adapter::context>, uint64_t, const canonical_namespace_admission*);
    void begin_entry(const audit_log_entry&);
    void end_entry() noexcept;
    std::string entry_guard() const;
    void script(const std::string&);
public:
    canonical_upstream_delivery(const canonical_upstream_delivery&) = delete;
    canonical_upstream_delivery& operator=(const canonical_upstream_delivery&) = delete;
    const std::shared_ptr<database>& writer() const noexcept {return writer_;}
    uint64_t revision() const noexcept {return revision_;}
    void validate_chunk(lattice_db&, database&) const;
    void validate_envelope(const std::vector<audit_log_entry>&, const std::optional<std::string>&) const;
    const std::unordered_map<std::string, column_type>& schema(const std::string&) const;
    void execute(const std::string&, const std::vector<column_value_t>& = {});
    std::vector<database::row_t> query(const std::string&, const std::vector<column_value_t>& = {});
    class entry_scope {
        canonical_upstream_delivery& delivery_;
    public:
        entry_scope(canonical_upstream_delivery& d,const audit_log_entry& e):delivery_(d){d.begin_entry(e);}
        ~entry_scope(){delivery_.end_entry();}
        entry_scope(const entry_scope&) = delete;
        entry_scope& operator=(const entry_scope&) = delete;
        bool duplicate() const;
        void validate_payload() const;
        void accept(canonical_receipt_outcome);
    };
};
} // namespace lattice::detail
