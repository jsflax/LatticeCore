#pragma once
#include "canonical_change_store.hpp"
#include "canonical_source_capture.hpp"
#include "lattice/sync.hpp"
#include <memory>
#include <string>
#include <vector>

namespace lattice::detail {
class canonical_upstream_delivery;
struct canonical_source_session_test_access;
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
struct canonical_upstream_limits {
    size_t entries, field_bytes, delivery_bytes; // explicit finite caller budgets
};
class canonical_writer_adapter {
    friend void require_canonical_relation(database&, const std::string&);
    friend class canonical_upstream_delivery;
    struct context;
    static bool matches_connection(const database&, sqlite3*) noexcept;
    std::shared_ptr<database> writer_;
    std::shared_ptr<context> context_;
    explicit canonical_writer_adapter(lattice_db&, const canonical_writer_profile&,
                                      const canonical_upstream_limits* = nullptr);
    friend struct canonical_source_session_test_access;
    sync_recovery::owned_canonical_capture capture_recovery_impl(std::shared_ptr<lattice_db>,
        const canonical_store_binding&, std::optional<int64_t>,
        const std::vector<sync_recovery::canonical_capture_request>&,
        const sync_recovery::canonical_capture_limits&,
        const std::function<void(size_t,uint64_t)>&,
        const std::function<void()>&, const std::function<void()>&);

public:
    static std::unique_ptr<canonical_writer_adapter> attach(lattice_db&, const canonical_writer_profile&);
    // Inactive qualification only. Requires upstream_requested and an idle,
    // retained owner with no configured sync/IPC. No borrowed upstream route.
    static std::unique_ptr<canonical_writer_adapter> attach_upstream_for_qualification(
        std::shared_ptr<lattice_db>, const canonical_writer_profile&, canonical_upstream_limits);
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
    const audit_log_entry* entry_ = nullptr;
    std::string original_, target_;
    bool finalizing_ = false;
    canonical_upstream_delivery* previous_ = nullptr;
    static thread_local canonical_upstream_delivery* current_;
    canonical_upstream_delivery(std::shared_ptr<lattice_db>, std::shared_ptr<database>,
        std::shared_ptr<canonical_writer_adapter::context>, uint64_t);
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
