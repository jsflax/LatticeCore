#pragma once
#include "../include/lattice/ordinary_context.hpp"
#include "ordinary_installation_manifest.hpp"
#include "ordinary_owned_launch.hpp"
#include "ordinary_target_staging_connection.hpp"
#include <memory>
#include <atomic>
#include <cstdint>
#include <exception>
#include <string>
#include <utility>

namespace lattice { class lattice_db; class database; }
namespace lattice::detail {
class ordinary_export_access;
class ordinary_unpublished_group_target;
class ordinary_target_staging_io;
class ordinary_managed_ipc_access;
class ordinary_cohort_attachment_access;
class ordinary_cohort_attachment_binding;
class ordinary_managed_cohort_link;
namespace ordinary_installation { struct managed_history_receiver; }

// Actual inherited-child permission to inspect/enroll the specifically fenced
// historical generation. This is separate from its public ordinary-open view.
// The latter intentionally has no conversion back into this stronger type.
class ordinary_managed_child_admission final {
#ifdef LATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE
    friend struct ordinary_target_reclamation_fixture;
#endif
    friend class ordinary_target_staging_connection;
    friend struct ordinary_installation::managed_history_receiver;
    friend class ordinary_open_context;
    friend class ordinary_export_access;
    friend class ordinary_unpublished_group_target;
    friend class ordinary_managed_profile;
    friend class ordinary_managed_cohort_link;
    std::shared_ptr<ordinary_managed_cohort_link> claim_cohort_link(
        std::shared_ptr<const ordinary_managed_child_admission>,ordinary_launch::deadline)const;
    struct implementation;
    std::shared_ptr<implementation> impl_;
    explicit ordinary_managed_child_admission(std::shared_ptr<implementation>);
    void validate_open(const std::string&) const;
    void validate_physical(const std::string&, std::uint64_t, std::uint64_t) const;
    void validate_physical_until(const std::string&,std::uint64_t,std::uint64_t,ordinary_launch::deadline)const;
    std::string primary_path() const;
    std::vector<std::string> paths() const;
    std::string enrollment_binding(const std::string&,std::uint64_t,std::uint64_t) const;
    std::string enrollment_binding_until(const std::string&,std::uint64_t,std::uint64_t,ordinary_launch::deadline) const;
    std::pair<std::string,std::string> ipc_member_binding(
        const std::string&,std::uint64_t,std::uint64_t,ordinary_launch::deadline) const;
    // A physical open role is consumed BEFORE native construction. The two
    // named callers share one slot; catalog/scheduler differences cannot open
    // the same admitted main under a second role or after an unknown close.
    enum class opening_role : std::uint8_t { unclaimed, source, group_receiver };
    void claim_opening_until(const std::string&,opening_role,ordinary_launch::deadline) const;
    void require_group_opening_until(const std::string&,ordinary_launch::deadline) const;
    void remember_opening_failure(const std::string&,std::exception_ptr) const noexcept;
    void claim_enrollment(const std::string&) const;
    void claim_enrollment_until(const std::string&,ordinary_launch::deadline) const;
    // Private, exact old-member association; no caller path/open permission.
    void retain_group_target(const std::shared_ptr<const ordinary_managed_child_admission>&,
        const std::string&,std::uint64_t,std::uint64_t,ordinary_launch::deadline,
        std::shared_ptr<const ordinary_unpublished_group_target>&)const;
    void verify_group_target(std::size_t,const ordinary_unpublished_group_target*,ordinary_launch::deadline)const;
    void fail_enrollment() const noexcept;
public:
    ~ordinary_managed_child_admission();
    ordinary_managed_child_admission(const ordinary_managed_child_admission&) = delete;
    ordinary_managed_child_admission& operator=(const ordinary_managed_child_admission&) = delete;
};

// An unpublished empty directory role issued by the actual paired controller.
// No path/model/DB accessor, public constructor, cleanup or activation exists.
class ordinary_unpublished_group_target final {
    friend class ordinary_target_staging_connection;
    friend class ordinary_target_staging_io;
    friend class ordinary_managed_child_admission;
    friend class ordinary_export_access;
    mutable ordinary_target_scope_root_tail terminal_receiving_tail_{this,9}; // First: actual role members end first.
    const std::shared_ptr<const ordinary_managed_child_admission> admission_;
    const std::size_t slot_;
    mutable std::atomic<bool> claimed_{false};
    // Terminal disposition of this actual issued unpublished original. This
    // baseline has no target activation/transfer API. A cleanup claim seals
    // all subsequent staging work; it is never a new admission or publication.
    mutable std::atomic<bool> abandonment_claimed_{false};
    // Separate general terminal seal; this grants no constructor-only deletion.
    mutable std::atomic<bool> terminal_sealed_{false};
    bool seal_terminal_staging(const ordinary_target_staging_connection*)const noexcept;
    mutable std::atomic<bool> terminal_namespace_claimed_{false};
    struct terminal_namespace_binding {
        int stage=-1,root=-1,member=-1;
        const std::string* root_leaf=nullptr;const std::string* member_leaf=nullptr;
        std::uint64_t stage_device=0,stage_inode=0,root_device=0,root_inode=0,member_device=0,member_inode=0;
    };
    bool claim_terminal_namespace(const ordinary_target_staging_connection*,terminal_namespace_binding&)const noexcept;
    bool terminal_namespace_historical_current(const ordinary_target_staging_connection*,const terminal_namespace_binding&)const noexcept;
    struct abandonment_binding {
        int stage=-1,root=-1,member=-1;
        const std::string* root_leaf=nullptr;
        const std::string* member_leaf=nullptr;
        std::uint64_t stage_device=0,stage_inode=0,root_device=0,root_inode=0,member_device=0,member_inode=0;
    };
    bool claim_staging_abandonment(const ordinary_target_staging_connection*,abandonment_binding&)const noexcept;
    ordinary_unpublished_group_target(std::shared_ptr<const ordinary_managed_child_admission> owner,std::size_t slot)
        :admission_(std::move(owner)),slot_(slot){terminal_receiving_tail_.construction_returned_=true;}
    void require_staging_original(ordinary_launch::deadline)const;
    std::string staging_main_path(ordinary_launch::deadline)const;
    void bind_staging_connection(const std::shared_ptr<ordinary_target_staging_connection>&,
        ordinary_launch::deadline)const;
    int staging_member_descriptor(const ordinary_target_staging_connection*,ordinary_launch::deadline)const;
public:
    ~ordinary_unpublished_group_target();
    ordinary_unpublished_group_target(const ordinary_unpublished_group_target&)=delete;
    ordinary_unpublished_group_target& operator=(const ordinary_unpublished_group_target&)=delete;
};

// Post-COMMIT profile is intentionally a different type. Only actual source/
// schema enrollment inside ordinary_export_access may construct it after a
// clean owned transaction. The IPC access implementation must verify both
// store profiles and actual cohort plus its separately issued recipient role.
// No DTO, wire profile, copied journal row or ordinary_context is a factory.
class ordinary_managed_profile final {
    friend class ordinary_export_access;
    friend class ordinary_managed_ipc_access;
    friend class ordinary_cohort_attachment_access;
    friend class ordinary_cohort_attachment_binding;
    std::shared_ptr<const ordinary_managed_child_admission> attachment_admission(
        const ordinary_context&,const std::string&,std::uint64_t,std::uint64_t,ordinary_launch::deadline)const;
    struct implementation;
    std::shared_ptr<const implementation> impl_;
    explicit ordinary_managed_profile(std::shared_ptr<const implementation>);
    void validate(lattice_db&, database&) const;
    // Transport-only handoff after actual profile validation by the fixed
    // product issuer. Single-use; it is not a principal or partition grant.
    std::shared_ptr<ordinary_managed_cohort_link> claim_cohort_link(
        lattice_db&,database&,ordinary_launch::deadline)const;
public:
    ~ordinary_managed_profile();
    ordinary_managed_profile(const ordinary_managed_profile&) = delete;
    ordinary_managed_profile& operator=(const ordinary_managed_profile&) = delete;
};

namespace ordinary_installation {
struct managed_history_startup final {
    ordinary_context opens;
    std::shared_ptr<const ordinary_managed_child_admission> admission;
};
struct managed_history_receiver final {
    // Takes inherited channel 3 and directories 4...9 on the actual fixed
    // managed early-entry path. The process open gate closes before decoding.
    // A duplicate call or any earlier uncontrolled native open refuses.
    static managed_history_startup receive(ordinary_launch::deadline);
    static managed_history_startup receive_paired(ordinary_launch::deadline);
    // Separate fixed memory-sync entry, before any daemon work/native open.
    // The method validates the actual daemon image and the joint closed cohort.
    static managed_history_startup receive_daemon(ordinary_launch::deadline);
private:
    static managed_history_startup receive_impl(ordinary_launch::deadline,bool daemon_entry,bool paired_entry);
};
}
}
