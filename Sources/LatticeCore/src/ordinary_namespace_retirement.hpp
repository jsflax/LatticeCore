#pragma once
#include "ordinary_installation_cohort.hpp"
#include "ordinary_historical_generation.hpp"
#include "ordinary_managed_runtime_retirement.hpp"
#include <memory>
#include <functional>
#include <utility>
#include <vector>

namespace lattice::detail::ordinary_installation {
class retained_engram_upgrade_session;
class engram_namespace_retirement;
class retained_historical_generation;
// Private live handoff only. Durable facts cannot construct this type. The
// actual staged controller supplies a verifier retaining its complete role
// session; the namespace rechecks it before and after the real exchanges.
class historical_namespace_custody final {
    friend class retained_engram_upgrade_session;
    friend class engram_namespace_retirement;
    file_identity data_, control_;
    digest barrier_{};
    std::function<void(ordinary_launch::deadline)> verify_;
    historical_namespace_custody(file_identity data,file_identity control,digest barrier,
        std::function<void(ordinary_launch::deadline)> verify)
        :data_(data),control_(control),barrier_(barrier),verify_(std::move(verify)){}
public:
    historical_namespace_custody(const historical_namespace_custody&)=delete;
    historical_namespace_custody& operator=(const historical_namespace_custody&)=delete;
};
// The first actual role phase only permits inspection/finalization of an
// installer-created provisional root. It cannot authorize preservation.
class provisional_namespace_custody final {
    friend class retained_engram_upgrade_session;
    friend class engram_namespace_retirement;
    file_identity data_, control_;
    digest binding_{};
    std::function<void(ordinary_launch::deadline)> verify_;
    provisional_namespace_custody(file_identity data, file_identity control, digest binding,
        std::function<void(ordinary_launch::deadline)> verify)
        : data_(data), control_(control), binding_(binding), verify_(std::move(verify)) {}
public:
    provisional_namespace_custody(const provisional_namespace_custody&) = delete;
    provisional_namespace_custody& operator=(const provisional_namespace_custody&) = delete;
};
struct historical_namespace_binding {
    file_identity data, control;
    digest barrier{};
    std::vector<file_identity> originals;
};
enum class namespace_retirement_error_code {
    unavailable, invalid, changed, incomplete, expired, durability_unproved,
    unsupported_filesystem, inherited_use
};
class namespace_retirement_error : public std::runtime_error {
public:
    const namespace_retirement_error_code code;
    explicit namespace_retirement_error(namespace_retirement_error_code value)
        : std::runtime_error("ordinary namespace retirement unavailable"), code(value) {}
};
struct namespace_retirement_observation {
    file_identity data, control;
    // Observed filesystem state only. None of these fields is retirement,
    // historical admission, descendant closure or a writable generation token.
    bool primary_denied = false, sync_denied = false;
    bool primary_swapped = false, sync_swapped = false;
    bool preserved_unadopted = false;
};

// Private installer-owned filesystem transition. It does not open SQLite or
// change its bytes. Old paths remain denied permanently; no rollback, symlink
// redirect, chmod repair of an old path or delete-and-recreate operation exists.
class engram_namespace_retirement final {
    friend class retained_engram_upgrade_session;
    // Separate fixed-controller entry. Generic historical resume/reconcile
    // remains exact and never accepts post-managed sidecar churn.
    static engram_namespace_retirement resume_after_managed_runtime(int,const std::string&,int,const std::string&,
        const retained_managed_runtime_retirement&,ordinary_launch::deadline);
    historical_namespace_binding managed_original_binding(const retained_managed_runtime_retirement&,
        bool preliminary,ordinary_launch::deadline) const;
    retained_historical_generation retain_after_managed_runtime(
        const retained_managed_runtime_retirement&,const historical_namespace_custody&,ordinary_launch::deadline);
    static engram_namespace_retirement stage_historical(int, const std::string&, int,
        const std::string&, ordinary_launch::deadline, bool resume,
        const created_launch_cohort::store_namespace* created = nullptr);
    bool has_provisional_root() const;
    historical_namespace_binding provisional_binding(ordinary_launch::deadline) const;
    void finalize_provisional_root(const provisional_namespace_custody&, ordinary_launch::deadline);
    historical_namespace_binding denied_historical_binding(ordinary_launch::deadline) const;
    void preserve_historical_namespace(const historical_namespace_custody&,ordinary_launch::deadline);
    retained_historical_generation retain_preserved_generation(
        const historical_namespace_custody&, ordinary_launch::deadline);
    static engram_namespace_retirement resume_impl(int, const std::string&, int,
        const std::string&, ordinary_launch::deadline, bool);
    struct implementation;
    std::shared_ptr<implementation> impl_;
    explicit engram_namespace_retirement(std::shared_ptr<implementation>);
public:
    // Parents must be retained, independently authorized installer directories.
    // data_leaf names an existing Engram directory; control_leaf is exclusive.
    // Complete known primary/sync members are retained before any denial.
    static engram_namespace_retirement stage(int data_parent, const std::string& data_leaf,
        int control_parent, const std::string& control_leaf, ordinary_launch::deadline);
    // Historical controller staging variant: persist exact root/mode intent,
    // freeze directory mutation, then seal the final actual member inventory.
    // Shared-parent metadata/config writes are part of the managed restart.
    // Existing files can still be written through old descriptors until actual
    // retirement; this barrier is not a database snapshot or custody grant.
    // Originals use a version-bound 0400 denial profile: regular files deny
    // writes and the sync directory denies traversal. This permits actual
    // O_RDONLY metadata reopen/fsync after controller restart without adding
    // any access. Final exchanged refusals stay mode 0. Legacy stage() mode-0
    // journals keep their distinct limitations and are never upgraded in place.
    static engram_namespace_retirement stage_with_mutation_barrier(int data_parent,
        const std::string& data_leaf, int control_parent, const std::string& control_leaf,
        ordinary_launch::deadline);
    // Same filesystem protocol for a namespace actually created and retained by
    // this controller. The private handoff closes launch admission before any
    // mode change; it grants cleanup custody only, never historical adoption.
    static engram_namespace_retirement stage_created_with_mutation_barrier(
        const created_launch_cohort::store_namespace&, int data_parent,
        const std::string& data_leaf, int control_parent, const std::string& control_leaf,
        ordinary_launch::deadline);
    // Separate positive filesystem driver for an actually controller-created
    // namespace with an initially absent optional root. The same private
    // irreversible handoff closes grants before the parent barrier. These
    // methods cannot reconstruct a historical cohort or authorize adoption.
    static engram_namespace_retirement stage_created_provisional(
        const created_launch_cohort::store_namespace&, int data_parent,
        const std::string& data_leaf, int control_parent, const std::string& control_leaf,
        ordinary_launch::deadline, bool resume = false);
    void finalize_created_provisional_after_join(
        const created_launch_cohort::store_namespace&, ordinary_launch::deadline);
    // Decoding restores only reconciliation state, never retirement authority.
    // A partial record or an unrecognized name/inode arrangement refuses.
    static engram_namespace_retirement resume(int data_parent, const std::string& data_leaf,
        int control_parent, const std::string& control_leaf, ordinary_launch::deadline);
    // Explicit mutation-barrier restart, including unpublished initialization.
    // Only known private pending prefixes may be rebuilt; published records and
    // every identity they bind remain immutable. Legacy stage() is not inferred
    // or upgraded. This restores no live retirement/adoption authority.
    static engram_namespace_retirement resume_with_mutation_barrier(int data_parent,
        const std::string& data_leaf, int control_parent, const std::string& control_leaf,
        ordinary_launch::deadline);
    ~engram_namespace_retirement();
    engram_namespace_retirement(engram_namespace_retirement&&) noexcept;
    engram_namespace_retirement& operator=(engram_namespace_retirement&&) noexcept;
    engram_namespace_retirement(const engram_namespace_retirement&) = delete;
    engram_namespace_retirement& operator=(const engram_namespace_retirement&) = delete;
    void deny_new_opens(ordinary_launch::deadline);
    namespace_retirement_observation observe(ordinary_launch::deadline) const;

    // Positive lower-level path ONLY for the actual controller-created
    // namespace held by this object. The retained cohort must already be
    // durably closed with every directly owned child joined. It cannot name a
    // historical directory or reconstruct custody from a completion struct.
    // The separate historical entry point is private to the retained upgrade
    // session; no bool/JSON/test completion hook can enter either path.
    void preserve_created_namespace_after_join(
        const created_launch_cohort::store_namespace&, ordinary_launch::deadline);
};

namespace namespace_retirement_test_hooks {
// Nil-default failure/rendezvous at actual boundaries. Cannot report syscall
// success or grant retirement; returning merely allows the real checks to run.
enum class boundary { before_denial, after_denial, before_swap, after_swap, before_directory_sync,
    before_mutation_barrier, after_mutation_barrier, before_parent_restore,
    after_created_handoff, before_event_publish, after_event_publish,
    before_provisional_publish, after_provisional_publish, after_provisional_freeze };
using observer = void (*)(boundary, const char*);
inline thread_local observer observe = nullptr;
}
}
