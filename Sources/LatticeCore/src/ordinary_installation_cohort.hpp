#pragma once
#include "ordinary_owned_launch.hpp"
#include "ordinary_installation_files.hpp"
#include "ordinary_installation_manifest.hpp"
#include <memory>
#include <string>
#include <vector>

// Durable ownership of a controller-created launch cohort. This is a launch
// gate, not a store-open capability or an assertion about historical processes.
// A direct-child completion record does not settle descendant/GUI/host custody.
namespace lattice::detail::ordinary_installation {
enum class cohort_error_code { unavailable, inherited_use, wrong_thread, closed,
    changed, unfinished_launch, durability_unproved, incomplete, limit, timed_out };
class cohort_error : public std::runtime_error {
public:
    const cohort_error_code code;
    explicit cohort_error(cohort_error_code value)
        : std::runtime_error("ordinary installation cohort unavailable"), code(value) {}
};
struct direct_cohort_completion {
    file_identity directory, gate;
    std::uint64_t revision = 0;
    std::vector<ordinary_launch::terminal_observation> children;
    // No ready/adopted/descendantsGone field and no context conversion.
};
class seeded_installation_store;
class created_launch_cohort {
    friend class seeded_installation_store;
    // Registration may use only this still-retained, durably closed cohort.
    // No public descriptor factory can recover this custody from decoded facts.
    int duplicate_closed_root(ordinary_launch::deadline) const;
    retained_installation_file commit_origin_anchor(const std::vector<std::uint8_t>&, ordinary_launch::deadline);
    int duplicate_accepting_root() const;
    int duplicate_installer_parent() const;
    struct implementation;
    std::shared_ptr<implementation> impl_;
    explicit created_launch_cohort(std::shared_ptr<implementation>);
public:
    class store_namespace {
        friend class created_launch_cohort;
        std::shared_ptr<implementation> owner_;
        std::size_t index_ = 0;
        store_namespace(std::shared_ptr<implementation>, std::size_t);
    public:
        store_namespace(store_namespace&&) noexcept;
        store_namespace& operator=(store_namespace&&) noexcept;
        store_namespace(const store_namespace&) = delete;
        store_namespace& operator=(const store_namespace&) = delete;
        ~store_namespace();
        // Caller owns the duplicate. Only the retained native controller passes
        // it to its actual approved child; this descriptor grants no DB context.
        int duplicate_for_child() const;
        file_identity directory_identity() const;
        // Physical fact observed only after the actual cohort has joined. No
        // legacy history or store-open permission is inferred from this result.
        file_identity inspect_after_join(const std::string& leaf) const;
    };
    // Creates and retains a new data namespace before the first child. It may
    // never acquire a pre-existing directory by name to bless historical files.
    store_namespace create_store_namespace(const std::string& leaf);
    // Creates a new directory exclusively before its first child. Existing
    // names/partial records are refused; no legacy catalog is discovered here.
    // The actual product controller retains this creation and its own complete
    // launcher scope; it cannot retroactively own a pre-existing installation.
    static created_launch_cohort create_before_child(int parent_directory, const std::string& leaf);
    ~created_launch_cohort();
    created_launch_cohort(created_launch_cohort&&) noexcept;
    created_launch_cohort& operator=(created_launch_cohort&&) noexcept;
    created_launch_cohort(const created_launch_cohort&) = delete;
    created_launch_cohort& operator=(const created_launch_cohort&) = delete;
    // Internal controller call after its actual approved-role validation. This
    // primitive does not approve arbitrary executable/invocation strings.
    std::size_t launch(const ordinary_launch::launch_specification&);
    ordinary_launch::process_identity child_identity(std::size_t) const;
    void send(std::size_t, const ordinary_launch::frame&, ordinary_launch::deadline);
    ordinary_launch::frame receive(std::size_t, ordinary_launch::deadline);
    // Records a real natural child exit without signaling it. Empty remains
    // an active observation; pending/lost custody and durability failures throw.
    std::optional<ordinary_launch::terminal_observation> observe_terminal(std::size_t, ordinary_launch::deadline);
    // Persist the sticky role-launch fence without stopping current children.
    // The controller uses this before requesting real historical handoffs.
    void close_launch_admission(ordinary_launch::deadline);
    // Durable, sticky launch closure happens before any child is signaled.
    // Never clears intent to reopen. Failure retains the actual child owners.
    direct_cohort_completion close_and_join(ordinary_launch::deadline);
    static std::size_t unresolved_cohorts();
};
} // namespace lattice::detail::ordinary_installation
