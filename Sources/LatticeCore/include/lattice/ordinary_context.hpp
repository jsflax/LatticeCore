#pragma once
#include <cstdint>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace lattice::detail {
class ordinary_managed_child_admission;
class ordinary_managed_profile;
class ordinary_export_access;
class ordinary_cohort_attachment_access;
namespace ordinary_installation { struct normal_receiver; struct managed_history_receiver; }
// Opaque, process-bound ownership issued only by the actual managed launch
// receiver. There is no constructor from a path, record, Boolean or role name.
// Ordinary operation remains unadopted history; this grants no recovery or
// schema-replacement authority. A process-retained generation lease outlives
// every wrapper, escaped statement and unproved close until real process exit.
class ordinary_open_context final {
    friend class ordinary_export_access;
    bool is_managed_history_view()const;
    bool matches_managed_open_admission(const std::shared_ptr<const ordinary_managed_child_admission>&,const std::string&)const;
    friend class ordinary_managed_profile;
    friend class ordinary_cohort_attachment_access;
    void require_attachment_creator()const;
    void validate_managed_attachment(const std::shared_ptr<const ordinary_managed_child_admission>&,
        const std::string&,std::uint64_t,std::uint64_t,std::chrono::steady_clock::time_point)const;
    friend struct ordinary_installation::normal_receiver;
    friend struct ordinary_installation::managed_history_receiver;
    friend void ordinary_require_creator(const std::shared_ptr<const ordinary_open_context>&);
    friend void ordinary_before_open(const std::string&, const std::shared_ptr<const ordinary_open_context>&);
    struct implementation;
    std::shared_ptr<implementation> impl_;
    explicit ordinary_open_context(std::shared_ptr<implementation>);
public:
    ~ordinary_open_context();
    ordinary_open_context(const ordinary_open_context&) = delete;
    ordinary_open_context& operator=(const ordinary_open_context&) = delete;
    std::string primary_path() const;
    // Only an actual managed-history family may derive a view for one of its
    // registered physical stores. The returned view is bound to this exact
    // path; it is still only open custody, never source/recovery permission.
    std::shared_ptr<const ordinary_open_context> for_managed_path(const std::string&) const;
    std::vector<std::string> managed_paths() const;
    // Descriptor, anchor, current journal and exact parent-process checks.
    // Called at open/cache admission, never per query or under cache mutexes.
    void validate_open(const std::string& path) const;
    void validate_physical(std::uint64_t device, std::uint64_t inode) const;
};
using ordinary_context = std::shared_ptr<const ordinary_open_context>;
// Before the first SQLite open or a live/in-flight cache return. Unmanaged
// native-open attempts permanently prevent later context installation. In a
// managed process missing/different capabilities refuse before native open.
void ordinary_before_open(const std::string& path, const ordinary_context&);
// The initial primary-only profile has no unmanaged raw/ATTACH escape route.
// These checks are independent of URI spelling or the apparent target path.
bool ordinary_requires_attachment_guard();
// Native creator checks before inherited connection/cache locks. No issuer.
void ordinary_require_creator(const ordinary_context&);
bool ordinary_creator_current(const ordinary_context&) noexcept;
void ordinary_require_no_raw_escape(const ordinary_context&);
void ordinary_require_no_attachment(const ordinary_context&);
} // namespace lattice::detail
