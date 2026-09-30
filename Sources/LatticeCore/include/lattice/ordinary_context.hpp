#pragma once
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

namespace lattice::detail {
namespace ordinary_installation { struct normal_receiver; }
// Opaque, process-bound ownership issued only by the actual managed launch
// receiver. There is no constructor from a path, record, Boolean or role name.
// Ordinary operation remains unadopted history; this grants no recovery or
// schema-replacement authority. A process-retained generation lease outlives
// every wrapper, escaped statement and unproved close until real process exit.
class ordinary_open_context final {
    friend struct ordinary_installation::normal_receiver;
    struct implementation;
    std::shared_ptr<implementation> impl_;
    explicit ordinary_open_context(std::shared_ptr<implementation>);
public:
    ~ordinary_open_context();
    ordinary_open_context(const ordinary_open_context&) = delete;
    ordinary_open_context& operator=(const ordinary_open_context&) = delete;
    std::string primary_path() const;
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
void ordinary_require_no_raw_escape(const ordinary_context&);
void ordinary_require_no_attachment(const ordinary_context&);
} // namespace lattice::detail
