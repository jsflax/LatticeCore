#pragma once
#include "ordinary_installation_manifest.hpp"
#include "ordinary_owned_launch.hpp"
#include <memory>

// Descriptor/name/byte checks used by the installation controller. These are
// physical observations, not bootstrap, launcher authority or store admission.
namespace lattice::detail::ordinary_installation {
enum class file_error_code { unavailable, invalid_path, changed, too_large, timed_out, inherited_use, wrong_thread };
class file_error : public std::runtime_error {
public:
    const file_error_code code;
    explicit file_error(file_error_code value)
        : std::runtime_error("ordinary installation file unavailable"), code(value) {}
};
class retained_installation_file {
    struct implementation;
    std::unique_ptr<implementation> impl_;
    explicit retained_installation_file(std::unique_ptr<implementation>);
public:
    // Opens every absolute path component without following symlinks. Parent
    // directories must be current-user/root owned and not group/other writable.
    // No existing object's identity is learned by a catalog caller: expected
    // identity and digest come from its separately committed installation plan.
    static retained_installation_file open_executable(const executable_fact&, ordinary_launch::deadline);
    // Descriptor-relative variant used for a retained catalog/registration.
    // The parent descriptor remains caller-owned and is duplicated here.
    static retained_installation_file open_record(int parent, const std::string& leaf,
        const file_identity&, const digest&, std::size_t maximum_bytes, ordinary_launch::deadline);
    ~retained_installation_file();
    retained_installation_file(retained_installation_file&&) noexcept;
    retained_installation_file& operator=(retained_installation_file&&) noexcept;
    retained_installation_file(const retained_installation_file&) = delete;
    retained_installation_file& operator=(const retained_installation_file&) = delete;
    // All operations remain on the creating thread. Revalidates the original retained path and bytes; never reopens/relearns
    // a replacement. A failed observation permanently poisons this wrapper.
    void verify(ordinary_launch::deadline) const;
    std::vector<std::uint8_t> read_record(ordinary_launch::deadline) const;
    file_identity identity() const;
};
} // namespace lattice::detail::ordinary_installation
