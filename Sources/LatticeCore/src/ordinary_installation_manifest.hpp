#pragma once

#include "ordinary_store_admission.hpp"
#include <array>
#include <cstdint>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

// Private installation catalog facts. Parsing, matching a checksum or opening
// these records does not authorize launch, bootstrap or any database operation.
// The actual retained installation authority must bind the external registration,
// catalog descriptors, durable launch gate and owned child exchange separately.
namespace lattice::detail::ordinary_installation {
using identifier = ordinary_admission::identifier;
using file_identity = ordinary_admission::file_identity;
using digest = std::array<std::uint8_t, 32>;
inline constexpr std::size_t maximum_manifest_bytes = 120 * 1024;
inline constexpr std::size_t maximum_stores = 64;
inline constexpr std::size_t maximum_roles = 64;
inline constexpr std::size_t maximum_aliases_per_store = 16;

enum class product : std::uint8_t { engram = 1, orbital = 2 };
enum class lifetime : std::uint8_t { caller_bound = 1, installation_bound = 2 };
enum class manifest_error_code { invalid_manifest, unsupported_version, unavailable };
class manifest_error : public std::runtime_error {
public:
    const manifest_error_code code;
    explicit manifest_error(manifest_error_code value)
        : std::runtime_error("ordinary installation manifest unavailable"), code(value) {}
};
struct alias_fact {
    std::string parent_path, leaf;
    file_identity parent;
    bool operator==(const alias_fact&) const = default;
};
struct store_fact {
    ordinary_admission::store_binding binding;
    ordinary_admission::control_binding controls;
    std::string control_leaf;
    std::vector<alias_fact> aliases;
    bool operator==(const store_fact&) const = default;
};
struct executable_fact {
    std::string path;
    file_identity identity;
    digest content{};
    bool operator==(const executable_fact&) const = default;
};
struct role_fact {
    std::string name;
    executable_fact executable;
    std::string working_directory;
    std::vector<std::string> arguments, environment;
    std::vector<identifier> stores;
    lifetime custody = lifetime::caller_bound;
    bool operator==(const role_fact&) const = default;
};
struct manifest {
    product application = product::engram;
    identifier installation{};
    std::uint64_t revision = 0;
    file_identity catalog_directory, launch_gate;
    executable_fact supervisor;
    std::vector<store_fact> stores;
    std::vector<role_fact> roles;
    bool operator==(const manifest&) const = default;
};
// Canonical bounded bytes, including corruption checksum. The checksum is not
// authentication. No bool, state bit or decoded class represents retired owners.
std::vector<std::uint8_t> encode_manifest(const manifest&);
manifest decode_manifest(const std::vector<std::uint8_t>&);
} // namespace lattice::detail::ordinary_installation
