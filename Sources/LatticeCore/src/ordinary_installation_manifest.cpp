#include "ordinary_installation_manifest.hpp"
#include "vendor/picosha2/picosha2.h"
#include <algorithm>
#include <set>
#include <utility>

namespace lattice::detail::ordinary_installation {
namespace {
using bytes = std::vector<std::uint8_t>;
[[noreturn]] void invalid() { throw manifest_error(manifest_error_code::invalid_manifest); }
template <typename T> bool zero(const T& value) {
    return std::all_of(value.begin(), value.end(), [](auto byte) { return byte == 0; });
}
bool leaf(const std::string& value, std::size_t bound = 255) {
    return !value.empty() && value.size() <= bound && value != "." && value != ".." &&
        value.find('/') == std::string::npos && value.find('\0') == std::string::npos;
}
bool absolute_path(const std::string& value) {
    if (value.empty() || value.size() > 4096 || value.front() != '/' || value.find('\0') != std::string::npos)
        return false;
    if (value == "/") return true;
    std::size_t at = 1;
    while (at < value.size()) {
        const auto end = value.find('/', at);
        const auto component = value.substr(at, end == std::string::npos ? value.size() - at : end - at);
        if (!leaf(component)) return false;
        if (end == std::string::npos) return true;
        at = end + 1;
    }
    return false;
}
bool valid_identity(const file_identity& value) { return value.inode != 0; }
void executable(const executable_fact& value) {
    if (!absolute_path(value.path) || value.path == "/" || !valid_identity(value.identity) || zero(value.content)) invalid();
}
void validate(const manifest& value) {
    if ((value.application != product::engram && value.application != product::orbital) ||
        zero(value.installation) || !value.revision || !valid_identity(value.catalog_directory) ||
        !valid_identity(value.launch_gate) || value.stores.empty() || value.stores.size() > maximum_stores ||
        value.roles.empty() || value.roles.size() > maximum_roles) invalid();
    executable(value.supervisor);
    std::set<identifier> store_ids;
    std::set<std::pair<std::uint64_t, std::uint64_t>> physical_stores, control_objects;
    std::set<std::string> control_names;
    std::set<std::pair<std::string, std::string>> aliases;
    for (const auto& store : value.stores) {
        const auto& binding = store.binding;
        const auto& controls = store.controls;
        if (binding.installation != value.installation || zero(binding.store) || zero(binding.epoch) ||
            !valid_identity(binding.main) || !valid_identity(binding.parent) ||
            !valid_identity(controls.control) || !valid_identity(controls.entry) || !valid_identity(controls.generation) ||
            controls.entry == controls.generation || !leaf(store.control_leaf, 128) ||
            store.aliases.empty() || store.aliases.size() > maximum_aliases_per_store ||
            !store_ids.insert(binding.store).second ||
            !physical_stores.emplace(binding.main.device, binding.main.inode).second ||
            !control_names.insert(store.control_leaf).second) invalid();
        for (const auto identity : {controls.control, controls.entry, controls.generation})
            if (!control_objects.emplace(identity.device, identity.inode).second) invalid();
        bool original_parent = false;
        for (const auto& alias : store.aliases) {
            if (!absolute_path(alias.parent_path) || !leaf(alias.leaf) || !valid_identity(alias.parent) ||
                !aliases.emplace(alias.parent_path, alias.leaf).second) invalid();
            original_parent |= alias.parent == binding.parent;
        }
        if (!original_parent) invalid();
    }
    std::set<std::string> roles;
    for (const auto& role : value.roles) {
        if (!leaf(role.name, 128) || !roles.insert(role.name).second || !absolute_path(role.working_directory) ||
            role.arguments.size() > 128 || role.environment.size() > 128 || role.stores.empty() ||
            role.stores.size() > maximum_stores ||
            (role.custody != lifetime::caller_bound && role.custody != lifetime::installation_bound)) invalid();
        executable(role.executable);
        std::size_t total = role.executable.path.size() + role.working_directory.size();
        for (const auto& argument : role.arguments) {
            if (argument.size() > 8192 || argument.find('\0') != std::string::npos) invalid();
            total += argument.size();
        }
        std::set<std::string> environment_names;
        for (const auto& environment : role.environment) {
            const auto equals = environment.find('=');
            if (environment.size() > 8192 || environment.find('\0') != std::string::npos || equals == 0 ||
                equals == std::string::npos || !environment_names.insert(environment.substr(0, equals)).second) invalid();
            total += environment.size();
        }
        if (total > 256 * 1024) invalid();
        std::set<identifier> role_stores;
        for (const auto& id : role.stores)
            if (!store_ids.contains(id) || !role_stores.insert(id).second) invalid();
    }
}
struct writer {
    bytes data;
    void raw(const std::uint8_t* first, std::size_t size) {
        if (size > maximum_manifest_bytes - 32 || data.size() > maximum_manifest_bytes - 32 - size) invalid();
        data.insert(data.end(), first, first + size);
    }
    void u64(std::uint64_t value) {
        std::array<std::uint8_t, 8> encoded{};
        for (unsigned shift = 0; shift < 64; shift += 8) encoded[shift / 8] = static_cast<std::uint8_t>(value >> shift);
        raw(encoded.data(), encoded.size());
    }
    void text(const std::string& value) { u64(value.size()); raw(reinterpret_cast<const std::uint8_t*>(value.data()), value.size()); }
    template <std::size_t N> void fixed(const std::array<std::uint8_t, N>& value) { raw(value.data(), N); }
    void identity(const file_identity& value) { u64(value.device); u64(value.inode); }
    void binary(const executable_fact& value) { text(value.path); identity(value.identity); fixed(value.content); }
    void strings(const std::vector<std::string>& values) { u64(values.size()); for (const auto& value : values) text(value); }
};
struct reader {
    const bytes& data;
    std::size_t at = 0;
    void raw(std::uint8_t* destination, std::size_t size) {
        if (at > data.size() - 32 || size > data.size() - 32 - at) invalid();
        std::copy_n(data.data() + at, size, destination); at += size;
    }
    std::uint64_t u64() {
        std::array<std::uint8_t, 8> encoded{}; raw(encoded.data(), encoded.size());
        std::uint64_t value = 0;
        for (unsigned shift = 0; shift < 64; shift += 8) value |= std::uint64_t(encoded[shift / 8]) << shift;
        return value;
    }
    std::size_t count(std::size_t maximum) { const auto value = u64(); if (value > maximum) invalid(); return static_cast<std::size_t>(value); }
    std::string text(std::size_t maximum) {
        const auto size = count(maximum);
        if (at > data.size() - 32 || size > data.size() - 32 - at) invalid();
        std::string value(reinterpret_cast<const char*>(data.data() + at), size); at += size; return value;
    }
    template <std::size_t N> std::array<std::uint8_t, N> fixed() { std::array<std::uint8_t, N> value{}; raw(value.data(), N); return value; }
    file_identity identity() { const auto device = u64(); return {device, u64()}; }
    executable_fact binary() { auto path = text(4096); const auto id = identity(); return {std::move(path), id, fixed<32>()}; }
    std::vector<std::string> strings() {
        const auto size = count(128); std::vector<std::string> values; values.reserve(size);
        for (std::size_t i = 0; i != size; ++i) values.push_back(text(8192));
        return values;
    }
};
constexpr std::array<std::uint8_t, 8> magic{'L','A','T','I','N','S','1',0};
}
std::vector<std::uint8_t> encode_manifest(const manifest& value) {
    validate(value);
    writer out; out.fixed(magic); out.u64(1); out.u64(static_cast<std::uint8_t>(value.application));
    out.fixed(value.installation); out.u64(value.revision); out.identity(value.catalog_directory); out.identity(value.launch_gate);
    out.binary(value.supervisor); out.u64(value.stores.size());
    for (const auto& store : value.stores) {
        out.fixed(store.binding.store); out.fixed(store.binding.epoch); out.identity(store.binding.main); out.identity(store.binding.parent);
        out.identity(store.controls.control); out.identity(store.controls.entry); out.identity(store.controls.generation);
        out.text(store.control_leaf); out.u64(store.aliases.size());
        for (const auto& alias : store.aliases) { out.text(alias.parent_path); out.text(alias.leaf); out.identity(alias.parent); }
    }
    out.u64(value.roles.size());
    for (const auto& role : value.roles) {
        out.text(role.name); out.binary(role.executable); out.text(role.working_directory); out.strings(role.arguments); out.strings(role.environment);
        out.u64(static_cast<std::uint8_t>(role.custody)); out.u64(role.stores.size());
        for (const auto& store : role.stores) out.fixed(store);
    }
    digest checksum{}; picosha2::hash256(out.data.begin(), out.data.end(), checksum.begin(), checksum.end());
    out.data.insert(out.data.end(), checksum.begin(), checksum.end()); return out.data;
}
manifest decode_manifest(const std::vector<std::uint8_t>& bytes) {
    if (bytes.size() < 64 || bytes.size() > maximum_manifest_bytes) invalid();
    reader in{bytes};
    if (in.fixed<8>() != magic || in.u64() != 1) throw manifest_error(manifest_error_code::unsupported_version);
    manifest value;
    const auto application = in.u64();
    if (application != 1 && application != 2) invalid();
    value.application = static_cast<product>(application); value.installation = in.fixed<16>(); value.revision = in.u64();
    value.catalog_directory = in.identity(); value.launch_gate = in.identity(); value.supervisor = in.binary();
    const auto stores = in.count(maximum_stores); value.stores.reserve(stores);
    for (std::size_t i = 0; i != stores; ++i) {
        store_fact store; store.binding.installation = value.installation; store.binding.store = in.fixed<16>(); store.binding.epoch = in.fixed<16>();
        store.binding.main = in.identity(); store.binding.parent = in.identity(); store.controls.control = in.identity();
        store.controls.entry = in.identity(); store.controls.generation = in.identity(); store.control_leaf = in.text(128);
        const auto aliases = in.count(maximum_aliases_per_store); store.aliases.reserve(aliases);
        for (std::size_t j = 0; j != aliases; ++j) {
            auto parent = in.text(4096); auto leaf = in.text(255); const auto identity = in.identity();
            store.aliases.push_back({std::move(parent), std::move(leaf), identity});
        }
        value.stores.push_back(std::move(store));
    }
    const auto roles = in.count(maximum_roles); value.roles.reserve(roles);
    for (std::size_t i = 0; i != roles; ++i) {
        role_fact role; role.name = in.text(128); role.executable = in.binary(); role.working_directory = in.text(4096);
        role.arguments = in.strings(); role.environment = in.strings(); const auto custody = in.u64();
        if (custody != 1 && custody != 2) invalid();
        role.custody = static_cast<lifetime>(custody); const auto stores = in.count(maximum_stores); role.stores.reserve(stores);
        for (std::size_t j = 0; j != stores; ++j) role.stores.push_back(in.fixed<16>());
        value.roles.push_back(std::move(role));
    }
    if (in.at != bytes.size() - 32 || encode_manifest(value) != bytes) invalid();
    return value;
}
} // namespace lattice::detail::ordinary_installation
