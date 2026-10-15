#include "engram_restart_preparation.hpp"
#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <dirent.h>
#include <fcntl.h>
#include <pwd.h>
#include <sys/stat.h>
#include <unistd.h>
#define LATTICE_ENGRAM_RESTART_POSIX 1
#endif
namespace lattice::detail::ordinary_installation {
namespace {
[[noreturn]] void refuse() { throw std::runtime_error("historical managed restart preparation refused"); }
void bounded(ordinary_launch::deadline end) {
    if (std::chrono::steady_clock::now() >= end) refuse();
}
#ifdef LATTICE_ENGRAM_RESTART_POSIX
struct owned_fd {
    int value = -1;
    explicit owned_fd(int fd) : value(fd) { if (fd < 0) refuse(); }
    ~owned_fd() { if (value >= 0) ::close(value); }
    owned_fd(owned_fd&& other) noexcept : value(std::exchange(other.value, -1)) {}
    owned_fd& operator=(owned_fd&& other) noexcept {
        if (this != &other) { if (value >= 0) ::close(value); value = std::exchange(other.value, -1); }
        return *this;
    }
    owned_fd(const owned_fd&) = delete;
};
file_identity id(const struct stat& s) {
    return {static_cast<std::uint64_t>(s.st_dev), static_cast<std::uint64_t>(s.st_ino)};
}
bool same_file(const struct stat& a, const struct stat& b) {
    return id(a) == id(b) && a.st_mode == b.st_mode && a.st_uid == b.st_uid && a.st_nlink == b.st_nlink;
}
struct stat metadata(int fd, bool directory) {
    struct stat s{};
    if (::fstat(fd, &s) || s.st_uid != ::geteuid() || (s.st_mode & 0022) ||
        (directory ? !S_ISDIR(s.st_mode) : (!S_ISREG(s.st_mode) || s.st_nlink != 1 || s.st_size < 0))) refuse();
    return s;
}
std::vector<std::string> names(int fd, ordinary_launch::deadline end) {
    // A separate open-file description avoids consuming the retained fd's
    // directory offset. The scan never follows an entry or opens SQLite.
    owned_fd scan(::openat(fd, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    DIR* raw = ::fdopendir(scan.value);
    if (!raw) refuse();
    scan.value = -1;
    std::unique_ptr<DIR, decltype(&::closedir)> directory(raw, &::closedir);
    std::vector<std::string> result;
    while (true) {
        bounded(end); errno = 0;
        const auto* entry = ::readdir(directory.get());
        if (!entry) { if (errno) refuse(); break; }
        const std::string name(entry->d_name);
        if (name == "." || name == "..") continue;
        if (name.empty() || name.size() > 255 || result.size() == 4096) refuse();
        result.push_back(name);
    }
    std::sort(result.begin(), result.end());
    if (std::adjacent_find(result.begin(), result.end()) != result.end()) refuse();
    return result;
}
bool safe_leaf(std::string_view value) {
    return !value.empty() && value.size() <= 120 && value != "." && value != ".." &&
        std::all_of(value.begin(), value.end(), [](unsigned char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.';
        });
}
std::string lower(std::string value) {
    for (auto& c : value) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    return value;
}
bool uuid(std::string_view value) {
    if (value.size() != 36) return false;
    for (std::size_t i = 0; i != value.size(); ++i) {
        const char c = value[i];
        if (i == 8 || i == 13 || i == 18 || i == 23) { if (c != '-') return false; }
        else if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) return false;
    }
    return true;
}
bool store_shaped(const std::string& name) {
    const auto value = lower(name);
    return value.find(".sqlite") != std::string::npos || value.find(".lattice") != std::string::npos ||
        value.ends_with(".db") || value.find(".db-") != std::string::npos || value.starts_with("group-");
}
std::string hex(const digest& value) {
    static constexpr char alphabet[] = "0123456789abcdef";
    std::string result;
    for (auto byte : value) { result += alphabet[byte >> 4]; result += alphabet[byte & 15]; }
    return result;
}
void write_all(int fd, std::string_view value, ordinary_launch::deadline end) {
    std::size_t offset = 0;
    while (offset != value.size()) {
        bounded(end);
        const auto count = ::write(fd, value.data() + offset, value.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) refuse();
        offset += static_cast<std::size_t>(count);
    }
    bounded(end);
    if (::fsync(fd)) refuse();
    bounded(end);
}
#endif
}

struct retained_engram_restart_preparation::implementation {
#ifdef LATTICE_ENGRAM_RESTART_POSIX
    struct directory {
        owned_fd fd;
        struct stat original{};
        std::vector<std::string> entries;
        std::string prefix;
        directory(int value, std::string path, ordinary_launch::deadline end)
            : fd(value), original(metadata(value, true)), entries(names(value, end)), prefix(std::move(path)) {}
    };
    struct file {
        owned_fd fd;
        struct stat original{};
        std::size_t directory_index;
        std::string leaf, role;
        std::optional<digest> content;
        file(int descriptor, std::size_t index, std::string name, std::string kind)
            : fd(descriptor), original(metadata(descriptor, false)), directory_index(index),
              leaf(std::move(name)), role(std::move(kind)) {}
    };
    struct policy_node {
        owned_fd fd;
        int parent;
        std::string leaf;
        struct stat original{};
        bool directory;
        policy_node(int value, int parent_fd, std::string name, bool is_directory)
            : fd(value), parent(parent_fd), leaf(std::move(name)), directory(is_directory) {
            if (::fstat(value, &original) || (original.st_uid != ::geteuid() && original.st_uid != 0) ||
                (original.st_mode & 0022) || (directory ? !S_ISDIR(original.st_mode) :
                 (!S_ISREG(original.st_mode) || original.st_uid != ::geteuid() || original.st_nlink != 1))) refuse();
        }
    };
    struct policy_observation {
        std::string label;
        int parent;
        std::string leaf;
        std::optional<std::size_t> node;
        digest hash{};
        std::size_t bytes = 0;
    };
    const ordinary_launch::process_identity owner = ordinary_launch::current_process();
    const std::thread::id thread = std::this_thread::get_id();
    const digest installed_package;
    std::vector<directory> directories;
    std::vector<file> files;
    std::vector<policy_node> policy_nodes;
    std::vector<policy_observation> policies;
    std::set<std::pair<std::uint64_t, std::uint64_t>> physical;
    std::size_t stores = 0;
    bool published = false;
    bool consumed = false;

    static digest read_policy(int fd, std::size_t size, ordinary_launch::deadline end) {
        if (size > 512 * 1024) refuse();
        std::vector<std::uint8_t> bytes(size);
        std::size_t at = 0;
        while (at != bytes.size()) {
            bounded(end);
            const auto n = ::pread(fd, bytes.data() + at, bytes.size() - at, static_cast<off_t>(at));
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) refuse();
            at += static_cast<std::size_t>(n);
        }
        digest result{}; picosha2::hash256(bytes.begin(), bytes.end(), result.begin(), result.end());
        return result;
    }
    void observe_policy(const std::vector<std::string>& components, const std::string& label,
                        ordinary_launch::deadline end) {
        int parent = -1;
        policy_nodes.emplace_back(::open("/", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC), -1, "", true);
        parent = policy_nodes.back().fd.value;
        for (std::size_t i = 0; i != components.size(); ++i) {
            check_owner(end);
            const bool directory = i + 1 != components.size();
            const auto& leaf = components[i];
            if (leaf.empty() || leaf == "." || leaf == ".." || leaf.size() > 255 || leaf.find('/') != std::string::npos) refuse();
            const int fd = ::openat(parent, leaf.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC |
                (directory ? O_DIRECTORY : 0));
            if (fd < 0) {
                if (errno != ENOENT) refuse();
                policies.push_back({label, parent, leaf, std::nullopt, {}, 0});
                return;
            }
            policy_nodes.emplace_back(fd, parent, leaf, directory);
            const auto& node = policy_nodes.back();
            if (!directory) {
                if (node.original.st_size < 0 || node.original.st_size > 512 * 1024) refuse();
                const auto size = static_cast<std::size_t>(node.original.st_size);
                policies.push_back({label, parent, leaf, policy_nodes.size() - 1,
                                    read_policy(node.fd.value, size, end), size});
                return;
            }
            parent = node.fd.value;
        }
        refuse();
    }
    void observe_registered_launch_paths(ordinary_launch::deadline end) {
        // Registration files are observations, not host completion receipts.
        // Obtain the account root independently, never from HOME or data argv.
        std::array<char, 16384> buffer{}; struct passwd account{}; struct passwd* result = nullptr;
        if (::getpwuid_r(::geteuid(), &account, buffer.data(), buffer.size(), &result) || !result ||
            account.pw_uid != ::geteuid() || !account.pw_dir) refuse();
        const std::string home(account.pw_dir);
        if (home.size() > 4096 || home.empty() || home.front() != '/' || home.back() == '/') refuse();
        std::vector<std::string> root;
        for (std::size_t at = 1; at < home.size();) {
            const auto next = home.find('/', at);
            root.push_back(home.substr(at, next == std::string::npos ? next : next - at));
            if (root.size() > 32) refuse();
            if (next == std::string::npos) break;
            at = next + 1;
        }
        auto capture = [&](std::initializer_list<const char*> tail, const char* label) {
            auto path = root; for (const auto* part : tail) path.emplace_back(part);
            observe_policy(path, label, end);
        };
        capture({".claude.json"}, "claude-mcp-registration");
        capture({".claude", "settings.json"}, "claude-hook-registration");
        capture({".codex", "config.toml"}, "codex-host-registration");
        capture({"Library", "LaunchAgents", "io.engram.sync.plist"}, "launchd-current-daemon-registration");
        capture({"Library", "LaunchAgents", "io.engram.sync-daemon.plist"}, "launchd-legacy-daemon-registration");
    }

    void check_owner(ordinary_launch::deadline end) const {
        bounded(end);
        if (ordinary_launch::current_process() != owner || std::this_thread::get_id() != thread) refuse();
    }
    void retain(std::size_t directory_index, const std::string& leaf, const std::string& role,
                ordinary_launch::deadline end) {
        check_owner(end);
        if (files.size() >= 256 || !safe_leaf(leaf)) refuse();
        const auto& dir = directories[directory_index];
        file next(::openat(dir.fd.value, leaf.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC),
                  directory_index, leaf, role);
        struct stat named{};
        if (::fstatat(dir.fd.value, leaf.c_str(), &named, AT_SYMLINK_NOFOLLOW) || !same_file(next.original, named)) refuse();
        const auto identity = id(next.original);
        if (!physical.emplace(identity.device, identity.inode).second) refuse();
        files.push_back(std::move(next));
    }
    void scan(std::size_t index, ordinary_launch::deadline end) {
        const auto entries = directories[index].entries;
        std::set<std::string> accepted, group_ids;
        std::vector<std::pair<std::string, std::string>> mains;
        for (const auto& leaf : entries) {
            if (index == 0 && leaf == "memory.sqlite") mains.emplace_back(leaf, "primary");
            if (index != 0 && leaf == "memory-synced.sqlite") mains.emplace_back(leaf, "personal-spoke");
            if (index != 0 && leaf.starts_with("group-")) {
                std::string_view value(leaf);
                const bool revoked = value.ends_with(".sqlite.revoked");
                if (!revoked && !value.ends_with(".sqlite")) continue;
                const auto suffix = revoked ? std::string_view(".sqlite.revoked") : std::string_view(".sqlite");
                const auto group = value.substr(6, value.size() - 6 - suffix.size());
                if (!uuid(group)) refuse();
                // Active and revoked versions of one logical group may both
                // exist and are separate preservation members, never aliases.
                if (!group_ids.insert(lower(std::string(group)) + (revoked ? ":revoked" : ":active")).second) refuse();
                mains.emplace_back(leaf, revoked ? "revoked-group-preservation" : "active-group-spoke");
            }
        }
        if (index == 0 && !std::binary_search(entries.begin(), entries.end(), "memory.sqlite")) refuse();
        for (const auto& [main, role] : mains) {
            if (++stores > 64) refuse();
            accepted.insert(main); retain(index, main, role, end);
            for (const std::string suffix : {"-wal", "-shm", "-journal"}) {
                const auto companion = main + suffix;
                if (std::binary_search(entries.begin(), entries.end(), companion)) {
                    accepted.insert(companion); retain(index, companion, role + suffix, end);
                }
            }
        }
        for (const auto& leaf : entries)
            if (store_shaped(leaf) && !accepted.contains(leaf)) refuse();
        if (index != 0 && std::binary_search(entries.begin(), entries.end(), "groups.json")) {
            retain(index, "groups.json", "group-directory-membership-facts", end);
            auto& member = files.back();
            if (member.original.st_size > 512 * 1024) refuse();
            member.content = read_policy(member.fd.value, static_cast<std::size_t>(member.original.st_size), end);
        }
    }
    explicit implementation(int data, const digest& package, ordinary_launch::deadline end)
        : installed_package(package) {
        check_owner(end);
        directories.emplace_back(::fcntl(data, F_DUPFD_CLOEXEC, 3), "", end);
        scan(0, end);
        const auto& top = directories.front();
        if (std::binary_search(top.entries.begin(), top.entries.end(), "sync")) {
            directories.emplace_back(::openat(top.fd.value, "sync", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC), "sync/", end);
            scan(1, end);
        }
        observe_registered_launch_paths(end);
        verify(end);
    }
    void verify(ordinary_launch::deadline end) const {
        check_owner(end);
        for (const auto& dir : directories)
            if (!same_file(dir.original, metadata(dir.fd.value, true)) || names(dir.fd.value, end) != dir.entries) refuse();
        if (directories.size() == 2) {
            struct stat named{};
            if (::fstatat(directories[0].fd.value, "sync", &named, AT_SYMLINK_NOFOLLOW) ||
                !same_file(directories[1].original, named)) refuse();
        }
        for (const auto& value : files) {
            struct stat named{};
            if (!same_file(value.original, metadata(value.fd.value, false)) ||
                ::fstatat(directories[value.directory_index].fd.value, value.leaf.c_str(), &named, AT_SYMLINK_NOFOLLOW) ||
                !same_file(value.original, named)) refuse();
            if (value.content && (named.st_size != value.original.st_size ||
                read_policy(value.fd.value, static_cast<std::size_t>(value.original.st_size), end) != *value.content)) refuse();
        }
        for (const auto& node : policy_nodes) {
            struct stat held{}, named{};
            if (::fstat(node.fd.value, &held) || !same_file(node.original, held) ||
                (node.parent >= 0 && (::fstatat(node.parent, node.leaf.c_str(), &named, AT_SYMLINK_NOFOLLOW) ||
                                     !same_file(node.original, named)))) refuse();
        }
        for (const auto& policy : policies) {
            struct stat named{};
            if (!policy.node) {
                if (::fstatat(policy.parent, policy.leaf.c_str(), &named, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT) refuse();
            } else {
                const auto& node = policy_nodes[*policy.node];
                if (::fstat(node.fd.value, &named) || named.st_size < 0 ||
                    static_cast<std::uint64_t>(named.st_size) != policy.bytes ||
                    read_policy(node.fd.value, policy.bytes, end) != policy.hash) refuse();
            }
        }
        // Size/time/content may still change under actual historical writers.
        // This membership census is deliberately not a consistent data snapshot.
    }
    std::string consume_for_retirement(ordinary_launch::deadline end) {
        verify(end);
        if (consumed) refuse();
        consumed = true;
        // These are required actual lifecycle adapters, not supplied ready bits.
        // In particular launchctl completion cannot discharge daemon debt, and
        // neither a PID scan nor changed MCP config joins an old hosted child.
        return "[\"external-mcp-host-retirement-and-relaunch-fence\","
               "\"hook-and-detached-learner-custody\",\"launchd-daemon-generation-retirement\","
               "\"gui-and-updater-external-owner\",\"unknown-manual-or-copied-launcher-coverage\","
               "\"orbital-overlapping-embedded-engram-owner\",\"wss-ipc-source-intake-cutover\"]";
    }
    std::string record(const std::string& debt) const {
        std::ostringstream out;
        out << "{\n\"format\":\"engram-managed-restart-preparation-v1\",\n"
               "\"verifiedInstalledPackageSHA256\":\"" << hex(installed_package) << "\",\n"
               "\"phase\":\"historical-unadopted-prepared\",\n"
               "\"nextPhase\":\"actual-external-lifecycle-retirement-required\",\n"
               "\"isAdmissionJournal\":false,\"decodedRecordGrantsCustody\":false,\n"
               "\"oldLaunchesFenced\":false,\"oldProcessesRetired\":false,\"sourceIntakeClosed\":false,\n"
               "\"dataSnapshotComplete\":false,\"adoptionAuthorized\":false,\n"
               "\"pendingLifecycleObligations\":" << debt << ",\n"
               "\"controller\":{\"pid\":" << owner.pid << ",\"parent\":" << owner.parent
            << ",\"birthMajor\":" << owner.birth_major << ",\"birthMinor\":" << owner.birth_minor << "},\n"
               "\"dataDirectory\":{\"device\":" << id(directories[0].original).device
            << ",\"inode\":" << id(directories[0].original).inode << "},\n"
               "\"maximumStores\":64,\"maximumRetainedFiles\":256,\"storeCount\":" << stores << ",\n\"files\":[\n";
        for (std::size_t i = 0; i != files.size(); ++i) {
            const auto& f = files[i];
            if (i) out << ",\n";
            out << "{\"path\":\"" << directories[f.directory_index].prefix << f.leaf
                << "\",\"role\":\"" << f.role << "\",\"device\":" << id(f.original).device
                << ",\"inode\":" << id(f.original).inode << ",\"sizeAtCapture\":" << f.original.st_size;
            if (f.content) out << ",\"sha256\":\"" << hex(*f.content) << "\"";
            out << "}";
        }
        out << "\n],\n\"launcherRegistrationObservations\":[\n";
        for (std::size_t i = 0; i != policies.size(); ++i) {
            const auto& policy = policies[i];
            if (i) out << ",\n";
            out << "{\"role\":\"" << policy.label << "\",\"present\":" << (policy.node ? "true" : "false");
            if (policy.node) {
                const auto& node = policy_nodes[*policy.node];
                out << ",\"device\":" << id(node.original).device << ",\"inode\":" << id(node.original).inode
                    << ",\"bytes\":" << policy.bytes << ",\"sha256\":\"" << hex(policy.hash) << "\"";
            }
            out << ",\"provesHostRetirement\":false}";
        }
        out << "\n],\"pluginCacheAndExternalHostsFullyEnumerated\":false}\n";
        auto bytes = out.str(); if (bytes.size() > 128 * 1024) refuse();
        return bytes;
    }
#else
    explicit implementation(int, const digest&, ordinary_launch::deadline) { refuse(); }
#endif
};
retained_engram_restart_preparation::retained_engram_restart_preparation(int data, const digest& package,
                                                                       ordinary_launch::deadline end)
    : impl_(std::make_unique<implementation>(data, package, end)) {}
retained_engram_restart_preparation::~retained_engram_restart_preparation() = default;
void retained_engram_restart_preparation::publish_retirement_plan(int parent, const std::string& leaf,
                                                                  ordinary_launch::deadline end) {
#ifdef LATTICE_ENGRAM_RESTART_POSIX
    impl_->check_owner(end);
    if (impl_->published || !safe_leaf(leaf)) refuse();
    const auto parent_metadata = metadata(parent, true);
    // Output must be separate from every retained source directory. No file in
    // the historical store namespace is used as a new control/authority record.
    for (const auto& dir : impl_->directories) if (id(parent_metadata) == id(dir.original)) refuse();
    for (const auto& node : impl_->policy_nodes)
        if (node.directory && id(parent_metadata) == id(node.original)) refuse();
    {
        owned_fd ancestor(::fcntl(parent, F_DUPFD_CLOEXEC, 3));
        bool reached_root = false;
        for (std::size_t count = 0; count != 256; ++count) {
            bounded(end); struct stat current{}, above{};
            if (::fstat(ancestor.value, &current) || !S_ISDIR(current.st_mode)) refuse();
            for (const auto& dir : impl_->directories) if (id(current) == id(dir.original)) refuse();
            owned_fd next(::openat(ancestor.value, "..", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
            if (::fstat(next.value, &above) || !S_ISDIR(above.st_mode)) refuse();
            if (id(current) == id(above)) { reached_root = true; break; }
            ancestor = std::move(next);
        }
        if (!reached_root) refuse();
    }
    const auto debt = impl_->consume_for_retirement(end);
    const auto bytes = impl_->record(debt);
    if (::mkdirat(parent, leaf.c_str(), 0700)) refuse(); // Exclusive; no repair/reuse.
    owned_fd output(::openat(parent, leaf.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    const auto created = metadata(output.value, true);
    if ((created.st_mode & 0777) != 0700) refuse();
    owned_fd record(::openat(output.value, "preparation.json", O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600));
    write_all(record.value, bytes, end);
    digest checksum{}; picosha2::hash256(bytes.begin(), bytes.end(), checksum.begin(), checksum.end());
    owned_fd hash(::openat(output.value, "preparation.sha256", O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600));
    write_all(hash.value, hex(checksum) + "\n", end);
    impl_->verify(end);
    struct stat named{};
    const auto parent_after = metadata(parent, true);
    if (::fstatat(parent, leaf.c_str(), &named, AT_SYMLINK_NOFOLLOW) || !same_file(created, named) ||
        id(parent_metadata) != id(parent_after) || parent_metadata.st_mode != parent_after.st_mode ||
        parent_metadata.st_uid != parent_after.st_uid) refuse();
    // mkdir changed the output parent's link count on filesystems such as
    // ext4. Its identity/type/ownership/mode remain exact; the historical
    // source directories and their entry sets above are still unchanged.
    if (::fsync(output.value) || ::fsync(parent)) refuse();
    bounded(end);
    impl_->published = true;
    // Returning only completes preparation. No receipt is converted into an
    // adopted journal, resumed role, ordinary context or finished cutover.
#else
    (void)parent; (void)leaf; (void)end; refuse();
#endif
}
}
