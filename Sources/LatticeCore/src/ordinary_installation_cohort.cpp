#include "ordinary_installation_cohort.hpp"
#include "vendor/picosha2/picosha2.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <exception>
#include <limits>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#define LATTICE_INSTALLATION_COHORT_POSIX 1
#endif

namespace lattice::detail::ordinary_installation {
namespace {
[[noreturn]] void cohort_fail(cohort_error_code code) { throw cohort_error(code); }
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
const auto cohort_initial_pid = ::getpid();
struct cohort_fd {
    int value = -1;
    explicit cohort_fd(int fd = -1) : value(fd) {}
    ~cohort_fd() { if (value >= 0) ::close(value); }
    cohort_fd(cohort_fd&& other) noexcept : value(std::exchange(other.value, -1)) {}
    cohort_fd& operator=(cohort_fd&& other) noexcept {
        if (this != &other) { if (value >= 0) ::close(value); value = std::exchange(other.value, -1); }
        return *this;
    }
    cohort_fd(const cohort_fd&) = delete;
};
file_identity cohort_identity(const struct stat& value) {
    if constexpr (sizeof(value.st_dev) > sizeof(std::uint64_t) || sizeof(value.st_ino) > sizeof(std::uint64_t))
        cohort_fail(cohort_error_code::unavailable);
    return {static_cast<std::uint64_t>(value.st_dev), static_cast<std::uint64_t>(value.st_ino)};
}
struct stat cohort_inspect(int fd, bool directory, bool parent = false) {
    struct stat value{};
    if (::fstat(fd, &value) || value.st_uid != ::geteuid() ||
        (directory ? (!S_ISDIR(value.st_mode) || (parent ? (value.st_mode & 0022) != 0 : (value.st_mode & 07777) != 0700)) :
         (!S_ISREG(value.st_mode) || (value.st_mode & 07777) != 0600 || value.st_nlink != 1)))
        cohort_fail(cohort_error_code::unavailable);
    return value;
}
void cohort_bound(const std::optional<ordinary_launch::deadline>& end) {
    if (end && std::chrono::steady_clock::now() >= *end) cohort_fail(cohort_error_code::timed_out);
}
void cohort_sync(int fd, const std::optional<ordinary_launch::deadline>& end = std::nullopt) {
    cohort_bound(end);
    if (::fsync(fd)) cohort_fail(cohort_error_code::durability_unproved);
}
void append64(std::vector<std::uint8_t>& bytes, std::uint64_t value) {
    for (unsigned shift = 0; shift != 64; shift += 8) bytes.push_back(static_cast<std::uint8_t>(value >> shift));
}
void append_identity(std::vector<std::uint8_t>& bytes, const file_identity& value) {
    append64(bytes, value.device); append64(bytes, value.inode);
}
void append_process(std::vector<std::uint8_t>& bytes, const ordinary_launch::process_identity& value) {
    append64(bytes, static_cast<std::uint64_t>(value.pid)); append64(bytes, static_cast<std::uint64_t>(value.parent));
    append64(bytes, value.birth_major); append64(bytes, value.birth_minor);
}
#endif
}
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
struct created_launch_cohort::implementation {
    const pid_t creator = ::getpid();
    const std::thread::id thread = std::this_thread::get_id();
    cohort_fd parent, directory, gate;
    std::string leaf;
    file_identity parent_identity, directory_identity, gate_identity, snapshot_identity;
    ordinary_launch::process_identity owner_identity;
    std::uint64_t revision = 0;
    enum class phase : std::uint64_t { accepting = 1, closing = 2, direct_terminal = 3 };
    phase state = phase::accepting;
    bool tainted = false;
    struct entry {
        std::optional<ordinary_launch::owned_child> child;
        ordinary_launch::process_identity identity;
        bool terminal = false;
        int status = 0;
    };
    std::vector<entry> entries;
    struct owned_namespace { cohort_fd directory; std::string leaf; file_identity identity; };
    std::vector<owned_namespace> namespaces;
    std::vector<std::uint8_t> accepted_bytes;
    struct retained_registry {
        std::mutex mutex;
        std::array<std::shared_ptr<implementation>, 64> cohorts;
    };
    static retained_registry& registry() {
        if (::getpid() != cohort_initial_pid) cohort_fail(cohort_error_code::inherited_use);
        static auto* values = new retained_registry; // Intentionally process-retained failure custody.
        return *values;
    }
    static void retain(const std::shared_ptr<implementation>& value) {
        auto& values = registry(); std::lock_guard guard(values.mutex);
        for (auto& slot : values.cohorts) if (!slot) { slot = value; return; }
        cohort_fail(cohort_error_code::limit);
    }
    void release_retained() {
        auto& values = registry(); std::lock_guard guard(values.mutex);
        for (auto& slot : values.cohorts) if (slot.get() == this) { slot.reset(); return; }
    }
    void owner() const {
        if (::getpid() != creator) cohort_fail(cohort_error_code::inherited_use);
        if (std::this_thread::get_id() != thread) cohort_fail(cohort_error_code::wrong_thread);
    }
    void named(int root, const char* name, int fd, const file_identity& expected, bool is_directory) const {
        struct stat value{};
        if (cohort_identity(cohort_inspect(fd, is_directory)) != expected ||
            ::fstatat(root, name, &value, AT_SYMLINK_NOFOLLOW) ||
            (is_directory ? !S_ISDIR(value.st_mode) : !S_ISREG(value.st_mode)) || cohort_identity(value) != expected)
            cohort_fail(cohort_error_code::changed);
    }
    void check_objects() const {
        owner();
        if (cohort_identity(cohort_inspect(parent.value, true, true)) != parent_identity)
            cohort_fail(cohort_error_code::changed);
        named(parent.value, leaf.c_str(), directory.value, directory_identity, true);
        named(directory.value, "launch.lock", gate.value, gate_identity, false);
        for (const auto& space : namespaces)
            named(directory.value, space.leaf.c_str(), space.directory.value, space.identity, true);
    }
    std::vector<std::uint8_t> bytes(std::uint64_t next_revision) const {
        std::vector<std::uint8_t> result{'L','A','T','C','O','H','1',0};
        append64(result, 1); append64(result, next_revision); append64(result, static_cast<std::uint64_t>(state));
        append_identity(result, parent_identity); append_identity(result, directory_identity); append_identity(result, gate_identity);
        append_process(result, owner_identity);
        append64(result, namespaces.size());
        for (const auto& space : namespaces) {
            append_identity(result, space.identity); append64(result, space.leaf.size());
            result.insert(result.end(), space.leaf.begin(), space.leaf.end());
        }
        append64(result, entries.size());
        for (const auto& item : entries) {
            append64(result, item.child ? (item.terminal ? 2 : 1) : 0);
            append_process(result, item.identity);
            append64(result, item.terminal ? static_cast<std::uint64_t>(static_cast<std::uint32_t>(item.status)) : 0);
        }
        digest checksum{}; picosha2::hash256(result.begin(), result.end(), checksum.begin(), checksum.end());
        result.insert(result.end(), checksum.begin(), checksum.end()); return result;
    }
    void verify_snapshot(const std::optional<ordinary_launch::deadline>& end = std::nullopt) const {
        cohort_bound(end); check_objects();
        struct stat pending{};
        if (::fstatat(directory.value, "launch.pending", &pending, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT)
            cohort_fail(cohort_error_code::durability_unproved);
        if (accepted_bytes.empty()) {
            struct stat existing{};
            if (::fstatat(directory.value, "launch.v1", &existing, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT)
                cohort_fail(cohort_error_code::changed);
            return;
        }
        cohort_fd source(::openat(directory.value, "launch.v1", O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
        if (source.value < 0 || cohort_inspect(source.value, false).st_size != static_cast<off_t>(accepted_bytes.size()))
            cohort_fail(cohort_error_code::changed);
        named(directory.value, "launch.v1", source.value, snapshot_identity, false);
        std::vector<std::uint8_t> actual(accepted_bytes.size());
        std::size_t at = 0;
        while (at != actual.size()) {
            cohort_bound(end);
            const auto got = ::read(source.value, actual.data() + at, actual.size() - at);
            if (got < 0 && errno == EINTR) continue;
            if (got <= 0) cohort_fail(cohort_error_code::changed);
            at += static_cast<std::size_t>(got);
        }
        char extra;
        if (::read(source.value, &extra, 1) != 0 || actual != accepted_bytes) cohort_fail(cohort_error_code::changed);
        named(directory.value, "launch.v1", source.value, snapshot_identity, false);
    }
    void save(const std::optional<ordinary_launch::deadline>& end = std::nullopt) {
        try {
            cohort_bound(end); verify_snapshot(end);
            if (revision == std::numeric_limits<std::uint64_t>::max()) cohort_fail(cohort_error_code::limit);
            const auto next = bytes(revision + 1);
            cohort_fd pending(::openat(directory.value, "launch.pending", O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600));
            if (pending.value < 0) cohort_fail(cohort_error_code::durability_unproved);
            const auto identity = cohort_identity(cohort_inspect(pending.value, false));
            std::size_t at = 0;
            while (at != next.size()) {
                cohort_bound(end);
                const auto wrote = ::write(pending.value, next.data() + at, next.size() - at);
                if (wrote < 0 && errno == EINTR) continue;
                if (wrote <= 0) cohort_fail(cohort_error_code::durability_unproved);
                at += static_cast<std::size_t>(wrote);
            }
            cohort_sync(pending.value, end);
            named(directory.value, "launch.pending", pending.value, identity, false);
            if (::renameat(directory.value, "launch.pending", directory.value, "launch.v1"))
                cohort_fail(cohort_error_code::durability_unproved);
            named(directory.value, "launch.v1", pending.value, identity, false);
            cohort_sync(directory.value, end);
            named(directory.value, "launch.v1", pending.value, identity, false);
            // Install accepted in-memory facts only after durable publication.
            accepted_bytes = next; snapshot_identity = identity; ++revision;
        } catch (...) { tainted = true; throw; }
    }
    direct_cohort_completion completion() const {
        direct_cohort_completion value{directory_identity, gate_identity, revision, {}};
        value.children.reserve(entries.size());
        for (const auto& item : entries) {
            if (!item.child || !item.terminal) cohort_fail(cohort_error_code::incomplete);
            value.children.push_back({item.identity, item.status});
        }
        return value;
    }
    void close_admission(ordinary_launch::deadline end) {
        owner();
        if (state == phase::accepting) {
            state = phase::closing;
            if (tainted) cohort_fail(cohort_error_code::incomplete);
            save(end);
        } else {
            if (tainted) cohort_fail(cohort_error_code::incomplete);
            verify_snapshot(end);
        }
    }
    direct_cohort_completion close(ordinary_launch::deadline end) {
        owner();
        if (state == phase::direct_terminal && !tainted) { verify_snapshot(end); return completion(); }
        std::exception_ptr primary;
        try { close_admission(end); } catch (...) { primary = std::current_exception(); }
        // A failed durable close never permits future launch. Still retire the
        // known actual children; failure to publish must not abandon cleanup.
        for (auto& item : entries) {
            if (!item.child || item.terminal) continue;
            try {
                const auto terminal = item.child->stop_and_join(end);
                item.identity = terminal.child; item.status = terminal.wait_status; item.terminal = true;
            } catch (...) { if (!primary) primary = std::current_exception(); }
        }
        if (primary) std::rethrow_exception(primary);
        if (tainted) cohort_fail(cohort_error_code::incomplete);
        auto result = completion(); // A pending/unreturned spawn always refuses.
        state = phase::direct_terminal; save(end); result.revision = revision;
        release_retained(); return result;
    }
    void abandon() noexcept {
        if (::getpid() != creator || std::this_thread::get_id() != thread) return;
        try { (void)close(std::chrono::steady_clock::now() + std::chrono::seconds(5)); }
        catch (...) { /* Retained registry preserves actual child owners and negative state. */ }
    }
};

created_launch_cohort created_launch_cohort::create_before_child(int parent_directory, const std::string& leaf) {
    if (leaf.empty() || leaf.size() > 128 || leaf == "." || leaf == ".." ||
        leaf.find('/') != std::string::npos || leaf.find('\0') != std::string::npos)
        cohort_fail(cohort_error_code::unavailable);
    auto state = std::make_shared<implementation>();
    state->parent = cohort_fd(::fcntl(parent_directory, F_DUPFD_CLOEXEC, 0));
    if (state->parent.value < 0) cohort_fail(cohort_error_code::unavailable);
    state->parent_identity = cohort_identity(cohort_inspect(state->parent.value, true, true));
    state->leaf = leaf;
    if (::mkdirat(state->parent.value, leaf.c_str(), 0700)) cohort_fail(cohort_error_code::unavailable);
    state->directory = cohort_fd(::openat(state->parent.value, leaf.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    if (state->directory.value < 0) cohort_fail(cohort_error_code::unavailable);
    state->directory_identity = cohort_identity(cohort_inspect(state->directory.value, true));
    state->gate = cohort_fd(::openat(state->directory.value, "launch.lock", O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600));
    if (state->gate.value < 0 || ::flock(state->gate.value, LOCK_EX | LOCK_NB)) cohort_fail(cohort_error_code::unavailable);
    state->gate_identity = cohort_identity(cohort_inspect(state->gate.value, false));
    state->owner_identity = ordinary_launch::current_process();
    cohort_sync(state->gate.value); cohort_sync(state->directory.value); cohort_sync(state->parent.value);
    state->save(); implementation::retain(state);
    return created_launch_cohort(std::move(state));
}
#else
struct created_launch_cohort::implementation {};
created_launch_cohort created_launch_cohort::create_before_child(int, const std::string&) { cohort_fail(cohort_error_code::unavailable); }
#endif
created_launch_cohort::created_launch_cohort(std::shared_ptr<implementation> value) : impl_(std::move(value)) {}
created_launch_cohort::~created_launch_cohort() {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    if (impl_) impl_->abandon();
#endif
}
created_launch_cohort::created_launch_cohort(created_launch_cohort&&) noexcept = default;
created_launch_cohort& created_launch_cohort::operator=(created_launch_cohort&& other) noexcept {
    if (this != &other) {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
        if (impl_) impl_->abandon();
#endif
        impl_ = std::move(other.impl_);
    }
    return *this;
}
created_launch_cohort::store_namespace::store_namespace(std::shared_ptr<implementation> owner, std::size_t index)
    : owner_(std::move(owner)), index_(index) {}
created_launch_cohort::store_namespace::~store_namespace() = default;
created_launch_cohort::store_namespace::store_namespace(store_namespace&&) noexcept = default;
created_launch_cohort::store_namespace& created_launch_cohort::store_namespace::operator=(store_namespace&&) noexcept = default;
created_launch_cohort::store_namespace created_launch_cohort::create_store_namespace(const std::string& leaf) {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    if (!impl_) cohort_fail(cohort_error_code::unavailable);
    impl_->owner();
    if (impl_->tainted) cohort_fail(cohort_error_code::incomplete);
    if (impl_->state != implementation::phase::accepting || !impl_->entries.empty()) cohort_fail(cohort_error_code::closed);
    if (impl_->namespaces.size() == 64) cohort_fail(cohort_error_code::limit);
    if (leaf.empty() || leaf.size() > 128 || leaf == "." || leaf == ".." ||
        leaf.find('/') != std::string::npos || leaf.find('\0') != std::string::npos ||
        leaf == "launch.lock" || leaf == "launch.v1" || leaf == "launch.pending") cohort_fail(cohort_error_code::unavailable);
    impl_->verify_snapshot();
    // All retained storage is reserved before creating the namespace; no active
    // child exists yet, so this cannot adopt a directory from an earlier role.
    impl_->namespaces.reserve(impl_->namespaces.size() + 1);
    implementation::owned_namespace space; space.leaf = leaf;
    if (::mkdirat(impl_->directory.value, leaf.c_str(), 0700)) cohort_fail(cohort_error_code::unavailable);
    try {
        space.directory = cohort_fd(::openat(impl_->directory.value, leaf.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
        if (space.directory.value < 0) cohort_fail(cohort_error_code::unavailable);
        space.identity = cohort_identity(cohort_inspect(space.directory.value, true));
        impl_->namespaces.push_back(std::move(space));
        cohort_sync(impl_->namespaces.back().directory.value); cohort_sync(impl_->directory.value);
        impl_->save();
    } catch (...) { impl_->tainted = true; throw; }
    return store_namespace(impl_, impl_->namespaces.size() - 1);
#else
    (void)leaf; cohort_fail(cohort_error_code::unavailable);
#endif
}
int created_launch_cohort::store_namespace::duplicate_for_child() const {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    if (!owner_) cohort_fail(cohort_error_code::unavailable); owner_->owner();
    if (owner_->tainted || owner_->state != implementation::phase::accepting) cohort_fail(cohort_error_code::closed);
    owner_->check_objects();
    if (index_ >= owner_->namespaces.size()) cohort_fail(cohort_error_code::changed);
    const auto fd = ::fcntl(owner_->namespaces[index_].directory.value, F_DUPFD_CLOEXEC, 3);
    if (fd < 0) cohort_fail(cohort_error_code::unavailable); return fd;
#else
    cohort_fail(cohort_error_code::unavailable);
#endif
}
file_identity created_launch_cohort::store_namespace::directory_identity() const {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    if (!owner_) cohort_fail(cohort_error_code::unavailable); owner_->owner(); owner_->check_objects();
    if (index_ >= owner_->namespaces.size()) cohort_fail(cohort_error_code::changed);
    return owner_->namespaces[index_].identity;
#else
    cohort_fail(cohort_error_code::unavailable);
#endif
}
file_identity created_launch_cohort::store_namespace::inspect_after_join(const std::string& leaf) const {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    if (!owner_) cohort_fail(cohort_error_code::unavailable); owner_->owner();
    if (owner_->tainted || owner_->state != implementation::phase::direct_terminal) cohort_fail(cohort_error_code::incomplete);
    if (leaf.empty() || leaf.size() > 255 || leaf == "." || leaf == ".." ||
        leaf.find('/') != std::string::npos || leaf.find('\0') != std::string::npos) cohort_fail(cohort_error_code::unavailable);
    owner_->verify_snapshot();
    if (index_ >= owner_->namespaces.size()) cohort_fail(cohort_error_code::changed);
    const auto& space = owner_->namespaces[index_];
    cohort_fd file(::openat(space.directory.value, leaf.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
    struct stat opened{}, named{};
    // Store mode need not equal the 0600 control-file mode, but an external
    // hard link, writable foreign principal or non-regular target is refused.
    if (file.value < 0 || ::fstat(file.value, &opened) || !S_ISREG(opened.st_mode) ||
        opened.st_uid != ::geteuid() || opened.st_nlink != 1 || (opened.st_mode & 0022) != 0 ||
        ::fstatat(space.directory.value, leaf.c_str(), &named, AT_SYMLINK_NOFOLLOW) ||
        !S_ISREG(named.st_mode) || cohort_identity(opened) != cohort_identity(named)) {
        owner_->tainted = true; // A later pathname repair cannot erase this unproved observation.
        cohort_fail(cohort_error_code::changed);
    }
    owner_->check_objects(); return cohort_identity(opened);
#else
    (void)leaf; cohort_fail(cohort_error_code::unavailable);
#endif
}
std::size_t created_launch_cohort::launch(const ordinary_launch::launch_specification& specification) {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    if (!impl_) cohort_fail(cohort_error_code::unavailable);
    impl_->owner();
    if (impl_->state != implementation::phase::accepting) cohort_fail(cohort_error_code::closed);
    if (impl_->tainted) cohort_fail(cohort_error_code::incomplete);
    if (impl_->entries.size() == 64) cohort_fail(cohort_error_code::limit);
    // Reserve all process-custody storage before recording/spawning anything.
    impl_->entries.reserve(impl_->entries.size() + 1);
    impl_->entries.emplace_back();
    impl_->save(); // Durable unknown/pending launch precedes the actual spawn.
    auto& entry = impl_->entries.back();
    try {
        auto child = ordinary_launch::owned_child::spawn(specification);
        entry.identity = child.identity();
        entry.child.emplace(std::move(child));
        impl_->save();
    } catch (...) { impl_->tainted = true; throw; }
    return impl_->entries.size() - 1;
#else
    (void)specification; cohort_fail(cohort_error_code::unavailable);
#endif
}
ordinary_launch::process_identity created_launch_cohort::child_identity(std::size_t index) const {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    if (!impl_) cohort_fail(cohort_error_code::unavailable);
    impl_->owner();
    if (index >= impl_->entries.size() || !impl_->entries[index].child) cohort_fail(cohort_error_code::unfinished_launch);
    return impl_->entries[index].child->identity();
#else
    (void)index; cohort_fail(cohort_error_code::unavailable);
#endif
}
void created_launch_cohort::send(std::size_t index, const ordinary_launch::frame& frame, ordinary_launch::deadline end) {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    (void)child_identity(index);
    if (impl_->state != implementation::phase::accepting || impl_->tainted) cohort_fail(cohort_error_code::closed);
    impl_->entries[index].child->send(frame, end);
#else
    (void)index; (void)frame; (void)end; cohort_fail(cohort_error_code::unavailable);
#endif
}
ordinary_launch::frame created_launch_cohort::receive(std::size_t index, ordinary_launch::deadline end) {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    (void)child_identity(index);
    if (impl_->state != implementation::phase::accepting || impl_->tainted) cohort_fail(cohort_error_code::closed);
    return impl_->entries[index].child->receive(end);
#else
    (void)index; (void)end; cohort_fail(cohort_error_code::unavailable);
#endif
}
std::optional<ordinary_launch::terminal_observation> created_launch_cohort::observe_terminal(std::size_t index, ordinary_launch::deadline end) {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    (void)child_identity(index);
    if (impl_->tainted) cohort_fail(cohort_error_code::incomplete);
    auto& entry = impl_->entries[index];
    if (entry.terminal) return ordinary_launch::terminal_observation{entry.identity, entry.status};
    const auto result = entry.child->observe_terminal(end);
    if (!result) return std::nullopt;
    entry.identity = result->child; entry.status = result->wait_status; entry.terminal = true;
    impl_->save(end); return result;
#else
    (void)index; (void)end; cohort_fail(cohort_error_code::unavailable);
#endif
}
void created_launch_cohort::close_launch_admission(ordinary_launch::deadline end) {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    if (!impl_) cohort_fail(cohort_error_code::unavailable); impl_->close_admission(end);
#else
    (void)end; cohort_fail(cohort_error_code::unavailable);
#endif
}
direct_cohort_completion created_launch_cohort::close_and_join(ordinary_launch::deadline end) {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    if (!impl_) cohort_fail(cohort_error_code::unavailable); return impl_->close(end);
#else
    (void)end; cohort_fail(cohort_error_code::unavailable);
#endif
}
int created_launch_cohort::duplicate_closed_root(ordinary_launch::deadline end) const {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    if (!impl_) cohort_fail(cohort_error_code::unavailable);
    impl_->owner(); cohort_bound(end);
    if (impl_->tainted || impl_->state != implementation::phase::direct_terminal)
        cohort_fail(cohort_error_code::incomplete);
    impl_->verify_snapshot(end);
    for (const auto& entry : impl_->entries)
        if (!entry.child || !entry.terminal) cohort_fail(cohort_error_code::incomplete);
    const auto fd = ::fcntl(impl_->directory.value, F_DUPFD_CLOEXEC, 3);
    if (fd < 0) cohort_fail(cohort_error_code::unavailable);
    return fd;
#else
    (void)end; cohort_fail(cohort_error_code::unavailable);
#endif
}
retained_installation_file created_launch_cohort::commit_origin_anchor(const std::vector<std::uint8_t>& bytes, ordinary_launch::deadline end) {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    cohort_fd root(duplicate_closed_root(end));
    if (bytes.size() != 320) cohort_fail(cohort_error_code::unavailable);
    const auto leaf = impl_->leaf + ".origin";
    if (leaf.size() > 255) cohort_fail(cohort_error_code::unavailable);
    // A separate installer-parent entry anchors the original catalog. A
    // missing/partial/changed record is refused; no future directory contents
    // may silently become the expected registration or clear launch closure.
    cohort_fd anchor(::openat(impl_->parent.value, leaf.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600));
    if (anchor.value < 0) cohort_fail(cohort_error_code::unavailable);
    try {
        std::size_t at = 0;
        while (at != bytes.size()) {
            cohort_bound(end);
            const auto count = ::write(anchor.value, bytes.data() + at, bytes.size() - at);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) cohort_fail(cohort_error_code::durability_unproved);
            at += static_cast<std::size_t>(count);
        }
        const auto opened = cohort_inspect(anchor.value, false);
        if (opened.st_size != static_cast<off_t>(bytes.size())) cohort_fail(cohort_error_code::changed);
        const auto expected = cohort_identity(opened);
        impl_->named(impl_->parent.value, leaf.c_str(), anchor.value, expected, false);
        cohort_sync(anchor.value, end); cohort_sync(impl_->parent.value, end);
        impl_->verify_snapshot(end);
        impl_->named(impl_->parent.value, leaf.c_str(), anchor.value, expected, false);
        digest checksum{};picosha2::hash256(bytes.begin(),bytes.end(),checksum.begin(),checksum.end());
        return retained_installation_file::open_record(impl_->parent.value,leaf,expected,checksum,bytes.size(),end);
    } catch (...) { impl_->tainted = true; throw; }
#else
    (void)bytes; (void)end; cohort_fail(cohort_error_code::unavailable);
#endif
}
int created_launch_cohort::duplicate_accepting_root() const {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    if(!impl_)cohort_fail(cohort_error_code::unavailable);
    impl_->owner();
    if(impl_->tainted || impl_->state!=implementation::phase::accepting)cohort_fail(cohort_error_code::closed);
    impl_->verify_snapshot();
    const auto fd=::fcntl(impl_->directory.value,F_DUPFD_CLOEXEC,3);
    if(fd<0)cohort_fail(cohort_error_code::unavailable);return fd;
#else
    cohort_fail(cohort_error_code::unavailable);
#endif
}
int created_launch_cohort::duplicate_installer_parent() const {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    if(!impl_)cohort_fail(cohort_error_code::unavailable);
    impl_->owner();
    if(impl_->tainted || impl_->state!=implementation::phase::direct_terminal)cohort_fail(cohort_error_code::closed);
    impl_->verify_snapshot();
    const auto fd=::fcntl(impl_->parent.value,F_DUPFD_CLOEXEC,3);
    if(fd<0)cohort_fail(cohort_error_code::unavailable);return fd;
#else
    cohort_fail(cohort_error_code::unavailable);
#endif
}
std::size_t created_launch_cohort::unresolved_cohorts() {
#ifdef LATTICE_INSTALLATION_COHORT_POSIX
    auto& values = implementation::registry(); std::lock_guard guard(values.mutex);
    return static_cast<std::size_t>(std::count_if(values.cohorts.begin(), values.cohorts.end(), [](const auto& value) { return static_cast<bool>(value); }));
#else
    return 0;
#endif
}
} // namespace lattice::detail::ordinary_installation
