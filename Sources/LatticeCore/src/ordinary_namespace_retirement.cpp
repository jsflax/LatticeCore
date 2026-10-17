#include "ordinary_namespace_retirement.hpp"
#include "ordinary_historical_generation.hpp"
#include "ordinary_historical_wal_checkpoint.hpp"
#include "vendor/picosha2/picosha2.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <set>
#include <map>
#include <optional>
#include <vector>
#include <thread>
#include <utility>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <stdio.h>
#include <sys/acl.h>
#else
#include <sys/syscall.h>
#include <sys/xattr.h>
#endif
#define LATTICE_NAMESPACE_POSIX 1
#endif

namespace lattice::detail::ordinary_installation {
namespace {
using code = namespace_retirement_error_code;
using deadline = ordinary_launch::deadline;
[[noreturn]] void fail(code c) { throw namespace_retirement_error(c); }
void bound(deadline end) { if (std::chrono::steady_clock::now() >= end) fail(code::expired); }
void hook(namespace_retirement_test_hooks::boundary b, const std::string& leaf) {
    if (const auto observer = namespace_retirement_test_hooks::observe) observer(b, leaf.c_str());
}
bool leaf_ok(const std::string& s) {
    return !s.empty() && s.size() <= 120 && s != "." && s != ".." &&
        std::all_of(s.begin(), s.end(), [](unsigned char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.';
        });
}
#ifdef LATTICE_NAMESPACE_POSIX
struct fd {
    int n = -1;
    explicit fd(int v = -1) : n(v) {}
    ~fd() { if (n >= 0) ::close(n); }
    fd(fd&& v) noexcept : n(std::exchange(v.n, -1)) {}
    fd& operator=(fd&& v) noexcept { if (this != &v) { if (n >= 0) ::close(n); n = std::exchange(v.n, -1); } return *this; }
    fd(const fd&) = delete;
};
file_identity identity(const struct stat& s) { return {std::uint64_t(s.st_dev), std::uint64_t(s.st_ino)}; }
struct stat held(int n) { struct stat s{}; if (n < 0 || ::fstat(n, &s)) fail(code::unavailable); return s; }
struct stat named(int parent, const std::string& leaf) {
    struct stat s{}; if (::fstatat(parent, leaf.c_str(), &s, AT_SYMLINK_NOFOLLOW)) fail(code::changed); return s;
}
void check_kind(const struct stat& s, bool directory) {
    if (s.st_uid != ::geteuid() || (s.st_mode & 07022) ||
        (directory ? !S_ISDIR(s.st_mode) : (!S_ISREG(s.st_mode) || s.st_nlink != 1))) fail(code::invalid);
}
void require_no_granting_acl(int descriptor) {
#if defined(__APPLE__)
    filesec_t security = ::filesec_init();
    if (!security) fail(code::unavailable);
    struct release_security { filesec_t value; ~release_security() { ::filesec_free(value); } } release_info{security};
    struct stat actual{};
    if (::fstatx_np(descriptor, &actual, security)) fail(code::unavailable);
    int present = 0;
    if (::filesec_query_property(security, FILESEC_ACL, &present)) fail(code::unavailable);
    if (!present) return; // Successful descriptor stat, explicitly absent ACL.
    acl_t acl = nullptr;
    if (::filesec_get_property(security, FILESEC_ACL, &acl) || !acl) fail(code::unavailable);
    struct release_acl { acl_t value; ~release_acl() { ::acl_free(value); } } release{acl};
    if (::acl_valid(acl)) fail(code::invalid);
    int cursor = ACL_FIRST_ENTRY;
    for (std::size_t count = 0; count != 1024; ++count) {
        acl_entry_t entry{}; errno = 0;
        if (::acl_get_entry(acl, cursor, &entry) != 0) {
            // Apple's retrieved valid ACL uses EINVAL at the end, including
            // an empty ACL. Linux libacl has different iteration semantics.
            if (errno == EINVAL) return;
            fail(code::unavailable);
        }
        acl_tag_t tag{};
        if (::acl_get_tag_type(entry, &tag) || tag != ACL_EXTENDED_DENY) fail(code::invalid);
        cursor = ACL_NEXT_ENTRY;
    }
    fail(code::invalid);
#else
    // Restrict this profile to inputs where absence of POSIX ACLs is actually
    // observable. Unsupported/denied xattr retrieval is unqualified, not an
    // assumption that mode bits exclude all permission-granting mechanisms.
    for (const auto* name : {"system.posix_acl_access", "system.posix_acl_default"}) {
        errno = 0;
        const auto size = ::fgetxattr(descriptor, name, nullptr, 0);
        if (size >= 0) fail(code::invalid);
        if (errno != ENODATA) fail(code::unavailable);
    }
#endif
}
fd duplicate(int n) { fd out(::fcntl(n, F_DUPFD_CLOEXEC, 3)); if (out.n < 0) fail(code::unavailable); return out; }
struct anchored_parent {
    struct node { fd descriptor; std::string leaf; file_identity original; };
    std::vector<node> nodes;
    explicit anchored_parent(int descriptor) {
        std::array<char, 4097> bytes{}; std::string path;
#if defined(__APPLE__)
        if (::fcntl(descriptor, F_GETPATH, bytes.data())) fail(code::unavailable);
        const auto end = std::find(bytes.begin(), bytes.end(), '\0');
        if (end == bytes.end()) fail(code::invalid); path.assign(bytes.begin(), end);
#else
        const auto link = "/proc/self/fd/" + std::to_string(descriptor);
        const auto count = ::readlink(link.c_str(), bytes.data(), bytes.size());
        if (count <= 0 || std::size_t(count) >= bytes.size()) fail(code::unavailable);
        path.assign(bytes.data(), std::size_t(count));
#endif
        if (path.empty() || path.front() != '/' || path.ends_with(" (deleted)")) fail(code::invalid);
        nodes.push_back({fd(::open("/", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)), {}, {}});
        nodes.back().original = identity(held(nodes.back().descriptor.n));
        for (std::size_t at = 1; at < path.size();) {
            const auto slash = path.find('/', at); const auto leaf = path.substr(at, slash == std::string::npos ? slash : slash - at);
            if (leaf.empty() || leaf == "." || leaf == ".." || leaf.size() > 255 || nodes.size() == 256) fail(code::invalid);
            fd next(::openat(nodes.back().descriptor.n, leaf.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
            const auto s = held(next.n);
            if (!S_ISDIR(s.st_mode) || (s.st_uid != ::geteuid() && s.st_uid != 0) || (s.st_mode & 0022)) fail(code::invalid);
            nodes.push_back({std::move(next), leaf, identity(s)});
            if (slash == std::string::npos) break; at = slash + 1;
        }
        if (nodes.back().original != identity(held(descriptor))) fail(code::changed);
        verify();
    }
    void verify() const {
        for (std::size_t i = 0; i != nodes.size(); ++i) {
            const auto& value = nodes[i]; const auto s = held(value.descriptor.n);
            if (identity(s) != value.original || !S_ISDIR(s.st_mode) ||
                (s.st_uid != ::geteuid() && s.st_uid != 0) || (s.st_mode & 0022)) fail(code::changed);
            if (i && identity(named(nodes[i-1].descriptor.n, value.leaf)) != value.original) fail(code::changed);
        }
    }
};
void sync_fd(int n, deadline end) {
    bound(end); if (::fsync(n)) fail(code::durability_unproved); bound(end);
}
void sync_dir(int n, const std::string& leaf, deadline end) {
    hook(namespace_retirement_test_hooks::boundary::before_directory_sync, leaf); sync_fd(n, end);
}
std::vector<std::string> list(int n, deadline end) {
    fd scan(::openat(n, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    if (scan.n < 0) fail(code::unavailable);
    DIR* raw = ::fdopendir(scan.n); if (!raw) fail(code::unavailable); scan.n = -1;
    std::unique_ptr<DIR, decltype(&::closedir)> owner(raw, &::closedir);
    std::vector<std::string> out;
    for (;;) {
        bound(end); errno = 0; const auto* entry = ::readdir(raw);
        if (!entry) { if (errno) fail(code::unavailable); break; }
        std::string name(entry->d_name); if (name == "." || name == "..") continue;
        if (name.size() > 255 || name.find('/') != std::string::npos || out.size() == 4096) fail(code::invalid);
        out.push_back(std::move(name));
    }
    std::sort(out.begin(), out.end()); return out;
}
bool store_shaped(std::string s) {
    for (auto& c : s) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    return s.find(".sqlite") != std::string::npos || s.find(".lattice") != std::string::npos ||
        s.ends_with(".db") || s.find(".db-") != std::string::npos || s.starts_with("group-");
}
bool group_main(const std::string& s) {
    if (!s.starts_with("group-")) return false;
    const auto suffix = s.ends_with(".sqlite.revoked") ? std::string(".sqlite.revoked") : std::string(".sqlite");
    if (!s.ends_with(suffix) || s.size() != 6 + 36 + suffix.size()) return false;
    for (std::size_t i = 0; i != 36; ++i) {
        const auto c = s[6 + i];
        if (i == 8 || i == 13 || i == 18 || i == 23) { if (c != '-') return false; }
        else if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) return false;
    }
    return true;
}
void append(std::vector<std::uint8_t>& b, std::uint64_t v) {
    for (unsigned i = 0; i != 64; i += 8) b.push_back(std::uint8_t(v >> i));
}
void append(std::vector<std::uint8_t>& b, file_identity v) { append(b, v.device); append(b, v.inode); }
void append(std::vector<std::uint8_t>& b, const std::string& s) { append(b, s.size()); b.insert(b.end(), s.begin(), s.end()); }
struct reader {
    const std::vector<std::uint8_t>& b; std::size_t at = 0;
    std::uint64_t u64() {
        if (at > b.size() || b.size() - at < 8) fail(code::invalid);
        std::uint64_t v = 0; for (unsigned i = 0; i != 64; i += 8) v |= std::uint64_t(b[at++]) << i; return v;
    }
    file_identity id() { return {u64(), u64()}; }
    std::string text() {
        const auto count = u64(); if (count > 120 || count > b.size() - at) fail(code::invalid);
        std::string s(b.begin() + at, b.begin() + at + count); at += count;
        if (!leaf_ok(s)) fail(code::invalid); return s;
    }
};
std::vector<std::uint8_t> checked_read(int n, deadline end) {
    const auto original = held(n); check_kind(original, false);
    if ((original.st_mode & 07777) != 0600 || original.st_size < 32 || original.st_size > 65536) fail(code::invalid);
    std::vector<std::uint8_t> b(std::size_t(original.st_size)); std::size_t at = 0;
    while (at != b.size()) {
        bound(end); const auto got = ::pread(n, b.data() + at, b.size() - at, off_t(at));
        if (got < 0 && errno == EINTR) continue; if (got <= 0) fail(code::invalid); at += std::size_t(got);
    }
    digest hash{}; picosha2::hash256(b.begin(), b.end() - 32, hash.begin(), hash.end());
    if (!std::equal(hash.begin(), hash.end(), b.end() - 32)) fail(code::invalid);
    const auto after = held(n); check_kind(after, false);
    if (identity(after) != identity(original) || after.st_size != original.st_size ||
        after.st_mode != original.st_mode || after.st_uid != original.st_uid || after.st_nlink != original.st_nlink)
        fail(code::changed);
#if defined(__APPLE__)
    if (after.st_mtimespec.tv_sec != original.st_mtimespec.tv_sec || after.st_mtimespec.tv_nsec != original.st_mtimespec.tv_nsec ||
        after.st_ctimespec.tv_sec != original.st_ctimespec.tv_sec || after.st_ctimespec.tv_nsec != original.st_ctimespec.tv_nsec) fail(code::changed);
#else
    if (after.st_mtim.tv_sec != original.st_mtim.tv_sec || after.st_mtim.tv_nsec != original.st_mtim.tv_nsec ||
        after.st_ctim.tv_sec != original.st_ctim.tv_sec || after.st_ctim.tv_nsec != original.st_ctim.tv_nsec) fail(code::changed);
#endif
    b.resize(b.size() - 32); return b;
}
// A durable immutable event can be retried only if its full bytes match. A
// torn record stays an explicit refusal; no truncate/overwrite repairs history.
// Final events are immutable. New records are fully flushed under a private
// pending name before exclusive publication; a torn final predecessor still
// refuses. No source effect may depend on an unpublished pending record.
void event(int directory, const std::string& leaf, std::vector<std::uint8_t> b, deadline end) {
    if (b.size() > 65504 || !leaf_ok(leaf)) fail(code::invalid);
    struct stat named_event{};
    if (::fstatat(directory, leaf.c_str(), &named_event, AT_SYMLINK_NOFOLLOW) == 0) {
        fd prior(::openat(directory, leaf.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
        if (identity(held(prior.n)) != identity(named_event) || checked_read(prior.n, end) != b) fail(code::changed);
        sync_fd(prior.n, end); sync_dir(directory, leaf, end); return;
    }
    if (errno != ENOENT) fail(code::durability_unproved);
    const auto pending = "pending." + leaf;
    struct stat stale{};
    if (::fstatat(directory, pending.c_str(), &stale, AT_SYMLINK_NOFOLLOW) == 0) {
        check_kind(stale, false);
        if ((stale.st_mode & 07777) != 0600 || stale.st_size < 0 || stale.st_size > 65536) fail(code::changed);
        // No final record exists, so this known private prefix cannot have
        // authorized chmod, signaling, parent restoration or exchange.
        bound(end); if (::unlinkat(directory, pending.c_str(), 0)) fail(code::durability_unproved);
        sync_dir(directory, pending, end);
    } else if (errno != ENOENT) fail(code::durability_unproved);
    digest hash{}; picosha2::hash256(b.begin(), b.end(), hash.begin(), hash.end());
    b.insert(b.end(), hash.begin(), hash.end());
    fd out(::openat(directory, pending.c_str(), O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600));
    if (out.n < 0 || ::fchmod(out.n, 0600)) fail(code::durability_unproved);
    std::size_t at = 0;
    while (at != b.size()) {
        bound(end); const auto n = ::write(out.n, b.data() + at, b.size() - at);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) fail(code::durability_unproved); at += std::size_t(n);
    }
    sync_fd(out.n, end);
    hook(namespace_retirement_test_hooks::boundary::before_event_publish, leaf); bound(end);
    if (identity(held(out.n)) != identity(named(directory, pending))) fail(code::changed);
#if defined(__APPLE__)
    const int published = ::renameatx_np(directory, pending.c_str(), directory, leaf.c_str(), RENAME_EXCL);
#elif defined(SYS_renameat2)
    const long published = ::syscall(SYS_renameat2, directory, pending.c_str(), directory, leaf.c_str(), 1U);
#else
    const int published = -1; errno = ENOSYS;
#endif
    if (published != 0) fail(code::durability_unproved);
    hook(namespace_retirement_test_hooks::boundary::after_event_publish, leaf);
    sync_dir(directory, leaf, end);
    if (identity(named(directory, leaf)) != identity(held(out.n))) fail(code::changed);
}

void exchange(int from, const std::string& leaf, int to) {
#if defined(__APPLE__)
    const int result = ::renameatx_np(from, leaf.c_str(), to, leaf.c_str(), RENAME_SWAP);
#elif defined(SYS_renameat2)
    const long result = ::syscall(SYS_renameat2, from, leaf.c_str(), to, leaf.c_str(), 2U); // RENAME_EXCHANGE
#else
    const int result = -1; errno = ENOSYS;
#endif
    if (result != 0) fail(code::unsupported_filesystem);
}
#endif
}

struct engram_namespace_retirement::implementation {
#ifdef LATTICE_NAMESPACE_POSIX
    struct item {
        std::string leaf; file_identity original{}, tombstone{}; mode_t mode = 0; bool directory = false;
        fd retained, refusal;
    };
    struct member { std::string leaf; file_identity original{}; mode_t mode = 0; fd retained; };
    const ordinary_launch::process_identity creator = ordinary_launch::current_process();
    const std::thread::id thread = std::this_thread::get_id();
    fd data_parent, data, control_parent, control, generation, lock;
    anchored_parent data_anchor, control_anchor;
    std::string data_leaf, control_leaf;
    file_identity data_parent_id{}, data_id{}, control_parent_id{}, control_id{}, generation_id{}, lock_id{};
    std::vector<item> items;
    std::vector<member> sync_members;
    std::vector<std::uint8_t> intent;
    std::vector<std::uint8_t> barrier_intent;
    file_identity barrier_sync_id{};
    mode_t data_original_mode = 0, sync_original_mode = 0;
    fd barrier_sync;
    bool use_mutation_barrier = false;
    static mode_t frozen_mode(mode_t value) { return value & ~mode_t(0222); }
    mode_t denied_mode() const { return use_mutation_barrier ? 0400 : 0; }

    implementation(int dp, const std::string& dl, int cp, const std::string& cl)
        : data_parent(duplicate(dp)), control_parent(duplicate(cp)), data_anchor(dp), control_anchor(cp), data_leaf(dl), control_leaf(cl) {
        if (!leaf_ok(dl) || !leaf_ok(cl)) fail(code::invalid);
        for (int n : {dp, cp}) check_kind(held(n), true);
        data_parent_id = identity(held(dp)); control_parent_id = identity(held(cp));
        data = fd(::openat(dp, dl.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
        check_kind(held(data.n), true); data_id = identity(held(data.n));
        if (identity(named(dp, dl)) != data_id) fail(code::changed);
        // Generation is a sibling/outside namespace on the same filesystem.
        // Never put control beneath the historical root, including via a
        // retained descriptor alias. No working-directory or path spelling test.
        auto ancestor = duplicate(cp); bool root = false;
        for (std::size_t count = 0; count != 256; ++count) {
            const auto current = identity(held(ancestor.n)); if (current == data_id) fail(code::invalid);
            fd next(::openat(ancestor.n, "..", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
            const auto above = identity(held(next.n)); if (current == above) { root = true; break; }
            ancestor = std::move(next);
        }
        if (!root || data_id.device != control_parent_id.device) fail(code::unsupported_filesystem);
    }
    void owner(deadline end) const {
        bound(end); if (ordinary_launch::current_process() != creator || std::this_thread::get_id() != thread) fail(code::inherited_use);
        data_anchor.verify(); control_anchor.verify();
        check_kind(held(data_parent.n), true); check_kind(held(control_parent.n), true); check_kind(held(data.n), true);
        if (identity(held(data_parent.n)) != data_parent_id || identity(held(control_parent.n)) != control_parent_id ||
            identity(named(data_parent.n, data_leaf)) != data_id || identity(held(data.n)) != data_id) fail(code::changed);
        if (control.n >= 0 && (identity(named(control_parent.n, control_leaf)) != control_id ||
            identity(held(control.n)) != control_id || identity(named(control.n, "generation")) != generation_id ||
            identity(held(generation.n)) != generation_id || identity(held(lock.n)) != lock_id ||
            identity(named(control.n, "controller.lock")) != lock_id)) fail(code::changed);
        if (control.n >= 0) {
            check_kind(held(control.n), true); check_kind(held(generation.n), true); check_kind(held(lock.n), false);
            if ((held(control.n).st_mode & 07777) != 0700 || (held(generation.n).st_mode & 07777) != 0700 ||
                (held(lock.n).st_mode & 07777) != 0600 || held(lock.n).st_size != 0) fail(code::changed);
        }
        if (use_mutation_barrier) {
            for (const auto& node : data_anchor.nodes) require_no_granting_acl(node.descriptor.n);
            for (const auto& node : control_anchor.nodes) require_no_granting_acl(node.descriptor.n);
            require_no_granting_acl(data.n);
            if (control.n >= 0) { require_no_granting_acl(control.n); require_no_granting_acl(generation.n); }
            if (empty_slot.n >= 0) {
                const auto s=held(empty_slot.n);check_kind(s,true);require_no_granting_acl(empty_slot.n);
                if(identity(s)!=empty_slot_id || identity(named(control.n,"empty-sync"))!=empty_slot_id ||
                    (s.st_mode&07777)!=0700)fail(code::changed);
            }
        }
    }
    void open_control(bool create, deadline end, bool recover_unpublished = false) {
        bound(end);
        if (create && ::mkdirat(control_parent.n, control_leaf.c_str(), 0700)) fail(code::invalid);
        control = fd(::openat(control_parent.n, control_leaf.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
        const auto cs = held(control.n); check_kind(cs, true); if ((cs.st_mode & 07777) != 0700) fail(code::invalid);
        control_id = identity(cs);
        // Recovery may finish an unpublished initialization prefix only. Once
        // ANY final record exists, none of its bound identities can be remade.
        const auto unpublished = [&] {
            for (const auto& name : list(control.n, end)) {
                if (name == "controller.lock" || name == "generation") continue;
                if (name != "pending.barrier.intent") return false;
                const auto pending = named(control.n, name); check_kind(pending, false);
                if ((pending.st_mode & 07777) != 0600 || pending.st_size < 0 || pending.st_size > 65536) return false;
            }
            struct stat existing{};
            if (::fstatat(control.n, "generation", &existing, AT_SYMLINK_NOFOLLOW) == 0) {
                check_kind(existing, true); if ((existing.st_mode & 07777) != 0700) return false;
                fd contents(::openat(control.n, "generation", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
                if (identity(held(contents.n)) != identity(existing) || !list(contents.n, end).empty()) return false;
            } else if (errno != ENOENT) fail(code::changed);
            return true;
        };
        bool create_lock = create;
        struct stat before{};
        if (!create && ::fstatat(control.n, "controller.lock", &before, AT_SYMLINK_NOFOLLOW) != 0) {
            if (errno != ENOENT || !recover_unpublished || !unpublished()) fail(code::incomplete);
            create_lock = true;
        }
        // flock is controller serialization only, never old-writer retirement.
        lock = fd(::openat(control.n, "controller.lock", O_RDWR | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC |
            (create_lock ? O_CREAT | O_EXCL : 0), 0600));
        const auto ls = held(lock.n); check_kind(ls, false);
        if (ls.st_size != 0 || (ls.st_mode & 07777) != 0600 ||
            identity(named(control.n, "controller.lock")) != identity(ls) || ::flock(lock.n, LOCK_EX | LOCK_NB)) fail(code::incomplete);
        lock_id = identity(ls);
        bool create_generation = create;
        if (!create && ::fstatat(control.n, "generation", &before, AT_SYMLINK_NOFOLLOW) != 0) {
            if (errno != ENOENT || !recover_unpublished || !unpublished()) fail(code::incomplete);
            create_generation = true;
        }
        if (create_generation && ::mkdirat(control.n, "generation", 0700)) fail(code::invalid);
        generation = fd(::openat(control.n, "generation", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
        const auto gs = held(generation.n); check_kind(gs, true);
        if ((gs.st_mode & 07777) != 0700 || gs.st_dev != cs.st_dev) fail(code::invalid);
        generation_id = identity(gs); owner(end);
        // A flushed event inside control does not persist control's own name.
        // Settle the whole actual initialization before it can authorize any
        // source denial, including on a resumed ambiguous publication.
        sync_fd(lock.n, end); sync_dir(generation.n, "initialize:generation", end);
        sync_dir(control.n, "initialize:control", end); sync_dir(control_parent.n, "initialize:control-parent", end);
        owner(end);
    }
    void verify_unpublished_barrier_prefix(deadline end) const {
        for (const auto& name : list(control.n, end))
            if (name != "controller.lock" && name != "generation" && name != "pending.barrier.intent") fail(code::changed);
        if (!list(generation.n, end).empty()) fail(code::changed);
    }
#include "ordinary_namespace_provisional.inc"
    void retain_item(const std::string& leaf, bool directory) {
        item value; value.leaf = leaf; value.directory = directory;
        value.retained = fd(::openat(data.n, leaf.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC | (directory ? O_DIRECTORY : 0)));
        const auto s = held(value.retained.n); check_kind(s, directory);
        if ((s.st_mode & 07700) != (directory ? (use_mutation_barrier ? 0500 : 0700) : 0600)) fail(code::invalid);
        value.original = identity(s); value.mode = s.st_mode & 07777;
        if (directory && use_mutation_barrier) {
            if (leaf != "sync" || value.original != barrier_sync_id || value.mode != frozen_mode(sync_original_mode)) fail(code::changed);
            value.mode = sync_original_mode;
        }
        if (value.original.device != data_id.device) fail(code::unsupported_filesystem);
        if (identity(named(data.n, leaf)) != value.original) fail(code::changed);
        items.push_back(std::move(value));
    }
    void census(deadline end) {
        items.clear(); sync_members.clear();
        const auto top = list(data.n, end); std::set<std::string> accepted;
        retain_item("memory.sqlite", false); accepted.insert("memory.sqlite");
        for (const std::string suffix : {"-wal", "-shm", "-journal"}) {
            const auto leaf = "memory.sqlite" + suffix;
            if (std::binary_search(top.begin(), top.end(), leaf)) { retain_item(leaf, false); accepted.insert(leaf); }
        }
        for (const auto& leaf : top) if (store_shaped(leaf) && !accepted.contains(leaf)) fail(code::invalid);
        // An absent sync root cannot be left creatable. This first adapter
        // requires the controller to retain a real existing root; no mkdir of
        // historical source is performed to manufacture an empty origin.
        if (!std::binary_search(top.begin(), top.end(), "sync")) fail(code::incomplete);
        retain_item("sync", true);
        census_sync_members(end);
    }
    void census_sync_members(deadline end) {
        sync_members.clear();
        const auto children = list(items.back().retained.n, end); std::set<std::string> allowed;
        std::size_t mains = 1;
        for (const auto& leaf : children) if (leaf == "memory-synced.sqlite" || group_main(leaf)) {
            if (++mains > 64) fail(code::invalid); allowed.insert(leaf);
            for (const std::string suffix : {"-wal", "-shm", "-journal"})
                if (std::binary_search(children.begin(), children.end(), leaf + suffix)) allowed.insert(leaf + suffix);
        }
        for (const auto* extra : {"groups.json", "groups.json.tmp", "groups-refresh-requested"}) allowed.insert(extra);
        std::set<std::pair<std::uint64_t, std::uint64_t>> physical;
        for (const auto& value : items) if (!physical.emplace(value.original.device, value.original.inode).second) fail(code::invalid);
        for (const auto& leaf : children) {
            bound(end); if (!allowed.contains(leaf) || sync_members.size() + items.size() == 256) fail(code::invalid);
            member value; value.leaf = leaf;
            value.retained = fd(::openat(items.back().retained.n, leaf.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
            const auto s = held(value.retained.n); check_kind(s, false);
            value.original = identity(s); value.mode = s.st_mode & 07777;
            if (value.original.device != data_id.device || !physical.emplace(value.original.device, value.original.inode).second ||
                identity(named(items.back().retained.n, leaf)) != value.original) fail(code::changed);
            sync_members.push_back(std::move(value));
        }
    }
    std::vector<std::uint8_t> encode_barrier() const {
        std::vector<std::uint8_t> b{'L','A','T','N','B','R','2',0};
        for (const auto value : {data_parent_id, data_id, control_parent_id, control_id, generation_id, lock_id, barrier_sync_id}) append(b, value);
        append(b, data_leaf); append(b, control_leaf); append(b, data_original_mode); append(b, sync_original_mode);
        append(b, 0400); // Exact read-only/no-search original-denial profile.
        return b;
    }
    void decode_barrier(const std::vector<std::uint8_t>& bytes) {
        reader in{bytes}; const std::vector<std::uint8_t> magic{'L','A','T','N','B','R','2',0};
        if (bytes.size() < 8 || !std::equal(magic.begin(), magic.end(), bytes.begin())) fail(code::invalid);
        in.at = 8;
        for (const auto value : {data_parent_id, data_id, control_parent_id, control_id, generation_id, lock_id})
            if (in.id() != value) fail(code::changed);
        barrier_sync_id = in.id();
        if (in.text() != data_leaf || in.text() != control_leaf) fail(code::changed);
        const auto dm = in.u64(), sm = in.u64();
        if (dm > 0777 || sm > 0777 || (dm & 0022) || (sm & 0022) || (dm & 0700) != 0700 ||
            (sm & 0700) != 0700 || barrier_sync_id.device != data_id.device || !barrier_sync_id.inode ||
            barrier_sync_id == data_id || barrier_sync_id == control_id || barrier_sync_id == generation_id) fail(code::invalid);
        data_original_mode = mode_t(dm); sync_original_mode = mode_t(sm);
        if (in.u64() != 0400) fail(code::invalid);
        if (in.at != bytes.size() || encode_barrier() != bytes) fail(code::invalid);
        use_mutation_barrier = true; barrier_intent = bytes;
    }
    void write_barrier_phase(const std::string& phase, deadline end) {
        auto bytes = barrier_intent; append(bytes, phase); event(control.n, phase, std::move(bytes), end);
    }
    void verify_barrier_complete(deadline end) const {
        fd input(::openat(control.n, "barrier.complete", O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
        auto expected = barrier_intent; append(expected, std::string("barrier.complete"));
        if (checked_read(input.n, end) != expected ||
            identity(held(input.n)) != identity(named(control.n, "barrier.complete"))) fail(code::changed);
    }
    bool has_record(const std::string& leaf) const {
        struct stat value{};
        if (::fstatat(control.n, leaf.c_str(), &value, AT_SYMLINK_NOFOLLOW) == 0) return true;
        if (errno == ENOENT) return false; fail(code::changed);
    }
    void start_barrier(deadline end) {
        // Called after an informational preflight. Only directory identities and
        // original modes become durable here; member names are still mutable.
        const auto ds = held(data.n), ss = named(data.n, "sync"); check_kind(ds, true); check_kind(ss, true);
        data_original_mode = ds.st_mode & 07777; sync_original_mode = ss.st_mode & 07777;
        if ((data_original_mode & 0700) != 0700 || (sync_original_mode & 0700) != 0700) fail(code::invalid);
        barrier_sync_id = identity(ss);
        barrier_sync = fd(::openat(data.n, "sync", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
        if (identity(held(barrier_sync.n)) != barrier_sync_id) fail(code::changed);
        // Reads/metadata observations only, before any barrier/denial chmod.
        // A granting ACL could override the mode bits this protocol relies on.
        for (const auto& node : data_anchor.nodes) require_no_granting_acl(node.descriptor.n);
        for (const auto& node : control_anchor.nodes) require_no_granting_acl(node.descriptor.n);
        for (const int descriptor : {data.n, barrier_sync.n, control.n, generation.n}) require_no_granting_acl(descriptor);
        for (const auto& value : items) require_no_granting_acl(value.retained.n);
        for (const auto& value : sync_members) require_no_granting_acl(value.retained.n);
        barrier_intent = encode_barrier(); event(control.n, "barrier.intent", barrier_intent, end);
        use_mutation_barrier = true;
    }
    void freeze_names(deadline end) {
        owner(end);
        if (!use_mutation_barrier || !intent.empty()) fail(code::invalid);
        const auto ds = held(data.n), ss = named(data.n, "sync"); check_kind(ds, true); check_kind(ss, true);
        if (identity(ss) != barrier_sync_id ||
            ((ds.st_mode & 07777) != data_original_mode && (ds.st_mode & 07777) != frozen_mode(data_original_mode)) ||
            ((ss.st_mode & 07777) != sync_original_mode && (ss.st_mode & 07777) != frozen_mode(sync_original_mode))) fail(code::changed);
        if (barrier_sync.n < 0) barrier_sync = fd(::openat(data.n, "sync", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
        if (identity(held(barrier_sync.n)) != barrier_sync_id) fail(code::changed);
        hook(namespace_retirement_test_hooks::boundary::before_mutation_barrier, data_leaf); owner(end);
        // The supported old code does not repair an existing visible root.
        // Even retained directory descriptors cannot bypass these write bits.
        if (::fchmod(data.n, frozen_mode(data_original_mode)) || ::fchmod(barrier_sync.n, frozen_mode(sync_original_mode))) fail(code::durability_unproved);
        sync_dir(barrier_sync.n, "barrier:sync", end);
        sync_dir(data.n, "barrier:data", end);
        sync_dir(data_parent.n, "barrier:data-parent", end);
        if (identity(named(data.n, "sync")) != barrier_sync_id ||
            (held(data.n).st_mode & 07777) != frozen_mode(data_original_mode) ||
            (held(barrier_sync.n).st_mode & 07777) != frozen_mode(sync_original_mode)) fail(code::changed);
        write_barrier_phase("barrier.complete", end);
        hook(namespace_retirement_test_hooks::boundary::after_mutation_barrier, data_leaf);
    }
    void verify_parent_barrier(deadline end) const {
        if (!use_mutation_barrier || intent.empty()) return;
        verify_barrier_complete(end);
        const auto mode = held(data.n).st_mode & 07777;
        if (mode == frozen_mode(data_original_mode)) return;
        if (mode != data_original_mode || !recorded("old-paths.fenced", end)) fail(code::changed);
        for (const auto& value : items) {
            const bool exchanged = swapped(value);
            if ((named(data.n, value.leaf).st_mode & 07777) != (exchanged ? 0 : denied_mode())) fail(code::changed);
        }
    }
    void restore_parent_after_fences(deadline end) {
        if (!use_mutation_barrier) return;
        // An observed mode is not proof that its inode metadata was flushed.
        // This profile permits an actual O_RDONLY metadata reopen at 0400;
        // deny() must finish those flushes before parent mutation is restored.
        if (!recorded("deny.complete", end)) fail(code::durability_unproved);
        // Called only under the caller's still-live terminal custody. Directory
        // write permission is also needed by this same-UID controller to
        // exchange names. Restore it only after every actual source/refusal is
        // write/search denied; never restore any old store inode's original
        // write/search permissions in its old namespace. Exchanged refusals
        // remain mode zero. Pinned old relaunch paths must also be qualified.
        for (const auto& value : items) {
            const bool exchanged = swapped(value);
            if ((named(data.n, value.leaf).st_mode & 07777) != (exchanged ? 0 : denied_mode())) fail(code::incomplete);
            fd original(::openat(exchanged ? destination(value) : data.n, value.leaf.c_str(),
                O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC | (value.directory ? O_DIRECTORY : 0)));
            if (identity(held(original.n)) != value.original) fail(code::changed);
            require_no_granting_acl(original.n);
            if(empty_provisional&&value.directory&&!read_directory_names(original.n,end).empty())fail(code::changed);
        }
        sync_dir(data.n, "final-fences:data", end); sync_dir(generation.n, "final-fences:generation", end);
        mark("old-paths.fenced", end);
        hook(namespace_retirement_test_hooks::boundary::before_parent_restore, data_leaf);
        reconcile(end);
        if (::fchmod(data.n, data_original_mode)) fail(code::durability_unproved);
        sync_dir(data.n, "restored:data", end); sync_dir(data_parent.n, "restored:data-parent", end);
        reconcile(end);
    }
    void discard_uncommitted_refusals(deadline end) {
        // No source name can have been denied/exchanged by this protocol before
        // durable final intent. Only exclusive staging objects are disposable.
        if (has_record("absent.final-census") || has_record("intent.v1") || has_record("deny.started") || has_record("deny.complete") || has_record("swap.started") ||
            has_record("old-paths.fenced") || has_record("preserved.unadopted")) fail(code::changed);
        const auto discard = [&](int target, bool empty_only) {
        for (const auto& leaf : list(target, end)) {
            const auto found = std::find_if(items.begin(), items.end(), [&](const item& value) { return value.leaf == leaf; });
            if (found == items.end() || (empty_only && leaf != "sync") || destination(*found) != target) fail(code::changed);
            const auto before = named(target, leaf); check_kind(before, found->directory);
            if (identity(before) == found->original || identity(before).device != generation_id.device ||
                ((before.st_mode & 07777) != 0 && (before.st_mode & 07777) != (found->directory ? 0700 : 0600))) fail(code::changed);
            bound(end);
            if (::unlinkat(target, leaf.c_str(), found->directory ? AT_REMOVEDIR : 0)) fail(code::durability_unproved);
        }
        sync_dir(target, "uncommitted-refusals", end);
        };
        discard(generation.n,false); if(empty_provisional)discard(empty_slot.n,true);
    }
    void stage_refusals(deadline end) {
        for (auto& value : items) {
            bound(end); const int target = destination(value);
            if (value.directory) {
                if (::mkdirat(target, value.leaf.c_str(), 0700)) fail(code::invalid);
                value.refusal = fd(::openat(target, value.leaf.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
            } else {
                value.refusal = fd(::openat(target, value.leaf.c_str(), O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600));
                const std::array<char, 128> marker{'L','A','T','T','I','C','E','-','R','E','T','I','R','E','D','-','N','O','T','-','A','-','D','A','T','A','B','A','S','E','\n'};
                std::size_t at = 0;
                while (at != marker.size()) {
                    bound(end); const auto written = ::write(value.refusal.n, marker.data() + at, marker.size() - at);
                    if (written < 0 && errno == EINTR) continue;
                    if (written <= 0) fail(code::durability_unproved); at += std::size_t(written);
                }
            }
            const auto s = held(value.refusal.n); check_kind(s, value.directory); value.tombstone = identity(s);
            if (::fchmod(value.refusal.n, 0000)) fail(code::durability_unproved);
            sync_fd(value.refusal.n, end);
        }
        if(empty_provisional)sync_dir(empty_slot.n,"empty-sync",end);
        sync_dir(generation.n, "generation", end); sync_dir(control.n, control_leaf, end); sync_dir(control_parent.n, control_leaf, end);
    }
    std::vector<std::uint8_t> encode() const {
        std::vector<std::uint8_t> b{'L','A','T','N','S','R',std::uint8_t(provisional_root ? '4' : (use_mutation_barrier ? '3' : '1')),0};
        for (const auto id : {data_parent_id, data_id, control_parent_id, control_id, generation_id, lock_id}) append(b, id);
        append(b, data_leaf); append(b, control_leaf); append(b, items.size());
        for (const auto& v : items) { append(b,v.leaf); append(b,v.original); append(b,v.tombstone); append(b,v.mode); append(b,v.directory ? 1 : 0); }
        append(b, sync_members.size());
        for (const auto& v : sync_members) { append(b,v.leaf); append(b,v.original); append(b,v.mode); }
        if (use_mutation_barrier) { append(b, barrier_intent.size()); b.insert(b.end(), barrier_intent.begin(), barrier_intent.end()); }
        return b;
    }
    void decode(const std::vector<std::uint8_t>& b) {
        reader in{b}; const std::vector<std::uint8_t> magic{'L','A','T','N','S','R',std::uint8_t(provisional_root ? '4' : (use_mutation_barrier ? '3' : '1')),0};
        if (b.size() < magic.size() || !std::equal(magic.begin(), magic.end(), b.begin())) fail(code::invalid); in.at = 8;
        for (const auto id : {data_parent_id, data_id, control_parent_id, control_id, generation_id, lock_id}) if (in.id() != id) fail(code::changed);
        if (in.text() != data_leaf || in.text() != control_leaf) fail(code::changed);
        const auto count = in.u64(); if (count < 2 || count > 5) fail(code::invalid);
        std::set<std::string> names;
        std::set<std::pair<std::uint64_t, std::uint64_t>> physical;
        const auto remember = [&](file_identity value) {
            if (!value.inode || value.device != data_id.device || !physical.emplace(value.device, value.inode).second) fail(code::invalid);
        };
        for (const auto value : {data_parent_id, data_id, control_id, generation_id, lock_id}) remember(value);
        // data/control parents may intentionally be the same directory.
        if (control_parent_id != data_parent_id) remember(control_parent_id);
        for (std::uint64_t i = 0; i != count; ++i) {
            item v; v.leaf = in.text(); v.original = in.id(); v.tombstone = in.id(); const auto mode = in.u64(), dir = in.u64();
            if (!names.insert(v.leaf).second || mode > 0777 || (mode & 0022) || dir > 1 || !v.original.inode || !v.tombstone.inode || v.original == v.tombstone) fail(code::invalid);
            v.mode = mode_t(mode); v.directory = dir != 0;
            if ((mode & 07700) != (v.directory ? (empty_provisional ? 0400 : 0700) : 0600)) fail(code::invalid);
            remember(v.original); remember(v.tombstone);
            if ((i == 0 && v.leaf != "memory.sqlite") || (v.directory != (i == count - 1)) ||
                (v.directory ? v.leaf != "sync" : !(v.leaf == "memory.sqlite" || v.leaf == "memory.sqlite-wal" || v.leaf == "memory.sqlite-shm" || v.leaf == "memory.sqlite-journal"))) fail(code::invalid);
            items.push_back(std::move(v));
        }
        const auto members = in.u64(); if (members + count > 256) fail(code::invalid);
        names.clear();
        for (std::uint64_t i = 0; i != members; ++i) {
            member v; v.leaf = in.text(); v.original = in.id(); const auto mode = in.u64();
            if (!names.insert(v.leaf).second || !v.original.inode || mode > 0777 || (mode & 0022)) fail(code::invalid);
            remember(v.original); v.mode = mode_t(mode); sync_members.push_back(std::move(v));
        }
        std::set<std::string> allowed;
        std::size_t mains = 1;
        for (const auto& value : sync_members) if (value.leaf == "memory-synced.sqlite" || group_main(value.leaf)) {
            if (++mains > 64) fail(code::invalid); allowed.insert(value.leaf);
            for (const std::string suffix : {"-wal", "-shm", "-journal"}) allowed.insert(value.leaf + suffix);
        }
        for (const auto* extra : {"groups.json", "groups.json.tmp", "groups-refresh-requested"}) allowed.insert(extra);
        for (const auto& value : sync_members) if (!allowed.contains(value.leaf)) fail(code::invalid);
        if (use_mutation_barrier) {
            const auto count = in.u64();
            if (count != barrier_intent.size() || count > b.size() - in.at ||
                !std::equal(barrier_intent.begin(), barrier_intent.end(), b.begin() + in.at)) fail(code::changed);
            in.at += std::size_t(count);
        }
        if (in.at != b.size() || encode() != b) fail(code::invalid);
    }
    bool swapped(const item& v) const {
        const auto a = named(data.n, v.leaf), b = named(destination(v), v.leaf); check_kind(a, v.directory); check_kind(b, v.directory);
        if (identity(a) == v.original && identity(b) == v.tombstone) {
            if ((b.st_mode & 07777) != 0 || ((a.st_mode & 07777) != denied_mode() && (a.st_mode & 07777) != v.mode &&
                !(use_mutation_barrier && v.directory && ((a.st_mode & 07777) == frozen_mode(v.mode) || (provisional_root && (a.st_mode & 07777) == 0500))))) fail(code::changed);
            return false;
        }
        if (identity(a) == v.tombstone && identity(b) == v.original) {
            if ((a.st_mode & 07777) != 0 || ((b.st_mode & 07777) != denied_mode() && (b.st_mode & 07777) != v.mode)) fail(code::changed);
            return true;
        }
        fail(code::changed);
    }
    retained_historical_generation::observation inspect_managed(
        const retained_managed_runtime_retirement::state&,deadline) const;
    void reconcile(deadline end) const {
        owner(end); verify_parent_barrier(end); for (const auto& v : items) { bound(end); (void)swapped(v); }
        const bool denied_recorded = recorded("deny.complete", end) || recorded("swap.started", end);
        if (denied_recorded) for (const auto& v : items)
            if ((named(data.n, v.leaf).st_mode & 07777) != (swapped(v) ? 0 : denied_mode())) fail(code::changed);
        std::set<std::string> expected;
        for (const auto& v : items) expected.insert(v.leaf);
        for (const auto& name : list(data.n, end)) if (store_shaped(name) && !expected.contains(name)) fail(code::changed);
        if (empty_provisional) {
            expected.erase("sync");
            if (list(empty_slot.n,end) != std::vector<std::string>{"sync"}) fail(code::changed);
        }
        if (list(generation.n, end) != std::vector<std::string>(expected.begin(), expected.end())) fail(code::changed);
    }
    void mark(const std::string& label, deadline end) {
        auto bytes = intent; append(bytes, label); event(control.n, label, std::move(bytes), end);
    }
    bool recorded(const std::string& label, deadline end) const {
        struct stat s{};
        if (::fstatat(control.n, label.c_str(), &s, AT_SYMLINK_NOFOLLOW)) {
            if (errno == ENOENT) return false; fail(code::changed);
        }
        fd input(::openat(control.n, label.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
        auto expected = intent; append(expected, label);
        if (checked_read(input.n, end) != expected || identity(held(input.n)) != identity(s)) fail(code::changed);
        return true;
    }
    void verify_sync_before_deny(deadline end) const {
        const auto& directory = items.back();
        if (swapped(directory) || (named(data.n, directory.leaf).st_mode & 07777) == denied_mode()) return;
        fd opened(::openat(data.n, directory.leaf.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
        if (identity(held(opened.n)) != directory.original) fail(code::changed);
        std::vector<std::string> expected;
        for (const auto& value : sync_members) {
            bound(end); expected.push_back(value.leaf); const auto s = named(opened.n, value.leaf); check_kind(s, false);
            if (identity(s) != value.original || (s.st_mode & 07777) != value.mode) fail(code::changed);
        }
        std::sort(expected.begin(), expected.end()); if (list(opened.n, end) != expected) fail(code::changed);
    }
    void deny(deadline end) {
        reconcile(end); mark("deny.started", end);
        bool unproved = false;
        for (auto& v : items) {
            if (swapped(v)) continue;
            if (v.directory) verify_sync_before_deny(end);
            const auto s = named(data.n, v.leaf);
            if (v.retained.n < 0 && (s.st_mode & 07777) != 0) {
                v.retained = fd(::openat(data.n, v.leaf.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC | (v.directory ? O_DIRECTORY : 0)));
                if (identity(held(v.retained.n)) != v.original) fail(code::changed);
            }
            hook(namespace_retirement_test_hooks::boundary::before_denial, v.leaf); owner(end);
            if (v.retained.n < 0) { unproved = true; continue; } // No temporary reopening on restart.
            if (use_mutation_barrier) require_no_granting_acl(v.retained.n);
            if (identity(held(v.retained.n)) != v.original || ::fchmod(v.retained.n, denied_mode())) fail(code::changed);
            hook(namespace_retirement_test_hooks::boundary::after_denial, v.leaf);
            sync_fd(v.retained.n, end); sync_dir(data.n, v.leaf, end);
            if (identity(named(data.n, v.leaf)) != v.original || (named(data.n, v.leaf).st_mode & 07777) != denied_mode()) fail(code::changed);
        }
        if (unproved) fail(code::durability_unproved);
        reconcile(end); mark("deny.complete", end);
    }
    void preserve(deadline end) {
        reconcile(end);
        // Caller has just checked actual retained created-cohort custody.
        // Never swap a still-writable/searchable source or read completion from
        // an event. A resumed denial may have an unproved fsync; the original
        // remains denied until moved, then its actual descriptor is synced.
        for (const auto& v : items) if (!swapped(v) && (named(data.n, v.leaf).st_mode & 07777) != denied_mode()) fail(code::incomplete);
        restore_parent_after_fences(end);
        mark("swap.started", end);
        for (auto& v : items) {
            if (!swapped(v)) {
                hook(namespace_retirement_test_hooks::boundary::before_swap, v.leaf); reconcile(end);
                exchange(data.n, v.leaf, destination(v));
                hook(namespace_retirement_test_hooks::boundary::after_swap, v.leaf);
            }
            if (!swapped(v)) fail(code::changed);
            // Persist BOTH exchange parents before making the original inode
            // accessible again. This also runs for a resumed already-swapped
            // observation: inode position alone is not crash durability. A
            // failed sync keeps the original write/search-denied and the
            // permanent refusal mode-zero (including the legacy profile).
            sync_dir(data.n, "old-parent:" + v.leaf, end);
            sync_dir(destination(v), "generation-parent:" + v.leaf, end);
            if (!swapped(v)) fail(code::changed);
            if (empty_provisional && v.directory) {
                fd archived(::openat(empty_slot.n,"sync",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
                if (identity(held(archived.n)) != v.original || (held(archived.n).st_mode & 07777) != 0400 ||
                    !read_directory_names(archived.n,end).empty()) fail(code::changed);
                sync_fd(archived.n,end); continue; // private empty object, never historical generation
            }
            // Only the relocated original can regain its old mode, after
            // actual retirement. The original old name is still mode-zero.
            if (::fchmodat(generation.n, v.leaf.c_str(), v.mode, AT_SYMLINK_NOFOLLOW)) fail(code::durability_unproved);
            fd original(::openat(generation.n, v.leaf.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC | (v.directory ? O_DIRECTORY : 0)));
            if (identity(held(original.n)) != v.original || !swapped(v)) fail(code::changed);
            sync_fd(original.n, end); sync_dir(data.n, v.leaf, end); sync_dir(generation.n, v.leaf, end);
            if (v.directory) {
                std::vector<std::string> expected;
                for (const auto& m : sync_members) {
                    expected.push_back(m.leaf); const auto s = named(original.n, m.leaf); check_kind(s, false);
                    if (identity(s) != m.original || (s.st_mode & 07777) != m.mode) fail(code::changed);
                }
                std::sort(expected.begin(), expected.end()); if (list(original.n, end) != expected) fail(code::changed);
            }
        }
        reconcile(end); mark("preserved.unadopted", end);
    }
#else
    implementation(int, const std::string&, int, const std::string&) { fail(code::unavailable); }
#endif
};

engram_namespace_retirement::engram_namespace_retirement(std::shared_ptr<implementation> p) : impl_(std::move(p)) {}
engram_namespace_retirement::~engram_namespace_retirement() = default;
engram_namespace_retirement::engram_namespace_retirement(engram_namespace_retirement&&) noexcept = default;
engram_namespace_retirement& engram_namespace_retirement::operator=(engram_namespace_retirement&&) noexcept = default;
engram_namespace_retirement engram_namespace_retirement::stage_historical(int dp, const std::string& dl,
    int cp, const std::string& cl, deadline end, bool resume,
    const created_launch_cohort::store_namespace* created) {
#ifdef LATTICE_NAMESPACE_POSIX
    auto p=std::make_unique<implementation>(dp,dl,cp,cl);
    if(created&&created->directory_identity()!=p->data_id)fail(code::changed);
    p->open_control(!resume,end,resume);
    if(p->has_record("absent.start")) {
        p->decode_provisional(p->read_phase("absent.start",end));
    } else if(p->has_record("barrier.intent") || p->has_record("intent.v1")) {
        if(created)fail(code::incomplete);
        p.reset(); return resume_with_mutation_barrier(dp,dl,cp,cl,end);
    } else {
        struct stat actual{}, prefix{};
        const bool missing=::fstatat(p->data.n,"sync",&actual,AT_SYMLINK_NOFOLLOW)!=0;
        if(missing&&errno!=ENOENT)fail(code::changed);
        const bool candidate=::fstatat(p->control.n,"provisional.sync",&prefix,AT_SYMLINK_NOFOLLOW)==0;
        if(!candidate&&errno!=ENOENT)fail(code::changed);
        if(!missing&&!candidate&&!p->has_record("pending.absent.start")) {
            if(created)fail(code::incomplete);
            p->verify_unpublished_barrier_prefix(end);p->census(end);p->start_barrier(end);
            p->freeze_names(end);p->census(end);p->stage_refusals(end);p->intent=p->encode();
            event(p->control.n,"intent.v1",p->intent,end);p->reconcile(end);
            return engram_namespace_retirement(std::move(p));
        }
        p->prepare_provisional(end);
    }
    p->load_winner(end);
    if(p->has_record("absent.final-census")) {
        p->decode_primary(p->read_phase("absent.primary",end));p->load_final_census(end);
        p->publish_final_census(end);p->reconcile(end);
    } else {
        p->publish_provisional(end);
        if(created){
            const auto initial=p->encode_provisional();digest binding{};
            picosha2::hash256(initial.begin(),initial.end(),binding.begin(),binding.end());
            created->handoff_to_namespace_retirement(p->data_id,p->control_id,binding,end);
            hook(namespace_retirement_test_hooks::boundary::after_created_handoff,dl);
        }
        p->establish_primary_denial(end);
    }
    return engram_namespace_retirement(std::move(p));
#else
    (void)dp;(void)dl;(void)cp;(void)cl;(void)end;(void)resume;(void)created;fail(code::unavailable);
#endif
}
engram_namespace_retirement engram_namespace_retirement::stage_created_provisional(
    const created_launch_cohort::store_namespace& created,int dp,const std::string& dl,
    int cp,const std::string& cl,deadline end,bool resume){
    return stage_historical(dp,dl,cp,cl,end,resume,&created);
}
void engram_namespace_retirement::finalize_created_provisional_after_join(
    const created_launch_cohort::store_namespace& created,deadline end){
#ifdef LATTICE_NAMESPACE_POSIX
    if(!impl_||!impl_->provisional_root)fail(code::incomplete);
    const auto verify=[&](deadline until){
        if(created.verify_direct_terminal_custody(until)!=impl_->data_id)fail(code::changed);
        const auto initial=impl_->encode_provisional();digest binding{};
        picosha2::hash256(initial.begin(),initial.end(),binding.begin(),binding.end());
        created.verify_namespace_retirement_binding(impl_->control_id,binding,until);
    };
    verify(end);impl_->finalize_absent(verify,end);
#else
    (void)created;(void)end;fail(code::unavailable);
#endif
}
bool engram_namespace_retirement::has_provisional_root() const {
#ifdef LATTICE_NAMESPACE_POSIX
    return impl_&&impl_->provisional_root;
#else
    return false;
#endif
}
historical_namespace_binding engram_namespace_retirement::provisional_binding(deadline end) const {
#ifdef LATTICE_NAMESPACE_POSIX
    if(!impl_)fail(code::unavailable);return impl_->preliminary_binding(end);
#else
    (void)end;fail(code::unavailable);
#endif
}
void engram_namespace_retirement::finalize_provisional_root(const provisional_namespace_custody& custody,deadline end) {
#ifdef LATTICE_NAMESPACE_POSIX
    const auto binding=provisional_binding(end);
    if(!custody.verify_||custody.data_!=binding.data||custody.control_!=binding.control||custody.binding_!=binding.barrier)
        fail(code::incomplete);
    impl_->finalize_absent(custody.verify_,end);
#else
    (void)custody;(void)end;fail(code::unavailable);
#endif
}
engram_namespace_retirement engram_namespace_retirement::stage(int dp, const std::string& dl, int cp, const std::string& cl, deadline end) {
    bound(end); auto p = std::make_unique<implementation>(dp, dl, cp, cl);
#ifdef LATTICE_NAMESPACE_POSIX
    p->census(end); p->open_control(true, end); p->stage_refusals(end); p->intent = p->encode();
    event(p->control.n, "intent.v1", p->intent, end); p->reconcile(end);
#endif
    return engram_namespace_retirement(std::move(p));
}
engram_namespace_retirement engram_namespace_retirement::stage_with_mutation_barrier(int dp, const std::string& dl, int cp, const std::string& cl, deadline end) {
    bound(end); auto p = std::make_unique<implementation>(dp, dl, cp, cl);
#ifdef LATTICE_NAMESPACE_POSIX
    p->census(end); p->open_control(true, end); p->start_barrier(end); p->freeze_names(end);
    p->census(end); p->stage_refusals(end); p->intent = p->encode();
    event(p->control.n, "intent.v1", p->intent, end); p->reconcile(end);
#endif
    return engram_namespace_retirement(std::move(p));
}
engram_namespace_retirement engram_namespace_retirement::stage_created_with_mutation_barrier(
    const created_launch_cohort::store_namespace& space, int dp, const std::string& dl,
    int cp, const std::string& cl, deadline end) {
    bound(end); auto p = std::make_unique<implementation>(dp, dl, cp, cl);
#ifdef LATTICE_NAMESPACE_POSIX
    // Reject a wrong live namespace before creating any controller records.
    if (space.directory_identity() != p->data_id) fail(code::changed);
    p->census(end); p->open_control(true, end); p->start_barrier(end);
    digest binding{}; picosha2::hash256(p->barrier_intent.begin(), p->barrier_intent.end(), binding.begin(), binding.end());
    space.handoff_to_namespace_retirement(p->data_id, p->control_id, binding, end);
    hook(namespace_retirement_test_hooks::boundary::after_created_handoff, dl);
    p->freeze_names(end); p->census(end); p->stage_refusals(end); p->intent = p->encode();
    event(p->control.n, "intent.v1", p->intent, end); p->reconcile(end);
#else
    (void)space;
#endif
    return engram_namespace_retirement(std::move(p));
}
engram_namespace_retirement engram_namespace_retirement::resume_impl(int dp, const std::string& dl, int cp, const std::string& cl, deadline end, bool recover_staging) {
    bound(end); auto p = std::make_unique<implementation>(dp, dl, cp, cl);
#ifdef LATTICE_NAMESPACE_POSIX
    p->open_control(false, end, recover_staging);
    if (p->has_record("barrier.intent")) {
        fd barrier(::openat(p->control.n, "barrier.intent", O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
        if (identity(held(barrier.n)) != identity(named(p->control.n, "barrier.intent"))) fail(code::changed);
        p->decode_barrier(checked_read(barrier.n, end));
    } else if (recover_staging && !p->has_record("intent.v1")) {
        // Only this explicit protocol entry can recover an unpublished
        // mutation-barrier prefix. Final records are never replaced, and a
        // partial legacy mode-zero stage cannot be guessed or upgraded.
        p->verify_unpublished_barrier_prefix(end);
        p->census(end); p->start_barrier(end);
    }
    if (p->use_mutation_barrier && !p->has_record("intent.v1")) {
        // Final member authority was never sealed. Re-establish actual
        // directory barriers before the only permitted fresh census.
        p->freeze_names(end); p->census(end); p->discard_uncommitted_refusals(end); p->stage_refusals(end); p->intent = p->encode();
        event(p->control.n, "intent.v1", p->intent, end);
    }
    if (p->intent.empty()) {
        fd input(::openat(p->control.n, "intent.v1", O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
        if (identity(held(input.n)) != identity(named(p->control.n, "intent.v1"))) fail(code::changed);
        p->intent = checked_read(input.n, end); p->decode(p->intent);
        if (p->use_mutation_barrier && (p->items.back().original != p->barrier_sync_id || p->items.back().mode != p->sync_original_mode)) fail(code::changed);
    }
    p->reconcile(end);
#else
    (void)recover_staging;
#endif
    return engram_namespace_retirement(std::move(p));
}
engram_namespace_retirement engram_namespace_retirement::resume(int dp, const std::string& dl, int cp, const std::string& cl, deadline end) {
    return resume_impl(dp, dl, cp, cl, end, false);
}
engram_namespace_retirement engram_namespace_retirement::resume_with_mutation_barrier(int dp, const std::string& dl, int cp, const std::string& cl, deadline end) {
    return resume_impl(dp, dl, cp, cl, end, true);
}
void engram_namespace_retirement::deny_new_opens(deadline end) {
#ifdef LATTICE_NAMESPACE_POSIX
    if (!impl_) fail(code::unavailable); impl_->deny(end);
#else
    (void)end; fail(code::unavailable);
#endif
}
namespace_retirement_observation engram_namespace_retirement::observe(deadline end) const {
#ifdef LATTICE_NAMESPACE_POSIX
    if (!impl_) fail(code::unavailable); impl_->reconcile(end); namespace_retirement_observation out;
    out.data = impl_->data_id; out.control = impl_->control_id; bool all = true;
    for (const auto& v : impl_->items) {
        const bool swapped = impl_->swapped(v), denied = (named(impl_->data.n, v.leaf).st_mode & 07777) == (swapped ? 0 : impl_->denied_mode());
        if (v.leaf == "memory.sqlite") { out.primary_denied = denied; out.primary_swapped = swapped; }
        if (v.leaf == "sync") { out.sync_denied = denied; out.sync_swapped = swapped; }
        all = all && swapped;
    }
    struct stat s{};
    if (::fstatat(impl_->control.n, "preserved.unadopted", &s, AT_SYMLINK_NOFOLLOW) == 0) {
        fd record(::openat(impl_->control.n, "preserved.unadopted", O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
        auto expected = impl_->intent; append(expected, std::string("preserved.unadopted"));
        if (checked_read(record.n, end) != expected || !all) fail(code::changed);
        out.preserved_unadopted = true;
    } else if (errno != ENOENT) fail(code::changed);
    return out;
#else
    (void)end; fail(code::unavailable);
#endif
}
historical_namespace_binding engram_namespace_retirement::denied_historical_binding(deadline end) const {
#ifdef LATTICE_NAMESPACE_POSIX
    if(!impl_||!impl_->use_mutation_barrier)fail(code::incomplete);
    impl_->reconcile(end);
    if(!impl_->recorded("deny.complete",end))fail(code::incomplete);
    historical_namespace_binding out;out.data=impl_->data_id;out.control=impl_->control_id;
    picosha2::hash256(impl_->barrier_intent.begin(),impl_->barrier_intent.end(),out.barrier.begin(),out.barrier.end());
    for(const auto& value:impl_->items)out.originals.push_back(value.original);
    for(const auto& value:impl_->sync_members)out.originals.push_back(value.original);
    return out;
#else
    (void)end;fail(code::unavailable);
#endif
}
void engram_namespace_retirement::preserve_historical_namespace(const historical_namespace_custody& custody,deadline end){
#ifdef LATTICE_NAMESPACE_POSIX
    const auto binding=denied_historical_binding(end);
    if(!custody.verify_||custody.data_!=binding.data||custody.control_!=binding.control||custody.barrier_!=binding.barrier)
        fail(code::incomplete);
    custody.verify_(end);
    impl_->preserve(end);
    custody.verify_(end);
#else
    (void)custody;(void)end;fail(code::unavailable);
#endif
}
retained_historical_generation engram_namespace_retirement::retain_preserved_generation(
    const historical_namespace_custody& custody, deadline end) {
#ifdef LATTICE_NAMESPACE_POSIX
    const auto binding = denied_historical_binding(end);
    if (!custody.verify_ || custody.data_ != binding.data || custody.control_ != binding.control ||
        custody.barrier_ != binding.barrier) fail(code::incomplete);
    custody.verify_(end);
    if (!observe(end).preserved_unadopted) fail(code::incomplete);
    auto owner = impl_;
    auto verify_retirement = custody.verify_;
    auto state = std::make_shared<retained_historical_generation::implementation>();
    state->verify = [owner, verify_retirement, binding, original_end = end](deadline check_end) {
        if (check_end != original_end) fail(code::invalid);
        verify_retirement(check_end);
        owner->reconcile(check_end);
        if (!owner->recorded("preserved.unadopted", check_end)) fail(code::incomplete);
        digest barrier{};
        picosha2::hash256(owner->barrier_intent.begin(), owner->barrier_intent.end(), barrier.begin(), barrier.end());
        if (owner->data_id != binding.data || owner->control_id != binding.control || barrier != binding.barrier)
            fail(code::changed);
        retained_historical_generation::observation result;
        result.historical_parent = owner->data_id; result.control = owner->control_id;
        result.generation = owner->generation_id; result.barrier = barrier;
        if (owner->items.size() + owner->sync_members.size() > 256) fail(code::invalid);
        result.members.reserve(owner->items.size() + owner->sync_members.size());
        for (const auto& value : owner->items) {
            // The empty provisional sync refusal is not an existing store,
            // but its permanent old-path fence is still part of the handoff.
            result.old_refusals.push_back({value.leaf,value.original,value.tombstone,std::uint32_t(value.mode),value.directory});
            if (owner->empty_provisional && value.leaf == "sync") continue;
            const auto old = named(owner->data.n, value.leaf);
            const auto current = named(owner->generation.n, value.leaf);
            if (!owner->swapped(value) || (old.st_mode & 07777) != 0 ||
                identity(current) != value.original || (current.st_mode & 07777) != value.mode)
                fail(code::changed);
            check_kind(current, value.directory);
            result.members.push_back({value.leaf, value.original, identity(old), std::uint32_t(value.mode), value.directory});
        }
        if (!owner->sync_members.empty()) {
            fd sync(::openat(owner->generation.n, "sync", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
            const auto sync_item = std::find_if(owner->items.begin(), owner->items.end(),
                [](const auto& value) { return value.leaf == "sync" && value.directory; });
            if (sync_item == owner->items.end() || identity(held(sync.n)) != sync_item->original)
                fail(code::changed);
            std::vector<std::string> expected;
            expected.reserve(owner->sync_members.size());
            for (const auto& value : owner->sync_members) {
                const auto current = named(sync.n, value.leaf); check_kind(current, false);
                if (identity(current) != value.original || (current.st_mode & 07777) != value.mode)
                    fail(code::changed);
                expected.push_back(value.leaf);
                result.members.push_back({"sync/" + value.leaf, value.original, {}, std::uint32_t(value.mode), false});
            }
            std::sort(expected.begin(), expected.end());
            if (list(sync.n, check_end) != expected) fail(code::changed);
        }
        verify_retirement(check_end); owner->reconcile(check_end);
        return result;
    };
    // Revalidation occurs both before and after duplicating the actual held
    // generation. The duplicate is used only by the fixed managed launcher.
    auto attempt = std::make_shared<retained_historical_generation::implementation::drain_attempt>();
    state->drain_state = attempt;
    state->duplicate_generation = [owner, verify = state->verify, attempt](deadline check_end) {
        (void)verify(check_end);
        bound(check_end);
        if (attempt->first_failure) std::rethrow_exception(attempt->first_failure);
        if (!attempt->entered || !attempt->terminal_success) fail(code::incomplete);
        auto copy = duplicate(owner->generation.n);
        if (identity(held(copy.n)) != owner->generation_id) fail(code::changed);
        (void)verify(check_end);
        bound(check_end);
        if (attempt->first_failure) std::rethrow_exception(attempt->first_failure);
        if (!attempt->entered || !attempt->terminal_success) fail(code::incomplete);
        return std::exchange(copy.n, -1);
    };
    // This private operation follows actual old-role/namespace retirement. The
    // generic filesystem transition above still never opens SQLite. Exact old
    // names, sidecar identities and permanent refusals remain verifier inputs.
    state->drain = [owner, verify = state->verify, attempt, original_end = end](deadline until) {
        if (until != original_end) fail(code::invalid);
        const auto observed = verify(until); // Actual creator/currentness before mutable attempt facts.
        if (attempt->entered) {
            if (attempt->first_failure) std::rethrow_exception(attempt->first_failure);
            fail(code::incomplete); // A successful first call never reissues an attempt.
        }
        attempt->entered = true; // Before any checkpoint/native-close effect.
        try {
            const std::function<void(deadline)> reverify = [verify, original_end](deadline check_end) {
                if (check_end != original_end) fail(code::invalid);
                (void)verify(check_end);
            };
            historical_wal_checkpoint::drain(owner->generation.n, observed, until, reverify);
            (void)verify(until); // No expired, changed or unproved ending becomes success.
            bound(until);
            attempt->terminal_success = true;
        } catch (...) {
            attempt->first_failure = std::current_exception();
            throw;
        }
    };
    (void)state->verify(end);
    return retained_historical_generation(std::move(state));
#else
    (void)custody; (void)end; fail(code::unavailable);
#endif
}
#include "ordinary_managed_namespace_restart.inc"

retained_historical_generation::observation retained_historical_generation::before_launch(deadline end) const {
    if (!impl_ || !impl_->verify) fail(code::incomplete);
    auto observed = impl_->verify(end);
    bound(end);
    const auto& attempt = impl_->drain_state;
    if (!attempt) fail(code::incomplete);
    if (attempt->first_failure) std::rethrow_exception(attempt->first_failure);
    if (!attempt->entered || !attempt->terminal_success) fail(code::incomplete);
    return observed;
}
int retained_historical_generation::duplicate_before_launch(deadline end) const {
    if (!impl_ || !impl_->duplicate_generation) fail(code::incomplete);
    return impl_->duplicate_generation(end);
}
void retained_historical_generation::drain_oversized_wals(deadline end) const {
    if (!impl_ || !impl_->drain) fail(code::incomplete);
    impl_->drain(end);
}

void engram_namespace_retirement::preserve_created_namespace_after_join(const created_launch_cohort::store_namespace& custody, deadline end) {
#ifdef LATTICE_NAMESPACE_POSIX
    if (!impl_ || custody.verify_direct_terminal_custody(end) != impl_->data_id) fail(code::incomplete);
    if (impl_->use_mutation_barrier) {
        digest binding{};
        const auto bound_intent=impl_->provisional_root?impl_->encode_provisional():impl_->barrier_intent;
        picosha2::hash256(bound_intent.begin(), bound_intent.end(), binding.begin(), binding.end());
        custody.verify_namespace_retirement_binding(impl_->control_id, binding, end);
    }
    impl_->preserve(end);
    if (custody.verify_direct_terminal_custody(end) != impl_->data_id) fail(code::changed);
#else
    (void)custody; (void)end; fail(code::unavailable);
#endif
}
}
