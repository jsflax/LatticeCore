#include "ordinary_store_admission.hpp"
#include "vendor/picosha2/picosha2.h"
#include <algorithm>
#include <cstdio>
#include <limits>
#include <utility>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <cerrno>
#include <dirent.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#define LATTICE_ORDINARY_JOURNAL_POSIX 1
#endif

namespace lattice::detail::ordinary_admission {
namespace {
[[noreturn]] void fail(error_code code, const char* message) { throw error(code, message); }
bool empty(const identifier& id) { return std::all_of(id.begin(), id.end(), [](auto c) { return c == 0; }); }
void validate(const record& value) {
    if (!value.revision || empty(value.binding.installation) || empty(value.binding.store) ||
        empty(value.binding.epoch) || !value.binding.main.inode || !value.binding.parent.inode ||
        !value.control.inode || !value.entry.inode || !value.generation.inode ||
        value.entry == value.generation ||
        (value.state != stage::unadopted && value.state != stage::retirement_requested) ||
        (value.state == stage::unadopted ? !empty(value.cutover) : empty(value.cutover)))
        fail(error_code::invalid_record, "ordinary admission record invalid");
}
void put64(encoded_record& bytes, std::size_t& at, std::uint64_t value) {
    for (unsigned shift = 0; shift != 64; shift += 8) bytes[at++] = static_cast<std::uint8_t>(value >> shift);
}
std::uint64_t get64(const encoded_record& bytes, std::size_t& at) {
    std::uint64_t value = 0;
    for (unsigned shift = 0; shift != 64; shift += 8) value |= std::uint64_t(bytes[at++]) << shift;
    return value;
}
}

encoded_record encode(const record& value) {
    validate(value);
    encoded_record bytes{};
    constexpr std::array<std::uint8_t, 8> magic{'L','A','T','A','D','M','1',0};
    std::copy(magic.begin(), magic.end(), bytes.begin());
    bytes[8] = 1; // u32 version, little endian
    bytes[13] = 1; // u32 length = 256
    std::size_t at = 16;
    put64(bytes, at, value.revision);
    bytes[24] = static_cast<std::uint8_t>(value.state);
    at = 32;
    for (const auto* id : {&value.binding.installation, &value.binding.store, &value.binding.epoch, &value.cutover}) {
        std::copy(id->begin(), id->end(), bytes.begin() + at); at += id->size();
    }
    for (const auto* identity : {&value.binding.main, &value.binding.parent, &value.control, &value.entry, &value.generation}) {
        put64(bytes, at, identity->device); put64(bytes, at, identity->inode);
    }
    picosha2::hash256(bytes.begin(), bytes.begin() + 224, bytes.begin() + 224, bytes.end());
    return bytes;
}

record decode(const encoded_record& bytes) {
    record value;
    std::size_t at = 16;
    value.revision = get64(bytes, at);
    value.state = static_cast<stage>(bytes[24]);
    at = 32;
    for (auto* id : {&value.binding.installation, &value.binding.store, &value.binding.epoch, &value.cutover}) {
        std::copy_n(bytes.begin() + at, id->size(), id->begin()); at += id->size();
    }
    for (auto* identity : {&value.binding.main, &value.binding.parent, &value.control, &value.entry, &value.generation}) {
        identity->device = get64(bytes, at); identity->inode = get64(bytes, at);
    }
    // Re-encoding also verifies magic/version/size, reserved zeros, enum/ID
    // invariants, and the complete checksum. No permissive version fallback.
    if (encode(value) != bytes) fail(error_code::invalid_record, "ordinary admission bytes invalid");
    return value;
}

#ifdef LATTICE_ORDINARY_JOURNAL_POSIX
namespace {
struct fd_owner {
    int fd = -1;
    explicit fd_owner(int fd = -1) noexcept : fd(fd) {}
    ~fd_owner() { if (fd >= 0) ::close(fd); }
    fd_owner(fd_owner&& other) noexcept : fd(std::exchange(other.fd, -1)) {}
    fd_owner& operator=(fd_owner&& other) noexcept {
        if (this != &other) { if (fd >= 0) ::close(fd); fd = std::exchange(other.fd, -1); } return *this;
    }
    fd_owner(const fd_owner&) = delete;
    int release() noexcept { return std::exchange(fd, -1); }
};
file_identity identity(const struct stat& st) {
    if constexpr (sizeof(st.st_dev) > sizeof(std::uint64_t) || sizeof(st.st_ino) > sizeof(std::uint64_t))
        fail(error_code::unavailable, "ordinary admission native identity width unavailable");
    return {static_cast<std::uint64_t>(st.st_dev), static_cast<std::uint64_t>(st.st_ino)};
}
struct stat inspect(int fd, bool directory) {
    struct stat st{};
    if (::fstat(fd, &st) || st.st_uid != ::geteuid() ||
        (directory ? (!S_ISDIR(st.st_mode) || (st.st_mode & 07777) != 0700)
                   : (!S_ISREG(st.st_mode) || (st.st_mode & 07777) != 0600 || st.st_nlink != 1)))
        fail(error_code::unavailable, "ordinary admission control ownership or mode invalid");
    return st;
}
fd_owner file(int dir, const char* name, bool create = false) {
    if (!create) {
        struct stat named{};
        if (::fstatat(dir, name, &named, AT_SYMLINK_NOFOLLOW) || !S_ISREG(named.st_mode))
            fail(error_code::unavailable, "ordinary admission control name is not a regular file");
    }
    fd_owner fd(::openat(dir, name, O_RDWR | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC | (create ? O_CREAT | O_EXCL : 0), 0600));
    if (fd.fd < 0) fail(error_code::unavailable, "ordinary admission control file unavailable");
    inspect(fd.fd, false); return fd;
}
void lock(int fd, int mode) {
    if (::flock(fd, mode | LOCK_NB))
        fail(errno == EWOULDBLOCK || errno == EAGAIN ? error_code::busy : error_code::unavailable,
             "ordinary admission control lock unavailable");
}
void fault(test_hooks::boundary point) { if (test_hooks::current) test_hooks::current(point); }
void durable(int fd) {
    if (::fsync(fd)) fail(error_code::durability_unproved, "ordinary admission durability unproved");
}
}

struct journal::implementation {
    fd_owner directory;
    const pid_t creator = ::getpid();
    store_binding binding;
    file_identity control;
    file_identity accepted_entry{}, accepted_generation{};
    bool locks_bound = false;
    explicit implementation(int fd, const store_binding& binding)
        : directory(::fcntl(fd, F_DUPFD_CLOEXEC, 0)), binding(binding) {
        if (directory.fd < 0) fail(error_code::unavailable, "ordinary admission control directory unavailable");
        control = identity(inspect(directory.fd, true));
    }
    void check() const {
        if (::getpid() != creator) fail(error_code::inherited_use, "ordinary admission inherited use refused");
        if (identity(inspect(directory.fd, true)) != control)
            fail(error_code::identity_changed, "ordinary admission control directory changed");
    }
    fd_owner entry() const {
        check(); auto fd = file(directory.fd, "entry.lock");
        if (locks_bound) check_named("entry.lock", fd.fd, accepted_entry);
        lock(fd.fd, LOCK_EX); return fd;
    }
    void check_named(const char* name, int fd, const file_identity& expected) const {
        struct stat named{};
        if (identity(inspect(fd, false)) != expected ||
            ::fstatat(directory.fd, name, &named, AT_SYMLINK_NOFOLLOW) || !S_ISREG(named.st_mode) ||
            identity(named) != expected)
            fail(error_code::identity_changed, "ordinary admission control object changed");
    }
    void bind_locks(const record& value) {
        if (locks_bound) fail(error_code::identity_changed, "ordinary admission control binding is immutable");
        accepted_entry = value.entry; accepted_generation = value.generation; locks_bound = true;
    }
    record load(int entry_fd) const {
        struct stat pending{};
        if (::fstatat(directory.fd, "admission.pending", &pending, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT)
            fail(error_code::durability_unproved, "ordinary admission unfinished publication retained");
        auto source = file(directory.fd, "admission.v1");
        const auto source_stat = inspect(source.fd, false);
        const auto source_identity = identity(source_stat);
        if (source_stat.st_size != static_cast<off_t>(encoded_record{}.size()))
            fail(error_code::invalid_record, "ordinary admission snapshot size invalid");
        encoded_record bytes{}; std::size_t done = 0;
        while (done < bytes.size()) {
            const auto count = ::read(source.fd, bytes.data() + done, bytes.size() - done);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) fail(error_code::invalid_record, "ordinary admission snapshot incomplete");
            done += static_cast<std::size_t>(count);
        }
        char extra;
        const auto tail = ::read(source.fd, &extra, 1);
        if (tail != 0) fail(error_code::invalid_record, "ordinary admission snapshot trailing bytes");
        auto value = decode(bytes);
        if (value.binding != binding || value.control != control)
            fail(error_code::identity_changed, "ordinary admission binding changed");
        // Retained instances must never let a coherent rewritten snapshot
        // redefine the lock objects while an old generation hold still lives.
        if (locks_bound && (value.entry != accepted_entry || value.generation != accepted_generation))
            fail(error_code::identity_changed, "ordinary admission retained lock binding changed");
        check_named("admission.v1", source.fd, source_identity);
        check_named("entry.lock", entry_fd, value.entry);
        auto generation = file(directory.fd, "generation.lock");
        check_named("generation.lock", generation.fd, value.generation);
        // An earlier caller could have lost the response after rename but
        // before directory fsync. Re-establish durability before a retry can
        // report the same intent or obtain even a non-admitting hold.
        durable(source.fd); durable(directory.fd);
        check_named("admission.v1", source.fd, source_identity);
        return value;
    }
    void save(const record& value) const {
        const auto bytes = encode(value);
        auto pending = file(directory.fd, "admission.pending", true);
        const auto pending_identity = identity(inspect(pending.fd, false));
        fault(test_hooks::boundary::before_write);
        std::size_t done = 0;
        while (done < bytes.size()) {
            const auto count = ::write(pending.fd, bytes.data() + done, bytes.size() - done);
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) fail(error_code::durability_unproved, "ordinary admission write incomplete");
            done += static_cast<std::size_t>(count);
        }
        fault(test_hooks::boundary::after_write); durable(pending.fd);
        fault(test_hooks::boundary::after_file_sync);
        check_named("admission.pending", pending.fd, pending_identity);
        if (::renameat(directory.fd, "admission.pending", directory.fd, "admission.v1"))
            fail(error_code::durability_unproved, "ordinary admission snapshot publication failed");
        fault(test_hooks::boundary::after_rename);
        check_named("admission.v1", pending.fd, pending_identity);
        durable(directory.fd);
        fault(test_hooks::boundary::after_directory_sync);
        check_named("admission.v1", pending.fd, pending_identity);
    }
};

journal journal::create_unadopted(int directory_fd, const store_binding& binding) {
    auto state = std::make_unique<implementation>(directory_fd, binding);
    // Validate caller IDs before creating files. These are bindings, never
    // caller-provided claims about old writers or source-intake retirement.
    record value; value.binding = binding; value.control = state->control;
    value.entry = {0, 1}; value.generation = {0, 2}; (void)encode(value);
    fd_owner scan_fd(::openat(state->directory.fd, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    if (scan_fd.fd < 0) fail(error_code::unavailable, "ordinary admission directory inspection unavailable");
    DIR* raw_scan = ::fdopendir(scan_fd.fd);
    if (!raw_scan) fail(error_code::unavailable, "ordinary admission directory inspection failed");
    (void)scan_fd.release();
    const std::unique_ptr<DIR, decltype(&::closedir)> scan(raw_scan, &::closedir);
    errno = 0;
    while (const auto* item = ::readdir(scan.get())) {
        const std::string name(item->d_name);
        if (name != "." && name != "..") fail(error_code::unavailable, "ordinary admission bootstrap directory is not empty");
        errno = 0;
    }
    if (errno) fail(error_code::unavailable, "ordinary admission directory inspection incomplete");
    auto entry = file(state->directory.fd, "entry.lock", true); lock(entry.fd, LOCK_EX);
    auto generation = file(state->directory.fd, "generation.lock", true);
    value.entry = identity(inspect(entry.fd, false)); value.generation = identity(inspect(generation.fd, false));
    state->bind_locks(value);
    durable(entry.fd); durable(generation.fd); durable(state->directory.fd);
    // An existing snapshot/pending record, even with missing lock objects, is
    // not an empty installation. Never overwrite it as bootstrap.
    struct stat prior{};
    if (::fstatat(state->directory.fd, "admission.v1", &prior, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT)
        fail(error_code::unavailable, "ordinary admission bootstrap is not fresh");
    state->save(value); return journal(std::move(state));
}
journal journal::open_existing(int directory_fd, const store_binding& expected) {
    auto state = std::make_unique<implementation>(directory_fd, expected);
    auto entry = state->entry(); const auto value = state->load(entry.fd);
    state->bind_locks(value); return journal(std::move(state));
}
journal journal::open_existing(int directory_fd, const store_binding& expected,
                               const control_binding& expected_controls) {
    if (!expected_controls.control.inode || !expected_controls.entry.inode ||
        !expected_controls.generation.inode || expected_controls.entry == expected_controls.generation)
        fail(error_code::invalid_record, "ordinary admission expected control binding invalid");
    auto state = std::make_unique<implementation>(directory_fd, expected);
    if (state->control != expected_controls.control)
        fail(error_code::identity_changed, "ordinary admission anchored control directory changed");
    // Bind before opening entry.lock or reading admission.v1. A coherent
    // replacement snapshot must not redefine first-open expectations.
    record anchored; anchored.entry = expected_controls.entry;
    anchored.generation = expected_controls.generation;
    state->bind_locks(anchored);
    auto entry = state->entry(); (void)state->load(entry.fd);
    return journal(std::move(state));
}
record journal::read() const {
    if (!impl_) fail(error_code::unavailable, "ordinary admission moved journal");
    auto entry = impl_->entry(); return impl_->load(entry.fd);
}
generation_hold journal::try_hold_generation() const {
    if (!impl_) fail(error_code::unavailable, "ordinary admission moved journal");
    auto entry = impl_->entry(); const auto value = impl_->load(entry.fd);
    if (value.state != stage::unadopted) fail(error_code::busy, "ordinary admission retirement intent closes holds");
    auto lease = file(impl_->directory.fd, "generation.lock");
    impl_->check_named("generation.lock", lease.fd, value.generation);
    lock(lease.fd, LOCK_SH); return generation_hold(lease.release());
}
record journal::begin_retirement(const identifier& cutover) {
    if (!impl_) fail(error_code::unavailable, "ordinary admission moved journal");
    if (empty(cutover)) fail(error_code::invalid_record, "ordinary admission cutover is empty");
    auto entry = impl_->entry(); auto value = impl_->load(entry.fd);
    if (value.state == stage::retirement_requested) {
        if (value.cutover != cutover) fail(error_code::conflicting_cutover, "ordinary admission cutover conflicts");
        return value;
    }
    if (value.revision == std::numeric_limits<std::uint64_t>::max())
        fail(error_code::revision_exhausted, "ordinary admission revision exhausted");
    ++value.revision; value.state = stage::retirement_requested; value.cutover = cutover;
    impl_->save(value); return value;
}
bool journal::generation_busy() const {
    if (!impl_) fail(error_code::unavailable, "ordinary admission moved journal");
    auto entry = impl_->entry(); const auto value = impl_->load(entry.fd);
    auto lease = file(impl_->directory.fd, "generation.lock");
    impl_->check_named("generation.lock", lease.fd, value.generation);
    if (::flock(lease.fd, LOCK_EX | LOCK_NB) == 0) return false;
    if (errno == EWOULDBLOCK || errno == EAGAIN) return true;
    fail(error_code::unavailable, "ordinary admission generation probe unavailable");
}
generation_hold::~generation_hold() { if (fd_ >= 0) ::close(fd_); }
generation_hold& generation_hold::operator=(generation_hold&& other) noexcept {
    if (this != &other) { if (fd_ >= 0) ::close(fd_); fd_ = std::exchange(other.fd_, -1); } return *this;
}
#else
struct journal::implementation {};
namespace { [[noreturn]] void unsupported() { fail(error_code::unavailable, "ordinary admission platform unavailable"); } }
journal journal::create_unadopted(int, const store_binding&) { unsupported(); }
journal journal::open_existing(int, const store_binding&) { unsupported(); }
journal journal::open_existing(int, const store_binding&, const control_binding&) { unsupported(); }
record journal::read() const { unsupported(); }
generation_hold journal::try_hold_generation() const { unsupported(); }
record journal::begin_retirement(const identifier&) { unsupported(); }
bool journal::generation_busy() const { unsupported(); }
generation_hold::~generation_hold() = default;
generation_hold& generation_hold::operator=(generation_hold&& other) noexcept {
    fd_ = std::exchange(other.fd_, -1); return *this;
}
#endif
generation_hold::generation_hold(generation_hold&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
journal::journal(std::unique_ptr<implementation> value) : impl_(std::move(value)) {}
journal::~journal() = default;
journal::journal(journal&&) noexcept = default;
journal& journal::operator=(journal&&) noexcept = default;
} // namespace lattice::detail::ordinary_admission
