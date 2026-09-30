#include "ordinary_installation_files.hpp"
#include "vendor/picosha2/picosha2.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <limits>
#include <thread>
#include <utility>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#define LATTICE_INSTALLATION_FILES_POSIX 1
#endif

namespace lattice::detail::ordinary_installation {
namespace {
[[noreturn]] void file_fail(file_error_code code) { throw file_error(code); }
#ifdef LATTICE_INSTALLATION_FILES_POSIX
struct retained_fd {
    int value = -1;
    explicit retained_fd(int fd = -1) : value(fd) {}
    ~retained_fd() { if (value >= 0) ::close(value); }
    retained_fd(retained_fd&& value) noexcept : value(std::exchange(value.value, -1)) {}
    retained_fd& operator=(retained_fd&& other) noexcept {
        if (this != &other) { if (value >= 0) ::close(value); value = std::exchange(other.value, -1); }
        return *this;
    }
    retained_fd(const retained_fd&) = delete;
};
file_identity observed_identity(const struct stat& value) {
    if constexpr (sizeof(value.st_dev) > sizeof(std::uint64_t) || sizeof(value.st_ino) > sizeof(std::uint64_t))
        file_fail(file_error_code::unavailable);
    return {static_cast<std::uint64_t>(value.st_dev), static_cast<std::uint64_t>(value.st_ino)};
}
void before(ordinary_launch::deadline end) {
    if (std::chrono::steady_clock::now() >= end) file_fail(file_error_code::timed_out);
}
void valid_leaf(const std::string& leaf) {
    if (leaf.empty() || leaf.size() > 255 || leaf == "." || leaf == ".." ||
        leaf.find('/') != std::string::npos || leaf.find('\0') != std::string::npos) file_fail(file_error_code::invalid_path);
}
struct stat inspect_directory(int fd) {
    struct stat value{};
    if (::fstat(fd, &value) || !S_ISDIR(value.st_mode) ||
        (value.st_uid != ::geteuid() && value.st_uid != 0) || (value.st_mode & 0022) != 0)
        file_fail(file_error_code::unavailable);
    return value;
}
// Comparing both identity and stable metadata detects ordinary concurrent
// edits. This is not a claim against hostile in-place mutation by trusted code.
bool same_metadata(const struct stat& a, const struct stat& b) {
#if defined(__APPLE__)
    const bool times = a.st_mtimespec.tv_sec == b.st_mtimespec.tv_sec && a.st_mtimespec.tv_nsec == b.st_mtimespec.tv_nsec &&
        a.st_ctimespec.tv_sec == b.st_ctimespec.tv_sec && a.st_ctimespec.tv_nsec == b.st_ctimespec.tv_nsec;
#else
    const bool times = a.st_mtim.tv_sec == b.st_mtim.tv_sec && a.st_mtim.tv_nsec == b.st_mtim.tv_nsec &&
        a.st_ctim.tv_sec == b.st_ctim.tv_sec && a.st_ctim.tv_nsec == b.st_ctim.tv_nsec;
#endif
    return observed_identity(a) == observed_identity(b) && a.st_size == b.st_size &&
        a.st_mode == b.st_mode && a.st_uid == b.st_uid && a.st_nlink == b.st_nlink && times;
}
#endif
}

#ifdef LATTICE_INSTALLATION_FILES_POSIX
struct retained_installation_file::implementation {
    const pid_t creator = ::getpid();
    const std::thread::id thread = std::this_thread::get_id();
    struct parent { retained_fd fd; file_identity identity; std::string name; };
    std::vector<parent> parents;
    retained_fd file;
    std::string leaf;
    file_identity expected;
    digest content{};
    std::size_t limit = 0;
    bool executable = false;
    mutable bool poisoned = false;
    void owner() const {
        if (::getpid() != creator) file_fail(file_error_code::inherited_use);
        if (std::this_thread::get_id() != thread) file_fail(file_error_code::wrong_thread);
        if (poisoned) file_fail(file_error_code::changed);
    }
    void names() const {
        owner();
        for (std::size_t i = 0; i != parents.size(); ++i) {
            if (observed_identity(inspect_directory(parents[i].fd.value)) != parents[i].identity)
                file_fail(file_error_code::changed);
            if (i) {
                struct stat named{};
                if (::fstatat(parents[i-1].fd.value, parents[i].name.c_str(), &named, AT_SYMLINK_NOFOLLOW) ||
                    !S_ISDIR(named.st_mode) || observed_identity(named) != parents[i].identity)
                    file_fail(file_error_code::changed);
            }
        }
        struct stat named{};
        if (parents.empty() || ::fstatat(parents.back().fd.value, leaf.c_str(), &named, AT_SYMLINK_NOFOLLOW) ||
            !S_ISREG(named.st_mode) || observed_identity(named) != expected) file_fail(file_error_code::changed);
    }
    struct stat metadata() const {
        struct stat value{};
        if (::fstat(file.value, &value) || !S_ISREG(value.st_mode) || value.st_nlink != 1 ||
            observed_identity(value) != expected || value.st_size <= 0 ||
            (value.st_uid != ::geteuid() && (!executable || value.st_uid != 0)) ||
            (value.st_mode & 0022) != 0 || (value.st_mode & 06000) != 0 ||
            (executable ? (value.st_mode & 0111) == 0 : (value.st_mode & 07777) != 0600))
            file_fail(file_error_code::changed);
        if (static_cast<std::uintmax_t>(value.st_size) > limit) file_fail(file_error_code::too_large);
        return value;
    }
    std::vector<std::uint8_t> inspect(ordinary_launch::deadline end, bool copy) const {
        owner();
        try {
            before(end); names(); const auto initial = metadata();
            std::vector<std::uint8_t> result;
            if (copy) result.reserve(static_cast<std::size_t>(initial.st_size));
            picosha2::hash256_one_by_one hasher;
            std::array<std::uint8_t, 64 * 1024> buffer{};
            off_t offset = 0;
            while (offset != initial.st_size) {
                before(end);
                const auto remaining = static_cast<std::size_t>(initial.st_size - offset);
                const auto count = ::pread(file.value, buffer.data(), std::min(buffer.size(), remaining), offset);
                if (count < 0 && errno == EINTR) continue;
                if (count <= 0) file_fail(file_error_code::changed);
                hasher.process(buffer.begin(), buffer.begin() + count);
                if (copy) result.insert(result.end(), buffer.begin(), buffer.begin() + count);
                offset += count;
            }
            before(end); hasher.finish(); digest actual{}; hasher.get_hash_bytes(actual.begin(), actual.end());
            if (actual != content || !same_metadata(initial, metadata())) file_fail(file_error_code::changed);
            names(); before(end); return result;
        } catch (...) { poisoned = true; throw; }
    }
};
retained_installation_file retained_installation_file::open_executable(const executable_fact& expected, ordinary_launch::deadline end) {
    if (expected.path.empty() || expected.path.front() != '/' || expected.path.size() > 4096 ||
        expected.path.back() == '/' || !expected.identity.inode ||
        std::all_of(expected.content.begin(), expected.content.end(), [](auto value) { return value == 0; }))
        file_fail(file_error_code::invalid_path);
    auto state = std::make_unique<implementation>();
    state->expected = expected.identity; state->content = expected.content;
    state->limit = 1024ull * 1024 * 1024; state->executable = true;
    retained_fd root(::open("/", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
    const auto identity = observed_identity(inspect_directory(root.value));
    state->parents.push_back({std::move(root), identity, {}});
    std::size_t at = 1;
    for (;;) {
        before(end); const auto slash = expected.path.find('/', at);
        auto component = expected.path.substr(at, slash == std::string::npos ? std::string::npos : slash - at);
        valid_leaf(component);
        if (slash == std::string::npos) { state->leaf = std::move(component); break; }
        retained_fd next(::openat(state->parents.back().fd.value, component.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
        const auto identity = observed_identity(inspect_directory(next.value));
        state->parents.push_back({std::move(next), identity, std::move(component)}); at = slash + 1;
    }
    state->file = retained_fd(::openat(state->parents.back().fd.value, state->leaf.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
    (void)state->inspect(end, false); return retained_installation_file(std::move(state));
}
retained_installation_file retained_installation_file::open_record(int parent, const std::string& leaf,
        const file_identity& expected, const digest& content, std::size_t limit, ordinary_launch::deadline end) {
    valid_leaf(leaf);
    if (!expected.inode || limit == 0 || limit > maximum_manifest_bytes ||
        std::all_of(content.begin(), content.end(), [](auto value) { return value == 0; })) file_fail(file_error_code::invalid_path);
    auto state = std::make_unique<implementation>();
    retained_fd directory(::fcntl(parent, F_DUPFD_CLOEXEC, 3));
    const auto identity = observed_identity(inspect_directory(directory.value));
    state->parents.push_back({std::move(directory), identity, {}});
    state->leaf = leaf; state->expected = expected; state->content = content; state->limit = limit;
    state->file = retained_fd(::openat(state->parents.back().fd.value, leaf.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC));
    (void)state->inspect(end, false); return retained_installation_file(std::move(state));
}
#else
struct retained_installation_file::implementation {};
retained_installation_file retained_installation_file::open_executable(const executable_fact&, ordinary_launch::deadline) { file_fail(file_error_code::unavailable); }
retained_installation_file retained_installation_file::open_record(int, const std::string&, const file_identity&, const digest&, std::size_t, ordinary_launch::deadline) { file_fail(file_error_code::unavailable); }
#endif
retained_installation_file::retained_installation_file(std::unique_ptr<implementation> value) : impl_(std::move(value)) {}
retained_installation_file::~retained_installation_file() = default;
retained_installation_file::retained_installation_file(retained_installation_file&&) noexcept = default;
retained_installation_file& retained_installation_file::operator=(retained_installation_file&&) noexcept = default;
void retained_installation_file::verify(ordinary_launch::deadline end) const {
#ifdef LATTICE_INSTALLATION_FILES_POSIX
    if (!impl_) file_fail(file_error_code::unavailable); (void)impl_->inspect(end, false);
#else
    (void)end; file_fail(file_error_code::unavailable);
#endif
}
std::vector<std::uint8_t> retained_installation_file::read_record(ordinary_launch::deadline end) const {
#ifdef LATTICE_INSTALLATION_FILES_POSIX
    if (!impl_ || impl_->executable) file_fail(file_error_code::unavailable); return impl_->inspect(end, true);
#else
    (void)end; file_fail(file_error_code::unavailable);
#endif
}
file_identity retained_installation_file::identity() const {
#ifdef LATTICE_INSTALLATION_FILES_POSIX
    if (!impl_) file_fail(file_error_code::unavailable); impl_->owner(); return impl_->expected;
#else
    file_fail(file_error_code::unavailable);
#endif
}
} // namespace lattice::detail::ordinary_installation
