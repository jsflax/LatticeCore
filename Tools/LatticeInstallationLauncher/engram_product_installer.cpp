#include "engram_product_installer.hpp"
#include "engram_build_stamp.hpp"
#include "../../Sources/LatticeCore/src/ordinary_normal_startup.hpp"
#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <limits>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
#define LATTICE_ENGRAM_INSTALLER_POSIX 1
#endif
namespace lattice::detail::ordinary_installation {
namespace {
[[noreturn]] void refused() { throw seed_error(seed_error_code::unavailable); }
#ifdef LATTICE_ENGRAM_INSTALLER_POSIX
struct fd_owner {
    int value;
    explicit fd_owner(int fd) : value(fd) { if (fd < 0) refused(); }
    ~fd_owner() { ::close(value); }
    fd_owner(const fd_owner&) = delete;
};
void bounded(ordinary_launch::deadline end) { if (std::chrono::steady_clock::now() >= end) refused(); }
file_identity identity(const struct stat& value) {
    if constexpr (sizeof(value.st_dev) > sizeof(std::uint64_t) || sizeof(value.st_ino) > sizeof(std::uint64_t)) refused();
    return {static_cast<std::uint64_t>(value.st_dev), static_cast<std::uint64_t>(value.st_ino)};
}
bool stable(const struct stat& a, const struct stat& b) {
#if defined(__APPLE__)
    const bool times = a.st_mtimespec.tv_sec == b.st_mtimespec.tv_sec && a.st_mtimespec.tv_nsec == b.st_mtimespec.tv_nsec &&
        a.st_ctimespec.tv_sec == b.st_ctimespec.tv_sec && a.st_ctimespec.tv_nsec == b.st_ctimespec.tv_nsec;
#else
    const bool times = a.st_mtim.tv_sec == b.st_mtim.tv_sec && a.st_mtim.tv_nsec == b.st_mtim.tv_nsec &&
        a.st_ctim.tv_sec == b.st_ctim.tv_sec && a.st_ctim.tv_nsec == b.st_ctim.tv_nsec;
#endif
    return identity(a) == identity(b) && a.st_size == b.st_size && a.st_uid == b.st_uid &&
        a.st_mode == b.st_mode && a.st_nlink == b.st_nlink && times;
}
std::string image_path() {
    std::array<char, 4097> path{};
#if defined(__APPLE__)
    std::uint32_t size = static_cast<std::uint32_t>(path.size());
    if (::_NSGetExecutablePath(path.data(), &size)) refused();
    // Packaging/install mutation exclusion remains mandatory. Resolving the
    // loaded image path does not prove immunity to hostile pathname replacement.
    std::array<char, 4097> canonical{};
    if (!::realpath(path.data(), canonical.data())) refused();
    return canonical.data();
#else
    const auto count = ::readlink("/proc/self/exe", path.data(), path.size());
    if (count <= 0 || static_cast<std::size_t>(count) >= path.size()) refused();
    const std::string value(path.data(), static_cast<std::size_t>(count));
    if (value.ends_with(" (deleted)")) refused();
    return value;
#endif
}
std::array<std::uint8_t, 20> revision(std::string_view text) {
    if (text.size() != 40) refused();
    auto nibble = [](char byte) -> unsigned {
        if (byte >= '0' && byte <= '9') return static_cast<unsigned>(byte - '0');
        if (byte >= 'a' && byte <= 'f') return static_cast<unsigned>(byte - 'a' + 10);
        refused();
    };
    std::array<std::uint8_t, 20> result{};
    for (std::size_t i = 0; i != result.size(); ++i)
        result[i] = static_cast<std::uint8_t>((nibble(text[2*i]) << 4) | nibble(text[2*i+1]));
    return result;
}
struct package_observation {
    fd_owner directory;
    fd_owner record;
    const file_identity directory_identity;
    const struct stat original;
    std::array<std::uint8_t, 128> bytes{};
    static struct stat metadata(int fd, bool directory) {
        struct stat result{};
        if (::fstat(fd, &result) || (result.st_uid != ::geteuid() && result.st_uid != 0) ||
            (result.st_mode & 0022) || (directory ? !S_ISDIR(result.st_mode) :
             (!S_ISREG(result.st_mode) || result.st_nlink != 1 || result.st_size != 128 || (result.st_mode & 06111)))) refused();
        return result;
    }
    explicit package_observation(const std::string& path, ordinary_launch::deadline end)
        : directory(::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)),
          record(::openat(directory.value, "engram-installation.provenance", O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC)),
          directory_identity(identity(metadata(directory.value, true))), original(metadata(record.value, false)) {
        std::size_t at = 0;
        while (at != bytes.size()) {
            bounded(end);
            const auto got = ::pread(record.value, bytes.data() + at, bytes.size() - at, static_cast<off_t>(at));
            if (got < 0 && errno == EINTR) continue;
            if (got <= 0) refused();
            at += static_cast<std::size_t>(got);
        }
        verify(end);
    }
    void verify(ordinary_launch::deadline end) const {
        bounded(end); struct stat named{};
        if (identity(metadata(directory.value, true)) != directory_identity ||
            !stable(original, metadata(record.value, false)) ||
            ::fstatat(directory.value, "engram-installation.provenance", &named, AT_SYMLINK_NOFOLLOW) ||
            !stable(original, named)) refused();
        std::array<std::uint8_t, 128> current{}; std::size_t at = 0;
        while (at != current.size()) {
            bounded(end);
            const auto got = ::pread(record.value, current.data() + at, current.size() - at, static_cast<off_t>(at));
            if (got < 0 && errno == EINTR) continue;
            if (got <= 0) refused();
            at += static_cast<std::size_t>(got);
        }
        if (current != bytes || !stable(original, metadata(record.value, false))) refused();
        bounded(end);
    }
    executable_fact executable(const std::string& path, const char* leaf, std::size_t digest_offset) const {
        struct stat value{};
        if (::fstatat(directory.value, leaf, &value, AT_SYMLINK_NOFOLLOW) || !S_ISREG(value.st_mode)) refused();
        executable_fact result{path, identity(value), {}};
        std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(digest_offset), result.content.size(), result.content.begin());
        return result;
    }
};
#endif
}
seeded_installation_store engram_product_installer::prepare_registered(int parent, const std::string& leaf, ordinary_launch::deadline end,
        int initializer_output) {
#ifdef LATTICE_ENGRAM_INSTALLER_POSIX
    // The default Core build has no product-byte stamp and fails before any
    // namespace or child exists. Neither runtime CLI facts nor decoded package
    // records can enable an unstamped binary.
    if (!engram_build_stamp::available || engram_build_stamp::product != "engram" ||
        engram_build_stamp::initializer_leaf != "memory") refused();
    bounded(end);
    const auto source = revision(engram_build_stamp::source_revision);
    const auto core = revision(engram_build_stamp::core_revision);
    const auto self = image_path();
    const auto slash = self.rfind('/');
    if (slash == std::string::npos || self.substr(slash + 1) != "memory-installation-launcher") refused();
    const auto siblings = self.substr(0, slash);
    package_observation package(siblings, end);
    const std::array<std::uint8_t, 24> prefix{'L','A','T','P','K','G','1',0, 1,0,0,0,0,0,0,0, 1,0,0,0,0,0,0,0};
    if (!std::equal(prefix.begin(), prefix.end(), package.bytes.begin()) ||
        !std::equal(source.begin(), source.end(), package.bytes.begin() + 24) ||
        !std::equal(core.begin(), core.end(), package.bytes.begin() + 44) ||
        !std::equal(engram_build_stamp::initializer_sha256.begin(), engram_build_stamp::initializer_sha256.end(), package.bytes.begin() + 64)) refused();
    const auto initializer = package.executable(siblings + "/memory", "memory", 64);
    const auto launcher = package.executable(self, "memory-installation-launcher", 96);
    auto held_launcher = retained_installation_file::open_executable(launcher, end);
#if defined(__linux__)
    fd_owner kernel_image(::open("/proc/self/exe", O_RDONLY | O_CLOEXEC));
    struct stat loaded{};
    if (::fstat(kernel_image.value, &loaded) || identity(loaded) != launcher.identity) refused();
#endif
    package.verify(end);
    digest provenance{}; picosha2::hash256(package.bytes.begin(), package.bytes.end(), provenance.begin(), provenance.end());
    // Only the new normal-role path requires installed-root authority. The
    // original create-engram operation remains a closed unadopted registration.
    if(initializer_output==2)verify_normal_installed_root(initializer,launcher,identity(package.original),provenance,end);
    // Registration remains on this live controller; no serialized phase4,
    // child status, database identity or test seed can take this call's place.
    auto seed = seeded_installation_store::create_engram_with_output(parent, leaf, initializer, end, initializer_output);
    package.verify(end); held_launcher.verify(end);
    seed.register_unadopted(launcher, provenance, std::string(engram_build_stamp::source_revision),
        std::string(engram_build_stamp::core_revision), end);
    package.verify(end); held_launcher.verify(end);
    return seed;
#else
    (void)parent; (void)leaf; (void)end; (void)initializer_output; refused();
#endif
}
void engram_product_installer::create_registered(int parent, const std::string& leaf, ordinary_launch::deadline end) {
    (void)prepare_registered(parent,leaf,end,1);
}
int engram_product_installer::create_and_run_primary_mcp(int parent,const std::string& leaf,ordinary_launch::deadline end) {
    // Keep every initializer diagnostic off the stream reserved for the
    // subsequent actual MCP child. The existing create-engram path keeps fd 1.
    auto seed=prepare_registered(parent,leaf,end,2);
    return seed.run_registered_mcp(std::chrono::steady_clock::now()+std::chrono::seconds(30));
}
}
