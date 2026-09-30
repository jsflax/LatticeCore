#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/ordinary_installation_files.hpp"
#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <utility>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
namespace {
namespace files = lattice::detail::ordinary_installation;
using namespace std::chrono_literals;
struct file_area {
    std::filesystem::path path;
    int fd = -1;
    file_area() {
        auto name = (std::filesystem::temp_directory_path() / "lattice-installation-file-XXXXXX").string();
        std::vector<char> bytes(name.begin(), name.end()); bytes.push_back(0);
        if (!::mkdtemp(bytes.data())) throw std::runtime_error("file test directory unavailable");
        path = bytes.data(); fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd < 0) throw std::runtime_error("file test descriptor unavailable");
    }
    ~file_area() { if (fd >= 0) ::close(fd); std::error_code ignored; std::filesystem::remove_all(path, ignored); }
    files::file_identity write(const char* leaf, const std::vector<std::uint8_t>& value) {
        const auto file = ::openat(fd, leaf, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
        if (file < 0) throw std::runtime_error("file test write unavailable");
        const auto wrote = ::write(file, value.data(), value.size());
        struct stat status{}; const auto stated = ::fstat(file, &status); const auto closed = ::close(file);
        if (wrote != static_cast<ssize_t>(value.size()) || stated || closed) throw std::runtime_error("file test write failed");
        return {static_cast<std::uint64_t>(status.st_dev), static_cast<std::uint64_t>(status.st_ino)};
    }
};
auto file_deadline() { return std::chrono::steady_clock::now() + 5s; }
files::digest file_digest(const std::vector<std::uint8_t>& bytes) {
    files::digest hash{}; picosha2::hash256(bytes.begin(), bytes.end(), hash.begin(), hash.end()); return hash;
}
template <typename F> void expect_file_error(files::file_error_code code, F&& action) {
    try { action(); ADD_FAILURE() << "Expected installation file refusal"; }
    catch (const files::file_error& error) { EXPECT_EQ(error.code, code); }
    catch (...) { ADD_FAILURE() << "Unexpected installation file exception"; }
}
}
TEST(OrdinaryInstallationFiles, ExactRecordIsRetainedAfterCallerDescriptorCloses) {
    file_area area; const std::vector<std::uint8_t> bytes{4, 5, 6, 7};
    const auto expected = area.write("record", bytes);
    const auto duplicate = ::fcntl(area.fd, F_DUPFD_CLOEXEC, 3); ASSERT_GE(duplicate, 3);
    auto file = files::retained_installation_file::open_record(duplicate, "record", expected, file_digest(bytes), 4, file_deadline());
    ASSERT_EQ(::close(duplicate), 0);
    EXPECT_EQ(file.identity(), expected); EXPECT_EQ(file.read_record(file_deadline()), bytes);
    auto moved = std::move(file); EXPECT_EQ(moved.read_record(file_deadline()), bytes);
    expect_file_error(files::file_error_code::unavailable, [&] { file.verify(file_deadline()); });
}
TEST(OrdinaryInstallationFiles, SameBytesAtReplacementInodeCannotRepairARefusedWrapper) {
    file_area area; const std::vector<std::uint8_t> bytes{8, 9};
    const auto expected = area.write("record", bytes);
    auto file = files::retained_installation_file::open_record(area.fd, "record", expected, file_digest(bytes), 2, file_deadline());
    ASSERT_EQ(::renameat(area.fd, "record", area.fd, "original"), 0);
    const auto replaced = area.write("record", bytes); ASSERT_NE(replaced, expected);
    expect_file_error(files::file_error_code::changed, [&] { file.verify(file_deadline()); });
    ASSERT_EQ(::unlinkat(area.fd, "record", 0), 0);
    ASSERT_EQ(::renameat(area.fd, "original", area.fd, "record"), 0);
    expect_file_error(files::file_error_code::changed, [&] { (void)file.read_record(file_deadline()); });
}
TEST(OrdinaryInstallationFiles, InPlaceBytesAreCheckedAndOriginalDigestIsNeverRelearned) {
    file_area area; const std::vector<std::uint8_t> bytes{1, 1, 1, 1};
    const auto expected = area.write("record", bytes);
    auto file = files::retained_installation_file::open_record(area.fd, "record", expected, file_digest(bytes), 4, file_deadline());
    const auto writer = ::openat(area.fd, "record", O_WRONLY | O_CLOEXEC); ASSERT_GE(writer, 0);
    const std::uint8_t replacement = 2; const auto wrote = ::pwrite(writer, &replacement, 1, 2); const auto closed = ::close(writer);
    ASSERT_EQ(wrote, 1); ASSERT_EQ(closed, 0);
    expect_file_error(files::file_error_code::changed, [&] { file.verify(file_deadline()); });
    expect_file_error(files::file_error_code::changed, [&] {
        (void)files::retained_installation_file::open_record(area.fd, "record", expected, file_digest(bytes), 4, file_deadline());
    });
}
TEST(OrdinaryInstallationFiles, HardLinksAndWritableRecordsRefuse) {
    file_area area; const std::vector<std::uint8_t> bytes{5}; const auto expected = area.write("record", bytes);
    ASSERT_EQ(::linkat(area.fd, "record", area.fd, "alias", 0), 0);
    expect_file_error(files::file_error_code::changed, [&] {
        (void)files::retained_installation_file::open_record(area.fd, "record", expected, file_digest(bytes), 1, file_deadline());
    });
    ASSERT_EQ(::unlinkat(area.fd, "alias", 0), 0); ASSERT_EQ(::fchmodat(area.fd, "record", 0660, 0), 0);
    expect_file_error(files::file_error_code::changed, [&] {
        (void)files::retained_installation_file::open_record(area.fd, "record", expected, file_digest(bytes), 1, file_deadline());
    });
}
TEST(OrdinaryInstallationFiles, SymlinkRecordNeverBecomesTheExpectedFile) {
    file_area area; const std::vector<std::uint8_t> bytes{6}; const auto expected = area.write("actual", bytes);
    ASSERT_EQ(::symlinkat("actual", area.fd, "record"), 0);
    expect_file_error(files::file_error_code::changed, [&] {
        (void)files::retained_installation_file::open_record(area.fd, "record", expected, file_digest(bytes), 1, file_deadline());
    });
}
TEST(OrdinaryInstallationFiles, BoundsAndDeadlineDoNotGrantARecord) {
    file_area area; const std::vector<std::uint8_t> bytes{1, 2}; const auto expected = area.write("record", bytes);
    expect_file_error(files::file_error_code::too_large, [&] {
        (void)files::retained_installation_file::open_record(area.fd, "record", expected, file_digest(bytes), 1, file_deadline());
    });
    expect_file_error(files::file_error_code::timed_out, [&] {
        (void)files::retained_installation_file::open_record(area.fd, "record", expected, file_digest(bytes), 2, std::chrono::steady_clock::now());
    });
    auto file = files::retained_installation_file::open_record(area.fd, "record", expected, file_digest(bytes), 2, file_deadline());
    expect_file_error(files::file_error_code::timed_out, [&] { file.verify(std::chrono::steady_clock::now()); });
    expect_file_error(files::file_error_code::changed, [&] { file.verify(file_deadline()); });
}
TEST(OrdinaryInstallationFiles, WrongThreadRefusesBeforeChangingOwnerState) {
    file_area area; const std::vector<std::uint8_t> bytes{3}; const auto expected = area.write("record", bytes);
    auto file = files::retained_installation_file::open_record(area.fd, "record", expected, file_digest(bytes), 1, file_deadline());
    std::optional<files::file_error_code> code;
    std::thread worker([&] { try { file.verify(file_deadline()); } catch (const files::file_error& error) { code = error.code; } });
    worker.join(); ASSERT_TRUE(code); EXPECT_EQ(*code, files::file_error_code::wrong_thread);
    EXPECT_EQ(file.read_record(file_deadline()), bytes);
}
TEST(OrdinaryInstallationFiles, ActualSystemExecutableMatchesRetainedIdentityAndAllBytes) {
    const auto path = std::filesystem::canonical("/usr/bin/true");
    struct stat status{}; ASSERT_EQ(::stat(path.c_str(), &status), 0);
    ASSERT_GT(status.st_size, 0); ASSERT_LE(status.st_size, 16 * 1024 * 1024);
    std::ifstream stream(path, std::ios::binary);
    const std::vector<std::uint8_t> bytes((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
    ASSERT_EQ(bytes.size(), static_cast<std::size_t>(status.st_size));
    files::executable_fact expected{path.string(), {static_cast<std::uint64_t>(status.st_dev), static_cast<std::uint64_t>(status.st_ino)}, file_digest(bytes)};
    auto retained = files::retained_installation_file::open_executable(expected, file_deadline());
    EXPECT_EQ(retained.identity(), expected.identity); retained.verify(file_deadline());
    expect_file_error(files::file_error_code::unavailable, [&] { (void)retained.read_record(file_deadline()); });
    expected.content[0] ^= 1;
    expect_file_error(files::file_error_code::changed, [&] { (void)files::retained_installation_file::open_executable(expected, file_deadline()); });
    // Reading an approved executable is not execution or context issuance.
}
#endif
