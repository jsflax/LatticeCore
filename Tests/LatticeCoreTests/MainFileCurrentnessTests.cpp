#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <lattice.hpp>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>

namespace {
struct CurrentnessFiles {
    std::filesystem::path directory, file, replacement, retired;
    CurrentnessFiles() {
        const char* configured = std::getenv("LATTICE_TEST_ARTIFACTS");
        const auto root = configured ? std::filesystem::path(configured) : std::filesystem::temp_directory_path();
        static std::atomic<unsigned> next{0};
        directory = root / ("main-currentness-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-" + std::to_string(next++));
        if (!std::filesystem::create_directories(directory)) throw std::runtime_error("Fixture directory already exists");
        directory = std::filesystem::canonical(directory);
        file = directory / "original.sqlite"; replacement = directory / "replacement.sqlite"; retired = directory / "retired.sqlite";
    }
    ~CurrentnessFiles() { std::error_code ignored; std::filesystem::remove_all(directory, ignored); }
};
std::unique_ptr<lattice::swift_lattice> open_currentness(const std::filesystem::path& path, bool read_only = false) {
    lattice::swift_configuration config(path.string()); config.read_only = read_only;
    return std::make_unique<lattice::swift_lattice>(config, lattice::SchemaVector{});
}
void require_current(lattice::swift_lattice& owner, const std::filesystem::path& expected) {
    static_cast<lattice::lattice_db&>(owner).require_main_file_unmoved(expected.string());
}
std::string image(const std::filesystem::path& path) {
    std::ifstream stream(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>()};
}
}

TEST(MainFileCurrentness, LivePrimaryAndReadOnlyProbeIssueNoSQL) {
    CurrentnessFiles files;
    auto writer = open_currentness(files.file);
    auto reader = open_currentness(files.file, true);
    const auto before = lattice::database::thread_statement_count();
    EXPECT_NO_THROW(require_current(*writer, files.file));
    EXPECT_NO_THROW(require_current(*reader, files.file));
    EXPECT_EQ(lattice::database::thread_statement_count(), before);
}

TEST(MainFileCurrentness, MainFilePathReturnsSQLiteNameWithoutSQLOrAliasResolution) {
    CurrentnessFiles files;
    { auto seed = open_currentness(files.file); }
    const auto alias = files.directory / "selected.sqlite";
    std::filesystem::create_symlink(files.file, alias);
    auto writer = open_currentness(alias);
    auto reader = open_currentness(files.file, true);
    const std::string opened_name = sqlite3_db_filename(writer->db().handle(), "main");
    const auto before = lattice::database::thread_statement_count();
    const auto path = writer->main_file_path();
    EXPECT_EQ(path, opened_name);
    EXPECT_TRUE(lattice::last_bridge_error().empty());
    EXPECT_NO_THROW(require_current(*writer, path));
    EXPECT_EQ(reader->main_file_path(), sqlite3_db_filename(reader->db().handle(), "main"));
    EXPECT_EQ(lattice::database::thread_statement_count(), before);

    std::filesystem::remove(alias);
    std::filesystem::create_symlink(files.replacement, alias);
    // Reading the open name never follows a newly retargeted configured alias.
    EXPECT_EQ(writer->main_file_path(), opened_name);
    EXPECT_TRUE(lattice::last_bridge_error().empty());
}

TEST(MainFileCurrentness, MainFilePathKeepsTheOpenedNameAfterReplacement) {
    CurrentnessFiles files;
    auto writer = open_currentness(files.file);
    const auto opened_name = writer->main_file_path();
    std::filesystem::rename(files.file, files.retired);
    auto replacement = open_currentness(files.file);
    writer->reopen_read_db();
    EXPECT_EQ(writer->main_file_path(), opened_name);
    EXPECT_TRUE(lattice::last_bridge_error().empty());
    EXPECT_THROW(require_current(*writer, opened_name), lattice::db_error);
    EXPECT_NO_THROW(require_current(*replacement, replacement->main_file_path()));
}

TEST(MainFileCurrentness, MainFilePathSealsClosedMemoryAndAbsentPrimaryErrors) {
    CurrentnessFiles files;
    auto owner = open_currentness(files.file);
    owner->close();
    EXPECT_TRUE(owner->main_file_path().empty());
    EXPECT_FALSE(lattice::last_bridge_error().empty());
    auto absent = open_currentness(files.replacement);
    absent->close_write_db();
    EXPECT_TRUE(absent->main_file_path().empty());
    EXPECT_FALSE(lattice::last_bridge_error().empty());
    for (const auto& path : {std::string(":memory:"), std::string("file:filename-memory?mode=memory&cache=shared")}) {
        lattice::swift_configuration config(path);
        lattice::swift_lattice memory(config, lattice::SchemaVector{});
        EXPECT_TRUE(memory.main_file_path().empty());
        EXPECT_FALSE(lattice::last_bridge_error().empty());
    }
    auto live = open_currentness(files.file);
    EXPECT_FALSE(live->main_file_path().empty());
    EXPECT_TRUE(lattice::last_bridge_error().empty());
}

TEST(MainFileCurrentness, IdenticalReplacementCannotBeAdmittedByOldPrimaryOrNewReader) {
    CurrentnessFiles files;
    auto writer = open_currentness(files.file);
    writer->db().query("PRAGMA wal_checkpoint(TRUNCATE)");
    const auto old_fence = static_cast<lattice::lattice_db&>(*writer).read_audit_read_fence();
    std::filesystem::copy_file(files.file, files.replacement);
    ASSERT_EQ(image(files.file), image(files.replacement));
    std::filesystem::rename(files.file, files.retired);
    std::filesystem::rename(files.replacement, files.file);
    auto reader = open_currentness(files.file, true);
    const auto new_fence = static_cast<lattice::lattice_db&>(*reader).read_audit_read_fence();
    EXPECT_EQ(old_fence.cursor.head, new_fence.cursor.head);
    EXPECT_EQ(old_fence.cursor.floor, new_fence.cursor.floor);
    EXPECT_EQ(old_fence.cursor.head_global_id, new_fence.cursor.head_global_id);
    EXPECT_EQ(old_fence.history_generation, new_fence.history_generation);
    EXPECT_EQ(old_fence.has_history_generation, new_fence.has_history_generation);
    EXPECT_NO_THROW(require_current(*reader, files.file));
    EXPECT_THROW(require_current(*writer, files.file), lattice::db_error);
    // Replacing the ordinary read connection cannot launder the still-old primary.
    writer->reopen_read_db();
    EXPECT_THROW(require_current(*writer, files.file), lattice::db_error);
}

TEST(MainFileCurrentness, MissingClosedAndAbsentPrimaryRefuse) {
    CurrentnessFiles files;
    auto owner = open_currentness(files.file);
    std::filesystem::rename(files.file, files.retired);
    EXPECT_THROW(require_current(*owner, files.file), lattice::db_error);
    owner->close();
    EXPECT_THROW(require_current(*owner, files.file), lattice::db_error);
    auto other = open_currentness(files.replacement);
    other->close_write_db();
    EXPECT_THROW(require_current(*other, files.replacement), lattice::db_error);
}

TEST(MainFileCurrentness, AnonymousAndNamedMemoryRefuse) {
    for (const auto& path : {std::string(":memory:"), std::string("file:currentness-memory?mode=memory&cache=shared")}) {
        lattice::swift_configuration config(path);
        lattice::swift_lattice owner(config, lattice::SchemaVector{});
        EXPECT_THROW(require_current(owner, (std::filesystem::temp_directory_path() / "not-an-open-file.sqlite").string()), lattice::db_error);
    }
}

TEST(MainFileCurrentness, ExpectedPathAndRetargetedAliasCannotSubstituteAnotherFile) {
    CurrentnessFiles files;
    { auto seed = open_currentness(files.file); }
    auto second = open_currentness(files.replacement);
    const auto alias = files.directory / "selected.sqlite";
    std::filesystem::create_symlink(files.file, alias);
    auto first = open_currentness(alias);
    const auto originally_selected = std::filesystem::canonical(alias);
    // Some VFSes canonicalize this name; others retain the alias. Exact mismatch
    // is deliberately refused rather than resolving the stored name afresh.
    const std::string opened_name = sqlite3_db_filename(first->db().handle(), "main");
    if (opened_name == originally_selected.string()) EXPECT_NO_THROW(require_current(*first, originally_selected));
    else EXPECT_THROW(require_current(*first, originally_selected), lattice::db_error);
    std::filesystem::remove(alias);
    std::filesystem::create_symlink(files.replacement, alias);
    const auto now_selected = std::filesystem::canonical(alias);
    EXPECT_THROW(require_current(*first, now_selected), lattice::db_error);
    EXPECT_NO_THROW(require_current(*second, now_selected));
    EXPECT_THROW(static_cast<lattice::lattice_db&>(*first).require_main_file_unmoved(std::string("bad\0path",8)), lattice::db_error);
    EXPECT_THROW(static_cast<lattice::lattice_db&>(*first).require_main_file_unmoved(std::string(4097,'x')), lattice::db_error);
}

TEST(MainFileCurrentness, BridgeRefusalSealsAnErrorInsteadOfThrowingAcrossSwift) {
    CurrentnessFiles files;
    auto owner = open_currentness(files.file);
    EXPECT_TRUE(owner->require_main_file_unmoved(files.file.string()));
    EXPECT_TRUE(lattice::last_bridge_error().empty());
    std::filesystem::rename(files.file, files.retired);
    EXPECT_FALSE(owner->require_main_file_unmoved(files.file.string()));
    EXPECT_FALSE(lattice::last_bridge_error().empty());
}
