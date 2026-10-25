#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <lattice/sync.hpp>
#include <atomic>
#include <filesystem>
#include <future>
#include <random>
#include <thread>

#if defined(__APPLE__) || defined(__linux__)
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
extern char** environ;
#endif

struct LocalMetadataMutation { std::string value; };
LATTICE_SCHEMA(LocalMetadataMutation, value);

namespace {
using namespace lattice;
constexpr const char* ns = "engram.consumer-operations.v1";
struct MetadataFile {
    std::filesystem::path path = std::filesystem::temp_directory_path() /
        ("local-metadata-" + std::to_string(std::random_device{}()) + ".sqlite");
    ~MetadataFile() {
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
        std::filesystem::remove(path.string() + "-wal", ignored);
        std::filesystem::remove(path.string() + "-shm", ignored);
    }
    configuration config() const {
        configuration result(path.string());
        result.busy_timeout_ms = 2000;
        result.audit_retention_seconds = 0;
        return result;
    }
};
int64_t scalar(lattice_db& db, const std::string& sql) {
    return std::get<int64_t>(db.query_read(sql).at(0).at("n"));
}
bool commit_once(lattice_db& db, const std::string& operation, const std::string& payload) {
    bool inserted = false;
    db.write([&] {
        if (auto existing = db.local_metadata(ns, operation)) {
            if (*existing != payload) throw std::runtime_error("payload conflict");
            return;
        }
        db.add(LocalMetadataMutation{payload});
        inserted = db.insert_local_metadata_if_absent(ns, operation, payload);
        if (!inserted) throw std::runtime_error("unexpected duplicate under writer ownership");
    });
    return inserted;
}
}

TEST(LocalMetadata, ReadDoesNotCreateAndInsertNeedsOwnedTransaction) {
    MetadataFile file;
    lattice_db db(file.config());
    EXPECT_FALSE(db.local_metadata(ns, "op"));
    EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM sqlite_master WHERE name='_lattice_local_metadata_v1'"), 0);
    EXPECT_THROW(db.insert_local_metadata_if_absent(ns, "op", "result"), db_error);
    EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM sqlite_master WHERE name='_lattice_local_metadata_v1'"), 0);
    db.begin_transaction();
    std::atomic<bool> rejected{false};
    std::thread other([&] {
        try { db.insert_local_metadata_if_absent(ns, "op", "foreign-thread"); }
        catch (const db_error&) { rejected = true; }
    });
    other.join();
    EXPECT_TRUE(rejected);
    db.rollback();
}

TEST(LocalMetadata, MutationAndReceiptRollbackTogetherThenSurviveReopen) {
    MetadataFile file;
    {
        lattice_db db(file.config());
        EXPECT_THROW(db.write([&] {
            db.add(LocalMetadataMutation{"rolled-back"});
            EXPECT_TRUE(db.insert_local_metadata_if_absent(ns, "abort", "receipt"));
            EXPECT_EQ(db.local_metadata(ns, "abort"), "receipt");
            throw std::runtime_error("abort operation");
        }), std::runtime_error);
        EXPECT_FALSE(db.local_metadata(ns, "abort"));
        EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM LocalMetadataMutation"), 0);
        EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM AuditLog"), 0);
        EXPECT_TRUE(commit_once(db, "committed", "original"));
        EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM AuditLog"), 1);
    }
    {
        lattice_db reopened(file.config());
        EXPECT_EQ(reopened.local_metadata(ns, "committed"), "original");
        EXPECT_FALSE(commit_once(reopened, "committed", "original"));
        EXPECT_THROW(commit_once(reopened, "committed", "changed"), std::runtime_error);
        EXPECT_EQ(scalar(reopened, "SELECT COUNT(*) AS n FROM LocalMetadataMutation"), 1);
        EXPECT_EQ(scalar(reopened, "SELECT COUNT(*) AS n FROM AuditLog"), 1);
        EXPECT_TRUE(commit_once(reopened, "different-operation", "original"));
        EXPECT_EQ(scalar(reopened, "SELECT COUNT(*) AS n FROM LocalMetadataMutation"), 2);
    }
}

TEST(LocalMetadata, ImmutableKeysAreNamespacedAndBounded) {
    MetadataFile file;
    lattice_db db(file.config());
    db.write([&] {
        EXPECT_TRUE(db.insert_local_metadata_if_absent("first", "same-key", "first-value"));
        EXPECT_FALSE(db.insert_local_metadata_if_absent("first", "same-key", "conflicting-value"));
        EXPECT_TRUE(db.insert_local_metadata_if_absent("second", "same-key", "second-value"));
        EXPECT_TRUE(db.insert_local_metadata_if_absent(ns, "quote'\";--", "unicode \xe2\x9c\x93"));
        EXPECT_THROW(db.insert_local_metadata_if_absent("lattice.reserved", "key", "value"), db_error);
        EXPECT_THROW(db.insert_local_metadata_if_absent("UPPER", "key", "value"), db_error);
        EXPECT_THROW(db.insert_local_metadata_if_absent(ns, "", "value"), db_error);
        EXPECT_THROW(db.insert_local_metadata_if_absent(ns, std::string(513, 'k'), "value"), db_error);
        EXPECT_THROW(db.insert_local_metadata_if_absent(ns, "large", std::string(65'537, 'v')), db_error);
    });
    EXPECT_EQ(db.local_metadata("first", "same-key"), "first-value");
    EXPECT_EQ(db.local_metadata("second", "same-key"), "second-value");
    EXPECT_EQ(db.local_metadata(ns, "quote'\";--"), "unicode \xe2\x9c\x93");
    EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM AuditLog"), 0);
}

TEST(LocalMetadata, IndependentHandlesSerializeDuplicateOperations) {
    MetadataFile file;
    { lattice_db seed(file.config()); }
    std::atomic<int> inserted{0};
    auto writer = [&] {
        lattice_db db(file.config());
        if (commit_once(db, "concurrent", "payload")) ++inserted;
    };
    auto a = std::async(std::launch::async, writer);
    auto b = std::async(std::launch::async, writer);
    ASSERT_NO_THROW(a.get());
    ASSERT_NO_THROW(b.get());
    EXPECT_EQ(inserted, 1);
    lattice_db db(file.config());
    EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM LocalMetadataMutation"), 1);
    EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM AuditLog"), 1);
}

TEST(LocalMetadata, MetadataNeverAppearsInAuditOrRegeneratedHistory) {
    MetadataFile file;
    lattice_db db(file.config());
    ASSERT_TRUE(commit_once(db, "local", "receipt-only"));
    db.generate_history();
    db.force_compact_audit_log();
    EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM AuditLog WHERE tableName <> 'LocalMetadataMutation'"), 0);
    EXPECT_EQ(db.local_metadata(ns, "local"), "receipt-only");
    EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM sqlite_master WHERE type='trigger' AND tbl_name='_lattice_local_metadata_v1'"), 0);
}

TEST(LocalMetadata, SyncCannotCreateOrMutateReservedMetadata) {
    MetadataFile file;
    lattice_db db(file.config());
    int index = 0;
    for (int phase = 0; phase < 2; ++phase) {
        for (const auto& table : {"_lattice_local_metadata_v1", "_LATTICE_LOCAL_METADATA_V1", "main.\"_lattice_local_metadata_v1\""}) {
            audit_log_entry forged;
            forged.global_id = "forged-entry-" + std::to_string(index++);
            forged.global_row_id = "forged-row";
            forged.table_name = table;
            forged.operation = "INSERT";
            EXPECT_FALSE(audit_log_entry::from_json(forged.to_json()).has_value());
            EXPECT_TRUE(apply_remote_changes(db, {forged}).empty());
        }
        if (phase == 0) {
            EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM sqlite_master WHERE name='_lattice_local_metadata_v1'"), 0);
            db.write([&] { ASSERT_TRUE(db.insert_local_metadata_if_absent(ns, "local", "original")); });
        }
    }
    EXPECT_EQ(db.local_metadata(ns, "local"), "original");
    EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM AuditLog"), 0);
}

TEST(LocalMetadata, CorruptValueIsAnErrorRatherThanAbsence) {
    MetadataFile file;
    lattice_db db(file.config());
    db.write([&] { db.insert_local_metadata_if_absent(ns, "local", "original"); });
    db.db().execute("UPDATE _lattice_local_metadata_v1 SET value = X'0102'");
    EXPECT_THROW(db.local_metadata(ns, "local"), db_error);
    db.db().execute("UPDATE _lattice_local_metadata_v1 SET value = CAST(zeroblob(65537) AS TEXT)");
    EXPECT_THROW(db.local_metadata(ns, "local"), db_error);
    for (const auto& hex : {"80", "C0AF", "EDA080", "F4908080", "E282"}) {
        db.db().execute(std::string("UPDATE _lattice_local_metadata_v1 SET value = CAST(X'") + hex + "' AS TEXT)");
        EXPECT_THROW(db.local_metadata(ns, "local"), db_error);
    }
    db.write([&] {
        EXPECT_THROW(db.insert_local_metadata_if_absent(ns, "bad-utf8", std::string("\xc0\xaf", 2)), db_error);
    });
}

#if defined(__APPLE__) || defined(__linux__)
TEST(LocalMetadataProcessChild, WriteOnce) {
    const char* path = std::getenv("LATTICE_METADATA_CHILD_PATH");
    if (!path) GTEST_SKIP() << "Only run by the cross-process qualification";
    lattice::configuration config(path);
    config.busy_timeout_ms = 2000;
    config.audit_retention_seconds = 0;
    lattice::lattice_db db(config);
    EXPECT_NO_THROW(commit_once(db, "process-concurrent", "payload"));
}

TEST(LocalMetadata, IndependentProcessesSerializeDuplicateOperations) {
    MetadataFile file;
    { lattice_db seed(file.config()); }
    char binary[4096];
#if defined(__APPLE__)
    uint32_t size = sizeof(binary);
    ASSERT_EQ(_NSGetExecutablePath(binary, &size), 0);
#else
    const auto size = readlink("/proc/self/exe", binary, sizeof(binary) - 1);
    ASSERT_GT(size, 0);
    binary[size] = 0;
#endif
    std::vector<std::string> values;
    for (char** entry = environ; *entry; ++entry) {
        if (std::string(*entry).rfind("LATTICE_METADATA_CHILD_PATH=", 0) != 0) values.emplace_back(*entry);
    }
    values.push_back("LATTICE_METADATA_CHILD_PATH=" + file.path.string());
    std::vector<char*> env;
    for (auto& value : values) env.push_back(value.data());
    env.push_back(nullptr);
    char filter[] = "--gtest_filter=LocalMetadataProcessChild.WriteOnce";
    char* args[] = {binary, filter, nullptr};
    pid_t a = 0, b = 0;
    ASSERT_EQ(posix_spawn(&a, binary, nullptr, nullptr, args, env.data()), 0);
    ASSERT_EQ(posix_spawn(&b, binary, nullptr, nullptr, args, env.data()), 0);
    int statusA = 0, statusB = 0;
    ASSERT_EQ(waitpid(a, &statusA, 0), a);
    ASSERT_EQ(waitpid(b, &statusB, 0), b);
    ASSERT_TRUE(WIFEXITED(statusA) && WIFEXITED(statusB));
    EXPECT_EQ(WEXITSTATUS(statusA), 0);
    EXPECT_EQ(WEXITSTATUS(statusB), 0);
    lattice_db db(file.config());
    EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM LocalMetadataMutation"), 1);
    EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM AuditLog"), 1);
    EXPECT_EQ(db.local_metadata(ns, "process-concurrent"), "payload");
}
#endif
