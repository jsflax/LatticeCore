#include <gtest/gtest.h>
#include <lattice/db.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <thread>

namespace {
using lattice::database;
using namespace std::chrono_literals;

struct CheckpointFile {
    std::filesystem::path directory;
    std::filesystem::path path;
    CheckpointFile() {
        static std::atomic<unsigned> sequence{0};
        const auto* artifacts = std::getenv("LATTICE_TEST_ARTIFACTS");
        const auto root = artifacts ? std::filesystem::path(artifacts)
                                    : std::filesystem::temp_directory_path();
        // No shared fixed filename and no dependency on the source checkout.
        directory = root / ("wal-checkpoint-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
            std::to_string(sequence.fetch_add(1)));
        std::filesystem::create_directories(directory);
        path = directory / "journal.sqlite";
    }
    ~CheckpointFile() {
        std::error_code ignored;
        std::filesystem::remove_all(directory, ignored);
    }
};

int64_t scalar(database& db, const std::string& sql) {
    const auto rows = db.query(sql);
    return std::get<int64_t>(rows.at(0).begin()->second);
}

void seed(database& db) {
    db.execute("PRAGMA wal_autocheckpoint=0");
    db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, payload TEXT)");
    db.execute("INSERT INTO events VALUES (1, 'initial')");
}

void expectNoOp(const database::checkpoint_result& result) {
    EXPECT_EQ(result.rc, SQLITE_OK);
    EXPECT_EQ(result.busy, 1);
    EXPECT_EQ(result.log_frames, -1);
    EXPECT_EQ(result.checkpointed, -1);
}
} // namespace

TEST(WALCheckpoint, TruncateSucceedsAndRestoresConfiguredTimeout) {
    CheckpointFile file;
    database db(file.path.string(), database::open_mode::read_write, 731);
    seed(db);
    const auto result = db.wal_checkpoint(true, 1);
    EXPECT_EQ(result.rc, SQLITE_OK);
    EXPECT_EQ(result.busy, 0);
    EXPECT_EQ(result.log_frames, 0);
    EXPECT_EQ(result.checkpointed, 0);
    EXPECT_EQ(scalar(db, "PRAGMA busy_timeout"), 731);
    EXPECT_EQ(scalar(db, "SELECT count(*) FROM events"), 1);
}

TEST(WALCheckpoint, HeldReaderRetainsBusyStatusThenTruncateRecovers) {
    CheckpointFile file;
    database writer(file.path.string(), database::open_mode::read_write, 731);
    seed(writer);
    database reader(file.path.string(), database::open_mode::read_only);
    reader.execute("BEGIN");
    EXPECT_EQ(scalar(reader, "SELECT count(*) FROM events"), 1);
    writer.execute("INSERT INTO events VALUES (2, 'after snapshot')");
    const auto busy = writer.wal_checkpoint(true, 1);
    EXPECT_EQ(busy.rc, SQLITE_OK); // PRAGMA-compatible status, not SQLITE_BUSY
    EXPECT_EQ(busy.busy, 1);
    EXPECT_GT(busy.log_frames, busy.checkpointed);
    EXPECT_EQ(scalar(writer, "PRAGMA busy_timeout"), 731);
    const auto partial = writer.wal_checkpoint(false);
    EXPECT_EQ(partial.rc, SQLITE_OK);
    EXPECT_EQ(partial.busy, 0); // PASSIVE does not wait for the pinned reader
    EXPECT_GT(partial.log_frames, partial.checkpointed);
    EXPECT_EQ(scalar(writer, "PRAGMA busy_timeout"), 731);
    reader.execute("COMMIT");
    const auto recovered = writer.wal_checkpoint(true, 1);
    EXPECT_EQ(recovered.rc, SQLITE_OK);
    EXPECT_EQ(recovered.busy, 0);
    EXPECT_EQ(recovered.log_frames, 0);
    EXPECT_EQ(recovered.checkpointed, 0);
}

TEST(WALCheckpoint, ActiveWriteTransactionReturnsErrorWithoutLosingTransaction) {
    CheckpointFile file;
    database db(file.path.string(), database::open_mode::read_write, 731);
    seed(db);
    db.begin_transaction();
    db.execute("INSERT INTO events VALUES (2, 'uncommitted')");
    const auto result = db.wal_checkpoint(true, 1);
    EXPECT_EQ(result.rc, SQLITE_ERROR); // old query() failure contract
    EXPECT_EQ(result.busy, 1);
    EXPECT_EQ(result.log_frames, -1);
    EXPECT_EQ(result.checkpointed, -1);
    EXPECT_TRUE(db.is_in_transaction());
    EXPECT_EQ(scalar(db, "PRAGMA busy_timeout"), 731);
    EXPECT_NO_THROW(db.commit());
    EXPECT_EQ(scalar(db, "SELECT count(*) FROM events"), 2);
}

TEST(WALCheckpoint, NoWALHasUnavailableFrameCounts) {
    database db(":memory:", database::open_mode::read_write, 731);
    seed(db);
    for (const bool truncate : {false, true}) {
        const auto result = db.wal_checkpoint(truncate, 1);
        EXPECT_EQ(result.rc, SQLITE_OK);
        EXPECT_EQ(result.busy, 0);
        EXPECT_EQ(result.log_frames, -1);
        EXPECT_EQ(result.checkpointed, -1);
        EXPECT_EQ(scalar(db, "PRAGMA busy_timeout"), 731);
    }
}

TEST(WALCheckpoint, ReadOnlyAndClosedConnectionsKeepNoOpStatus) {
    CheckpointFile file;
    database writer(file.path.string());
    seed(writer);
    database reader(file.path.string(), database::open_mode::read_only);
    expectNoOp(reader.wal_checkpoint(true));
    EXPECT_EQ(scalar(reader, "SELECT count(*) FROM events"), 1);
    writer.close();
    expectNoOp(writer.wal_checkpoint(false));
}

TEST(WALCheckpoint, UnqualifiedCheckpointStillIncludesAttachedWAL) {
    CheckpointFile file;
    database db(file.path.string());
    seed(db);
    const auto attached = file.directory / "attached.sqlite";
    db.execute("ATTACH DATABASE ? AS attached", {attached.string()});
    db.execute("PRAGMA attached.journal_mode=WAL");
    db.execute("CREATE TABLE attached.events (id INTEGER PRIMARY KEY)");
    db.execute("INSERT INTO attached.events VALUES (1)");
    ASSERT_GT(std::filesystem::file_size(attached.string() + "-wal"), 0);
    const auto result = db.wal_checkpoint(true, 1);
    EXPECT_EQ(result.rc, SQLITE_OK);
    EXPECT_EQ(result.busy, 0);
    EXPECT_EQ(std::filesystem::file_size(attached.string() + "-wal"), 0);
    EXPECT_EQ(scalar(db, "SELECT count(*) FROM attached.events"), 1);
}

// The focused CMake regression target compiles ONLY db.cpp with sqlite3_step renamed
// to the wrapper below. It calls the real SQLite API and pauses after it has
// returned a checkpoint row (and released SQLite's per-call mutex). No source
// seam, fake database, sleep race, or product hook is needed. Ordinary package
// tests retain the six semantic cases above; the regression target adds this interleaving.
#ifdef LATTICE_CHECKPOINT_STEP_INTERPOSITION
#include <lattice/log.hpp>

// The standalone regression links only database code, not lattice.cpp's
// production logging definition or the rest of the Core archive.
namespace lattice {
std::atomic<log_level> g_log_level{log_level::error};
}

namespace {
struct CheckpointInterleave {
    sqlite3* handle;
    std::mutex mutex;
    std::condition_variable condition;
    bool atRow = false;
    bool returned = false;
    bool writerFinished = false;
    bool timedOut = false;
};
std::atomic<CheckpointInterleave*> currentInterleave{nullptr};
} // namespace

extern "C" int lattice_checkpoint_test_step(sqlite3_stmt* statement) {
    const int rc = sqlite3_step(statement);
    auto* gate = currentInterleave.load(std::memory_order_acquire);
    const auto* sql = sqlite3_sql(statement);
    if (gate && rc == SQLITE_ROW && sqlite3_db_handle(statement) == gate->handle &&
        sql && std::string(sql).starts_with("PRAGMA wal_checkpoint(")) {
        std::unique_lock lock(gate->mutex);
        gate->atRow = true;
        gate->condition.notify_all();
        if (!gate->condition.wait_for(lock, 5s, [&] { return gate->writerFinished; })) {
            gate->timedOut = true;
        }
    }
    return rc;
}

TEST(WALCheckpoint, InterleavedCommitHasNoActiveCheckpointStatement) {
    CheckpointFile file;
    database db(file.path.string(), database::open_mode::read_write, 200);
    seed(db);
    CheckpointInterleave gate{db.handle()};
    currentInterleave.store(&gate, std::memory_order_release);
    database::checkpoint_result checkpoint;
    std::thread maintenance([&] {
        checkpoint = db.wal_checkpoint(true, 1);
        std::lock_guard lock(gate.mutex);
        gate.returned = true;
        gate.condition.notify_all();
    });
    bool ready;
    {
        std::unique_lock lock(gate.mutex);
        ready = gate.condition.wait_for(lock, 5s, [&] { return gate.atRow || gate.returned; });
    }
    int busyWriteStatements = 0;
    std::string transactionError;
    bool committed = false;
    if (ready) {
        for (auto* statement = sqlite3_next_stmt(db.handle(), nullptr); statement;
             statement = sqlite3_next_stmt(db.handle(), statement)) {
            busyWriteStatements += sqlite3_stmt_busy(statement) && !sqlite3_stmt_readonly(statement);
        }
        try {
            db.begin_transaction();
            db.execute("INSERT INTO events VALUES (2, 'same writer during maintenance')");
            db.commit();
            committed = true;
        } catch (const std::exception& error) {
            transactionError = error.what();
        }
    }
    {
        std::lock_guard lock(gate.mutex);
        gate.writerFinished = true;
        gate.condition.notify_all();
    }
    maintenance.join();
    currentInterleave.store(nullptr, std::memory_order_release);
    if (db.is_in_transaction()) db.rollback();
    std::cout << "CHECKPOINT_INTERLEAVING {\"atRow\":" << (gate.atRow ? "true" : "false")
              << ",\"busyWriteStatements\":" << busyWriteStatements
              << ",\"committed\":" << (committed ? "true" : "false")
              << ",\"error\":" << std::quoted(transactionError) << "}\n";
    EXPECT_TRUE(ready);
    EXPECT_FALSE(gate.timedOut);
    EXPECT_EQ(busyWriteStatements, 0);
    EXPECT_TRUE(committed) << transactionError;
    EXPECT_EQ(transactionError, "");
    EXPECT_EQ(checkpoint.rc, SQLITE_OK);
    EXPECT_EQ(checkpoint.busy, 0);
    EXPECT_EQ(scalar(db, "SELECT count(*) FROM events"), 2);
}
#endif
