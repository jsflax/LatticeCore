#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>

struct CursorRegressionItem { int sequence; };
LATTICE_SCHEMA(CursorRegressionItem, sequence);

namespace {
using statement_ptr = std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;
using sqlite_ptr = std::unique_ptr<sqlite3, decltype(&sqlite3_close)>;

class ObserverCursorRegression : public ::testing::Test {
protected:
    std::filesystem::path path;
    std::unique_ptr<lattice::lattice_db> owner;
    std::vector<int64_t> inserts;
    lattice::lattice_db::observer_id token = 0;

    void SetUp() override {
        ASSERT_EQ(std::getenv("LATTICE_DISABLE_XPROC"), nullptr)
            << "this regression needs explicit cross-process handler dispatch";
        path = std::filesystem::temp_directory_path() /
            ("observer-cursor-" + std::to_string(getpid()) + "-" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sqlite");
        owner = std::make_unique<lattice::lattice_db>(lattice::configuration(path.string()));
        auto* notifier = lattice::instance_registry::instance().get_or_create_notifier(path.string());
        ASSERT_NE(notifier, nullptr) << "requires the native Darwin/Linux notifier";
        // Keep the notifier object (and local cursor-advance path), but stop
        // its background source before writing anything. The test explicitly
        // invokes the real handler: no sleeps, races, private access or stubs.
        notifier->stop_listening();
        ASSERT_FALSE(notifier->is_listening());
        ASSERT_NE(&owner->read_db(), &owner->db());
        ASSERT_NE(&owner->xproc_read_db(), &owner->read_db());
        for (int i = 1; i <= 5; ++i) owner->add(CursorRegressionItem{i});
        ASSERT_EQ(audit_max(owner->db()), 5);
        token = owner->add_table_observer("CursorRegressionItem", [this](const auto& changes) {
            for (const auto& change : changes) {
                if (std::get<1>(change) == "INSERT") inserts.push_back(std::get<2>(change));
            }
        });
    }

    void TearDown() override {
        if (owner && token) owner->remove_table_observer("CursorRegressionItem", token);
        owner.reset();
        if (!path.empty()) {
            std::error_code ignored;
            for (const auto* suffix : {"", "-wal", "-shm", "-signal"}) {
                std::filesystem::remove(path.string() + suffix, ignored);
            }
        }
    }

    int64_t audit_max(lattice::database& db) {
        const auto rows = db.query("SELECT MAX(id) AS n FROM AuditLog");
        return std::get<int64_t>(rows.at(0).at("n"));
    }

    void rollback_three() {
        owner->begin_transaction();
        for (int i = 6; i <= 8; ++i) owner->add(CursorRegressionItem{i});
        ASSERT_EQ(audit_max(owner->db()), 8);
        owner->rollback();
        ASSERT_EQ(audit_max(owner->db()), 5);
        ASSERT_TRUE(inserts.empty());
    }

    // A real independent SQLite writer with model audit triggers, but no
    // Lattice update hook / same-process registry fan-out. This reproduces
    // the externally committed audit IDs the explicit handler consumes.
    void external_insert() {
        sqlite3* raw = nullptr;
        const auto open_rc = sqlite3_open_v2(path.string().c_str(), &raw,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, nullptr);
        sqlite_ptr remote(raw, sqlite3_close);
        ASSERT_EQ(open_rc, SQLITE_OK);
        ASSERT_EQ(sqlite3_busy_timeout(remote.get(), 1000), SQLITE_OK);
        ASSERT_EQ(sqlite3_create_function(remote.get(), "sync_disabled", 0,
            SQLITE_UTF8, nullptr, [](sqlite3_context* context, int, sqlite3_value**) {
                sqlite3_result_int(context, 0);
            }, nullptr, nullptr), SQLITE_OK);
        const auto rc = sqlite3_exec(remote.get(),
            "INSERT INTO CursorRegressionItem(globalId, sequence) "
            "VALUES('external-cursor-regression', 999)", nullptr, nullptr, nullptr);
        ASSERT_EQ(rc, SQLITE_OK) << sqlite3_errmsg(remote.get());
    }
};

TEST_F(ObserverCursorRegression, HeldReaderDoesNotReplayLocalInsert) {
    sqlite3_stmt* raw = nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(owner->read_db().handle(),
        "SELECT id FROM CursorRegressionItem ORDER BY id", -1, &raw, nullptr), SQLITE_OK);
    statement_ptr held(raw, sqlite3_finalize);
    ASSERT_EQ(sqlite3_step(held.get()), SQLITE_ROW);
    ASSERT_EQ(sqlite3_column_int64(held.get(), 0), 1);
    ASSERT_EQ(audit_max(owner->read_db()), 5);

    owner->add(CursorRegressionItem{6});
    ASSERT_EQ(inserts, (std::vector<int64_t>{6})) << "local delivery is synchronous";
    ASSERT_EQ(audit_max(owner->db()), 6);
    ASSERT_EQ(audit_max(owner->read_db()), 5) << "held reader still has the old snapshot";
    owner->handle_cross_process_notification();
    std::cout << "CURSOR_REPRO writer_max=6 reader_max=5 deliveries=" << inserts.size() << '\n';
    EXPECT_EQ(inserts, (std::vector<int64_t>{6})) << "one local INSERT must not replay through xproc";
    held.reset();
    owner->handle_cross_process_notification();
    EXPECT_EQ(inserts, (std::vector<int64_t>{6})) << "later empty notification must remain silent";
}

TEST_F(ObserverCursorRegression, UnheldReaderDeliversLocalInsertOnce) {
    owner->add(CursorRegressionItem{6});
    owner->handle_cross_process_notification();
    owner->handle_cross_process_notification();
    EXPECT_EQ(inserts, (std::vector<int64_t>{6}));
}

TEST_F(ObserverCursorRegression, RollbackThenLocalReuseStillAllowsNextExternalAuditID) {
    ASSERT_NO_FATAL_FAILURE(rollback_three());
    owner->add(CursorRegressionItem{66});
    ASSERT_EQ(audit_max(owner->db()), 6) << "rolled-back AUTOINCREMENT IDs can be reused";
    owner->handle_cross_process_notification();
    ASSERT_EQ(inserts, (std::vector<int64_t>{6}));
    ASSERT_NO_FATAL_FAILURE(external_insert());
    ASSERT_EQ(audit_max(owner->db()), 7);
    owner->handle_cross_process_notification();
    EXPECT_EQ(inserts, (std::vector<int64_t>{6, 7}));
}

TEST_F(ObserverCursorRegression, AuditDisabledCommitPreservesRollbackCursorRecovery) {
    ASSERT_NO_FATAL_FAILURE(rollback_three());
    owner->db().execute("UPDATE _SyncControl SET disabled = 1 WHERE id = 1");
    owner->add(CursorRegressionItem{66});
    owner->db().execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");
    ASSERT_EQ(audit_max(owner->db()), 5);
    ASSERT_EQ(inserts, (std::vector<int64_t>{6}));
    ASSERT_NO_FATAL_FAILURE(external_insert());
    ASSERT_EQ(audit_max(owner->db()), 6);
    owner->handle_cross_process_notification();
    EXPECT_EQ(inserts, (std::vector<int64_t>{6, 7}))
        << "a blanket monotonic cursor CAS would prevent this existing recovery";
}

} // namespace
