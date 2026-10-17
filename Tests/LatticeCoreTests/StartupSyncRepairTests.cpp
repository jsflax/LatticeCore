#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

// Standalone-friendly: no TestHelpers.hpp global /tmp log or model registry.
// The supervising runner supplies TMPDIR for all fixture artifacts.
namespace {
using namespace lattice;

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

struct OwnedFile {
    std::filesystem::path path;
    OwnedFile() {
        static std::atomic<unsigned> sequence{0};
        path = std::filesystem::temp_directory_path() /
            ("startup-sync-repair-" + std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
             std::to_string(sequence.fetch_add(1)) + ".sqlite");
        require(!std::filesystem::exists(path), "fresh fixture required");
    }
    ~OwnedFile() {
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
        std::filesystem::remove(path.string() + "-wal", ignored);
        std::filesystem::remove(path.string() + "-shm", ignored);
    }
};

configuration config(const OwnedFile& file) {
    configuration value(file.path.string());
    value.busy_timeout_ms = 100;
    value.audit_retention_seconds = 0;
    return value;
}

struct Fixture {
    OwnedFile file;
    lattice_db owner{config(file)};
    void slot(const std::string& id) {
        owner.db().execute("INSERT INTO _lattice_replication_slots(sync_id) VALUES(?)", {id});
    }
    void audit(int64_t id, bool synced = false) {
        owner.db().execute(
            "INSERT INTO AuditLog(id,tableName,operation,isSynchronized) VALUES(?,'fixture','INSERT',?)",
            {id, int64_t(synced)});
    }
    void state(int64_t id, const std::string& slot, bool synced = true) {
        owner.db().execute(
            "INSERT INTO _lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,?)",
            {id, slot, int64_t(synced)});
    }
};

int64_t scalar(database& db, const std::string& sql) {
    const auto rows = db.query(sql);
    require(rows.size() == 1 && rows[0].size() == 1, "exact scalar result required");
    return std::get<int64_t>(rows[0].begin()->second);
}

struct Image {
    std::vector<database::row_t> audit, state, slots, schema, metadata;
    int64_t schema_version;
    bool operator==(const Image&) const = default;
};
Image image(database& db) {
    return {db.query("SELECT * FROM AuditLog ORDER BY id"),
            db.query("SELECT * FROM _lattice_sync_state ORDER BY audit_entry_id,sync_id"),
            db.query("SELECT * FROM _lattice_replication_slots ORDER BY sync_id"),
            db.query("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"),
            db.query("SELECT * FROM _lattice_meta ORDER BY key"),
            scalar(db, "PRAGMA schema_version")};
}

struct Trace {
    int begins = 0, commits = 0, rollbacks = 0;
    std::function<void(const std::string&)> observe;
    std::exception_ptr failure;
    static int callback(unsigned event, void* context, void* statement, void*) noexcept {
        if (event != SQLITE_TRACE_STMT) return 0;
        auto& self = *static_cast<Trace*>(context);
        try {
            const char* raw = sqlite3_sql(static_cast<sqlite3_stmt*>(statement));
            if (!raw) return 0;
            const std::string sql(raw);
            if (sql.starts_with("BEGIN")) ++self.begins;
            if (sql == "COMMIT") ++self.commits;
            if (sql == "ROLLBACK") ++self.rollbacks;
            if (self.observe) self.observe(sql);
        } catch (...) { self.failure = std::current_exception(); }
        return 0;
    }
    void checked() const { if (failure) std::rethrow_exception(failure); }
};

struct TraceGuard {
    sqlite3* db;
    TraceGuard(sqlite3* db, Trace& trace) : db(db) {
        require(sqlite3_trace_v2(db, SQLITE_TRACE_STMT, Trace::callback, &trace) == SQLITE_OK,
                "trace installation");
    }
    ~TraceGuard() { sqlite3_trace_v2(db, 0, nullptr, nullptr); }
};

// The constructor does not expose its new SQLite handles. Use its existing
// logger instead of process-global SQLite auto extensions (unsupported on
// Apple). Under a retained foreign writer every attempted writer admission
// fails visibly; the legitimate deferred fingerprint read remains allowed.
struct ConstructorLog {
    std::filesystem::path path;
    FILE* file = nullptr;
    FILE* previous_file = nullptr;
    log_level previous_level;
    explicit ConstructorLog(const OwnedFile& owned) : path(owned.path.string() + ".reopen.log") {
        file = std::fopen(path.string().c_str(), "w+");
        require(file != nullptr, "constructor log open");
        std::lock_guard<std::mutex> hold(g_log_mutex());
        previous_file = g_log_file.load(std::memory_order_acquire);
        previous_level = get_log_level();
        g_log_file.store(file, std::memory_order_release);
        set_log_level(log_level::debug);
    }
    ~ConstructorLog() {
        {
            std::lock_guard<std::mutex> hold(g_log_mutex());
            g_log_file.store(previous_file, std::memory_order_release);
            set_log_level(previous_level);
            std::fclose(file);
        }
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
    }
    std::string snapshot() const {
        std::lock_guard<std::mutex> hold(g_log_mutex());
        require(std::fflush(file) == 0 && std::fseek(file, 0, SEEK_END) == 0,
                "constructor log flush");
        const auto length = std::ftell(file);
        require(length >= 0 && length <= 1024 * 1024, "bounded constructor log");
        require(std::fseek(file, 0, SEEK_SET) == 0, "constructor log rewind");
        std::string text(static_cast<size_t>(length), '\0');
        require(std::fread(text.data(), 1, text.size(), file) == text.size(), "constructor log read");
        require(std::fseek(file, 0, SEEK_END) == 0, "constructor log append position");
        return text;
    }
};

TEST(StartupSyncRepair, HealthyReopenDoesNotAdmitWriter) {
    Fixture f;
    f.slot("live-a");
    f.audit(1);
    // Nonempty, unconfirmed state must have the same fast path as empty state.
    f.state(1, "live-a", false);
    const auto before = image(f.owner.db());
    database blocker(f.file.path.string(), database::open_mode::read_write, 100);
    blocker.begin_transaction();
    {
        // Directly prove the repair's no-admission property on a supported
        // per-connection trace. The old unconditional BEGIN fails this check.
        Trace trace;
        TraceGuard guard(f.owner.db().handle(), trace);
        f.owner.heal_collapsed_sync_state();
        trace.checked();
        EXPECT_EQ(trace.begins, 0);
        EXPECT_EQ(trace.commits, 0);
        EXPECT_EQ(trace.rollbacks, 0);
        EXPECT_EQ(image(f.owner.db()), before);
    }
    {
        ConstructorLog log(f.file);
        lattice_db reopened{config(f.file)};
        EXPECT_EQ(image(reopened.db()), before);
        EXPECT_TRUE(blocker.is_in_transaction());
        EXPECT_EQ(sqlite3_txn_state(blocker.handle(), "main"), SQLITE_TXN_WRITE);
        EXPECT_FALSE(reopened.db().is_in_transaction());
        // Inspect only initialization, before reopened's destructor can run
        // checkpoint/maintenance. Successful markers prove logging is active.
        const auto startup = log.snapshot();
        SCOPED_TRACE(startup);
        EXPECT_NE(startup.find("ctor start path=" + f.file.path.string()), std::string::npos);
        EXPECT_NE(startup.find("ensure_tables: fast path (fingerprint match)"), std::string::npos);
        EXPECT_NE(startup.find("ctor done"), std::string::npos);
        EXPECT_EQ(startup.find("Failed to begin transaction"), std::string::npos);
        EXPECT_EQ(startup.find("heal_collapsed_sync_state failed"), std::string::npos);
    }
    blocker.rollback();
    EXPECT_EQ(image(f.owner.db()), before);
}

TEST(StartupSyncRepair, EmptyStateAndZeroSlotsDoNotBegin) {
    Fixture f;
    Trace trace;
    TraceGuard guard(f.owner.db().handle(), trace);
    f.owner.heal_collapsed_sync_state();
    f.slot("live-a");
    f.owner.heal_collapsed_sync_state();
    f.owner.db().execute("DELETE FROM _lattice_replication_slots");
    f.audit(1);
    f.state(1, "dead");
    const auto before = image(f.owner.db());
    f.owner.heal_collapsed_sync_state();
    EXPECT_EQ(image(f.owner.db()), before);
    EXPECT_EQ(trace.begins, 0);
}

TEST(StartupSyncRepair, EligibleRepairWithHeldWriterRemainsNonfatalAndUnchanged) {
    Fixture f;
    f.slot("live-a"); f.audit(1); f.state(1, "live-a");
    const auto before = image(f.owner.db());
    database blocker(f.file.path.string(), database::open_mode::read_write, 100);
    blocker.begin_transaction();
    Trace trace;
    TraceGuard guard(f.owner.db().handle(), trace);
    EXPECT_NO_THROW(f.owner.heal_collapsed_sync_state());
    EXPECT_GT(trace.begins, 0);
    EXPECT_EQ(trace.commits, 0); EXPECT_EQ(trace.rollbacks, 0);
    EXPECT_TRUE(blocker.is_in_transaction());
    EXPECT_EQ(sqlite3_txn_state(blocker.handle(), "main"), SQLITE_TXN_WRITE);
    EXPECT_FALSE(f.owner.db().is_in_transaction());
    EXPECT_FALSE(f.owner.db().is_closed());
    EXPECT_EQ(image(f.owner.db()), before);
    blocker.rollback();
    f.owner.heal_collapsed_sync_state();
    EXPECT_EQ(scalar(f.owner.db(), "SELECT isSynchronized FROM AuditLog WHERE id=1"), 1);
    EXPECT_EQ(scalar(f.owner.db(), "SELECT COUNT(*) FROM _lattice_sync_state"), 0);
}

TEST(StartupSyncRepair, DoesNotRollBackExistingCallerTransaction) {
    Fixture f;
    f.slot("live-a"); f.audit(1); f.state(1, "live-a");
    const auto before = image(f.owner.db());
    f.owner.db().begin_transaction();
    f.owner.db().execute("UPDATE AuditLog SET tableName='caller-change' WHERE id=1");
    const auto in_transaction = image(f.owner.db());
    Trace trace;
    TraceGuard guard(f.owner.db().handle(), trace);
    EXPECT_NO_THROW(f.owner.heal_collapsed_sync_state());
    EXPECT_TRUE(f.owner.db().is_in_transaction());
    EXPECT_EQ(trace.begins, 0); EXPECT_EQ(trace.commits, 0); EXPECT_EQ(trace.rollbacks, 0);
    EXPECT_EQ(image(f.owner.db()), in_transaction);
    f.owner.db().rollback();
    EXPECT_EQ(image(f.owner.db()), before);
}

TEST(StartupSyncRepair, RepairsOnlyEligibleRowsIncludingOrphanAndAlreadySynced) {
    Fixture f;
    f.slot("live-a"); f.slot("live-b");
    f.audit(1); f.audit(2); f.audit(3, true);
    for (const auto id : {1, 3, 999}) {
        f.state(id, "live-a"); f.state(id, "live-b");
        f.state(id, "dead", false);
    }
    f.state(2, "live-a"); f.state(2, "live-b", false);
    const auto partial = f.owner.db().query("SELECT * FROM _lattice_sync_state WHERE audit_entry_id=2 ORDER BY sync_id");
    Trace trace;
    TraceGuard guard(f.owner.db().handle(), trace);
    f.owner.heal_collapsed_sync_state();
    EXPECT_EQ(scalar(f.owner.db(), "SELECT COUNT(*) FROM AuditLog WHERE isSynchronized=1"), 2);
    EXPECT_EQ(scalar(f.owner.db(), "SELECT isSynchronized FROM AuditLog WHERE id=2"), 0);
    EXPECT_EQ(f.owner.db().query("SELECT * FROM _lattice_sync_state ORDER BY sync_id"), partial);
    EXPECT_EQ(trace.begins, 1); EXPECT_EQ(trace.commits, 1); EXPECT_EQ(trace.rollbacks, 0);
    const auto after = image(f.owner.db());
    f.owner.heal_collapsed_sync_state();
    EXPECT_EQ(image(f.owner.db()), after);
    EXPECT_EQ(trace.begins, 1); // Second repair needs no writer admission.
}

TEST(StartupSyncRepair, DeadConfirmationCannotReplaceUnacknowledgedLiveSlot) {
    Fixture f;
    f.slot("live-a"); f.slot("live-b");
    f.audit(1); f.state(1, "live-a"); f.state(1, "dead");
    const auto before = image(f.owner.db());
    Trace trace;
    TraceGuard guard(f.owner.db().handle(), trace);
    f.owner.heal_collapsed_sync_state();
    EXPECT_EQ(image(f.owner.db()), before);
    EXPECT_EQ(trace.begins, 0);
}

TEST(StartupSyncRepair, SlotAddedBeforeBeginPreventsStaleCountCollapse) {
    Fixture f;
    f.slot("live-a"); f.audit(1); f.state(1, "live-a");
    database peer(f.file.path.string(), database::open_mode::read_write, 100);
    Trace trace;
    trace.observe = [&](const std::string& sql) {
        if (sql == "BEGIN IMMEDIATE") {
            require(trace.begins == 1, "one acquisition expected");
            peer.execute("INSERT INTO _lattice_replication_slots(sync_id) VALUES('live-b')");
        }
    };
    TraceGuard guard(f.owner.db().handle(), trace);
    f.owner.heal_collapsed_sync_state();
    trace.checked();
    ASSERT_EQ(trace.begins, 1);
    EXPECT_EQ(scalar(f.owner.db(), "SELECT COUNT(*) FROM _lattice_replication_slots"), 2);
    EXPECT_EQ(scalar(f.owner.db(), "SELECT isSynchronized FROM AuditLog WHERE id=1"), 0);
    EXPECT_EQ(scalar(f.owner.db(), "SELECT COUNT(*) FROM _lattice_sync_state"), 1);
    EXPECT_EQ(trace.commits, 1); EXPECT_EQ(trace.rollbacks, 0);
}

TEST(StartupSyncRepair, SlotRemovedBeforeBeginRechecksPreviouslyPartialRow) {
    Fixture f;
    f.slot("live-a"); f.slot("live-b"); f.audit(1); f.audit(2);
    // Row 1 admits repair; row 2 becomes eligible only after removal.
    f.state(1, "live-a"); f.state(1, "live-b"); f.state(2, "live-a");
    database peer(f.file.path.string(), database::open_mode::read_write, 100);
    Trace trace;
    trace.observe = [&](const std::string& sql) {
        if (sql == "BEGIN IMMEDIATE") peer.execute("DELETE FROM _lattice_replication_slots WHERE sync_id='live-b'");
    };
    TraceGuard guard(f.owner.db().handle(), trace);
    f.owner.heal_collapsed_sync_state();
    trace.checked();
    EXPECT_EQ(trace.begins, 1);
    EXPECT_EQ(scalar(f.owner.db(), "SELECT COUNT(*) FROM AuditLog WHERE isSynchronized=1"), 2);
    EXPECT_EQ(scalar(f.owner.db(), "SELECT COUNT(*) FROM _lattice_sync_state"), 0);
}

TEST(StartupSyncRepair, RemovingLastSlotBeforeBeginDoesNotCollapse) {
    Fixture f;
    f.slot("live-a"); f.audit(1); f.state(1, "live-a");
    database peer(f.file.path.string(), database::open_mode::read_write, 100);
    Trace trace;
    trace.observe = [&](const std::string& sql) {
        if (sql == "BEGIN IMMEDIATE") peer.execute("DELETE FROM _lattice_replication_slots");
    };
    TraceGuard guard(f.owner.db().handle(), trace);
    f.owner.heal_collapsed_sync_state();
    trace.checked();
    EXPECT_EQ(trace.begins, 1);
    EXPECT_EQ(scalar(f.owner.db(), "SELECT isSynchronized FROM AuditLog WHERE id=1"), 0);
    EXPECT_EQ(scalar(f.owner.db(), "SELECT COUNT(*) FROM _lattice_sync_state"), 1);
}

struct Authorizer {
    sqlite3* db;
    int denied_action;
    const char* denied_table;
    int denials = 0;
    static int callback(void* context, int action, const char* table, const char*, const char*, const char*) noexcept {
        auto& self = *static_cast<Authorizer*>(context);
        if (action == self.denied_action && table && std::strcmp(table, self.denied_table) == 0) {
            ++self.denials;
            return SQLITE_DENY;
        }
        return SQLITE_OK;
    }
    Authorizer(sqlite3* db, int action, const char* table) : db(db), denied_action(action), denied_table(table) {
        require(sqlite3_set_authorizer(db, callback, this) == SQLITE_OK, "authorizer installation");
    }
    ~Authorizer() { sqlite3_set_authorizer(db, nullptr, nullptr); }
};

TEST(StartupSyncRepair, ReadFailureDoesNotBeginOrMutate) {
    Fixture f;
    f.slot("live-a"); f.audit(1); f.state(1, "live-a");
    const auto before = image(f.owner.db());
    Trace trace;
    TraceGuard guard(f.owner.db().handle(), trace);
    {
        Authorizer denial(f.owner.db().handle(), SQLITE_READ, "_lattice_sync_state");
        EXPECT_NO_THROW(f.owner.heal_collapsed_sync_state()); // Best-effort startup repair.
        EXPECT_GT(denial.denials, 0);
    }
    EXPECT_EQ(trace.begins, 0);
    EXPECT_FALSE(f.owner.db().is_in_transaction());
    EXPECT_EQ(image(f.owner.db()), before);
}

TEST(StartupSyncRepair, MutationFailuresRollBackBothTables) {
    for (const auto action : {SQLITE_UPDATE, SQLITE_DELETE}) {
        SCOPED_TRACE(action);
        Fixture f;
        f.slot("live-a"); f.audit(1); f.state(1, "live-a");
        const auto before = image(f.owner.db());
        Trace trace;
        TraceGuard guard(f.owner.db().handle(), trace);
        {
            Authorizer denial(f.owner.db().handle(), action,
                              action == SQLITE_UPDATE ? "AuditLog" : "_lattice_sync_state");
            EXPECT_NO_THROW(f.owner.heal_collapsed_sync_state());
            EXPECT_GT(denial.denials, 0);
        }
        EXPECT_EQ(trace.begins, 1); EXPECT_EQ(trace.commits, 0); EXPECT_EQ(trace.rollbacks, 1);
        EXPECT_FALSE(f.owner.db().is_in_transaction());
        EXPECT_FALSE(f.owner.db().is_closed());
        EXPECT_EQ(image(f.owner.db()), before);
    }
}

TEST(StartupSyncRepair, PositiveRepairHoldsWriterThroughBothMutations) {
    Fixture f;
    f.slot("live-a"); f.audit(1); f.state(1, "live-a");
    database peer(f.file.path.string(), database::open_mode::read_write, 100);
    sqlite3_busy_timeout(peer.handle(), 0);
    int attempts = 0;
    Trace trace;
    trace.observe = [&](const std::string& sql) {
        if (!sql.starts_with("UPDATE AuditLog SET isSynchronized") &&
            !sql.starts_with("DELETE FROM _lattice_sync_state WHERE audit_entry_id")) return;
        ++attempts;
        require(sqlite3_txn_state(f.owner.db().handle(), "main") == SQLITE_TXN_WRITE,
                "repair must own writer admission");
        const auto rc = sqlite3_exec(peer.handle(), "BEGIN IMMEDIATE", nullptr, nullptr, nullptr);
        if (rc == SQLITE_OK) sqlite3_exec(peer.handle(), "ROLLBACK", nullptr, nullptr, nullptr);
        require((rc & 0xff) == SQLITE_BUSY, "competing writer must remain excluded");
    };
    TraceGuard guard(f.owner.db().handle(), trace);
    f.owner.heal_collapsed_sync_state();
    trace.checked();
    EXPECT_EQ(attempts, 2);
    EXPECT_EQ(trace.begins, 1); EXPECT_EQ(trace.commits, 1);
    EXPECT_EQ(scalar(f.owner.db(), "SELECT isSynchronized FROM AuditLog WHERE id=1"), 1);
    EXPECT_EQ(scalar(f.owner.db(), "SELECT COUNT(*) FROM _lattice_sync_state"), 0);
    EXPECT_EQ(sqlite3_exec(peer.handle(), "BEGIN IMMEDIATE", nullptr, nullptr, nullptr), SQLITE_OK);
    EXPECT_EQ(sqlite3_exec(peer.handle(), "ROLLBACK", nullptr, nullptr, nullptr), SQLITE_OK);
}
} // namespace
