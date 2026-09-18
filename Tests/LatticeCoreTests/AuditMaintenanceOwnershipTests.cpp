#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <lattice/sync.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#if GTEST_HAS_DEATH_TEST && (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <csignal>
#include <unistd.h>

// Friend-only source seam: no public injection, runtime hook or new API.
namespace lattice {
struct audit_maintenance_test_access {
    template<class F> static int64_t owned(lattice_db& owner, F&& body) {
        return owner.with_audit_prune_transaction_(std::forward<F>(body));
    }
};
}

struct AuditMaintenanceRow { int64_t value = 0; };
LATTICE_SCHEMA(AuditMaintenanceRow, value);

namespace {
using namespace lattice;
using IDs = std::vector<std::pair<int64_t, std::string>>;
constexpr const char* target = "floor-race-writer";
constexpr const char* resolved = "resolved-writer";
constexpr int64_t row_count = 12;
void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
struct OwnedFile {
    std::filesystem::path path;
    OwnedFile() : path(std::filesystem::temp_directory_path() /
        ("retention-owned-" + std::to_string(getpid()) + ".sqlite")) {
        require(!std::filesystem::exists(path), "fresh owned fixture required");
    }
    ~OwnedFile() {
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
        std::filesystem::remove(path.string() + "-wal", ignored);
        std::filesystem::remove(path.string() + "-shm", ignored);
    }
};
configuration config(const std::string& path) {
    configuration value(path); value.audit_retention_seconds = 0; value.busy_timeout_ms = 5000;
    return value;
}
IDs ids(database& db) {
    IDs result;
    for (const auto& row : db.query("SELECT id,globalId FROM AuditLog ORDER BY id"))
        result.emplace_back(std::get<int64_t>(row.at("id")), std::get<std::string>(row.at("globalId")));
    return result;
}
IDs pending(database& db) {
    IDs result;
    for (const auto& entry : query_audit_log_for_sync(db, target, std::nullopt,
                                                    read_upload_floor(db, target), 32))
        result.emplace_back(entry.id, entry.global_id);
    return result;
}
int64_t raw_scalar(sqlite3* db, const char* sql) {
    sqlite3_stmt* raw = nullptr;
    require(sqlite3_prepare_v2(db, sql, -1, &raw, nullptr) == SQLITE_OK, "raw scalar prepare");
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement(raw, sqlite3_finalize);
    require(sqlite3_step(raw) == SQLITE_ROW, "raw scalar row");
    const auto value = sqlite3_column_int64(raw, 0);
    require(sqlite3_step(raw) == SQLITE_DONE, "raw scalar end");
    return value;
}
IDs seed(lattice_db& owner, bool reset) {
    owner.begin_transaction();
    for (int64_t i = 1; i <= row_count; ++i) owner.add(AuditMaintenanceRow{i});
    owner.commit();
    const auto* initial = reset ? target : resolved;
    register_replication_slot(owner.db(), initial);
    advance_upload_floor(owner.db(), initial, row_count);
    owner.record_audit_watermark(); owner.backdate_audit_watermarks(1200);
    owner.db().execute("CREATE TABLE IF NOT EXISTS _lattice_applied_receipts (globalId TEXT PRIMARY KEY)");
    const auto original = ids(owner.db());
    require(original.size() == row_count && original.front().first == 1 && original.back().first == row_count,
            "original twelve exact identities");
    require(raw_scalar(owner.db().handle(), "SELECT COUNT(*) FROM AuditLog WHERE isSynchronized=0") == row_count,
            "original pending premise");
    return original;
}
int64_t prune(lattice_db& owner, bool age) {
    return age ? owner.prune_audit_log(600) : owner.safe_compact_audit_log();
}
void mutate(lattice_db& owner, bool reset) {
    if (reset) owner.reset_sync_state(target);
    else register_replication_slot(owner.db(), target);
}

// The same four operation/mutation combinations as retained floor-race evidence,
// with both possible serial orders explicit. The old raw run remains unchanged.
void mutation_before_owner(bool age, bool reset) {
    OwnedFile file;
    lattice_db owner(config(file.path.string()));
    const auto original = seed(owner, reset);
    mutate(owner, reset);
    require(pending(owner.db()) == original, "new zero floor sees every original ID");
    require(prune(owner, age) == 0, "zero writer floor must prevent deletion");
    require(ids(owner.db()) == original && pending(owner.db()) == original,
            "every pending ID survives a prior committed registration/reset");
    require(raw_scalar(owner.db().handle(), "SELECT seq FROM sqlite_sequence WHERE name='AuditLog'") == row_count,
            "sequence unchanged");
}
struct FloorAttempt {
    sqlite3* owner = nullptr;
    sqlite3* peer = nullptr;
    bool reset = false;
    int calls = 0, begin_result = -1, mutation_result = -1, commit_result = -1;
    bool owner_writing = false;
    static int callback(unsigned event, void* context, void* pointer, void*) noexcept {
        if (event != SQLITE_TRACE_STMT) return 0;
        auto& self = *static_cast<FloorAttempt*>(context);
        const char* sql = sqlite3_sql(static_cast<sqlite3_stmt*>(pointer));
        // Original post-floor/pre-delete seam. Its local trace mutex alone
        // cannot block a DIFFERENT SQLite connection; BEGIN ownership must.
        if (!sql || std::strcmp(sql, "CREATE TABLE IF NOT EXISTS _lattice_applied_receipts (  globalId TEXT PRIMARY KEY)") != 0)
            return 0;
        ++self.calls;
        self.owner_writing = sqlite3_get_autocommit(self.owner) == 0 &&
                             sqlite3_txn_state(self.owner, "main") == SQLITE_TXN_WRITE;
        self.begin_result = sqlite3_exec(self.peer, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr);
        if (self.begin_result == SQLITE_OK) {
            // Exact fresh-fixture effects of registration/reset. Baseline can
            // admit and commit these here, then wrongly delete their history.
            const char* mutation = self.reset
                ? "DELETE FROM _lattice_sync_state WHERE sync_id='floor-race-writer';"
                  "DELETE FROM _lattice_sync_set WHERE sync_id='floor-race-writer';"
                  "UPDATE _lattice_replication_slots SET confirmed_audit_id=0,upload_floor=0 WHERE sync_id='floor-race-writer'"
                : "INSERT INTO _lattice_replication_slots(sync_id,last_active_at,is_observer) VALUES('floor-race-writer',datetime('now'),0)";
            self.mutation_result = sqlite3_exec(self.peer, mutation, nullptr, nullptr, nullptr);
            self.commit_result = sqlite3_exec(self.peer, self.mutation_result == SQLITE_OK ? "COMMIT" : "ROLLBACK",
                                              nullptr, nullptr, nullptr);
        }
        return 0;
    }
};
struct TraceGuard {
    sqlite3* connection;
    ~TraceGuard() { sqlite3_trace_v2(connection, 0, nullptr, nullptr); }
};
void mutation_while_owned(bool age, bool reset) {
    OwnedFile file;
    lattice_db owner(config(file.path.string()));
    const auto original = seed(owner, reset);
    database peer(file.path.string(), database::open_mode::read_write, 5000);
    auto* peer_handle = peer.handle();
    sqlite3_busy_timeout(peer_handle, 0); // One bounded acquisition attempt, no sleeping oracle.
    FloorAttempt trace{owner.db().handle(), peer_handle, reset};
    require(sqlite3_trace_v2(trace.owner, SQLITE_TRACE_STMT, FloorAttempt::callback, &trace) == SQLITE_OK,
            "floor trace install");
    TraceGuard guard{trace.owner};
    const auto removed = prune(owner, age);
    require(sqlite3_trace_v2(trace.owner, 0, nullptr, nullptr) == SQLITE_OK, "floor trace removal");
    require(trace.calls == 1 && trace.owner_writing, "floor and delete belong to one owned write transaction");
    require((trace.begin_result & 0xff) == SQLITE_BUSY && trace.mutation_result == -1 && trace.commit_result == -1,
            "competing floor mutation cannot commit after protected floor read");
    require(removed == row_count && ids(owner.db()).empty(), "maintenance-first serial order deletes only resolved history");
    require(raw_scalar(owner.db().handle(), "SELECT seq FROM sqlite_sequence WHERE name='AuditLog'") == original.back().first,
            "sequence retained");
    // The competing connection becomes writable after the owner commits.
    require(sqlite3_exec(peer_handle, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr) == SQLITE_OK, "peer proceeds after commit");
    require(sqlite3_exec(peer_handle, "ROLLBACK", nullptr, nullptr, nullptr) == SQLITE_OK, "peer rollback");
}
void public_bound_respects_floor() {
    OwnedFile file;
    lattice_db owner(config(file.path.string()));
    const auto original = seed(owner, true);
    owner.reset_sync_state(target);
    require(owner.delete_audit_below_(row_count, false) == 0, "caller bound cannot bypass zero writer floor");
    require(ids(owner.db()) == original && pending(owner.db()) == original, "public helper retains every pending ID");
}
void public_bound_respects_legacy_cursor() {
    OwnedFile file;
    lattice_db owner(config(file.path.string()));
    const auto original = seed(owner, false);
    owner.db().execute("UPDATE AuditLog SET isFromRemote=1 WHERE id=12");
    require(raw_scalar(owner.db().handle(), "SELECT COUNT(*) FROM _lattice_replication_slots WHERE last_received_event_id IS NULL") == 1,
            "legacy slot needs a cursor row");
    require(owner.delete_audit_below_(row_count, false) == row_count - 1, "false caller hint cannot bypass required cursor");
    require(ids(owner.db()) == IDs{original.back()}, "actual latest remote identity preserved");
}
void failed_delete_rolls_back_flag_and_history() {
    OwnedFile file;
    lattice_db owner(config(file.path.string()));
    const auto original = seed(owner, false);
    auto& db = owner.db(); auto* raw = db.handle();
    const auto disabled = raw_scalar(raw, "SELECT disabled FROM _SyncControl WHERE id=1");
    sqlite3_set_authorizer(raw, [](void*, int action, const char* first, const char*, const char*, const char*) {
        return action == SQLITE_DELETE && first && std::strcmp(first, "_lattice_sync_state") == 0 ? SQLITE_DENY : SQLITE_OK;
    }, nullptr);
    bool threw = false;
    try { owner.prune_audit_log(600); } catch (const db_error&) { threw = true; }
    sqlite3_set_authorizer(raw, nullptr, nullptr);
    require(threw && !db.is_closed() && sqlite3_get_autocommit(raw) == 1, "failed deletion rolls back cleanly");
    require(ids(db) == original && raw_scalar(raw, "SELECT disabled FROM _SyncControl WHERE id=1") == disabled,
            "audit history and prior trigger flag restored together");
}
void denied_disabled_read_aborts_without_guessing() {
    OwnedFile file;
    lattice_db owner(config(file.path.string()));
    const auto original = seed(owner, false);
    auto& db = owner.db(); auto* raw = db.handle();
    db.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
    sqlite3_set_authorizer(raw, [](void*, int action, const char* table, const char* column, const char*, const char*) {
        return action == SQLITE_READ && table && column && std::strcmp(table, "_SyncControl") == 0 &&
            std::strcmp(column, "disabled") == 0 ? SQLITE_DENY : SQLITE_OK;
    }, nullptr);
    bool threw = false;
    try { owner.prune_audit_log(600); } catch (const db_error&) { threw = true; }
    sqlite3_set_authorizer(raw, nullptr, nullptr);
    require(threw && sqlite3_get_autocommit(raw) == 1 && !db.is_closed(), "denied flag read aborts maintenance cleanly");
    require(ids(db) == original && raw_scalar(raw, "SELECT disabled FROM _SyncControl WHERE id=1") == 1,
            "unreadable disabled flag is never guessed as zero");
}
void rejects_attached_only_read() {
    lattice_db owner;
    auto& db = owner.db(); auto* raw = db.handle();
    db.execute("ATTACH DATABASE ':memory:' AS held; CREATE TABLE held.only_row(id INTEGER PRIMARY KEY,globalId TEXT,value INTEGER); INSERT INTO held.only_row VALUES(1,'retention-attached-only',7)");
    sqlite3_stmt* statement = nullptr;
    require(sqlite3_prepare_v2(raw, "SELECT value FROM held.only_row", -1, &statement, nullptr) == SQLITE_OK, "attached read prepare");
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> held(statement, sqlite3_finalize);
    require(sqlite3_step(statement) == SQLITE_ROW && sqlite3_get_autocommit(raw) == 1 &&
            sqlite3_txn_state(raw, "main") == SQLITE_TXN_NONE && sqlite3_txn_state(raw, "held") == SQLITE_TXN_READ,
            "only attached schema holds implicit read");
    bool called = false, rejected = false;
    try { audit_maintenance_test_access::owned(owner, [&]() -> int64_t { called = true; return 0; }); }
    catch (const db_error&) { rejected = true; }
    require(rejected && !called && sqlite3_column_int64(statement, 0) == 7,
            "attached reader rejected without touching caller statement");
    held.reset(); db.execute("DETACH DATABASE held");
}
struct UDFReentry {
    lattice_db* owner;
    int calls = 0;
    bool rejected = false, body_called = false, table_free_premise = false;
    bool gate_cycle = false, gate_ready = false, peer_completed = false, peer_failed = false;
    std::mutex ready_mutex;
    std::condition_variable ready;
    std::jthread peer;
    static void invoke(sqlite3_context* context, int, sqlite3_value**) noexcept {
        auto& self = *static_cast<UDFReentry*>(sqlite3_user_data(context));
        ++self.calls;
        auto* raw = sqlite3_context_db_handle(context);
        self.table_free_premise = sqlite3_get_autocommit(raw) == 1 && sqlite3_txn_state(raw, nullptr) == SQLITE_TXN_NONE;
        try {
            if (self.gate_cycle) {
                // The UDF already owns SQLite. The second thread takes the
                // store gate, then asks for SQLite, creating the exact order
                // that admission must reject BEFORE trying to take the gate.
                self.peer = std::jthread([&self] {
                    const auto gate = self.owner->store_write_gate();
                    std::lock_guard<std::recursive_timed_mutex> hold(*gate);
                    { std::lock_guard<std::mutex> lock(self.ready_mutex); self.gate_ready = true; }
                    self.ready.notify_one();
                    try {
                        const auto result = self.owner->db().query("SELECT 1 AS one");
                        self.peer_completed = result.size() == 1;
                    } catch (...) { self.peer_failed = true; }
                });
                std::unique_lock<std::mutex> lock(self.ready_mutex);
                if (!self.ready.wait_for(lock, std::chrono::seconds(1), [&] { return self.gate_ready; })) {
                    sqlite3_result_error(context, "gate holder did not enter", -1); return;
                }
            }
            audit_maintenance_test_access::owned(*self.owner, [&]() -> int64_t { self.body_called = true; return 0; });
        } catch (const db_error&) { self.rejected = true; }
        catch (...) { sqlite3_result_error(context, "unexpected maintenance error", -1); return; }
        sqlite3_result_int(context, self.rejected ? 1 : 0);
    }
};
struct UDFGuard {
    sqlite3* raw;
    ~UDFGuard() { sqlite3_create_function_v2(raw, "retention_reenter", 0, SQLITE_UTF8, nullptr, nullptr, nullptr, nullptr, nullptr); }
};
void rejects_table_free_udf_reentry(bool gate_cycle) {
    lattice_db owner(config(gate_cycle ? "file:retention-owned-udf?mode=memory&cache=shared" : ":memory:"));
    auto& db = owner.db(); auto* raw = db.handle();
    UDFReentry state{&owner}; state.gate_cycle = gate_cycle;
    if (gate_cycle) require(static_cast<bool>(owner.store_write_gate()), "shared-memory gate exists");
    require(sqlite3_create_function_v2(raw, "retention_reenter", 0, SQLITE_UTF8, &state,
                                     UDFReentry::invoke, nullptr, nullptr, nullptr) == SQLITE_OK, "UDF install");
    UDFGuard cleanup{raw};
    const auto rows = db.query("SELECT retention_reenter() AS rejected");
    if (state.peer.joinable()) state.peer.join();
    require(!gate_cycle || (state.gate_ready && state.peer_completed && !state.peer_failed), "competing gate holder progresses after UDF returns");
    require(state.calls == 1 && state.table_free_premise && state.rejected && !state.body_called,
            "table-free active statement rejects reentrant maintenance");
    require(rows.size() == 1 && std::get<int64_t>(rows[0].at("rejected")) == 1 && sqlite3_get_autocommit(raw) == 1,
            "outer caller statement completes unchanged");
}
void rejects_existing_transaction(bool implicit) {
    lattice_db owner;
    owner.add(AuditMaintenanceRow{1});
    auto& db = owner.db(); auto* raw = db.handle();
    sqlite3_stmt* statement = nullptr;
    if (implicit) {
        require(sqlite3_prepare_v2(raw, "SELECT * FROM AuditLog", -1, &statement, nullptr) == SQLITE_OK, "held read prepare");
        require(sqlite3_step(statement) == SQLITE_ROW, "held read first row");
    } else db.begin_transaction();
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> held(statement, sqlite3_finalize);
    bool called = false, rejected = false;
    try { audit_maintenance_test_access::owned(owner, [&]() -> int64_t { called = true; return 0; }); }
    catch (const db_error&) { rejected = true; }
    require(rejected && !called, "active caller transaction rejected before maintenance body");
    require(sqlite3_get_autocommit(raw) == (implicit ? 1 : 0), "caller transaction not committed or rolled back");
    held.reset();
    if (!implicit) db.rollback();
    require(raw_scalar(raw, "SELECT COUNT(*) FROM AuditLog") == 1, "caller data untouched");
}
struct LockResult { int sqlite = -1; bool gate = false; };
LockResult inspect_from_other_thread(sqlite3* raw, const std::shared_ptr<std::recursive_timed_mutex>& gate) {
    LockResult result;
    std::thread peer([&] {
        auto* mutex = sqlite3_db_mutex(raw);
        result.sqlite = sqlite3_mutex_try(mutex);
        if (result.sqlite == SQLITE_OK) sqlite3_mutex_leave(mutex);
        result.gate = gate->try_lock();
        if (result.gate) gate->unlock();
    });
    peer.join();
    return result;
}
void owns_between_statements_and_delivers_after_unlock() {
    lattice_db owner(config("file:retention-owned-callback?mode=memory&cache=shared"));
    owner.add(AuditMaintenanceRow{1});
    auto& db = owner.db(); auto* raw = db.handle(); const auto gate = owner.store_write_gate();
    require(static_cast<bool>(gate), "named memory store gate exists");
    int callbacks = 0; LockResult callback;
    // Test only the settled-drain seam; replacing its callbacks here does not
    // claim table-event payload coverage and adds no production hook.
    db.set_txn_hooks([&] { ++callbacks; callback = inspect_from_other_thread(raw, gate); }, [] {});
    const auto result = audit_maintenance_test_access::owned(owner, [&]() -> int64_t {
        db.execute("UPDATE AuditMaintenanceRow SET value=2");
        const auto inside = inspect_from_other_thread(raw, gate); // BETWEEN SQLite calls.
        require(inside.sqlite == SQLITE_BUSY && !inside.gate, "owner retains mutex and store gate between statements");
        require(callbacks == 0, "no callback before commit");
        return 7;
    });
    require(result == 7 && callbacks == 1, "one settled callback after commit");
    require(callback.sqlite == SQLITE_OK && callback.gate, "callback sees both outer locks released");
    require(raw_scalar(raw, "SELECT value FROM AuditMaintenanceRow") == 2, "owned write committed");
}
void close_during_owned_work(bool fail_body) {
    lattice_db owner;
    owner.add(AuditMaintenanceRow{1});
    auto& db = owner.db(); auto* raw = db.handle();
    bool threw = false, original_error = false;
    try {
        audit_maintenance_test_access::owned(owner, [&]() -> int64_t {
            db.execute("UPDATE AuditMaintenanceRow SET value=2");
            std::thread closer([&] { db.close(); }); closer.join();
            require(db.is_closed(), "logical close returns while operation owns transaction");
            db.execute("UPDATE AuditMaintenanceRow SET value=3");
            const auto admitted_rows = db.query("SELECT value FROM AuditMaintenanceRow");
            require(admitted_rows.size() == 1 && std::get<int64_t>(admitted_rows[0].at("value")) == 3,
                    "admitted owner can finish after close");
            if (fail_body) throw std::runtime_error("original-maintenance-failure");
            return 0;
        });
    } catch (const std::runtime_error& error) { threw = true; original_error = std::string(error.what()) == "original-maintenance-failure"; }
    require(threw == fail_body && (!threw || original_error), "original failure preserved without unexpected exceptions");
    require(sqlite3_get_autocommit(raw) == 1, "close never strands owned transaction");
    require(raw_scalar(raw, "SELECT value FROM AuditMaintenanceRow") == (fail_body ? 1 : 3), "atomic commit or rollback after close");
    require(db.query("SELECT value FROM AuditMaintenanceRow").empty(), "later ordinary callers remain closed");
}
void rollback_failure_poison() {
    lattice_db owner; owner.add(AuditMaintenanceRow{1});
    auto& db = owner.db(); auto* raw = db.handle();
    sqlite3_set_authorizer(raw, [](void*, int action, const char* first, const char*, const char*, const char*) {
        return action == SQLITE_TRANSACTION && first && std::strcmp(first, "ROLLBACK") == 0 ? SQLITE_DENY : SQLITE_OK;
    }, nullptr);
    bool original_error = false;
    try {
        audit_maintenance_test_access::owned(owner, [&]() -> int64_t {
            db.execute("UPDATE AuditMaintenanceRow SET value=2");
            throw std::runtime_error("original-maintenance-failure");
        });
    } catch (const std::runtime_error& error) { original_error = std::string(error.what()) == "original-maintenance-failure"; }
    sqlite3_set_authorizer(raw, nullptr, nullptr);
    const bool poisoned = db.is_closed();
    const bool ordinary_rejected = db.query("SELECT value FROM AuditMaintenanceRow").empty();
    // Explicit raw cleanup is test-only: poisoning is not a claim the failed
    // rollback magically settled. Normal callers must not keep using it.
    const bool remained_active = sqlite3_get_autocommit(raw) == 0;
    if (remained_active) require(sqlite3_exec(raw, "ROLLBACK", nullptr, nullptr, nullptr) == SQLITE_OK, "test raw rollback cleanup");
    require(original_error && poisoned && ordinary_rejected && remained_active, "failed rollback preserves original error and closes wrapper");
}
void close_from_file_commit_callback() {
    OwnedFile file;
    lattice_db owner(config(file.path.string())); owner.add(AuditMaintenanceRow{1});
    auto& db = owner.db(); auto* raw = db.handle(); int commits = 0;
    const auto token = owner.add_invalidation_hook([&](const auto&, auto reason) {
        if (reason == lattice_db::invalidation_reason::commit) { ++commits; db.close(); }
    }); // Callback does only scalar/atomic work; no SQL or waiting in WAL frame.
    audit_maintenance_test_access::owned(owner, [&]() -> int64_t {
        db.execute("UPDATE AuditMaintenanceRow SET value=2"); return 0;
    });
    owner.remove_invalidation_hook(token);
    require(commits == 1 && db.is_closed() && sqlite3_get_autocommit(raw) == 1, "reentrant post-commit close stays settled");
    require(raw_scalar(raw, "SELECT value FROM AuditMaintenanceRow") == 2, "post-commit close does not undo committed row");
}
void successful_commit_does_not_rollback_observer_transaction() {
    OwnedFile file;
    lattice_db owner(config(file.path.string()));
    const auto original = seed(owner, false);
    database peer(file.path.string(), database::open_mode::read_write, 5000);
    auto& db = owner.db();
    auto* raw = db.handle();
    bool callback_entered = false, callback_opened = false, callback_failed = false;
    // The default immediate scheduler invokes this during file WAL delivery.
    // The age-prune watermark is a real _lattice_meta change; no production
    // hook is replaced and the callback catches every exception locally.
    const auto token = owner.add_table_observer("_lattice_meta", [&](const auto&) noexcept {
        if (callback_entered) return;
        callback_entered = true;
        try {
            db.begin_transaction();
            db.execute("INSERT INTO _lattice_meta(key,value) VALUES('callback-owned-transaction','pending')");
            callback_opened = db.is_in_transaction();
        } catch (...) { callback_failed = true; }
    });
    struct Cleanup {
        lattice_db& owner;
        database& db;
        lattice_db::observer_id token;
        ~Cleanup() {
            owner.remove_table_observer("_lattice_meta", token);
            try { if (db.is_in_transaction()) db.rollback(); } catch (...) {}
        }
    } cleanup{owner, db, token};

    const auto removed = owner.prune_audit_log(600);
    require(removed == static_cast<int64_t>(original.size()), "maintenance returns its committed deletion count");
    require(callback_entered && callback_opened && !callback_failed,
            "immediate metadata observer opened its own transaction successfully");
    require(db.is_in_transaction(), "successful maintenance must not roll back callback-owned transaction");
    require(raw_scalar(raw, "SELECT COUNT(*) FROM _lattice_meta WHERE key='callback-owned-transaction'") == 1,
            "callback owner sees its pending row");
    require(raw_scalar(peer.handle(), "SELECT COUNT(*) FROM AuditLog") == 0,
            "separate connection sees maintenance commit");
    require(raw_scalar(peer.handle(), "SELECT COUNT(*) FROM _lattice_meta WHERE key='callback-owned-transaction'") == 0,
            "separate connection cannot see callback's uncommitted row");
    db.rollback();
    require(!db.is_in_transaction() &&
            raw_scalar(raw, "SELECT COUNT(*) FROM _lattice_meta WHERE key='callback-owned-transaction'") == 0,
            "callback transaction remains explicitly caller-resolvable");
}

void bounded(const std::function<void()>& body) {
    struct RestoreStyle {
        std::string previous = ::testing::FLAGS_gtest_death_test_style;
        ~RestoreStyle() { ::testing::FLAGS_gtest_death_test_style = std::move(previous); }
    } restore;
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT({
        sigset_t alarm_mask;
        sigemptyset(&alarm_mask); sigaddset(&alarm_mask, SIGALRM);
        if (std::signal(SIGALRM, SIG_DFL) == SIG_ERR ||
            sigprocmask(SIG_UNBLOCK, &alarm_mask, nullptr) != 0) _exit(2);
        alarm(10);
        try { body(); std::fputs("retention_ownership_case_complete\n", stderr); _exit(0); }
        catch (const std::exception& error) { std::fprintf(stderr, "retention_ownership_failure: %s\n", error.what()); _exit(1); }
    }, ::testing::ExitedWithCode(0), "retention_ownership_case_complete");
}
}

TEST(AuditMaintenanceOwnership, AgeRegisterBeforeOwnershipKeepsEveryPendingID) { bounded([] { mutation_before_owner(true, false); }); }
TEST(AuditMaintenanceOwnership, AgeResetBeforeOwnershipKeepsEveryPendingID) { bounded([] { mutation_before_owner(true, true); }); }
TEST(AuditMaintenanceOwnership, CompactRegisterBeforeOwnershipKeepsEveryPendingID) { bounded([] { mutation_before_owner(false, false); }); }
TEST(AuditMaintenanceOwnership, CompactResetBeforeOwnershipKeepsEveryPendingID) { bounded([] { mutation_before_owner(false, true); }); }
TEST(AuditMaintenanceOwnership, AgeRegisterCannotCrossProtectedFloorRead) { bounded([] { mutation_while_owned(true, false); }); }
TEST(AuditMaintenanceOwnership, AgeResetCannotCrossProtectedFloorRead) { bounded([] { mutation_while_owned(true, true); }); }
TEST(AuditMaintenanceOwnership, CompactRegisterCannotCrossProtectedFloorRead) { bounded([] { mutation_while_owned(false, false); }); }
TEST(AuditMaintenanceOwnership, CompactResetCannotCrossProtectedFloorRead) { bounded([] { mutation_while_owned(false, true); }); }
TEST(AuditMaintenanceOwnership, PublicBoundCannotBypassWriterFloor) { bounded(public_bound_respects_floor); }
TEST(AuditMaintenanceOwnership, PublicFalseHintCannotBypassLegacyCursor) { bounded(public_bound_respects_legacy_cursor); }
TEST(AuditMaintenanceOwnership, FailedDeleteRestoresHistoryAndTriggerFlag) { bounded(failed_delete_rolls_back_flag_and_history); }
TEST(AuditMaintenanceOwnership, UnreadableDisabledFlagNeverRestoresGuessedZero) { bounded(denied_disabled_read_aborts_without_guessing); }
TEST(AuditMaintenanceOwnership, AttachedOnlyImplicitReadIsNotJoined) { bounded(rejects_attached_only_read); }
TEST(AuditMaintenanceOwnership, TableFreeUDFCannotReenterMaintenance) { bounded([] { rejects_table_free_udf_reentry(false); }); }
TEST(AuditMaintenanceOwnership, UDFReentryRejectsBeforeContendedStoreGate) { bounded([] { rejects_table_free_udf_reentry(true); }); }
TEST(AuditMaintenanceOwnership, ExistingExplicitTransactionIsNotJoined) { bounded([] { rejects_existing_transaction(false); }); }
TEST(AuditMaintenanceOwnership, ExistingImplicitReadIsNotJoined) { bounded([] { rejects_existing_transaction(true); }); }
TEST(AuditMaintenanceOwnership, BetweenStatementsOwnedAndSettledCallbackUnlocked) { bounded(owns_between_statements_and_delivers_after_unlock); }
TEST(AuditMaintenanceOwnership, CloseAfterAdmissionStillCommits) { bounded([] { close_during_owned_work(false); }); }
TEST(AuditMaintenanceOwnership, CloseAfterAdmissionStillRollsBack) { bounded([] { close_during_owned_work(true); }); }
TEST(AuditMaintenanceOwnership, FailedRollbackPoisonsAndPreservesOriginalError) { bounded(rollback_failure_poison); }
TEST(AuditMaintenanceOwnership, CloseFromFileCommitCallbackRemainsSettled) { bounded(close_from_file_commit_callback); }
TEST(AuditMaintenanceOwnership, SuccessfulFileCommitDoesNotRollBackObserverTransaction) {
    bounded(successful_commit_does_not_rollback_observer_transaction);
}

#endif
