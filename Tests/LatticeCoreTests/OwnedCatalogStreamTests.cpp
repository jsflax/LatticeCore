#include "TestHelpers.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/catalog_test_probe.hpp"
#include <csignal>
#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#endif
#include <lattice/owned_catalog.hpp>
#include <nlohmann/json.hpp>
#include <chrono>
#include <functional>
#include <future>

namespace {
using namespace lattice;
using namespace std::chrono_literals;
using json = nlohmann::json;
constexpr int64_t budget = 30000000000LL;
const std::string request = R"({"version":1,"tables":["Fixture"]})";

struct Fixture {
    TempDB file{"owned_catalog"};
    std::shared_ptr<lattice_db> owner = std::make_shared<lattice_db>(configuration(file.str()));
    Fixture() {
        owner->db().execute("CREATE TABLE Fixture (id INTEGER PRIMARY KEY, globalId TEXT NOT NULL UNIQUE COLLATE NOCASE, body TEXT, payload BLOB, n INTEGER, x REAL)");
    }
    void add(int64_t id, const std::string& body = "old", const std::vector<uint8_t>& bytes = {}) {
        owner->db().execute("INSERT INTO Fixture VALUES (?,?,?,?,?,?)",
            {id, lattice::uuid_t::generate().to_string(), body, bytes, int64_t(-7), 1.25});
    }
    catalog_stream_operation operation(const std::string& tables = request, int64_t ns = budget) {
        return make_catalog_stream_operation(owner, tables, ns);
    }
};
struct Receiver {
    int tables = 0, rows = 0, table_ends = 0;
    size_t maximum = 0, body_bytes = 0, payload_bytes = 0;
    uint64_t body_sum = 0, payload_sum = 0;
    int64_t previous = 0;
    std::string column;
    std::function<bool(int32_t)> action;
    static bool callback(void* value, int32_t kind, const uint8_t* bytes, int64_t count) {
        return static_cast<Receiver*>(value)->receive(kind, bytes, count);
    }
    bool receive(int32_t kind, const uint8_t* bytes, int64_t count) {
        EXPECT_GE(count, 0); EXPECT_LE(count, 65536);
        maximum = std::max(maximum, static_cast<size_t>(count));
        if (kind == 0) {
            ++tables; previous = 0;
            const auto header = json::parse(bytes, bytes + count);
            EXPECT_GE(header.at("columns").size(), 2);
        } else if (kind == 1) {
            ++rows;
            const auto row = json::parse(bytes, bytes + count);
            const auto id = std::stoll(row.at("id").get<std::string>());
            EXPECT_GT(id, previous); previous = id;
        } else if (kind == 2) column = json::parse(bytes, bytes + count).at("column").get<std::string>();
        else if (kind == 3 && (column == "body" || column == "payload")) {
            auto& length = column == "body" ? body_bytes : payload_bytes;
            auto& sum = column == "body" ? body_sum : payload_sum;
            length += static_cast<size_t>(count);
            for (int64_t i = 0; i != count; ++i) sum += bytes[i];
        } else if (kind == 6) ++table_ends;
        return !action || action(kind);
    }
};
struct Gate {
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false, released = false;
    bool hold() {
        std::unique_lock<std::mutex> lock(mutex); entered = true; cv.notify_all();
        return cv.wait_for(lock, 3s, [&] { return released; });
    }
    bool wait() {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, 3s, [&] { return entered; });
    }
    void open() { std::lock_guard<std::mutex> lock(mutex); released = true; cv.notify_all(); }
};
void succeeded(const catalog_stream_result& result) {
    EXPECT_TRUE(result.success()) << result.error_message();
    EXPECT_FALSE(result.cleanup_failed());
    EXPECT_EQ(result.finalize_code(), SQLITE_OK);
    EXPECT_EQ(result.transaction_code(), SQLITE_OK);
    EXPECT_EQ(result.rollback_code(), -1);
    EXPECT_EQ(result.stop_code(), 0);
}
}

TEST(OwnedCatalogStream, OversizedSingleTextAndBlobUseBoundedChunksAndSameLengthChanges) {
    Fixture fixture;
    constexpr size_t bytes = 5 * 1024 * 1024;
    fixture.add(1, std::string(bytes, 'x'), std::vector<uint8_t>(bytes, 171));
    Receiver first;
    succeeded(fixture.operation().capture(&first, Receiver::callback));
    EXPECT_EQ(first.rows, 1); EXPECT_EQ(first.maximum, 65536);
    EXPECT_EQ(first.body_bytes, bytes); EXPECT_EQ(first.body_sum, bytes * uint64_t('x'));
    EXPECT_EQ(first.payload_bytes, bytes); EXPECT_EQ(first.payload_sum, bytes * uint64_t(171));
    fixture.owner->db().execute("UPDATE Fixture SET body=?,payload=? WHERE id=1",
        {std::string(bytes, 'y'), std::vector<uint8_t>(bytes, 42)});
    // Both genuine 10MiB writes exceed the unchanged 16MiB keeper threshold.
    // Preserve real pressure refusal; service the documented writer gap rather
    // than bypassing admission or resetting its pending/epoch counters.
    ASSERT_TRUE(fixture.owner->wal_eviction_pending());
    Receiver pressured;
    const auto refusal = fixture.operation().capture(&pressured, Receiver::callback);
    EXPECT_FALSE(refusal.success()); EXPECT_FALSE(refusal.cleanup_failed());
    EXPECT_EQ(refusal.transaction_code(), -1); EXPECT_EQ(pressured.tables, 0);
    EXPECT_NE(refusal.error_message().find("physical-store pressure or retirement"), std::string::npos);
    const auto original = fixture.owner->borrow_xproc_read_connection();
    const auto checkpoint = fixture.owner->db().wal_checkpoint(true, 250);
    ASSERT_EQ(checkpoint.rc, SQLITE_OK); ASSERT_EQ(checkpoint.busy, 0);
    EXPECT_EQ(checkpoint.log_frames, 0); EXPECT_EQ(checkpoint.checkpointed, 0);
    std::error_code wal_error;
    const auto wal_bytes = std::filesystem::file_size(fixture.file.str() + "-wal", wal_error);
    ASSERT_FALSE(wal_error); EXPECT_EQ(wal_bytes, 0u); // Actual file ending, not absence-as-success.
    RecordProperty("actual_pressure_checkpoint_rc", checkpoint.rc);
    RecordProperty("actual_pressure_checkpoint_busy", checkpoint.busy);
    RecordProperty("actual_pressure_wal_bytes", std::to_string(wal_bytes));
    fixture.owner->run_read_pool_maintenance(); // Actual existing gap/checkpoint/ack producer.
    EXPECT_FALSE(fixture.owner->wal_eviction_pending());
    EXPECT_EQ(fixture.owner->borrow_xproc_read_connection(), original);
    ASSERT_TRUE(original); EXPECT_FALSE(original->is_closed());
    Receiver second;
    succeeded(fixture.operation().capture(&second, Receiver::callback));
    EXPECT_EQ(second.body_bytes, first.body_bytes); EXPECT_NE(second.body_sum, first.body_sum);
    EXPECT_EQ(second.payload_bytes, first.payload_bytes); EXPECT_NE(second.payload_sum, first.payload_sum);
}

TEST(OwnedCatalogStream, MoreThan4096RowsAndEmptyTableHaveNoWholeCatalogCap) {
    Fixture fixture;
    fixture.owner->db().execute("CREATE TABLE Empty (id INTEGER PRIMARY KEY, globalId TEXT NOT NULL UNIQUE COLLATE NOCASE)");
    fixture.owner->begin_transaction();
    try { for (int64_t id = 1; id <= 4097; ++id) fixture.add(id, ""); fixture.owner->commit(); }
    catch (...) { fixture.owner->rollback(); throw; }
    Receiver receiver;
    succeeded(fixture.operation(R"({"version":1,"tables":["Fixture","Empty"]})").capture(&receiver, Receiver::callback));
    EXPECT_EQ(receiver.rows, 4097); EXPECT_EQ(receiver.tables, 2); EXPECT_EQ(receiver.table_ends, 2);
}

TEST(OwnedCatalogStream, SameParentWriterCommitsWhileOriginalSnapshotIsHeld) {
    Fixture fixture; fixture.add(1, "old");
    Gate gate;
    Receiver receiver;
    receiver.action = [&](int32_t kind) { return kind != 0 || gate.hold(); };
    catalog_stream_result result;
    std::thread read([&] { result = fixture.operation().capture(&receiver, Receiver::callback); });
    const bool entered = gate.wait();
    std::promise<void> written; auto completed = written.get_future(); std::exception_ptr failure;
    std::thread writer([&] {
        try { fixture.owner->db().execute("UPDATE Fixture SET body='new' WHERE id=1"); }
        catch (...) { failure = std::current_exception(); }
        written.set_value();
    });
    const bool committed_while_held = completed.wait_for(2s) == std::future_status::ready;
    gate.open(); writer.join(); read.join();
    ASSERT_TRUE(entered); ASSERT_TRUE(committed_while_held);
    if (failure) std::rethrow_exception(failure);
    succeeded(result);
    EXPECT_EQ(receiver.body_sum, uint64_t('o' + 'l' + 'd'));
    Receiver next; succeeded(fixture.operation().capture(&next, Receiver::callback));
    EXPECT_EQ(next.body_sum, uint64_t('n' + 'e' + 'w'));
}

TEST(OwnedCatalogStream, CancellationJoinsSnapshotAndOldControlCannotInterruptSuccessor) {
    Fixture fixture; fixture.add(1);
    const auto identity = fixture.owner->xproc_read_db().physical_identity("main");
    ASSERT_TRUE(identity);
    auto operation = fixture.operation();
    EXPECT_EQ(projection_store_readers(identity), 0u); // A queued control has no ticket.
    Gate gate; Receiver receiver;
    receiver.action = [&](int32_t kind) { return kind != 0 || gate.hold(); };
    catalog_stream_result result; std::atomic<bool> returned{false};
    std::thread read([&] { result = operation.capture(&receiver, Receiver::callback); returned.store(true); });
    const bool entered = gate.wait();
    const auto held = projection_store_readers(identity);
    operation.cancel();
    const bool premature = returned.load();
    gate.open(); read.join();
    ASSERT_TRUE(entered); EXPECT_EQ(held, 1u); EXPECT_FALSE(premature);
    EXPECT_FALSE(result.success()); EXPECT_FALSE(result.cleanup_failed());
    EXPECT_EQ(result.stop_code(), 2); EXPECT_EQ(result.rollback_code(), SQLITE_OK);
    EXPECT_EQ(projection_store_readers(identity), 0u);
    Receiver next; next.action = [&](int32_t) { operation.cancel(); return true; };
    succeeded(fixture.operation().capture(&next, Receiver::callback));
    EXPECT_EQ(next.rows, 1); EXPECT_EQ(projection_store_readers(identity), 0u);
}

TEST(OwnedCatalogStream, ConsumerRefusalRollsBackBeforeSameOwnerRecapture) {
    Fixture fixture; fixture.add(1);
    Receiver receiver; receiver.action = [](int32_t) { return false; };
    const auto failed = fixture.operation().capture(&receiver, Receiver::callback);
    EXPECT_FALSE(failed.success()); EXPECT_FALSE(failed.cleanup_failed());
    EXPECT_EQ(failed.rollback_code(), SQLITE_OK);
    fixture.owner->db().execute("UPDATE Fixture SET body='after' WHERE id=1");
    Receiver next; succeeded(fixture.operation().capture(&next, Receiver::callback));
    EXPECT_EQ(next.rows, 1);
}

TEST(OwnedCatalogStream, QueuedCancelExpiryAndClosedParentAdmitNoSnapshot) {
    Fixture fixture;
    const auto identity = fixture.owner->xproc_read_db().physical_identity("main");
    auto cancelled = fixture.operation(); cancelled.cancel();
    Receiver receiver; const auto cancel_result = cancelled.capture(&receiver, Receiver::callback);
    EXPECT_FALSE(cancel_result.success()); EXPECT_EQ(cancel_result.stop_code(), 2);
    EXPECT_EQ(cancel_result.transaction_code(), -1); EXPECT_EQ(receiver.tables, 0);
    auto expired = fixture.operation(request, 1000000);
    std::this_thread::sleep_for(2ms);
    const auto expiry = expired.capture(&receiver, Receiver::callback);
    EXPECT_FALSE(expiry.success()); EXPECT_EQ(expiry.stop_code(), 3);
    EXPECT_EQ(expiry.transaction_code(), -1);
    auto closed = fixture.operation(); fixture.owner->close();
    const auto close_result = closed.capture(&receiver, Receiver::callback);
    EXPECT_FALSE(close_result.success()); EXPECT_EQ(close_result.transaction_code(), -1);
    EXPECT_EQ(receiver.tables, 0); EXPECT_EQ(projection_store_readers(identity), 0u);
}

TEST(OwnedCatalogStream, RetiredOriginalReaderRefusesWithoutWriterFallback) {
    Fixture fixture; fixture.add(1);
    Gate gate; Receiver receiver;
    receiver.action = [&](int32_t kind) { return kind != 0 || gate.hold(); };
    catalog_stream_result result;
    std::thread read([&] { result = fixture.operation().capture(&receiver, Receiver::callback); });
    const bool entered = gate.wait();
    std::exception_ptr retirement_failure;
    try { fixture.owner->close_read_db(); } catch (...) { retirement_failure = std::current_exception(); }
    gate.open(); read.join();
    if (retirement_failure) std::rethrow_exception(retirement_failure);
    ASSERT_TRUE(entered); EXPECT_FALSE(result.success()); EXPECT_FALSE(result.cleanup_failed());
    EXPECT_EQ(result.rollback_code(), SQLITE_OK);
    Receiver absent; const auto refused = fixture.operation().capture(&absent, Receiver::callback);
    EXPECT_FALSE(refused.success()); EXPECT_EQ(refused.transaction_code(), -1); EXPECT_EQ(absent.tables, 0);
    fixture.owner->reopen_read_db();
    Receiver restored; succeeded(fixture.operation().capture(&restored, Receiver::callback));
}

TEST(OwnedCatalogStream, CaughtCallbackReentryStillPoisonsCaptureBeforeAnyParentEffect) {
    Fixture fixture; fixture.add(1);
    Fixture other;
    std::vector<std::function<void()>> actions{
        [&] { (void)fixture.owner->db().query("SELECT 1"); },
        [&] { (void)fixture.owner->xproc_read_db(); },
        [&] { fixture.owner->begin_transaction(); },
        [&] { fixture.owner->commit(); },
        [&] { fixture.owner->rollback(); },
        [&] { fixture.owner->close(); },
        [&] { fixture.owner->close_read_db(); },
        [&] { fixture.owner->close_write_db(); },
        [&] { fixture.owner->reopen_read_db(); },
        [&] { fixture.owner->reopen_write_db(); },
        [&] { fixture.owner->attach(*other.owner); },
        [&] { fixture.owner->detach(*other.owner); },
        [&] { projection_query q; q.table = "Fixture"; q.columns = {"id"}; (void)fixture.owner->start_projection(q); }
    };
    for (size_t i = 0; i != actions.size(); ++i) {
        SCOPED_TRACE(i); bool refused = false;
        Receiver receiver;
        receiver.action = [&](int32_t) { try { actions[i](); } catch (...) { refused = true; } return true; };
        const auto result = fixture.operation().capture(&receiver, Receiver::callback);
        EXPECT_TRUE(refused); EXPECT_FALSE(result.success()); EXPECT_FALSE(result.cleanup_failed());
        EXPECT_EQ(result.rollback_code(), SQLITE_OK); EXPECT_FALSE(fixture.owner->is_closed());
        Receiver fresh; succeeded(fixture.operation().capture(&fresh, Receiver::callback));
    }
}

TEST(OwnedCatalogStream, RawEscapedOriginalAndUnsupportedRoutesNeverFallback) {
    Fixture fixture;
    (void)fixture.owner->xproc_read_db().handle(); // Genuine existing public escape, not a test stamp.
    Receiver receiver;
    const auto escaped = fixture.operation().capture(&receiver, Receiver::callback);
    EXPECT_FALSE(escaped.success()); EXPECT_EQ(escaped.transaction_code(), -1); EXPECT_EQ(receiver.tables, 0);
    auto memory = std::make_shared<lattice_db>(":memory:");
    auto operation = make_catalog_stream_operation(memory, request, budget);
    EXPECT_FALSE(operation.capture(&receiver, Receiver::callback).success());
    EXPECT_EQ(receiver.tables, 0);
}

TEST(OwnedCatalogStream, InvalidUTF8AndSchemaRefuseWithRealRollback) {
    Fixture fixture; fixture.add(1, std::string(1, static_cast<char>(0xff)));
    Receiver invalid;
    const auto text = fixture.operation().capture(&invalid, Receiver::callback);
    EXPECT_FALSE(text.success()); EXPECT_FALSE(text.cleanup_failed()); EXPECT_EQ(text.rollback_code(), SQLITE_OK);
    fixture.owner->db().execute("UPDATE Fixture SET body='valid' WHERE id=1");
    Receiver valid; succeeded(fixture.operation().capture(&valid, Receiver::callback));
    fixture.owner->db().execute("CREATE TABLE NoRowid(id INTEGER PRIMARY KEY,globalId TEXT UNIQUE COLLATE NOCASE) WITHOUT ROWID");
    Receiver schema;
    const auto result = fixture.operation(R"({"version":1,"tables":["NoRowid"]})").capture(&schema, Receiver::callback);
    EXPECT_FALSE(result.success()); EXPECT_EQ(result.rollback_code(), SQLITE_OK); EXPECT_EQ(schema.tables, 0);
}

TEST(OwnedCatalogStream, RequestAndOneShotControlRefuseWithoutReusingFirstCapture) {
    Fixture fixture;
    EXPECT_THROW(fixture.operation(R"({"version":1,"version":1,"tables":["Fixture"]})"), db_error);
    EXPECT_THROW(fixture.operation(R"({"version":1,"tables":["Fixture","Fixture"]})"), db_error);
    EXPECT_THROW(fixture.operation(request, 0), db_error);
    EXPECT_THROW(fixture.operation(request, budget + 1), db_error);
    Receiver first; auto operation = fixture.operation();
    succeeded(operation.capture(&first, Receiver::callback));
    EXPECT_FALSE(operation.valid());
    Receiver duplicate; const auto result = operation.capture(&duplicate, Receiver::callback);
    EXPECT_FALSE(result.success()); EXPECT_EQ(result.transaction_code(), -1); EXPECT_EQ(duplicate.tables, 0);
}


TEST(OwnedCatalogStream, DeferredSyncOwnerWithoutGeneralReaderUsesItsActualOriginalXproc) {
    TempDB file("owned_catalog_deferred_sync");
    configuration config(file.str());
    config.websocket_url = "ws://127.0.0.1:9/sync";
    config.authorization_token = "fixture-no-transport";
    ASSERT_TRUE(config.is_sync_enabled());
    // This is the real public defer_sync constructor used by the typed bridge.
    // No setup_sync_if_configured/setup_ipc_if_configured call is issued.
    auto owner = std::make_shared<lattice_db>(config, true);
    const auto writer = &owner->db();
    const auto ordinary = owner->borrow_read_connection();
    const auto original_xproc = owner->borrow_xproc_read_connection();
    // Actual constructor correspondence: sync-configured read_db_ is absent,
    // so only the broad ordinary getter falls back to its real writer. The
    // catalog must instead retain the distinct, already-open xproc original.
    ASSERT_EQ(ordinary.get(), writer);
    ASSERT_TRUE(original_xproc);
    ASSERT_NE(original_xproc.get(), writer);
    ASSERT_NE(original_xproc.get(), ordinary.get());
    owner->db().execute("CREATE TABLE Fixture(id INTEGER PRIMARY KEY,globalId TEXT NOT NULL UNIQUE COLLATE NOCASE,body TEXT)");
    owner->db().execute("INSERT INTO Fixture VALUES(?,?,?)", {int64_t(1), lattice::uuid_t::generate().to_string(), std::string("configured")});
    const auto identity = original_xproc->physical_identity("main");
    ASSERT_TRUE(identity);
    auto operation = make_catalog_stream_operation(owner, request, budget);
    EXPECT_EQ(projection_store_readers(identity), 0u);
    Receiver receiver;
    succeeded(operation.capture(&receiver, Receiver::callback));
    EXPECT_EQ(receiver.tables, 1); EXPECT_EQ(receiver.rows, 1); EXPECT_EQ(receiver.table_ends, 1);
    EXPECT_EQ(receiver.body_bytes, 10u);
    EXPECT_EQ(projection_store_readers(identity), 0u);
    EXPECT_EQ(owner->borrow_xproc_read_connection(), original_xproc);
    EXPECT_EQ(owner->borrow_read_connection().get(), writer);
}


TEST(OwnedCatalogStream, CallbackDropsEveryPublicControlWhileRealCaptureStillReturns) {
    Fixture fixture; fixture.add(1);
    auto original = std::make_unique<catalog_stream_operation>(fixture.operation());
    auto alias = std::make_unique<catalog_stream_operation>(*original);
    Receiver receiver; bool dropped = false;
    receiver.action = [&](int32_t kind) {
        if (kind == 0 && !dropped) {
            dropped = true;
            original.reset(); alias.reset(); // Destroy the actual invoking facade, not merely a copy.
        }
        return true;
    };
    const auto result = original->capture(&receiver, Receiver::callback);
    EXPECT_TRUE(dropped); EXPECT_FALSE(original); EXPECT_FALSE(alias);
    succeeded(result); EXPECT_EQ(receiver.rows, 1);
    Receiver next; succeeded(fixture.operation().capture(&next, Receiver::callback));
}

TEST(OwnedCatalogStream, IssuedButNotSteppedOriginalStatementRefusesBeforeSnapshot) {
    Fixture fixture; fixture.add(1);
    auto reader = fixture.owner->borrow_xproc_read_connection();
    auto* native = detail::canonical_writer_custody_test_access::fault_handle(*reader);
    ASSERT_NE(native, nullptr);
    sqlite3_stmt* raw = nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(native, "SELECT body FROM Fixture", -1, &raw, nullptr), SQLITE_OK);
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement(raw, sqlite3_finalize);
    ASSERT_NE(raw, nullptr); ASSERT_EQ(sqlite3_stmt_busy(raw), 0);
    Receiver receiver;
    const auto result = fixture.operation().capture(&receiver, Receiver::callback);
    EXPECT_FALSE(result.success()); EXPECT_FALSE(result.cleanup_failed());
    EXPECT_EQ(result.transaction_code(), -1); EXPECT_EQ(receiver.tables, 0);
    EXPECT_EQ(sqlite3_get_autocommit(native), 1);
    ASSERT_EQ(sqlite3_finalize(statement.release()), SQLITE_OK);
    Receiver next; succeeded(fixture.operation().capture(&next, Receiver::callback));
}

#if GTEST_HAS_DEATH_TEST && (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
struct CatalogTestEnvironment {
    std::string name;
    std::optional<std::string> previous;
    CatalogTestEnvironment(const char* key, const std::string& value) : name(key) {
        if (const auto* before = std::getenv(key)) previous = before;
        if (::setenv(key, value.c_str(), 1) != 0) throw std::runtime_error("fault child environment unavailable");
    }
    ~CatalogTestEnvironment() {
        if (previous) (void)::setenv(name.c_str(), previous->c_str(), 1);
        else (void)::unsetenv(name.c_str());
    }
};
void real_unknown_cleanup(const std::string& path) {
    // Each invocation is a fresh exec. Unknown cleanup intentionally retains
    // this exact bank original until real process death; there is no test reset.
    auto owner = std::make_shared<lattice_db>(configuration(path));
    auto reader = owner->borrow_xproc_read_connection();
    ASSERT_TRUE(reader);
    auto* native = detail::canonical_writer_custody_test_access::fault_handle(*reader);
    ASSERT_NE(native, nullptr);
    const auto identity = reader->physical_identity("main");
    ASSERT_TRUE(identity);
    ASSERT_EQ(sqlite3_set_authorizer(native,
        [](void*, int action, const char* operation, const char*, const char*, const char*) {
            return action == SQLITE_TRANSACTION && operation && std::strcmp(operation, "ROLLBACK") == 0
                ? SQLITE_DENY : SQLITE_OK;
        }, nullptr), SQLITE_OK);
    Gate snapshot, prechecked;
    Receiver receiver; receiver.action = [&](int32_t kind) {
        if (kind == 0) { (void)snapshot.hold(); return false; }
        return true;
    };
    catalog_stream_result result;
    auto operation = make_catalog_stream_operation(owner, request, budget);
    std::thread capturing([&] { result = operation.capture(&receiver, Receiver::callback); });
    const bool snapshot_entered = snapshot.wait();
    std::exception_ptr queued_error; bool queued_succeeded = false;
    detail::catalog_test_probe::probe observer{
        [](void* context, const database*) { (void)static_cast<Gate*>(context)->hold(); }, &prechecked};
    std::thread queued([&] {
        detail::catalog_test_probe::current = &observer;
        try { (void)reader->query("SELECT body FROM Fixture"); queued_succeeded = true; }
        catch (...) { queued_error = std::current_exception(); }
        detail::catalog_test_probe::current = nullptr;
    });
    const bool passed_last_precheck = prechecked.wait();
    // The observer is past the last optimistic check and before real Native
    // admission. Release it BEFORE failing cleanup; it cannot pass the mutex
    // until capture has either settled or permanently quarantined this reader.
    prechecked.open(); snapshot.open();
    capturing.join(); queued.join();
    ASSERT_TRUE(snapshot_entered); ASSERT_TRUE(passed_last_precheck);
    EXPECT_FALSE(result.success()); EXPECT_TRUE(result.cleanup_failed());
    EXPECT_EQ(result.rollback_code(), SQLITE_AUTH);
    EXPECT_EQ(sqlite3_get_autocommit(native), 0); // Actual unsettled original, saved handle only.
    EXPECT_EQ(projection_store_readers(identity), 1u); // Debt remains, never a refund.
    EXPECT_FALSE(queued_succeeded); ASSERT_TRUE(queued_error);
    try { std::rethrow_exception(queued_error); }
    catch (const db_error& error) { EXPECT_NE(std::string(error.what()).find("unsettled cleanup"), std::string::npos); }
    catch (...) { FAIL() << "queued query lost the actual quarantine refusal"; }
    EXPECT_THROW(reader->query("SELECT 1"), db_error);
    EXPECT_THROW(owner->query_xproc("SELECT 1"), db_error);
    EXPECT_THROW(reader->table_exists("Fixture"), db_error);
    EXPECT_THROW(reader->get_table_info("Fixture"), db_error);
    EXPECT_THROW(reader->handle(), db_error);
    EXPECT_THROW(reader->physical_identity("main"), db_error);
    EXPECT_THROW(reader->is_in_transaction(), db_error);
    EXPECT_THROW(reader->refresh_wal_snapshot(), db_error);
    EXPECT_THROW(reader->close(), db_error);
    EXPECT_THROW(reader->wal_checkpoint(false), db_error);
    EXPECT_THROW(reader->changes(), db_error);
    EXPECT_THROW(reader->begin_transaction(), db_error);
    EXPECT_THROW(reader->try_begin_immediate(0), db_error);
    EXPECT_THROW(reader->set_txn_hooks({}, {}), db_error);
    EXPECT_THROW(detail::canonical_writer_custody_test_access::enter_maintenance(*reader), db_error);
    Receiver refused;
    EXPECT_FALSE(make_catalog_stream_operation(owner, request, budget).capture(&refused, Receiver::callback).success());
    EXPECT_EQ(refused.tables, 0); EXPECT_EQ(projection_store_readers(identity), 1u);
    // A real public move carries the physical retirement original. Keep it
    // alive through process ending rather than pretending this repays debt.
    auto moved = std::make_unique<database>(std::move(*reader));
    EXPECT_THROW(moved->query("SELECT 1"), db_error);
    EXPECT_THROW(moved->handle(), db_error);
    EXPECT_EQ(sqlite3_get_autocommit(native), 0);
    EXPECT_EQ(projection_store_readers(identity), 1u);
    (void)moved.release(); // Process-owned unresolved Native original, no test-side destruction/reset.
}
}
#endif

TEST(OwnedCatalogStream, UnknownRollbackQuarantinesAliasesAndPrestartedNativeAdmissionUntilExecEnds) {
#if GTEST_HAS_DEATH_TEST && (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
    struct Style {
        std::string previous = GTEST_FLAG_GET(death_test_style);
        ~Style() { GTEST_FLAG_SET(death_test_style, previous); }
    } style;
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    TempDB original_file("catalog_unknown_child");
    const bool child = ::testing::internal::InDeathTestChild();
    std::unique_ptr<CatalogTestEnvironment> path_environment, log_environment;
    if (!child) {
        // Only the parent owns this file's deletion, AFTER actual exec exit/reap.
        {
            lattice_db seed(original_file.str());
            seed.db().execute("CREATE TABLE Fixture(id INTEGER PRIMARY KEY,globalId TEXT NOT NULL UNIQUE COLLATE NOCASE,body TEXT)");
            seed.db().execute("INSERT INTO Fixture VALUES(?,?,?)", {int64_t(1), lattice::uuid_t::generate().to_string(), std::string("old")});
        }
        path_environment = std::make_unique<CatalogTestEnvironment>("LATTICE_CATALOG_FAULT_CHILD_STORE", original_file.str());
        log_environment = std::make_unique<CatalogTestEnvironment>("LATTICE_TEST_LOG_PATH", original_file.str() + ".child.log");
    }
    ASSERT_EXIT({
        sigset_t mask; sigemptyset(&mask); sigaddset(&mask, SIGALRM);
        if (std::signal(SIGALRM, SIG_DFL) == SIG_ERR || ::sigprocmask(SIG_UNBLOCK, &mask, nullptr) != 0) ::_exit(2);
        ::alarm(15);
        try {
            const auto* supplied = std::getenv("LATTICE_CATALOG_FAULT_CHILD_STORE");
            if (!supplied || !*supplied) throw std::runtime_error("no original child store");
            const auto path = std::filesystem::canonical(supplied);
            const auto temporary = std::filesystem::canonical(std::filesystem::temp_directory_path());
            const auto* home = std::getenv("HOME");
            if (!home || !*home) throw std::runtime_error("missing original home");
            const auto localdev = std::filesystem::canonical(std::filesystem::path(home) / "localdev");
            if (path.parent_path() != temporary || path.string().rfind(localdev.string() + "/", 0) != 0 ||
                path.filename().string().rfind("catalog_unknown_child_", 0) != 0)
                throw std::runtime_error("child store outside original owned temp directory");
            real_unknown_cleanup(path.string());
            if (::testing::Test::HasFailure()) ::_exit(1);
            std::fputs("catalog_unknown_cleanup_actual_child_complete\n", stderr); ::_exit(0);
        } catch (const std::exception& error) {
            std::fprintf(stderr, "catalog_unknown_child_failure: %s\n", error.what()); ::_exit(1);
        } catch (...) { ::_exit(1); }
    }, ::testing::ExitedWithCode(0), "catalog_unknown_cleanup_actual_child_complete");
#else
    GTEST_SKIP() << "fresh exec quarantine qualification requires supported native death-test custody";
#endif
}


TEST(OwnedCatalogStream, LogicalCloseReturnsWhileForeignNativeMutexIsStillHeld) {
    Fixture fixture;
    auto reader = fixture.owner->borrow_xproc_read_connection();
    ASSERT_TRUE(reader);
    auto* native = detail::canonical_writer_custody_test_access::fault_handle(*reader);
    ASSERT_NE(native, nullptr);
    auto* mutex = sqlite3_db_mutex(native);
    ASSERT_NE(mutex, nullptr);
    Gate held;
    std::atomic<bool> native_held{false};
    std::thread foreign([&] {
        sqlite3_mutex_enter(mutex);
        native_held.store(true, std::memory_order_release);
        (void)held.hold();
        native_held.store(false, std::memory_order_release);
        sqlite3_mutex_leave(mutex);
    });
    const bool entered = held.wait();
    std::promise<void> returned; auto completion = returned.get_future();
    std::exception_ptr failure;
    std::thread closing([&] {
        try { reader->close(); } catch (...) { failure = std::current_exception(); }
        returned.set_value();
    });
    const bool returned_before_release = completion.wait_for(1s) == std::future_status::ready;
    const bool still_held_at_observation = native_held.load(std::memory_order_acquire);
    held.open(); foreign.join(); closing.join(); // Always release/join before an assertion.
    ASSERT_TRUE(entered);
    EXPECT_TRUE(returned_before_release && still_held_at_observation);
    if (failure) std::rethrow_exception(failure);
    EXPECT_TRUE(reader->is_closed()); // Atomic facade fact only, never physical close/cleanup proof.
}


TEST(OwnedCatalogStream, PathOnlyOwnerWithoutDedicatedOriginalRefusesBeforeFallbackSnapshot) {
    TempDB file("catalog_absent_original");
    auto owner = std::make_shared<lattice_db>(file.str()); // Actual public legacy path-only constructor.
    auto ordinary = owner->borrow_read_connection();
    auto broad_xproc = owner->borrow_xproc_read_connection();
    ASSERT_TRUE(ordinary); ASSERT_NE(ordinary.get(), &owner->db());
    ASSERT_EQ(broad_xproc, ordinary); // Existing broad getter fallback is not catalog provenance.
    owner->db().execute("CREATE TABLE Fixture(id INTEGER PRIMARY KEY,globalId TEXT NOT NULL UNIQUE COLLATE NOCASE,body TEXT)");
    owner->db().execute("INSERT INTO Fixture VALUES(?,?,?)", {int64_t(1), lattice::uuid_t::generate().to_string(), std::string("valid")});
    const auto identity = ordinary->physical_identity("main"); ASSERT_TRUE(identity);
    Receiver receiver;
    const auto result = make_catalog_stream_operation(owner, request, budget).capture(&receiver, Receiver::callback);
    EXPECT_FALSE(result.success()); EXPECT_FALSE(result.cleanup_failed());
    EXPECT_NE(result.error_message().find("strict original xproc reader unavailable"), std::string::npos);
    EXPECT_EQ(result.transaction_code(), -1); EXPECT_EQ(result.rollback_code(), -1);
    EXPECT_EQ(receiver.tables, 0); EXPECT_EQ(receiver.rows, 0);
    EXPECT_EQ(projection_store_readers(identity), 0u);
    EXPECT_EQ(owner->db().query("SELECT id FROM Fixture").size(), 1u);
}

TEST(OwnedCatalogStream, MalformedStoredUUIDsStillRefuseBeforeAnyRowAndReallyRollBack) {
    Fixture fixture; fixture.add(1);
    std::string invalid_hex(36, 'a');
    for (auto index : {8u, 13u, 18u, 23u}) invalid_hex[index] = '-';
    invalid_hex.back() = 'z';
    // TestHelpers fake_uuid is intentionally malformed input here; positive
    // rows above use the genuine native UUID producer, never this legacy stub.
    for (const auto& invalid : {fake_uuid(1), invalid_hex}) {
        fixture.owner->db().execute("UPDATE Fixture SET globalId=? WHERE id=1", {invalid});
        Receiver receiver;
        const auto result = fixture.operation().capture(&receiver, Receiver::callback);
        EXPECT_FALSE(result.success()); EXPECT_FALSE(result.cleanup_failed());
        EXPECT_EQ(result.rollback_code(), SQLITE_OK);
        EXPECT_EQ(receiver.tables, 1); EXPECT_EQ(receiver.rows, 0); EXPECT_EQ(receiver.table_ends, 0);
    }
    fixture.owner->db().execute("UPDATE Fixture SET globalId=? WHERE id=1", {lattice::uuid_t::generate().to_string()});
    Receiver valid; succeeded(fixture.operation().capture(&valid, Receiver::callback));
    EXPECT_EQ(valid.rows, 1); EXPECT_EQ(valid.table_ends, 1);
}


TEST(OwnedCatalogStream, UnrelatedRtreeNeverCreatesStatementsAndRecaptureKeepsActualUserFence) {
    Fixture fixture; fixture.add(1);
    const auto registered_rtree = fixture.owner->db().query(
        "SELECT name FROM main.sqlite_schema WHERE type='table' AND name='_TestPlace_location_rtree'");
    ASSERT_EQ(registered_rtree.size(), 1u); // Genuine constructor-produced TestPlace RTree.
    auto reader = fixture.owner->borrow_xproc_read_connection();
    auto* native = detail::canonical_writer_custody_test_access::fault_handle(*reader);
    ASSERT_NE(native, nullptr);
    Receiver first; succeeded(fixture.operation().capture(&first, Receiver::callback));
    EXPECT_EQ(first.rows, 1);
    ASSERT_EQ(sqlite3_next_stmt(native, nullptr), nullptr); // Actual ending, no SQL-text cache exemption.
    sqlite3_stmt* raw = nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(native, "SELECT body FROM Fixture", -1, &raw, nullptr), SQLITE_OK);
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> own_statement(raw, sqlite3_finalize);
    ASSERT_NE(raw, nullptr); ASSERT_EQ(sqlite3_stmt_busy(raw), 0);
    Receiver refused;
    const auto failure = fixture.operation().capture(&refused, Receiver::callback);
    EXPECT_FALSE(failure.success()); EXPECT_FALSE(failure.cleanup_failed());
    EXPECT_EQ(failure.transaction_code(), -1); EXPECT_EQ(refused.tables, 0);
    EXPECT_NE(failure.error_message().find("already-issued original-reader statement"), std::string::npos);
    EXPECT_EQ(sqlite3_next_stmt(native, nullptr), raw); // Catalog did not finalize an unknown original.
    ASSERT_EQ(sqlite3_finalize(own_statement.release()), SQLITE_OK);
    Receiver next; succeeded(fixture.operation().capture(&next, Receiver::callback));
    EXPECT_EQ(next.rows, 1); EXPECT_EQ(sqlite3_next_stmt(native, nullptr), nullptr);
}

TEST(OwnedCatalogStream, EmptyWithoutRowidAndVirtualTargetsRefuseBeforeAnySchemaCallback) {
    Fixture fixture;
    fixture.owner->db().execute("CREATE TABLE EmptyWithout(id INTEGER PRIMARY KEY,globalId TEXT UNIQUE COLLATE NOCASE) WITHOUT ROWID");
    const auto registered_rtree = fixture.owner->db().query(
        "SELECT name FROM main.sqlite_schema WHERE type='table' AND name='_TestPlace_location_rtree'");
    ASSERT_EQ(registered_rtree.size(), 1u);
    EXPECT_TRUE(fixture.owner->db().query("SELECT id FROM EmptyWithout").empty());
    EXPECT_TRUE(fixture.owner->db().query("SELECT id FROM _TestPlace_location_rtree").empty());
    for (const auto& table : {"EmptyWithout", "_TestPlace_location_rtree"}) {
        Receiver receiver;
        const auto request = json{{"version", 1}, {"tables", json::array({table})}}.dump();
        const auto result = fixture.operation(request).capture(&receiver, Receiver::callback);
        EXPECT_FALSE(result.success()); EXPECT_FALSE(result.cleanup_failed());
        EXPECT_EQ(result.rollback_code(), SQLITE_OK);
        EXPECT_EQ(receiver.tables, 0); EXPECT_EQ(receiver.rows, 0); EXPECT_EQ(receiver.table_ends, 0);
        if (std::strcmp(table, "_TestPlace_location_rtree") == 0)
            EXPECT_NE(result.error_message().find("actual MAIN table btree"), std::string::npos);
        else EXPECT_NE(result.error_message().find("catalog metadata prepare failed"), std::string::npos);
    }
}

TEST(OwnedCatalogStream, QuotedTableNamesValidateTheActualMainBtreeAndRecapture) {
    Fixture fixture;
    fixture.owner->db().execute("CREATE TABLE \"odd\"\"name\"(id INTEGER PRIMARY KEY,globalId TEXT UNIQUE COLLATE NOCASE,body TEXT)");
    fixture.owner->db().execute("INSERT INTO \"odd\"\"name\" VALUES(?,?,?)",
        {int64_t(1), lattice::uuid_t::generate().to_string(), std::string("quoted")});
    const auto quoted_request = json{{"version", 1}, {"tables", json::array({"odd\"name"})}}.dump();
    Receiver first; succeeded(fixture.operation(quoted_request).capture(&first, Receiver::callback));
    Receiver next; succeeded(fixture.operation(quoted_request).capture(&next, Receiver::callback));
    EXPECT_EQ(first.tables, 1); EXPECT_EQ(first.rows, 1); EXPECT_EQ(first.body_bytes, 6u);
    EXPECT_EQ(next.tables, 1); EXPECT_EQ(next.rows, 1); EXPECT_EQ(next.body_sum, first.body_sum);
}
