#include <gtest/gtest.h>
#include <LatticeCore.hpp>
#include <array>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

// This file deliberately uses only in-memory stores. No allocator/global fault seam.
struct CheckedBindingRow {
    std::string body;
    std::optional<std::string> note;
};
LATTICE_SCHEMA(CheckedBindingRow, body, note);

namespace {
using namespace lattice;
constexpr int length_limit = 4096;
std::string oversized_text() { return std::string("ok\0", 3) + std::string(8192, 'x'); }

struct Statement {
    sqlite3_stmt* value = nullptr;
    Statement(sqlite3* raw, const char* sql) {
        const int rc = sqlite3_prepare_v2(raw, sql, -1, &value, nullptr);
        if (rc != SQLITE_OK) {
            sqlite3_finalize(value); value = nullptr;
            throw std::runtime_error("test statement prepare failed");
        }
    }
    ~Statement() { sqlite3_finalize(value); }
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;
};
std::set<sqlite3_stmt*> statements(sqlite3* raw) {
    std::set<sqlite3_stmt*> result;
    for (auto* p = sqlite3_next_stmt(raw, nullptr); p; p = sqlite3_next_stmt(raw, p)) result.insert(p);
    return result;
}
struct LengthLimit {
    sqlite3* raw;
    int previous;
    explicit LengthLimit(sqlite3* handle) : raw(handle), previous(sqlite3_limit(raw, SQLITE_LIMIT_LENGTH, length_limit)) {}
    ~LengthLimit() { sqlite3_limit(raw, SQLITE_LIMIT_LENGTH, previous); }
};
struct Hooks {
    database& db;
    explicit Hooks(database& value) : db(value) {}
    ~Hooks() { db.set_txn_hooks({}, {}); }
};
struct Rollback {
    database& db;
    ~Rollback() { try { if (db.is_in_transaction()) db.rollback(); } catch (...) {} }
};
struct Function {
    sqlite3* raw;
    const char* name;
    int arity;
    Function(sqlite3* handle, const char* function, int argc, void* context,
             void (*body)(sqlite3_context*, int, sqlite3_value**)) : raw(handle), name(function), arity(argc) {
        if (sqlite3_create_function_v2(raw, name, argc, SQLITE_UTF8, context, body, nullptr, nullptr, nullptr) != SQLITE_OK)
            throw std::runtime_error("test function registration failed");
    }
    ~Function() { sqlite3_create_function_v2(raw, name, arity, SQLITE_UTF8, nullptr, nullptr, nullptr, nullptr, nullptr); }
};
std::string error_from(const std::function<void()>& body) {
    try { body(); }
    catch (const db_error& error) { return error.what(); }
    catch (const std::exception& error) { ADD_FAILURE() << "expected db_error, got " << error.what(); return error.what(); }
    ADD_FAILURE() << "operation did not report its binding/step error";
    return {};
}
void binding_error(const std::function<void()>& body, int index, int code) {
    const auto message = error_from(body);
    EXPECT_NE(message.find("Parameter binding failed at index " + std::to_string(index) + " "), std::string::npos) << message;
    EXPECT_NE(message.find("SQLite code " + std::to_string(code) + ")"), std::string::npos) << message;
    EXPECT_EQ(message.find(std::string(64, 'x')), std::string::npos) << "diagnostic must not contain parameter bytes";
}
int64_t scalar(database& db, const std::string& sql) {
    return std::get<int64_t>(db.query(sql).at(0).at("n"));
}
struct Plain {
    database db{":memory:"};
    sqlite3* raw = db.handle();
    int vm_calls = 0;
    Function tick{raw, "checked_tick", 0, &vm_calls,
        [](sqlite3_context* context, int, sqlite3_value**) noexcept {
            ++*static_cast<int*>(sqlite3_user_data(context)); sqlite3_result_int(context, 1);
        }};
    Statement unrelated{raw, "SELECT 901"};
    Plain() {
        db.execute("CREATE TABLE CheckedTarget(id INTEGER PRIMARY KEY,globalId TEXT UNIQUE,body TEXT)");
        db.execute("INSERT INTO CheckedTarget VALUES(7,'seed','sentinel')");
        db.execute("CREATE TRIGGER checked_insert BEFORE INSERT ON CheckedTarget BEGIN SELECT checked_tick(); END");
        db.execute("CREATE TRIGGER checked_update BEFORE UPDATE ON CheckedTarget BEGIN SELECT checked_tick(); END");
    }
    void unchanged() {
        EXPECT_EQ(vm_calls, 0);
        EXPECT_EQ(statements(raw), std::set<sqlite3_stmt*>{unrelated.value});
        const auto rows = db.query("SELECT id,globalId,body FROM CheckedTarget");
        ASSERT_EQ(rows.size(), 1u);
        EXPECT_EQ(std::get<int64_t>(rows[0].at("id")), 7);
        EXPECT_EQ(std::get<std::string>(rows[0].at("body")), "sentinel");
        EXPECT_EQ(std::get<std::string>(rows[0].at("globalId")), "seed");
    }
};
configuration memory_config() {
    configuration result(":memory:"); result.audit_retention_seconds = 0; return result;
}
struct Bulk {
    lattice_db owner{memory_config()};
    database& db = owner.db();
    sqlite3* raw = db.handle();
    Statement unrelated{raw, "SELECT 902"};
    Bulk() {
        auto seed = owner.add(CheckedBindingRow{"seed", std::nullopt});
        owner.remove(seed); // Create normal model schema/triggers before lowering limits.
    }
    void failing_pair() {
        owner.add_bulk(std::vector<CheckedBindingRow>{{"first", std::nullopt}, {oversized_text(), std::string("second")}});
    }
    int64_t count() { return scalar(db, "SELECT count(*) AS n FROM CheckedBindingRow"); }
    void clean_statement() { EXPECT_EQ(statements(raw), std::set<sqlite3_stmt*>{unrelated.value}); }
};
struct DenyRollback {
    sqlite3* raw;
    int denied = 0;
    explicit DenyRollback(sqlite3* handle) : raw(handle) {
        if (sqlite3_set_authorizer(raw,
            [](void* context, int action, const char* operation, const char*, const char*, const char*) noexcept {
                if (action == SQLITE_TRANSACTION && operation && std::strcmp(operation, "ROLLBACK") == 0) {
                    ++static_cast<DenyRollback*>(context)->denied; return SQLITE_DENY;
                }
                return SQLITE_OK;
            }, this) != SQLITE_OK) throw std::runtime_error("test authorizer registration failed");
    }
    void clear() { if (raw) { sqlite3_set_authorizer(raw, nullptr, nullptr); raw = nullptr; } }
    ~DenyRollback() { clear(); }
};
}

TEST(CheckedBinding, EveryVisitorAlternativeChecksRangeAndCallerRetainsStatement) {
    database db(":memory:"); auto* raw = db.handle(); Statement statement(raw, "SELECT ?");
    const std::vector<column_value_t> values{nullptr, int64_t{4}, 2.5, std::string("text"), std::string{},
                                           std::vector<uint8_t>{1, 0, 2}, std::vector<uint8_t>{}};
    for (size_t n = 0; n < values.size(); ++n) {
        SCOPED_TRACE(n);
        binding_error([&] { db.bind_value(statement.value, 2, values[n]); }, 2, SQLITE_RANGE);
        EXPECT_EQ(statements(raw), std::set<sqlite3_stmt*>{statement.value});
        db.bind_value(statement.value, 1, int64_t{17});
        ASSERT_EQ(sqlite3_step(statement.value), SQLITE_ROW);
        EXPECT_EQ(sqlite3_column_int64(statement.value, 0), 17);
        ASSERT_EQ(sqlite3_reset(statement.value), SQLITE_OK);
        ASSERT_EQ(sqlite3_clear_bindings(statement.value), SQLITE_OK);
    }
    LengthLimit limit(raw);
    binding_error([&] { db.bind_value(statement.value, 1, std::vector<uint8_t>(8192, 0x5a)); }, 1, SQLITE_TOOBIG);
    EXPECT_EQ(statements(raw), std::set<sqlite3_stmt*>{statement.value});
}

TEST(CheckedBinding, TrueNulSuffixFailsBeforeLaterRangeAndUnderLimitBytesSurvive) {
    Plain f; LengthLimit limit(f.raw); const auto changes = sqlite3_total_changes64(f.raw);
    const auto sql = "UPDATE CheckedTarget SET body=? WHERE id=7";
    binding_error([&] { f.db.execute(sql, {oversized_text(), int64_t{9}}); }, 1, SQLITE_TOOBIG);
    binding_error([&] { f.db.execute(sql, {std::string(8195, 'x')}); }, 1, SQLITE_TOOBIG);
    binding_error([&] { f.db.execute(sql, {std::string("fits"), int64_t{9}}); }, 2, SQLITE_RANGE);
    EXPECT_EQ(sqlite3_total_changes64(f.raw), changes); f.unchanged();
    const std::string bytes("ok\0tail", 7);
    f.db.execute(sql, {bytes});
    const auto row = f.db.query("SELECT body,hex(body) AS bytes,length(CAST(body AS BLOB)) AS n FROM CheckedTarget").at(0);
    EXPECT_EQ(std::get<std::string>(row.at("body")), bytes);
    EXPECT_EQ(std::get<std::string>(row.at("bytes")), "6F6B007461696C");
    EXPECT_EQ(std::get<int64_t>(row.at("n")), 7);
    // Ordinary SQL still permits too-few bindings: the other placeholder is NULL.
    const auto unbound = f.db.query("SELECT ? AS a,? AS b", {int64_t{1}}).at(0);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(unbound.at("b")));
}

TEST(CheckedBinding, ExecuteInsertUpsertUpdateAndQueryRefuseBeforeAnyVmEffect) {
    Plain f; LengthLimit limit(f.raw);
    const std::vector<std::function<void()>> operations{
        [&] { f.db.execute("UPDATE CheckedTarget SET body=? WHERE id=7", {oversized_text()}); },
        [&] { f.db.insert("CheckedTarget", {{"body", oversized_text()}, {"globalId", std::string("new")}}); },
        [&] { f.db.insert("CheckedTarget", {{"body", oversized_text()}, {"globalId", std::string("seed")}}, {"globalId"}); },
        [&] { f.db.update("CheckedTarget", 7, {{"body", oversized_text()}}); },
        [&] { f.db.query("UPDATE CheckedTarget SET body=? WHERE id=7 RETURNING checked_tick() AS visited", {oversized_text()}); }
    };
    for (size_t n = 0; n < operations.size(); ++n) {
        SCOPED_TRACE(n); const auto changes = sqlite3_total_changes64(f.raw);
        binding_error(operations[n], 1, SQLITE_TOOBIG);
        EXPECT_EQ(sqlite3_total_changes64(f.raw), changes); f.unchanged();
    }
}

TEST(CheckedBinding, PreStepPrepareAndBindFailuresNeitherDrainNorDiscardPendingDirty) {
    Plain f; Hooks hooks(f.db); LengthLimit limit(f.raw); int settled = 0, discarded = 0;
    f.db.set_txn_hooks([&] { ++settled; EXPECT_EQ(statements(f.raw), std::set<sqlite3_stmt*>{f.unrelated.value}); },
                       [&]() noexcept { ++discarded; });
    const std::vector<std::function<void()>> operations{
        [&] { f.db.execute("NOT VALID SQL ?", {int64_t{1}}); },
        [&] { f.db.query("SELECT ? AS body", {oversized_text()}); },
        [&] { f.db.execute("SELECT ?", {int64_t{1}, int64_t{2}}); }
    };
    for (size_t n = 0; n < operations.size(); ++n) {
        SCOPED_TRACE(n); f.db.mark_txn_dirty(); EXPECT_FALSE(error_from(operations[n]).empty());
        EXPECT_EQ(settled, static_cast<int>(n)); EXPECT_EQ(discarded, 0);
        EXPECT_EQ(statements(f.raw), std::set<sqlite3_stmt*>{f.unrelated.value});
        f.db.query("SELECT 1 AS n"); EXPECT_EQ(settled, static_cast<int>(n + 1));
        f.db.query("SELECT 1 AS n"); EXPECT_EQ(settled, static_cast<int>(n + 1));
    }
}

TEST(CheckedBinding, OrdinaryBindFailureLeavesCallerTransactionAndEarlierDirtyWriteIntact) {
    Plain f; Hooks hooks(f.db); Rollback cleanup{f.db}; int settled = 0, discarded = 0;
    f.db.set_txn_hooks([&] { ++settled; }, [&]() noexcept { ++discarded; });
    f.db.begin_transaction(); f.db.execute("UPDATE CheckedTarget SET body='earlier' WHERE id=7"); f.db.mark_txn_dirty();
    { LengthLimit limit(f.raw); binding_error([&] { f.db.update("CheckedTarget", 7, {{"body", oversized_text()}}); }, 1, SQLITE_TOOBIG); }
    EXPECT_TRUE(f.db.is_in_transaction()); EXPECT_EQ(settled, 0); EXPECT_EQ(discarded, 0);
    EXPECT_EQ(std::get<std::string>(f.db.query("SELECT body FROM CheckedTarget").at(0).at("body")), "earlier");
    EXPECT_EQ(statements(f.raw), std::set<sqlite3_stmt*>{f.unrelated.value});
    f.db.commit(); EXPECT_FALSE(f.db.is_in_transaction()); EXPECT_EQ(settled, 1); EXPECT_EQ(discarded, 0);
}

TEST(CheckedBinding, StepErrorsKeepOwnedSentinelAndReleaseEachStatementIncludingLiveGetter) {
    Plain f;
    Function fail(f.raw, "checked_fail", 1, nullptr,
        [](sqlite3_context* context, int, sqlite3_value**) noexcept {
            sqlite3_result_error(context, "checked-step-sentinel", -1);
        });
    f.db.execute("CREATE TRIGGER checked_fail_insert BEFORE INSERT ON CheckedTarget WHEN NEW.body='fail' BEGIN SELECT checked_fail(0); END");
    f.db.execute("CREATE TRIGGER checked_fail_update BEFORE UPDATE ON CheckedTarget WHEN NEW.body='fail' BEGIN SELECT checked_fail(0); END");
    f.db.execute("CREATE VIEW CheckedReadFailure AS SELECT 1 AS id,checked_fail(0) AS value");
    managed<int64_t> live; live.assign(&f.db, nullptr, "CheckedReadFailure", "value", 1);
    const std::vector<std::function<void()>> operations{
        [&] { f.db.execute("SELECT checked_fail(?)", {int64_t{0}}); },
        [&] { f.db.insert("CheckedTarget", {{"body", std::string("fail")}, {"globalId", std::string("new")}}); },
        [&] { f.db.insert("CheckedTarget", {{"body", std::string("fail")}, {"globalId", std::string("seed")}}, {"globalId"}); },
        [&] { f.db.update("CheckedTarget", 7, {{"body", std::string("fail")}}); },
        [&] { f.db.query("UPDATE CheckedTarget SET body='fail' WHERE id=? RETURNING body", {int64_t{7}}); },
        [&] { (void)live.detach(); }
    };
    for (size_t n = 0; n < operations.size(); ++n) {
        SCOPED_TRACE(n); EXPECT_NE(error_from(operations[n]).find("checked-step-sentinel"), std::string::npos);
        EXPECT_EQ(statements(f.raw), std::set<sqlite3_stmt*>{f.unrelated.value});
        EXPECT_FALSE(f.db.is_in_transaction());
        EXPECT_EQ(std::get<std::string>(f.db.query("SELECT body FROM CheckedTarget").at(0).at("body")), "sentinel");
    }
}

TEST(CheckedBinding, SuccessfulInsertAndUpsertKeepIdentityAcrossReentrantSettledWrites) {
    Plain f; f.db.execute("CREATE TABLE CheckedSide(id INTEGER PRIMARY KEY,body TEXT)");
    f.db.execute("INSERT INTO CheckedSide VALUES(100,'seed')"); Hooks hooks(f.db); int settled = 0;
    f.db.set_txn_hooks([&] {
        ++settled; EXPECT_EQ(statements(f.raw), std::set<sqlite3_stmt*>{f.unrelated.value});
        f.db.insert("CheckedSide", {{"body", std::string("observer")}});
    }, []() noexcept {});
    f.db.mark_txn_dirty();
    EXPECT_EQ(f.db.insert("CheckedTarget", {{"body", std::string("new")}, {"globalId", std::string("new")}}), 8);
    EXPECT_EQ(settled, 1); EXPECT_EQ(sqlite3_last_insert_rowid(f.raw), 101);
    f.db.mark_txn_dirty();
    EXPECT_EQ(f.db.insert("CheckedTarget", {{"body", std::string("updated")}, {"globalId", std::string("seed")}}, {"globalId"}), 7);
    EXPECT_EQ(settled, 2); EXPECT_EQ(sqlite3_last_insert_rowid(f.raw), 102);
    EXPECT_EQ(f.db.insert("CheckedTarget", {{"globalId", std::string("seed")}}, {"globalId"}), 0);
    EXPECT_EQ(std::get<std::string>(f.db.query("SELECT body FROM CheckedTarget WHERE id=7").at(0).at("body")), "updated");
}

TEST(CheckedBinding, BulkSecondRowBindFailureRollsBackAndCanBeFollowedBySmallBatch) {
    Bulk f; Hooks hooks(f.db); int settled = 0, discarded = 0;
    f.db.set_txn_hooks([&] { ++settled; }, [&]() noexcept { ++discarded; });
    { LengthLimit limit(f.raw); binding_error([&] { f.failing_pair(); }, 2, SQLITE_TOOBIG); }
    f.clean_statement(); EXPECT_FALSE(f.db.is_in_transaction()); EXPECT_EQ(settled, 0); EXPECT_GE(discarded, 1);
    EXPECT_EQ(f.count(), 0); EXPECT_EQ(settled, 0) << "rolled-back rows must not deliver later";
    auto rows = f.owner.add_bulk(std::vector<CheckedBindingRow>{{"ok", std::nullopt}, {"also-ok", std::string("note")}});
    ASSERT_EQ(rows.size(), 2u); EXPECT_EQ(f.count(), 2); EXPECT_EQ(settled, 1); f.clean_statement();
}

TEST(CheckedBinding, BulkFailurePreservesExistingWholeCallerTransactionRollbackPolicy) {
    Bulk f; Hooks hooks(f.db); Rollback cleanup{f.db}; int settled = 0, discarded = 0;
    f.db.set_txn_hooks([&] { ++settled; }, [&]() noexcept { ++discarded; });
    f.owner.begin_transaction(); f.owner.add(CheckedBindingRow{"before-bulk", std::nullopt});
    { LengthLimit limit(f.raw); binding_error([&] { f.failing_pair(); }, 2, SQLITE_TOOBIG); }
    EXPECT_FALSE(f.db.is_in_transaction()); f.clean_statement(); EXPECT_EQ(f.count(), 0);
    EXPECT_EQ(settled, 0); EXPECT_GE(discarded, 1);
}

TEST(CheckedBinding, DeniedBulkRollbackCannotReplaceTheFirstBindErrorOrLeakItsStatement) {
    Bulk f; Rollback cleanup{f.db};
    { LengthLimit limit(f.raw); DenyRollback denied(f.raw);
      binding_error([&] { f.failing_pair(); }, 2, SQLITE_TOOBIG);
      EXPECT_GE(denied.denied, 1); EXPECT_TRUE(f.db.is_in_transaction()); f.clean_statement();
      denied.clear();
    }
    EXPECT_EQ(f.count(), 1) << "denied rollback must not be reported as having undone row one";
    f.db.rollback(); EXPECT_FALSE(f.db.is_in_transaction()); EXPECT_EQ(f.count(), 0); f.clean_statement();
}

TEST(CheckedBinding, BulkFinalizesBeforeSettledCallbackAndCallbackThrowDoesNotUndoCommit) {
    Bulk f; Hooks hooks(f.db); int settled = 0; bool statement_closed = false;
    f.db.set_txn_hooks([&] {
        ++settled; statement_closed = statements(f.raw) == std::set<sqlite3_stmt*>{f.unrelated.value};
        throw std::runtime_error("checked-settled-sentinel");
    }, []() noexcept {});
    try {
        f.owner.add_bulk(std::vector<CheckedBindingRow>{{"one", std::nullopt}, {"two", std::nullopt}});
        ADD_FAILURE() << "settled callback exception did not propagate";
    } catch (const std::runtime_error& error) { EXPECT_STREQ(error.what(), "checked-settled-sentinel"); }
    EXPECT_EQ(settled, 1); EXPECT_TRUE(statement_closed); EXPECT_FALSE(f.db.is_in_transaction());
    EXPECT_EQ(f.count(), 2); EXPECT_EQ(settled, 1); f.clean_statement();
}

TEST(CheckedBinding, BulkMissingPrimitiveAndExpandedGeoValuesRemainSqlNull) {
    Bulk f; auto schema = managed<CheckedBindingRow>::schema();
    property_descriptor absent; absent.name = "absent"; absent.type = column_type::text; absent.nullable = true;
    property_descriptor bounds; bounds.name = "bounds"; bounds.type = column_type::real; bounds.nullable = true; bounds.is_geo_bounds = true;
    schema.properties.push_back(absent); schema.properties.push_back(bounds);
    // Explicit schema can include fields absent from collected dynamic values.
    // Install every added column explicitly; ensure_table only checks existence.
    f.db.execute("ALTER TABLE CheckedBindingRow ADD COLUMN absent TEXT");
    for (const auto* name : {"bounds_minLat", "bounds_maxLat", "bounds_minLon", "bounds_maxLon"})
        f.db.execute(std::string("ALTER TABLE CheckedBindingRow ADD COLUMN ") + name + " REAL");
    auto rows = f.owner.add_bulk_with_schema(std::vector<CheckedBindingRow>{{"one", std::nullopt}, {"two", std::string("note")}}, schema);
    ASSERT_EQ(rows.size(), 2u);
    const auto values = f.db.query("SELECT note,absent,bounds_minLat,bounds_maxLat,bounds_minLon,bounds_maxLon FROM CheckedBindingRow ORDER BY id");
    ASSERT_EQ(values.size(), 2u); EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(values[0].at("note")));
    EXPECT_EQ(std::get<std::string>(values[1].at("note")), "note");
    for (const auto& row : values)
        for (const auto* name : {"absent", "bounds_minLat", "bounds_maxLat", "bounds_minLon", "bounds_maxLon"})
            EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(row.at(name))) << name;
    f.clean_statement();
}
