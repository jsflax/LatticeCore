#include "TestHelpers.hpp"
#include <LatticeCore.hpp>
#include <atomic>
#include <array>
#include <future>
#include <optional>
#include <thread>

struct OperationRow { int64_t value; };
LATTICE_SCHEMA(OperationRow, value);
struct OperationOptionalRow { std::optional<int64_t> value; };
LATTICE_SCHEMA(OperationOptionalRow, value);

namespace {
class OperationOwner : public lattice::lattice_db {
public:
    explicit OperationOwner(const lattice::configuration& config) : lattice_db(config) {}
    template<class F> auto unit(F&& fn) { return with_mutation_operation(std::forward<F>(fn)); }
};

class ManagedMutationOperation : public ::testing::Test {
protected:
    OperationOwner owner{lattice::configuration(":memory:")};
    std::vector<int64_t> inserts;
    lattice::lattice_db::observer_id token = 0;
    void SetUp() override {
        token = owner.add_table_observer("OperationRow", [this](const auto& changes) {
            for (const auto& change : changes)
                if (std::get<1>(change) == "INSERT") inserts.push_back(std::get<2>(change));
        });
    }
    void TearDown() override {
        owner.remove_table_observer("OperationRow", token);
        if (owner.db().is_in_transaction()) owner.rollback();
        owner.close();
    }
    std::vector<int64_t> values() {
        std::vector<int64_t> result;
        for (const auto& row : owner.db().query("SELECT value FROM OperationRow ORDER BY id"))
            result.push_back(std::get<int64_t>(row.at("value")));
        return result;
    }
    void refuse_second(bool full_rollback = false) {
        owner.db().execute(std::string("CREATE TRIGGER operation_second BEFORE INSERT ON OperationRow ") +
            "WHEN NEW.value = 22 BEGIN SELECT RAISE(" + (full_rollback ? "ROLLBACK" : "ABORT") +
            ", 'operation late second insert'); END");
    }
    void expect_original_first(const std::function<void()>& action) {
        try { action(); FAIL() << "real late INSERT refusal required"; }
        catch (const std::exception& error) {
            EXPECT_NE(std::string(error.what()).find("operation late second insert"), std::string::npos);
        }
    }
};

TEST_F(ManagedMutationOperation, CallerBulkFailurePreservesEarlierPrefixAndNativeDelivery) {
    refuse_second();
    owner.begin_transaction();
    owner.add(OperationRow{7});
    ASSERT_TRUE(inserts.empty());
    expect_original_first([&] { owner.add_bulk(std::vector<OperationRow>{{11}, {22}}); });
    ASSERT_TRUE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{7}));
    EXPECT_TRUE(inserts.empty());
    owner.commit();
    EXPECT_FALSE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{7}));
    EXPECT_EQ(inserts.size(), 1u) << "failed bulk's first row may not escape its Native checkpoint";
    owner.handle_cross_process_notification();
    EXPECT_EQ(inserts.size(), 1u);
}

TEST_F(ManagedMutationOperation, OwnedBulkFailureRollsBackWholeUnitAndDoesNotStrandLaterDelivery) {
    refuse_second();
    expect_original_first([&] { owner.add_bulk(std::vector<OperationRow>{{11}, {22}}); });
    EXPECT_FALSE(owner.db().is_in_transaction());
    EXPECT_TRUE(values().empty());
    EXPECT_TRUE(inserts.empty());
    const auto rows = owner.add_bulk(std::vector<OperationRow>{{33}, {44}});
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(values(), (std::vector<int64_t>{33, 44}));
    EXPECT_EQ(inserts.size(), 2u);
}

TEST_F(ManagedMutationOperation, PreDDLTableCreationRollsBackWithLateNotNullInsertFailure) {
    auto schema = lattice::managed<OperationOptionalRow>::schema();
    schema.table_name = "OperationUnregisteredDDL";
    for (auto& property : schema.properties) property.nullable = false;
    ASSERT_FALSE(owner.db().table_exists(schema.table_name));
    EXPECT_THROW(owner.add_bulk_with_schema(
        std::vector<OperationOptionalRow>{{int64_t(11)}, {std::nullopt}}, schema), std::exception);
    EXPECT_FALSE(owner.db().is_in_transaction());
    EXPECT_FALSE(owner.db().table_exists(schema.table_name)) << "DDL belongs inside the owned BEGIN";
    EXPECT_TRUE(inserts.empty());
}

TEST_F(ManagedMutationOperation, CallerSavepointFailureRemovesNewDDLWithoutEndingCaller) {
    auto schema = lattice::managed<OperationOptionalRow>::schema();
    schema.table_name = "OperationCallerDDL";
    for (auto& property : schema.properties) property.nullable = false;
    owner.begin_transaction(); owner.add(OperationRow{7});
    EXPECT_THROW(owner.add_bulk_with_schema(
        std::vector<OperationOptionalRow>{{int64_t(11)}, {std::nullopt}}, schema), std::exception);
    ASSERT_TRUE(owner.db().is_in_transaction());
    EXPECT_FALSE(owner.db().table_exists(schema.table_name));
    EXPECT_EQ(values(), (std::vector<int64_t>{7}));
    owner.commit();
    EXPECT_EQ(inserts.size(), 1u);
}

TEST_F(ManagedMutationOperation, RawCallerTransactionIsNotBorrowedOrRolledBack) {
    owner.db().execute("BEGIN IMMEDIATE");
    owner.db().execute("INSERT INTO OperationRow(globalId, value) VALUES('raw-prefix', 7)");
    EXPECT_THROW(owner.add_bulk(std::vector<OperationRow>{{11}, {33}}), std::exception);
    EXPECT_TRUE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{7}));
    EXPECT_TRUE(inserts.empty());
    owner.db().execute("ROLLBACK");
    EXPECT_TRUE(values().empty());
}

TEST_F(ManagedMutationOperation, ForeignThreadCannotBorrowGenuineCallerOriginal) {
    owner.begin_transaction(); owner.add(OperationRow{7});
    auto refused = std::async(std::launch::async, [&] {
        try { owner.add_bulk(std::vector<OperationRow>{{11}, {33}}); return false; }
        catch (const std::exception&) { return true; }
    });
    ASSERT_EQ(refused.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_TRUE(refused.get());
    ASSERT_TRUE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{7}));
    owner.commit();
    EXPECT_EQ(inserts.size(), 1u);
}

TEST_F(ManagedMutationOperation, ActualSQLiteWholeRollbackDoesNotResurrectCallerPrefix) {
    refuse_second(true);
    owner.begin_transaction(); owner.add(OperationRow{7});
    expect_original_first([&] { owner.add_bulk(std::vector<OperationRow>{{11}, {22}}); });
    EXPECT_FALSE(owner.db().is_in_transaction());
    EXPECT_TRUE(values().empty());
    EXPECT_TRUE(inserts.empty());
    // The ended old original was retired; a new caller begins independently.
    owner.begin_transaction(); owner.add(OperationRow{33}); owner.commit();
    EXPECT_EQ(values(), (std::vector<int64_t>{33}));
    EXPECT_EQ(inserts.size(), 1u);
}

TEST_F(ManagedMutationOperation, NestedBulkReleaseRemainsPendingUntilActualOuterCommit) {
    owner.unit([&](lattice::database&) {
        const auto rows = owner.add_bulk(std::vector<OperationRow>{{11}, {33}});
        ASSERT_EQ(rows.size(), 2u);
        EXPECT_TRUE(inserts.empty());
    });
    EXPECT_FALSE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{11, 33}));
    EXPECT_EQ(inserts.size(), 2u);
}

TEST_F(ManagedMutationOperation, OuterOperationFailureDiscardsSuccessfulInnerBulk) {
    EXPECT_THROW(owner.unit([&](lattice::database&) {
        owner.add_bulk(std::vector<OperationRow>{{11}, {33}});
        throw std::runtime_error("outer operation refusal");
    }), std::runtime_error);
    EXPECT_FALSE(owner.db().is_in_transaction());
    EXPECT_TRUE(values().empty());
    EXPECT_TRUE(inserts.empty());
}

TEST_F(ManagedMutationOperation, CallbackSuccessorHasNewTicketAndIsNotFlushedBeforeCommit) {
    bool started = false;
    const auto successor = owner.add_table_observer("OperationRow", [&](const auto&) {
        if (started) return;
        started = true;
        owner.begin_transaction();
        owner.add(OperationRow{99});
    });
    owner.begin_transaction(); owner.add(OperationRow{7}); owner.commit();
    ASSERT_TRUE(started);
    ASSERT_TRUE(owner.db().is_in_transaction());
    EXPECT_EQ(inserts.size(), 1u) << "old delivery may not flush a live successor's buffer";
    owner.remove_table_observer("OperationRow", successor);
    owner.commit();
    EXPECT_FALSE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{7, 99}));
    EXPECT_EQ(inserts.size(), 2u);
}
TEST_F(ManagedMutationOperation, FirstCallbackErrorCannotRollbackItsNewSuccessor) {
    bool started = false;
    const auto successor = owner.add_table_observer("OperationRow", [&](const auto&) {
        if (started) return;
        started = true;
        owner.begin_transaction(); owner.add(OperationRow{99});
        throw std::runtime_error("original callback failure");
    });
    std::string first;
    try { owner.write([&] { owner.add(OperationRow{7}); }); }
    catch (const std::exception& error) { first = error.what(); }
    EXPECT_EQ(first, "original callback failure");
    ASSERT_TRUE(started);
    ASSERT_TRUE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{7, 99}));
    owner.remove_table_observer("OperationRow", successor);
    owner.commit();
    EXPECT_FALSE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{7, 99}));
}

TEST_F(ManagedMutationOperation, AlreadyAdmittedNativeCloseStillSettlesItsActualUnit) {
    owner.unit([&](lattice::database& writer) {
        writer.close(); // actual logical close AFTER private Native admission
        writer.execute("INSERT INTO OperationRow(globalId,value) VALUES('admitted-close', 71)");
    });
    EXPECT_FALSE(owner.db().is_in_transaction());
    EXPECT_EQ(inserts.size(), 1u);
    // Export only AFTER this complete unit, to inspect the retained closed
    // handle. This is not an admission witness or a caller-transaction grant.
    auto* raw = owner.db().handle();
    sqlite3_stmt* statement = nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(raw, "SELECT value FROM OperationRow", -1, &statement, nullptr), SQLITE_OK);
    struct Finalize { sqlite3_stmt* value; ~Finalize() { sqlite3_finalize(value); } } ending{statement};
    ASSERT_EQ(sqlite3_step(statement), SQLITE_ROW);
    EXPECT_EQ(sqlite3_column_int64(statement, 0), 71);
    EXPECT_EQ(sqlite3_step(statement), SQLITE_DONE);
}

} // namespace

#include "../../Sources/LatticeCore/src/mutation_prefix_checkpoint_test_probe.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include <cstring>
#include <new>

namespace {
struct PrefixReserveFault {
    bool previous = lattice::detail::mutation_prefix_test_hooks::fail_next_restore_reserve;
    PrefixReserveFault() { lattice::detail::mutation_prefix_test_hooks::fail_next_restore_reserve = true; }
    ~PrefixReserveFault() { lattice::detail::mutation_prefix_test_hooks::fail_next_restore_reserve = previous; }
};

TEST_F(ManagedMutationOperation, SuccessfulRepeatedUnitsKeepExactCallerPrefixWithoutRecoveryReserve) {
    owner.begin_transaction();
    owner.add(OperationRow{7});
    std::vector<int64_t> expected{7};
    {
        PrefixReserveFault no_failure_path;
        for (int64_t value = 31; value != 39; ++value) {
            const auto rows = owner.add_bulk(std::vector<OperationRow>{{value}});
            ASSERT_EQ(rows.size(), 1u);
            expected.push_back(value);
        }
        EXPECT_TRUE(lattice::detail::mutation_prefix_test_hooks::fail_next_restore_reserve)
            << "successful operations never enter recovery allocation";
    }
    EXPECT_EQ(values(), expected);
    EXPECT_TRUE(inserts.empty());
    owner.commit();
    EXPECT_EQ(values(), expected);
    EXPECT_EQ(inserts.size(), expected.size());
    for (size_t i = 1; i < inserts.size(); ++i) EXPECT_LT(inserts[i - 1], inserts[i]);
    owner.handle_cross_process_notification();
    EXPECT_EQ(inserts.size(), expected.size());
}

TEST_F(ManagedMutationOperation, FailedNestedSavepointKeepsBothAncestorPrefixesAndExactDelivery) {
    refuse_second();
    owner.begin_transaction(); owner.add(OperationRow{7});
    owner.unit([&](lattice::database&) {
        owner.add(OperationRow{9});
        expect_original_first([&] { owner.add_bulk(std::vector<OperationRow>{{11}, {22}}); });
        EXPECT_TRUE(owner.db().is_in_transaction());
        EXPECT_EQ(values(), (std::vector<int64_t>{7, 9}));
        owner.add(OperationRow{33});
        EXPECT_TRUE(inserts.empty());
    });
    EXPECT_EQ(values(), (std::vector<int64_t>{7, 9, 33}));
    EXPECT_TRUE(inserts.empty());
    owner.commit();
    EXPECT_EQ(inserts.size(), 3u);
    owner.handle_cross_process_notification();
    EXPECT_EQ(inserts.size(), 3u);
}

TEST_F(ManagedMutationOperation, ProtectedDiscardRefusesBeforeDestroyingActualCallerPrefix) {
    owner.begin_transaction(); owner.add(OperationRow{7});
    std::string first;
    try {
        owner.unit([&](lattice::database&) {
            owner.add(OperationRow{11});
            owner.discard_change_buffer();
        });
        FAIL() << "protected discard must refuse before clearing the prefix";
    } catch (const std::exception& error) { first = error.what(); }
    EXPECT_EQ(first, "cannot discard a protected mutation prefix");
    ASSERT_TRUE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{7}));
    EXPECT_TRUE(inserts.empty());
    owner.commit();
    EXPECT_EQ(inserts.size(), 1u);
    EXPECT_EQ(values(), (std::vector<int64_t>{7}));
}

TEST_F(ManagedMutationOperation, RecoveryReserveFailurePreservesPrimaryAndFencesUntilActualWholeRollback) {
    owner.begin_transaction(); owner.add(OperationRow{7});
    const auto primary = std::make_exception_ptr(std::runtime_error("prefix body first"));
    bool observed = false;
    {
        PrefixReserveFault fail_recovery;
        try {
            owner.unit([&](lattice::database&) {
                owner.add(OperationRow{11});
                std::rethrow_exception(primary);
            });
        } catch (const lattice::mutation_operation_error& error) {
            observed = true;
            EXPECT_EQ(error.first(), primary);
            EXPECT_FALSE(error.rollback_failure());
            EXPECT_FALSE(error.release_failure());
            ASSERT_TRUE(error.capture_failure());
            EXPECT_THROW(std::rethrow_exception(error.capture_failure()), std::bad_alloc);
            EXPECT_FALSE(error.immediate_unit_settled());
            EXPECT_FALSE(error.outer_transaction_committed());
        }
        EXPECT_FALSE(lattice::detail::mutation_prefix_test_hooks::fail_next_restore_reserve)
            << "fault is consumed at actual post-rollback recovery reserve";
    }
    ASSERT_TRUE(observed);
    ASSERT_TRUE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{7})); // readonly diagnostics remain allowed
    EXPECT_THROW(owner.add_bulk(std::vector<OperationRow>{{33}}), std::exception);
    EXPECT_THROW(owner.commit(), std::exception);
    EXPECT_TRUE(inserts.empty());
    // The failed recovery never invents a usable prefix. A genuine whole
    // ROLLBACK restores the original pre-BEGIN snapshot and clears the fence.
    owner.rollback();
    EXPECT_FALSE(owner.db().is_in_transaction());
    EXPECT_TRUE(values().empty());
    EXPECT_TRUE(inserts.empty());
    owner.begin_transaction(); owner.add_bulk(std::vector<OperationRow>{{44}}); owner.commit();
    EXPECT_EQ(values(), (std::vector<int64_t>{44}));
    EXPECT_EQ(inserts.size(), 1u);
}

TEST_F(ManagedMutationOperation, DeniedActualRollbackKeepsCleanupDebtAndNeverRestoresByLengthAlone) {
    owner.begin_transaction(); owner.add(OperationRow{7});
    auto* native = lattice::detail::canonical_writer_custody_test_access::fault_handle(owner.db());
    struct Authorization {
        sqlite3* native;
        bool deny = true;
        int attempts = 0;
        static int check(void* context, int action, const char* operation,
                         const char*, const char*, const char*) noexcept {
            auto& self = *static_cast<Authorization*>(context);
            if (action == SQLITE_SAVEPOINT && operation && std::strcmp(operation, "ROLLBACK") == 0) {
                ++self.attempts;
                if (self.deny) return SQLITE_DENY;
            }
            return SQLITE_OK;
        }
        ~Authorization() { sqlite3_set_authorizer(native, nullptr, nullptr); }
    } authorization{native};
    ASSERT_EQ(sqlite3_set_authorizer(native, &Authorization::check, &authorization), SQLITE_OK);
    const auto primary = std::make_exception_ptr(std::runtime_error("rollback denial first"));
    bool observed = false;
    {
        PrefixReserveFault must_not_restore;
        try {
            owner.unit([&](lattice::database&) {
                owner.add(OperationRow{11});
                std::rethrow_exception(primary);
            });
        } catch (const lattice::mutation_operation_error& error) {
            observed = true;
            EXPECT_EQ(error.first(), primary);
            EXPECT_TRUE(error.rollback_failure());
            EXPECT_FALSE(error.release_failure());
            EXPECT_FALSE(error.capture_failure());
            EXPECT_FALSE(error.immediate_unit_settled());
        }
        EXPECT_TRUE(lattice::detail::mutation_prefix_test_hooks::fail_next_restore_reserve)
            << "a denied ROLLBACK TO cannot authorize prefix restoration";
    }
    EXPECT_EQ(authorization.attempts, 1);
    EXPECT_TRUE(observed);
    authorization.deny = false;
    EXPECT_THROW(owner.commit(), std::exception);
    EXPECT_TRUE(inserts.empty());
    owner.rollback();
    EXPECT_FALSE(owner.db().is_in_transaction());
    EXPECT_TRUE(values().empty());
    EXPECT_TRUE(inserts.empty());
    owner.add_bulk(std::vector<OperationRow>{{44}});
    EXPECT_EQ(values(), (std::vector<int64_t>{44}));
    EXPECT_EQ(inserts.size(), 1u);
}

TEST_F(ManagedMutationOperation, NestedActualWholeRollbackCannotRebuildEitherSavedOperationPrefix) {
    refuse_second(true);
    owner.begin_transaction(); owner.add(OperationRow{7});
    expect_original_first([&] {
        owner.unit([&](lattice::database&) {
            owner.add(OperationRow{9});
            owner.add_bulk(std::vector<OperationRow>{{11}, {22}});
        });
    });
    EXPECT_FALSE(owner.db().is_in_transaction());
    EXPECT_TRUE(values().empty());
    EXPECT_TRUE(inserts.empty());
    owner.begin_transaction(); owner.add_bulk(std::vector<OperationRow>{{44}}); owner.commit();
    EXPECT_EQ(values(), (std::vector<int64_t>{44}));
    EXPECT_EQ(inserts.size(), 1u);
    owner.handle_cross_process_notification();
    EXPECT_EQ(inserts.size(), 1u);
}

TEST_F(ManagedMutationOperation, FailedStepFinalizeEchoKeepsOriginalDbErrorAndCallerPrefix) {
    refuse_second();
    owner.begin_transaction(); owner.add(OperationRow{7});
    bool original = false;
    try {
        owner.unit([&](lattice::database&) {
            owner.add(OperationRow{11}); owner.add(OperationRow{22});
        });
        FAIL() << "the actual trigger must refuse the second insert";
    } catch (const lattice::db_error& error) {
        original = true;
        EXPECT_EQ(std::string(error.what()), "Insert failed: operation late second insert");
    } catch (const lattice::mutation_operation_error&) {
        FAIL() << "the same failed step returned by finalize is not independent cleanup debt";
    }
    ASSERT_TRUE(original);
    ASSERT_TRUE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{7}));
    EXPECT_TRUE(inserts.empty());
    owner.commit();
    EXPECT_EQ(values(), (std::vector<int64_t>{7}));
    EXPECT_EQ(inserts.size(), 1u);
}

TEST_F(ManagedMutationOperation, CaughtFailedStepFinalizeEchoStillRefusesUnitRelease) {
    refuse_second();
    owner.begin_transaction(); owner.add(OperationRow{7});
    bool caught_inside = false, original = false;
    try {
        owner.unit([&](lattice::database&) {
            owner.add(OperationRow{11});
            try { owner.add(OperationRow{22}); }
            catch (const lattice::db_error& error) {
                caught_inside = true;
                EXPECT_EQ(std::string(error.what()), "Insert failed: operation late second insert");
            }
            owner.add(OperationRow{33}); // still inside the same refused unit
        });
        FAIL() << "catching the real failed step cannot authorize RELEASE";
    } catch (const lattice::db_error& error) {
        original = true;
        EXPECT_EQ(std::string(error.what()), "Insert failed: operation late second insert");
    }
    ASSERT_TRUE(caught_inside);
    ASSERT_TRUE(original);
    ASSERT_TRUE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{7}));
    EXPECT_TRUE(inserts.empty());
    owner.commit();
    EXPECT_EQ(inserts.size(), 1u);
}

TEST_F(ManagedMutationOperation, UnknownFailedFinalizeKeepsSeparateDebtAndActualCallerPrefix) {
    refuse_second();
    owner.begin_transaction(); owner.add(OperationRow{7});
    bool observed = false;
    try {
        owner.unit([&](lattice::database& writer) {
            owner.add(OperationRow{11});
            // This query's engine scope uses the conservative unknown-result
            // finalizer. No known-failure token is inferred from text/code.
            writer.query("INSERT INTO OperationRow (globalId,value) VALUES "
                         "('b617a83d-c4b8-43dd-a58a-643900210022',22) RETURNING id");
        });
        FAIL() << "the actual RETURNING statement must refuse";
    } catch (const lattice::mutation_operation_error& error) {
        observed = true;
        ASSERT_TRUE(error.first());
        ASSERT_TRUE(error.finalize_failure());
        EXPECT_NE(error.first(), error.finalize_failure());
        try { std::rethrow_exception(error.first()); }
        catch (const lattice::db_error& first) {
            EXPECT_EQ(std::string(first.what()), "Query failed: operation late second insert");
        }
        EXPECT_THROW(std::rethrow_exception(error.finalize_failure()), lattice::db_error);
    }
    ASSERT_TRUE(observed);
    ASSERT_TRUE(owner.db().is_in_transaction());
    EXPECT_EQ(values(), (std::vector<int64_t>{7}));
    EXPECT_TRUE(inserts.empty());
    owner.commit();
    EXPECT_EQ(inserts.size(), 1u);
}

TEST_F(ManagedMutationOperation, UpsertUpdateAndDeleteEchoKeepEachOriginalFailureAndCallerPrefix) {
    owner.add(OperationRow{44}); inserts.clear();
    const auto existing = owner.db().query("SELECT id,globalId FROM OperationRow WHERE value=44").at(0);
    const auto id = std::get<int64_t>(existing.at("id"));
    const auto gid = std::get<std::string>(existing.at("globalId"));
    owner.db().execute("CREATE TRIGGER operation_upsert BEFORE INSERT ON OperationRow WHEN NEW.value=22 "
                       "BEGIN SELECT RAISE(ABORT,'native modification refused'); END");
    owner.db().execute("CREATE TRIGGER operation_update BEFORE UPDATE ON OperationRow WHEN NEW.value=22 "
                       "BEGIN SELECT RAISE(ABORT,'native modification refused'); END");
    owner.db().execute("CREATE TRIGGER operation_delete BEFORE DELETE ON OperationRow WHEN OLD.value=44 "
                       "BEGIN SELECT RAISE(ABORT,'native modification refused'); END");
    owner.begin_transaction(); owner.add(OperationRow{7});
    const std::array<const char*,3> messages{
        "Insert failed: native modification refused",
        "Update failed: native modification refused",
        "Delete failed: native modification refused"};
    for (size_t action=0; action<messages.size(); ++action) {
        bool original = false;
        try {
            owner.unit([&](lattice::database& writer) {
                owner.add(OperationRow{11});
                if (action==0) writer.insert("OperationRow", {{"globalId",gid},{"value",int64_t(22)}}, {"globalId"});
                else if (action==1) writer.update("OperationRow",id,{{"value",int64_t(22)}});
                else writer.remove("OperationRow",id);
            });
            FAIL() << "the actual modification trigger must refuse action " << action;
        } catch (const lattice::db_error& error) {
            original = true;
            EXPECT_EQ(std::string(error.what()), messages[action]);
        }
        ASSERT_TRUE(original);
        ASSERT_TRUE(owner.db().is_in_transaction());
        EXPECT_EQ(values(), (std::vector<int64_t>{44,7}));
        EXPECT_TRUE(inserts.empty());
    }
    owner.commit();
    EXPECT_EQ(values(), (std::vector<int64_t>{44,7}));
    EXPECT_EQ(inserts.size(),1u);
}
} // namespace

#include "RecoveryAdmissionTestRead.hpp"
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <mutex>

namespace {
// A deadlock is a real failed process, not an unbounded future destructor.
// Elapsed time is only a failure bound; it is never a positive wait witness.
struct operation_wait_watchdog {
    std::mutex mutex;
    std::condition_variable cv;
    bool done=false;
    std::thread worker{[this]{std::unique_lock<std::mutex> lock(mutex);
        if(!cv.wait_for(lock,std::chrono::seconds(15),[&]{return done;}))std::abort();}};
    ~operation_wait_watchdog(){
        {std::lock_guard<std::mutex> lock(mutex);done=true;}
        cv.notify_one();worker.join();
    }
};
struct operation_wait_observer {
    std::function<void(lattice::database&)> callback;
    operation_wait_observer* previous;
    void (*prior)(lattice::database&);
    static inline thread_local operation_wait_observer* current=nullptr;
    explicit operation_wait_observer(std::function<void(lattice::database&)> value)
        :callback(std::move(value)),previous(current),
         prior(lattice::detail::recovery_admission_test_hooks::after_mutation_owned_read_wait_unlocked){
        current=this;
        lattice::detail::recovery_admission_test_hooks::after_mutation_owned_read_wait_unlocked=[](auto& db){current->callback(db);};
    }
    ~operation_wait_observer(){
        lattice::detail::recovery_admission_test_hooks::after_mutation_owned_read_wait_unlocked=prior;current=previous;
    }
};
// Same genuine query + post-ROW hook as RecoveryAdmissionTestRead.hpp, with
// the full owned result retained for a snapshot oracle. No synthetic reader,
// cursor registration, raw handle escape or alternate admission is created.
class operation_materialization_pause {
    std::promise<void> arrived_,release_;
    std::future<void> arrived=arrived_.get_future(),released=release_.get_future();
    bool released_once=false;
    std::thread worker;
public:
    bool paused=false,readonly=false,busy=false;
    sqlite3_stmt* actual_statement=nullptr; // comparison DATA only after release
    std::thread::id actual_thread;
    std::vector<lattice::database::row_t> rows;
    std::exception_ptr error;
    explicit operation_materialization_pause(lattice::database& db,std::string sql){
        worker=std::thread([this,&db,sql=std::move(sql)]{
            bool signaled=false;
            try{
                recovery_admission_test::row_probe probe([&](auto& actual,auto* statement){
                    if(&actual!=&db||signaled)return;
                    const auto* actual_sql=sqlite3_sql(statement);if(!actual_sql||sql!=actual_sql)return;
                    actual_statement=statement;actual_thread=std::this_thread::get_id();
                    readonly=sqlite3_stmt_readonly(statement)!=0;busy=sqlite3_stmt_busy(statement)!=0;paused=true;
                    signaled=true;arrived_.set_value();
                    if(released.wait_for(std::chrono::seconds(10))!=std::future_status::ready)
                        throw std::runtime_error("actual owned query pause exceeded control budget");
                });
                rows=db.query(sql);
            }catch(...){error=std::current_exception();}
            if(!signaled)arrived_.set_value();
        });
        if(arrived.wait_for(std::chrono::seconds(5))!=std::future_status::ready){
            finish();throw std::runtime_error("actual materialization did not reach a ROW");
        }
    }
    void release()noexcept{if(!released_once){released_once=true;release_.set_value();}}
    void finish()noexcept{release();if(worker.joinable())worker.join();}
    ~operation_materialization_pause(){finish();}
};
std::vector<int64_t> operation_snapshot_values(const std::vector<lattice::database::row_t>& rows){
    std::vector<int64_t> result;for(const auto& row:rows)result.push_back(std::get<int64_t>(row.at("value")));return result;
}
// Read and observer releases precede the writer future's destructor on every
// ASSERT/exception path. The watchdog remains live through every actual join.
struct operation_pause_cleanup {
    operation_materialization_pause& read;
    std::promise<void>& observer_release;
    bool released=false;
    void release_observer()noexcept{if(!released){released=true;observer_release.set_value();}}
    ~operation_pause_cleanup(){release_observer();read.finish();}
};
bool operation_statement_present(sqlite3* native,sqlite3_stmt* comparison){
    for(auto* actual=sqlite3_next_stmt(native,nullptr);actual;actual=sqlite3_next_stmt(native,actual))
        if(actual==comparison)return true;
    return false;
}

TEST_F(ManagedMutationOperation, OwnedReadSnapshotEndsBeforeConcurrentManagedUnitAdmits) {
    operation_wait_watchdog bounded;
    std::vector<int64_t> delivered;
    lattice::configuration config("file:operation-owned-read-wait?mode=memory&cache=shared");
    OperationOwner target(config);target.add_bulk(std::vector<OperationRow>{{7},{9}});
    const auto token=target.add_table_observer("OperationRow",[&](const auto& changes){
        for(const auto& change:changes)if(std::get<1>(change)=="INSERT")delivered.push_back(std::get<2>(change));
    });
    const auto store_gate=target.store_write_gate();ASSERT_TRUE(store_gate);
    auto& db=target.db();auto* native=lattice::detail::canonical_writer_custody_test_access::fault_handle(db);
    operation_materialization_pause read(db,"SELECT value FROM OperationRow ORDER BY id");
    ASSERT_TRUE(read.paused);ASSERT_TRUE(read.readonly);ASSERT_TRUE(read.busy);
    ASSERT_NE(read.actual_thread,std::this_thread::get_id());
    std::promise<void> observed_,release_observer_;
    auto observed=observed_.get_future(),released=release_observer_.get_future();
    std::atomic<unsigned> observations{0};std::atomic<bool> body{false},body_saw_read{false};
    auto writer=std::async(std::launch::async,[&]{
        operation_wait_observer observe([&](auto& actual){
            if(&actual!=&db)throw std::runtime_error("wrong actual mutation read-wait owner");
            if(observations.fetch_add(1)!=0)return;
            observed_.set_value();
            if(released.wait_for(std::chrono::seconds(10))!=std::future_status::ready)
                throw std::runtime_error("actual mutation wait observer exceeded control budget");
        });
        target.unit([&](lattice::database& actual){
            body.store(true);body_saw_read.store(operation_statement_present(native,read.actual_statement));
            // Genuine managed unit, ordinary hooks and actual Native SQL.
            actual.insert("OperationRow",{{"globalId",std::string("wait-after-read-11")},{"value",int64_t(11)}});
            actual.insert("OperationRow",{{"globalId",std::string("wait-after-read-33")},{"value",int64_t(33)}});
        });
    });
    operation_pause_cleanup cleanup{read,release_observer_};
    // Only the proposed actual classified, off-Native wait emitter can signal
    // this. A pre-unit promise, sleep, exception text or fake read cannot pass.
    const auto wait_observed=observed.wait_for(std::chrono::seconds(5));
    if(wait_observed!=std::future_status::ready){
        cleanup.release_observer();read.finish();
        ASSERT_EQ(writer.wait_for(std::chrono::seconds(5)),std::future_status::ready);
        try{writer.get();}catch(const std::exception& error){ADD_FAILURE()<<"actual writer refusal: "<<error.what();}
        catch(...){ADD_FAILURE()<<"actual writer failed with a nonstandard exception";}
        FAIL()<<"actual classified off-Native owned-read wait was not observed";
    }
    EXPECT_EQ(observations.load(),1u);EXPECT_FALSE(body.load());
    auto* mutex=sqlite3_db_mutex(native);ASSERT_NE(mutex,nullptr);
    const int entered=sqlite3_mutex_try(mutex);EXPECT_EQ(entered,SQLITE_OK);
    if(entered==SQLITE_OK){
        EXPECT_TRUE(operation_statement_present(native,read.actual_statement));
        EXPECT_TRUE(sqlite3_stmt_busy(read.actual_statement));
        EXPECT_TRUE(sqlite3_stmt_readonly(read.actual_statement));
        EXPECT_TRUE(sqlite3_get_autocommit(native));
        sqlite3_mutex_leave(mutex);
    }
    // This is the actual existing shared-memory store gate, not a test gate
    // or a replacement owner. Its real acquisition must be possible while
    // the writer is stopped at its actual off-all-gates wait boundary.
    const bool entered_store=store_gate->try_lock();EXPECT_TRUE(entered_store);
    if(entered_store)store_gate->unlock();
    // Return from the passive observer while the actual first-row cursor is
    // STILL paused. Production must now perform its real finite wait, rather
    // than relying on a blocked test callback to postpone the mutation.
    cleanup.release_observer();
    EXPECT_EQ(writer.wait_for(std::chrono::milliseconds(100)),std::future_status::timeout);
    EXPECT_FALSE(body.load());
    // The timeout above only exposes premature completion after an ACTUAL
    // classified wait was witnessed; it is not the wait-entry/pass authority.
    // The decisive body invariant below scans the actual Native statement
    // corpus, while this full old snapshot checks every materialized row.
    read.finish();ASSERT_FALSE(read.error);
    EXPECT_EQ(operation_snapshot_values(read.rows),(std::vector<int64_t>{7,9}));
    ASSERT_EQ(writer.wait_for(std::chrono::seconds(5)),std::future_status::ready);
    ASSERT_NO_THROW(writer.get());EXPECT_TRUE(body.load());EXPECT_FALSE(body_saw_read.load());
    EXPECT_FALSE(db.is_in_transaction());
    EXPECT_EQ(operation_snapshot_values(db.query("SELECT value FROM OperationRow ORDER BY id")),(std::vector<int64_t>{7,9,11,33}));
    EXPECT_EQ(delivered.size(),2u);target.handle_cross_process_notification();EXPECT_EQ(delivered.size(),2u);
    target.remove_table_observer("OperationRow",token);
}

TEST_F(ManagedMutationOperation, UnregisteredReadonlyBusyCursorNeverBecomesOwnedReadWait) {
    operation_wait_watchdog bounded;
    owner.add_bulk(std::vector<OperationRow>{{7},{9}});inserts.clear();
    auto* native=lattice::detail::canonical_writer_custody_test_access::fault_handle(owner.db());
    unsigned waits=0;operation_wait_observer observer([&](auto&){++waits;});
    for(const auto* sql:{"SELECT 1 AS value","SELECT value FROM OperationRow ORDER BY id"}){
        sqlite3_stmt* raw=nullptr;
        ASSERT_EQ(sqlite3_prepare_v2(native,sql,-1,&raw,nullptr),SQLITE_OK);
        struct ending{sqlite3_stmt* value;~ending(){sqlite3_finalize(value);}} original{raw};
        ASSERT_EQ(sqlite3_step(raw),SQLITE_ROW);ASSERT_TRUE(sqlite3_stmt_readonly(raw));ASSERT_TRUE(sqlite3_stmt_busy(raw));
        // The fault_handle seam does not set raw_handle_escaped_: actual
        // missing engine registration must itself be enough to refuse.
        EXPECT_THROW(owner.add_bulk(std::vector<OperationRow>{{11}}),lattice::db_error);
        EXPECT_EQ(waits,0u);EXPECT_TRUE(inserts.empty());EXPECT_TRUE(sqlite3_get_autocommit(native));
    }
    EXPECT_EQ(values(),(std::vector<int64_t>{7,9}));
}

TEST_F(ManagedMutationOperation, ActualWriteReturningRowNeverBecomesOwnedReadonlyWait) {
    operation_wait_watchdog bounded;
    owner.add_bulk(std::vector<OperationRow>{{7},{9}});inserts.clear();
    operation_materialization_pause writing(owner.db(),"UPDATE OperationRow SET value=value+100 RETURNING value");
    ASSERT_TRUE(writing.paused);ASSERT_TRUE(writing.busy);ASSERT_FALSE(writing.readonly);
    unsigned waits=0;operation_wait_observer observer([&](auto&){++waits;});
    EXPECT_THROW(owner.add_bulk(std::vector<OperationRow>{{11}}),lattice::db_error);
    EXPECT_EQ(waits,0u);EXPECT_TRUE(inserts.empty());writing.finish();ASSERT_FALSE(writing.error);
    EXPECT_EQ(values(),(std::vector<int64_t>{107,109}));
}

TEST_F(ManagedMutationOperation, SameThreadActualReadRowReentryCannotAcquireWaitOutcome) {
    operation_wait_watchdog bounded;
    owner.add_bulk(std::vector<OperationRow>{{7},{9}});inserts.clear();
    unsigned waits=0,attempts=0;operation_wait_observer observer([&](auto&){++waits;});
    recovery_admission_test::row_probe probe([&](auto& actual,auto* statement){
        if(&actual!=&owner.db()||attempts)return;
        const auto* sql=sqlite3_sql(statement);if(!sql||std::strcmp(sql,"SELECT value FROM OperationRow ORDER BY id"))return;
        ++attempts;EXPECT_THROW(owner.add_bulk(std::vector<OperationRow>{{11}}),lattice::db_error);
    });
    const auto snapshot=owner.db().query("SELECT value FROM OperationRow ORDER BY id");
    EXPECT_EQ(attempts,1u);EXPECT_EQ(waits,0u);EXPECT_EQ(operation_snapshot_values(snapshot),(std::vector<int64_t>{7,9}));
    EXPECT_TRUE(inserts.empty());EXPECT_FALSE(owner.db().is_in_transaction());
}
} // namespace

namespace {
TEST_F(ManagedMutationOperation, ActualSQLiteCallbackCannotAcquireOwnedReadWaitOutcome) {
    operation_wait_watchdog bounded;
    owner.add_bulk(std::vector<OperationRow>{{7},{9}});inserts.clear();
    auto* native=lattice::detail::canonical_writer_custody_test_access::fault_handle(owner.db());
    struct context {
        OperationOwner& owner;unsigned calls=0;bool refused=false;std::exception_ptr unexpected;
        static void call(sqlite3_context* result,int,sqlite3_value**)noexcept{
            auto& actual=*static_cast<context*>(sqlite3_user_data(result));++actual.calls;
            try{actual.owner.add_bulk(std::vector<OperationRow>{{11}});}
            catch(const lattice::db_error&){actual.refused=true;}
            catch(...){actual.unexpected=std::current_exception();}
            sqlite3_result_int(result,1);
        }
    } callback{owner};
    unsigned waits=0;operation_wait_observer observer([&](auto&){++waits;});
    ASSERT_EQ(sqlite3_create_function_v2(native,"operation_reentry_control",0,SQLITE_UTF8,&callback,context::call,nullptr,nullptr,nullptr),SQLITE_OK);
    struct ending {
        sqlite3* native;
        ~ending(){if(sqlite3_create_function_v2(native,"operation_reentry_control",0,SQLITE_UTF8,nullptr,nullptr,nullptr,nullptr,nullptr)!=SQLITE_OK)
            ADD_FAILURE()<<"actual SQLite control function ending failed";}
    } registered{native};
    const auto rows=owner.db().query("SELECT operation_reentry_control() AS value");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<int64_t>(rows[0].at("value")),1);
    EXPECT_EQ(callback.calls,1u);EXPECT_TRUE(callback.refused);EXPECT_FALSE(callback.unexpected);EXPECT_EQ(waits,0u);
    EXPECT_EQ(values(),(std::vector<int64_t>{7,9}));EXPECT_TRUE(inserts.empty());EXPECT_FALSE(owner.db().is_in_transaction());
}
} // namespace

namespace {
TEST_F(ManagedMutationOperation, OwnedReadAdmissionExhaustsOneFiniteConfiguredBudgetWithoutMutation) {
    operation_wait_watchdog bounded;
    // A short positive timeout must reduce the cap; a long one must not
    // exceed the authorized two-second admission cap. These are bounded
    // failure completions, not elapsed-time evidence of read ownership.
    for(const auto configured:{80,30000}){
        std::vector<int64_t> delivered;
        lattice::configuration config(":memory:");config.busy_timeout_ms=configured;
        OperationOwner target(config);target.add_bulk(std::vector<OperationRow>{{7},{9}});
        const auto token=target.add_table_observer("OperationRow",[&](const auto& changes){
            for(const auto& change:changes)if(std::get<1>(change)=="INSERT")delivered.push_back(std::get<2>(change));
        });
        auto& db=target.db();auto* native=lattice::detail::canonical_writer_custody_test_access::fault_handle(db);
        operation_materialization_pause read(db,"SELECT value FROM OperationRow ORDER BY id");
        ASSERT_TRUE(read.paused);ASSERT_TRUE(read.readonly);ASSERT_TRUE(read.busy);
        std::promise<void> observed_;auto observed=observed_.get_future();
        std::atomic<unsigned> waits{0};std::atomic<bool> body{false};
        auto writer=std::async(std::launch::async,[&]()->std::exception_ptr{
            operation_wait_observer observation([&](auto& actual){
                if(&actual!=&db)throw std::runtime_error("wrong actual budget-wait owner");
                if(waits.fetch_add(1)==0)observed_.set_value();
            });
            try{target.unit([&](lattice::database& actual){body.store(true);
                actual.insert("OperationRow",{{"globalId",std::string("must-not-enter-expired-unit")},{"value",int64_t(11)}});
            });return {};}
            catch(...){return std::current_exception();}
        });
        struct cleanup{operation_materialization_pause& read;~cleanup(){read.finish();}} release_before_writer_join{read};
        ASSERT_EQ(observed.wait_for(std::chrono::seconds(5)),std::future_status::ready);
        const auto completion_bound=configured==80?std::chrono::milliseconds(600):std::chrono::milliseconds(3500);
        ASSERT_EQ(writer.wait_for(completion_bound),std::future_status::ready);
        const auto first=writer.get();ASSERT_TRUE(first);EXPECT_THROW(std::rethrow_exception(first),lattice::db_error);
        EXPECT_GT(waits.load(),0u);EXPECT_FALSE(body.load());EXPECT_TRUE(delivered.empty());
        // The actual earlier query is still paused and usable after expiry;
        // neither its statement nor transaction was borrowed/ended for a write.
        auto* mutex=sqlite3_db_mutex(native);ASSERT_NE(mutex,nullptr);const int entered=sqlite3_mutex_try(mutex);EXPECT_EQ(entered,SQLITE_OK);
        if(entered==SQLITE_OK){EXPECT_TRUE(operation_statement_present(native,read.actual_statement));
            EXPECT_TRUE(sqlite3_stmt_busy(read.actual_statement));EXPECT_TRUE(sqlite3_get_autocommit(native));sqlite3_mutex_leave(mutex);}
        read.finish();ASSERT_FALSE(read.error);EXPECT_EQ(operation_snapshot_values(read.rows),(std::vector<int64_t>{7,9}));
        EXPECT_FALSE(db.is_in_transaction());
        EXPECT_EQ(operation_snapshot_values(db.query("SELECT value FROM OperationRow ORDER BY id")),(std::vector<int64_t>{7,9}));
        target.add_bulk(std::vector<OperationRow>{{33}});
        EXPECT_EQ(operation_snapshot_values(db.query("SELECT value FROM OperationRow ORDER BY id")),(std::vector<int64_t>{7,9,33}));
        EXPECT_EQ(delivered.size(),1u);target.remove_table_observer("OperationRow",token);
    }
}

TEST_F(ManagedMutationOperation, ExplicitZeroBudgetRefusesOwnedReadImmediatelyWithoutWaitOutcome) {
    operation_wait_watchdog bounded;
    lattice::configuration config(":memory:");config.busy_timeout_ms=0;
    OperationOwner target(config);target.add_bulk(std::vector<OperationRow>{{7},{9}});
    operation_materialization_pause read(target.db(),"SELECT value FROM OperationRow ORDER BY id");
    ASSERT_TRUE(read.paused);ASSERT_TRUE(read.readonly);ASSERT_TRUE(read.busy);
    unsigned waits=0;bool body=false;operation_wait_observer observer([&](auto&){++waits;});
    EXPECT_THROW(target.unit([&](lattice::database& actual){body=true;
        actual.insert("OperationRow",{{"globalId",std::string("must-not-enter-zero-unit")},{"value",int64_t(11)}});
    }),lattice::db_error);
    EXPECT_EQ(waits,0u);EXPECT_FALSE(body);read.finish();ASSERT_FALSE(read.error);
    EXPECT_EQ(operation_snapshot_values(read.rows),(std::vector<int64_t>{7,9}));EXPECT_FALSE(target.db().is_in_transaction());
    EXPECT_EQ(operation_snapshot_values(target.db().query("SELECT value FROM OperationRow ORDER BY id")),(std::vector<int64_t>{7,9}));
    target.add_bulk(std::vector<OperationRow>{{33}});
    EXPECT_EQ(operation_snapshot_values(target.db().query("SELECT value FROM OperationRow ORDER BY id")),(std::vector<int64_t>{7,9,33}));
}
} // namespace
