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
