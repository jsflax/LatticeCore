#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"
#include "../../Sources/LatticeCore/src/receive_ledger.hpp"
#include <chrono>

namespace lattice::detail {
struct recovery_install_test_access {
    static recovery_install_result with_tail(std::shared_ptr<lattice_db> owner,
        const std::function<void(database&)>& body, const std::function<void()>& tail) {
        return recovery_writer_access::install_impl(std::move(owner), body, tail);
    }
};
}
namespace {
using access = lattice::detail::recovery_writer_access;
using test_access = lattice::detail::recovery_install_test_access;
using state = lattice::detail::recovery_install_state;

// A deadlock is an explicit process failure, not an unbounded test hang.
// The positive assertions use real completion/order, never elapsed time.
struct watchdog {
    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    std::thread thread{[this] {
        std::unique_lock<std::mutex> lock(mutex);
        if (!cv.wait_for(lock, std::chrono::seconds(15), [&] { return done; })) std::abort();
    }};
    ~watchdog() {
        { std::lock_guard<std::mutex> lock(mutex); done = true; }
        cv.notify_one();
        thread.join();
    }
};

std::shared_ptr<lattice::lattice_db> store(const std::string& path = ":memory:") {
    lattice::configuration config(path);
    config.audit_retention_seconds = 0;
    config.busy_timeout_ms = 100;
    auto owner = std::make_shared<lattice::lattice_db>(config);
    owner->add(TestPerson{"seed", 1, std::nullopt});
    if (!config.is_in_memory()) {
        auto* notifier = lattice::instance_registry::instance().get_or_create_notifier(path);
        if (notifier) notifier->stop_listening();
    }
    return owner;
}
void insert(lattice::database& writer, const std::string& id) {
    writer.execute("INSERT INTO TestPerson(globalId,name,age) VALUES(?,?,2)", {id, id});
}
int64_t count(lattice::database& writer, const std::string& id) {
    return std::get<int64_t>(writer.query("SELECT COUNT(*) AS n FROM TestPerson WHERE globalId=?", {id}).at(0).at("n"));
}
struct observed {
    std::shared_ptr<lattice::lattice_db> owner;
    lattice::lattice_db::observer_id token;
    std::vector<std::vector<std::string>> batches;
    explicit observed(std::shared_ptr<lattice::lattice_db> value) : owner(std::move(value)) {
        token = owner->add_table_observer("TestPerson", [this](const auto& changes) {
            std::vector<std::string> ids;
            for (const auto& change : changes) ids.push_back(std::get<3>(change));
            batches.push_back(std::move(ids));
        });
    }
    ~observed() { owner->remove_table_observer("TestPerson", token); }
};
void rollback_case(const std::string& path) {
    auto owner = store(path);
    observed observer(owner);
    auto result = access::install(owner, [&](auto& writer) {
        insert(writer, "rolled-back");
        throw std::runtime_error("body failure");
    });
    EXPECT_EQ(result.state, state::rolled_back);
    EXPECT_NE(result.primary_error, nullptr);
    EXPECT_EQ(result.cleanup_error, nullptr);
    EXPECT_EQ(count(owner->db(), "rolled-back"), 0);
    EXPECT_TRUE(observer.batches.empty());
    insert(owner->db(), "ordinary-next");
    ASSERT_EQ(observer.batches.size(), 1u);
    EXPECT_EQ(observer.batches[0], (std::vector<std::string>{"ordinary-next"}));
}
}

TEST(RecoveryInstallTransaction, ExplicitAndRawTransactionsAreRefusedWithoutJoining) {
    auto owner = store();
    bool called = false;
    owner->begin_transaction();
    auto first = access::install(owner, [&](auto&) { called = true; });
    EXPECT_EQ(first.state, state::refused);
    EXPECT_TRUE(owner->db().is_in_transaction());
    owner->rollback();
    owner->db().execute("BEGIN IMMEDIATE");
    EXPECT_EQ(access::active_writer(*owner), nullptr);
    auto second = access::install(owner, [&](auto&) { called = true; });
    EXPECT_EQ(second.state, state::refused);
    EXPECT_TRUE(owner->db().is_in_transaction());
    EXPECT_FALSE(called);
    owner->db().rollback();
}

TEST(RecoveryInstallTransaction, PrivateAuthorityWorksForRealLedgerAndEndsBeforeTail) {
    watchdog bounded;
    auto owner = store();
    lattice::detail::receive_ledger ledger(*owner, {4, 128, 16, 1024, 8, 512, 64});
    EXPECT_THROW(ledger.initialize(), lattice::detail::receive_ledger_error);
    auto result = test_access::with_tail(owner, [&](auto& writer) {
        EXPECT_EQ(access::active_writer(*owner), &writer);
        ledger.initialize();
        std::thread other([&] { EXPECT_EQ(access::active_writer(*owner), nullptr); });
        other.join();
    }, [&] { EXPECT_EQ(access::active_writer(*owner), nullptr); });
    EXPECT_EQ(result.state, state::committed);
    EXPECT_EQ(result.primary_error, nullptr);
    EXPECT_EQ(result.postcommit_error, nullptr);
    EXPECT_THROW(ledger.audit(), lattice::detail::receive_ledger_error);
    EXPECT_EQ(access::install(owner, [&](auto&) { ledger.audit(); }).state, state::committed);
}

TEST(RecoveryInstallTransaction, EmptyCommitInvalidatesExactlyOnceFileAndMemory) {
    TempDB file{"recovery_empty"};
    for (const auto& path : {std::string(":memory:"), file.str()}) {
        auto owner = store(path);
        int commits = 0;
        auto token = owner->add_invalidation_hook([&](const auto& tables, auto reason) {
            if (reason == lattice::lattice_db::invalidation_reason::commit) {
                ++commits;
                EXPECT_TRUE(tables.empty());
            }
        });
        auto result = access::install(owner, [](auto&) {});
        EXPECT_EQ(result.state, state::committed);
        EXPECT_EQ(commits, 1);
        owner->remove_invalidation_hook(token);
    }
}

TEST(RecoveryInstallTransaction, BodyFailureRollsBackMemoryWithoutPhantomDelivery) { rollback_case(":memory:"); }
TEST(RecoveryInstallTransaction, BodyFailureRollsBackFileWithoutPhantomDelivery) {
    TempDB file{"recovery_rollback"};
    rollback_case(file.str());
}

namespace {
void escaped_statement_case(const std::string& sql) {
    TempDB file{"recovery_escaped_statement"};
    for (const auto& path : {std::string(":memory:"), file.str()}) {
        auto owner = store(path);
        observed observer(owner);
        auto* handle = owner->db().handle();
        sqlite3_stmt* escaped = nullptr;
        const auto result = access::install(owner, [&](auto& writer) {
            insert(writer, "before-escaped-statement");
            ASSERT_EQ(sqlite3_prepare_v2(handle, sql.c_str(), -1, &escaped, nullptr), SQLITE_OK);
            ASSERT_EQ(sqlite3_step(escaped), SQLITE_ROW);
            ASSERT_TRUE(sqlite3_stmt_busy(escaped));
        });
        // The installer must refuse instead of consuming/resetting the caller's
        // statement to manufacture an idle connection. Cleanup belongs here.
        EXPECT_EQ(result.state, state::rolled_back);
        EXPECT_NE(result.primary_error, nullptr);
        EXPECT_EQ(result.cleanup_error, nullptr);
        EXPECT_EQ(result.postcommit_error, nullptr);
        EXPECT_TRUE(observer.batches.empty());
        sqlite3_finalize(escaped);
        EXPECT_EQ(count(owner->db(), "before-escaped-statement"), 0);
        EXPECT_EQ(count(owner->db(), "escaped-write"), 0);
        EXPECT_EQ(access::install(owner, [](auto& writer) { insert(writer, "valid-retry"); }).state,
                  state::committed);
        ASSERT_EQ(observer.batches.size(), 1u);
        EXPECT_EQ(observer.batches[0], (std::vector<std::string>{"valid-retry"}));
    }
}
}

TEST(RecoveryInstallTransaction, EscapedReadStatementStillRefusesWithoutPhantomDelivery) {
    escaped_statement_case("SELECT id FROM TestPerson ORDER BY id");
}

TEST(RecoveryInstallTransaction, EscapedReturningWriteStillRefusesWithoutPhantomDelivery) {
    escaped_statement_case("INSERT INTO TestPerson(globalId,name,age) "
                           "VALUES('escaped-write','escaped-write',3) RETURNING id");
}

TEST(RecoveryInstallTransaction, CallerBlobIsNotExemptedAsAnInternalCursor) {
    TempDB file{"recovery_escaped_blob"};
    for (const auto& path : {std::string(":memory:"), file.str()}) {
        auto owner = store(path);
        owner->db().execute("CREATE TABLE _CallerBlob(id INTEGER PRIMARY KEY,data BLOB)");
        owner->db().execute("INSERT INTO _CallerBlob VALUES(1,zeroblob(8))");
        observed observer(owner);
        auto* handle = owner->db().handle();
        sqlite3_blob* escaped = nullptr;
        const auto result = access::install(owner, [&](auto& writer) {
            insert(writer, "before-escaped-blob");
            ASSERT_EQ(sqlite3_blob_open(handle, "main", "_CallerBlob", "data", 1, 0, &escaped), SQLITE_OK);
        });
        EXPECT_EQ(result.state, state::rolled_back);
        EXPECT_NE(result.primary_error, nullptr);
        EXPECT_EQ(result.cleanup_error, nullptr);
        EXPECT_EQ(result.postcommit_error, nullptr);
        EXPECT_TRUE(observer.batches.empty());
        sqlite3_blob_close(escaped);
        EXPECT_EQ(count(owner->db(), "before-escaped-blob"), 0);
        EXPECT_EQ(access::install(owner, [](auto& writer) { insert(writer, "valid-retry"); }).state,
                  state::committed);
        ASSERT_EQ(observer.batches.size(), 1u);
        EXPECT_EQ(observer.batches[0], (std::vector<std::string>{"valid-retry"}));
    }
}

TEST(RecoveryInstallTransaction, VirtualTableSettlementBoundaryFailureRollsBackWholeInstall) {
    for (const auto* denied : {"BEGIN", "RELEASE"}) {
        TempDB file{"recovery_settlement_denied"};
        for (const auto& path : {std::string(":memory:"), file.str()}) {
            auto owner = store(path);
            observed observer(owner);
            auto* handle = owner->db().handle();
            struct fault_state { const char* denied; bool armed = false; int hits = 0; } fault{denied};
            ASSERT_EQ(sqlite3_set_authorizer(handle,
                [](void* raw, int action, const char* first, const char*, const char*, const char*) noexcept {
                    auto& fault = *static_cast<fault_state*>(raw);
                    if (fault.armed && action == SQLITE_SAVEPOINT && first &&
                        std::strcmp(first, fault.denied) == 0) {
                        ++fault.hits;
                        return SQLITE_DENY;
                    }
                    return SQLITE_OK;
                }, &fault), SQLITE_OK);
            const auto result = access::install(owner, [&](auto& writer) {
                insert(writer, "before-settlement-refusal");
                fault.armed = true;
            });
            ASSERT_EQ(sqlite3_set_authorizer(handle, nullptr, nullptr), SQLITE_OK);
            EXPECT_EQ(fault.hits, 1);
            EXPECT_EQ(result.state, state::rolled_back);
            EXPECT_NE(result.primary_error, nullptr);
            EXPECT_EQ(result.cleanup_error, nullptr);
            EXPECT_EQ(result.postcommit_error, nullptr);
            EXPECT_EQ(count(owner->db(), "before-settlement-refusal"), 0);
            EXPECT_TRUE(observer.batches.empty());
            EXPECT_EQ(access::install(owner, [](auto& writer) { insert(writer, "valid-retry"); }).state,
                      state::committed);
            ASSERT_EQ(observer.batches.size(), 1u);
            EXPECT_EQ(observer.batches[0], (std::vector<std::string>{"valid-retry"}));
        }
    }
}

TEST(RecoveryInstallTransaction, ObserverThrowPreservesDurableCommitOnBothStorageKinds) {
    TempDB file{"recovery_observer_error"};
    for (const auto& path : {std::string(":memory:"), file.str()}) {
        auto owner = store(path);
        auto token = owner->add_table_observer("TestPerson", [](const auto&) { throw std::runtime_error("observer"); });
        auto result = access::install(owner, [](auto& writer) { insert(writer, "committed"); });
        EXPECT_EQ(result.state, state::committed);
        EXPECT_EQ(result.primary_error, nullptr);
        EXPECT_NE(result.postcommit_error, nullptr);
        EXPECT_EQ(count(owner->db(), "committed"), 1);
        EXPECT_FALSE(owner->db().is_in_transaction());
        owner->remove_table_observer("TestPerson", token);
    }
}

TEST(RecoveryInstallTransaction, ThrowingObserverSuccessorTransactionIsNeverRolledBack) {
    auto owner = store();
    auto token = owner->add_table_observer("TestPerson", [&](const auto&) {
        EXPECT_EQ(access::active_writer(*owner), nullptr);
        owner->begin_transaction();
        insert(owner->db(), "successor");
        throw std::runtime_error("after successor BEGIN");
    });
    auto result = access::install(owner, [](auto& writer) { insert(writer, "original"); });
    EXPECT_EQ(result.state, state::committed);
    EXPECT_NE(result.postcommit_error, nullptr);
    EXPECT_TRUE(owner->db().is_in_transaction());
    EXPECT_EQ(count(owner->db(), "original"), 1);
    EXPECT_EQ(count(owner->db(), "successor"), 1);
    owner->remove_table_observer("TestPerson", token);
    owner->rollback();
    EXPECT_EQ(count(owner->db(), "original"), 1);
    EXPECT_EQ(count(owner->db(), "successor"), 0);
}

TEST(RecoveryInstallTransaction, CloseBeforeAdmissionRefusesBody) {
    auto owner = store();
    owner->close();
    bool called = false;
    auto result = access::install(owner, [&](auto&) { called = true; });
    EXPECT_EQ(result.state, state::refused);
    EXPECT_NE(result.primary_error, nullptr);
    EXPECT_FALSE(called);
}

TEST(RecoveryInstallTransaction, CloseAfterAdmissionRetainsPhysicalWriterAndCommits) {
    TempDB file{"recovery_close_admitted"};
    auto owner = store(file.str());
    int calls = 0;
    auto token = owner->add_table_observer("TestPerson", [&](const auto&) { ++calls; });
    auto result = access::install(owner, [&](auto& writer) {
        owner->close();
        EXPECT_EQ(access::active_writer(*owner), &writer);
        insert(writer, "after-close");
    });
    EXPECT_EQ(result.state, state::committed);
    EXPECT_EQ(result.primary_error, nullptr);
    EXPECT_EQ(result.postcommit_error, nullptr);
    EXPECT_EQ(calls, 0);
    lattice::database verifier(file.str());
    EXPECT_EQ(count(verifier, "after-close"), 1);
    owner->remove_table_observer("TestPerson", token);
}

TEST(RecoveryInstallTransaction, ActualOwnerSurvivesLastExternalReferenceAndObserverClose) {
    auto owner = store();
    std::weak_ptr<lattice::lattice_db> weak = owner;
    int calls = 0;
    owner->add_table_observer("TestPerson", [&](const auto&) {
        auto retained = weak.lock();
        ASSERT_NE(retained, nullptr);
        owner.reset();
        retained->close();
        ++calls;
        EXPECT_FALSE(weak.expired());
    });
    auto result = access::install(owner, [](auto& writer) { insert(writer, "lifetime"); });
    EXPECT_EQ(result.state, state::committed);
    EXPECT_EQ(result.postcommit_error, nullptr);
    EXPECT_EQ(calls, 1);
    EXPECT_TRUE(weak.expired());
}

TEST(RecoveryInstallTransaction, LaterWriterCannotStealOrMergeReservedBatch) {
    watchdog bounded;
    // The rendezvous occurs only after every owned lock and private authority
    // has ended. Joining a second real writer here is also a positive lock
    // release oracle, rather than a timing-based absence assertion.
    TempDB file{"recovery_later_writer"};
    for (const auto& path : {std::string(":memory:"), file.str()}) {
        auto owner = store(path);
        observed observer(owner);
        auto result = test_access::with_tail(owner, [](auto& writer) {
            insert(writer, "first-a");
            insert(writer, "first-b");
        }, [&] {
            ASSERT_TRUE(observer.batches.empty());
            EXPECT_EQ(access::active_writer(*owner), nullptr);
            std::exception_ptr later_error;
            std::thread later([&] { try { insert(owner->db(), "later"); } catch (...) { later_error = std::current_exception(); } });
            later.join();
            ASSERT_EQ(later_error, nullptr);
            ASSERT_EQ(observer.batches.size(), 1u);
            EXPECT_EQ(observer.batches[0], (std::vector<std::string>{"later"}));
        });
        EXPECT_EQ(result.state, state::committed);
        EXPECT_EQ(result.postcommit_error, nullptr);
        ASSERT_EQ(observer.batches.size(), 2u);
        EXPECT_EQ(observer.batches[1], (std::vector<std::string>{"first-a", "first-b"}));
    }
}

TEST(RecoveryInstallTransaction, SQLRollbackSettlesOriginalAndSuppressesItsBatch) {
    auto owner = store();
    observed observer(owner);
    owner->db().execute("CREATE TRIGGER fail_install BEFORE INSERT ON TestPerson WHEN NEW.name='bad' "
        "BEGIN SELECT RAISE(ROLLBACK, 'install rejected'); END");
    auto result = access::install(owner, [&](auto& writer) {
        insert(writer, "before-bad");
        insert(writer, "bad");
    });
    EXPECT_EQ(result.state, state::rolled_back);
    EXPECT_NE(result.primary_error, nullptr);
    EXPECT_EQ(count(owner->db(), "before-bad"), 0);
    EXPECT_TRUE(observer.batches.empty());
}

TEST(RecoveryInstallTransaction, OrdinaryFlushCannotConsumeReservedRowsBeforeCommit) {
    auto owner = store();
    observed observer(owner);
    auto result = access::install(owner, [&](auto& writer) {
        insert(writer, "reserved");
        EXPECT_FALSE(owner->flush_changes());
        EXPECT_TRUE(observer.batches.empty());
    });
    EXPECT_EQ(result.state, state::committed);
    ASSERT_EQ(observer.batches.size(), 1u);
    EXPECT_EQ(observer.batches[0], (std::vector<std::string>{"reserved"}));
}

TEST(RecoveryInstallTransaction, FailedRollbackReportsUnsettledAndQuarantinesWriter) {
    auto owner = store();
    auto* handle = owner->db().handle();
    ASSERT_EQ(sqlite3_set_authorizer(handle,
        [](void*, int action, const char* first, const char*, const char*, const char*) noexcept {
            return action == SQLITE_TRANSACTION && first && std::strcmp(first, "ROLLBACK") == 0
                ? SQLITE_DENY : SQLITE_OK;
        }, nullptr), SQLITE_OK);
    auto result = access::install(owner, [](auto& writer) {
        insert(writer, "unsettled");
        throw std::runtime_error("body error before denied cleanup");
    });
    EXPECT_EQ(result.state, state::unsettled);
    EXPECT_NE(result.primary_error, nullptr);
    EXPECT_NE(result.cleanup_error, nullptr);
    EXPECT_EQ(sqlite3_get_autocommit(handle), 0);
    EXPECT_TRUE(owner->db().is_closed());
    EXPECT_EQ(access::install(owner, [](auto&) {}).state, state::refused);
    // Test owns the raw fixture handle; repair only to leave no active txn.
    ASSERT_EQ(sqlite3_set_authorizer(handle, nullptr, nullptr), SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(handle, "ROLLBACK", nullptr, nullptr, nullptr), SQLITE_OK);
}

TEST(RecoveryInstallTransaction, RecursiveWriterRetirementCannotRetargetAdmittedHook) {
    auto owner = store();
    auto result = access::install(owner, [&](auto& writer) {
        EXPECT_THROW(owner->close_write_db(), lattice::db_error);
        EXPECT_THROW(owner->reopen_write_db(), lattice::db_error);
        EXPECT_EQ(access::active_writer(*owner), &writer);
        insert(writer, "still-original");
    });
    EXPECT_EQ(result.state, state::committed);
    EXPECT_EQ(result.primary_error, nullptr);
    EXPECT_EQ(count(owner->db(), "still-original"), 1);
}

TEST(RecoveryInstallTransaction, CommitRefusalAfterPreparedBatchRollsBackAndReleasesReservation) {
    TempDB file{"recovery_commit_refusal"};
    for (const auto& path : {std::string(":memory:"), file.str()}) {
        auto owner = store(path);
        observed observer(owner);
        int commit_attempts = 0, commit_invalidations = 0;
        const auto invalidation = owner->add_invalidation_hook([&](const auto&, auto reason) {
            if (reason == lattice::lattice_db::invalidation_reason::commit) ++commit_invalidations;
        });
        auto* handle = owner->db().handle();
        ASSERT_EQ(sqlite3_set_authorizer(handle,
            [](void* raw, int action, const char* first, const char*, const char*, const char*) noexcept {
                if (action == SQLITE_TRANSACTION && first && std::strcmp(first, "COMMIT") == 0) {
                    ++*static_cast<int*>(raw);
                    return SQLITE_DENY;
                }
                return SQLITE_OK;
            }, &commit_attempts), SQLITE_OK);
        // The production COMMIT follows the successful immutable-batch
        // preparation. Deny that exact boundary, after the body returned.
        const auto result = access::install(owner, [](auto& writer) { insert(writer, "denied-commit"); });
        ASSERT_EQ(sqlite3_set_authorizer(handle, nullptr, nullptr), SQLITE_OK);
        EXPECT_EQ(commit_attempts, 1);
        EXPECT_EQ(result.state, state::rolled_back);
        EXPECT_NE(result.primary_error, nullptr);
        EXPECT_EQ(result.cleanup_error, nullptr);
        EXPECT_EQ(result.postcommit_error, nullptr);
        EXPECT_EQ(sqlite3_get_autocommit(handle), 1);
        EXPECT_FALSE(owner->db().is_closed());
        EXPECT_EQ(count(owner->db(), "denied-commit"), 0);
        EXPECT_TRUE(observer.batches.empty());
        EXPECT_EQ(commit_invalidations, 0);
        insert(owner->db(), "after-denied-commit");
        ASSERT_EQ(observer.batches.size(), 1u);
        EXPECT_EQ(observer.batches[0], (std::vector<std::string>{"after-denied-commit"}));
        EXPECT_EQ(commit_invalidations, 1);
        owner->remove_invalidation_hook(invalidation);
    }
}

TEST(RecoveryInstallTransaction, PreparedBatchMetadataFailureRollsBackAndReleasesReservation) {
    TempDB file{"recovery_batch_read_refusal"};
    for (const auto& path : {std::string(":memory:"), file.str()}) {
        auto owner = store(path);
        observed observer(owner);
        struct fault_state { bool armed = false; int hits = 0; } fault;
        int commit_invalidations = 0;
        const auto invalidation = owner->add_invalidation_hook([&](const auto&, auto reason) {
            if (reason == lattice::lattice_db::invalidation_reason::commit) ++commit_invalidations;
        });
        auto* handle = owner->db().handle();
        ASSERT_EQ(sqlite3_set_authorizer(handle,
            [](void* raw, int action, const char* table, const char* column, const char*, const char*) noexcept {
                auto& fault = *static_cast<fault_state*>(raw);
                if (fault.armed && action == SQLITE_READ && table && column &&
                    std::strcmp(table, "_lattice_meta") == 0 && std::strcmp(column, "value") == 0) {
                    ++fault.hits;
                    return SQLITE_DENY;
                }
                return SQLITE_OK;
            }, &fault), SQLITE_OK);
        bool body_finished = false;
        const auto result = access::install(owner, [&](auto& writer) {
            insert(writer, "prepared-read-failure");
            EXPECT_EQ(count(writer, "prepared-read-failure"), 1);
            body_finished = true;
            // Arm only after the model effect and body validation succeeded.
            // The next targeted metadata read belongs to batch preparation.
            fault.armed = true;
        });
        ASSERT_EQ(sqlite3_set_authorizer(handle, nullptr, nullptr), SQLITE_OK);
        EXPECT_TRUE(body_finished);
        EXPECT_EQ(fault.hits, 1);
        EXPECT_EQ(result.state, state::rolled_back);
        EXPECT_NE(result.primary_error, nullptr);
        EXPECT_EQ(result.cleanup_error, nullptr);
        EXPECT_EQ(result.postcommit_error, nullptr);
        EXPECT_EQ(sqlite3_get_autocommit(handle), 1);
        EXPECT_FALSE(owner->db().is_closed());
        EXPECT_EQ(count(owner->db(), "prepared-read-failure"), 0);
        EXPECT_TRUE(observer.batches.empty());
        EXPECT_EQ(commit_invalidations, 0);
        insert(owner->db(), "after-preparation-failure");
        ASSERT_EQ(observer.batches.size(), 1u);
        EXPECT_EQ(observer.batches[0], (std::vector<std::string>{"after-preparation-failure"}));
        EXPECT_EQ(commit_invalidations, 1);
        owner->remove_invalidation_hook(invalidation);
    }
}
