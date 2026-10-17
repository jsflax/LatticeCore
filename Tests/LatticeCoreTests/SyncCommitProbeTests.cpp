#if defined(LATTICE_SYNC_COMMIT_PROBE)
#include "TestHelpers.hpp"
#include <lattice.hpp>
#include <lattice/sync_commit_probe.hpp>
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include <exception>
#include <stdexcept>
#include <utility>

struct ProbeProducerRow { std::string value; };
LATTICE_SCHEMA(ProbeProducerRow, value);

namespace {
namespace probe = lattice::sync_commit_probe_detail;
using Receipt = lattice::sync_commit_probe_receipt;

lattice::configuration probe_config(const std::string& path) {
    lattice::configuration config;
    config.path = path;
    config.audit_retention_seconds = 0;
    config.busy_timeout_ms = 100;
    return config;
}

size_t named_rows(lattice::lattice_db& reader, const std::string& value) {
    return reader.query_read("SELECT name FROM TestPerson WHERE name = ?", {value}).size();
}

class SyncCommitProbe : public ::testing::Test {
    void TearDown() override {
        // Failed ASSERTs must not contaminate a later test's native TLS slot.
        // finish compares this address only; it never dereferences an owner.
        const auto pending = probe::snapshot();
        if (pending.status != 2) {
            probe::finish(reinterpret_cast<const void*>(pending.owner_identity),
                          pending.operation_id, pending.attempt_id);
        }
    }
};

struct Observation {
    std::atomic<uint64_t> timestamp{0};
    std::atomic<uint64_t> operation{0};
    std::atomic<int> status{-1};
};
} // namespace

TEST_F(SyncCommitProbe, ExactWriteIsInvisibleBeforeCommitAndRecordedBeforeInvalidation) {
    TempDB path("sync_probe_commit");
    lattice::lattice_db writer(probe_config(path.str()));
    lattice::lattice_db reader(probe_config(path.str()));
    auto observed = std::make_shared<Observation>();
    const auto hook = writer.add_invalidation_hook_detailed(
        [observed](const auto&, auto reason) {
            const auto receipt = probe::snapshot();
            if (reason != lattice::lattice_db::invalidation_reason::commit ||
                receipt.operation_id != 101) return;
            observed->status.store(receipt.status);
            observed->operation.store(receipt.operation_id);
            observed->timestamp.store(lattice::sync_commit_probe_clock_ns());
        });
    writer.begin_transaction();
    writer.add(TestPerson{"operation-101", 101, std::nullopt});
    ASSERT_EQ(named_rows(reader, "operation-101"), 0u);
    ASSERT_EQ(writer.sync_commit_probe_arm(101, 1), 0);
    EXPECT_EQ(probe::snapshot().postcommit_ns, 0u);
    writer.commit();
    const auto returned = lattice::sync_commit_probe_clock_ns();
    const auto receipt = writer.sync_commit_probe_finish(101, 1);
    writer.remove_invalidation_hook(hook);
    EXPECT_EQ(receipt.status, 0);
    EXPECT_EQ(receipt.operation_id, 101u);
    EXPECT_EQ(receipt.attempt_id, 1u);
    EXPECT_EQ(receipt.owner_identity, reinterpret_cast<uintptr_t>(&writer));
    EXPECT_NE(receipt.connection_identity, 0u);
    EXPECT_NE(receipt.thread_identity, 0u);
    EXPECT_GT(receipt.postcommit_ns, 0u);
    EXPECT_LE(receipt.armed_ns, receipt.postcommit_ns);
    EXPECT_LE(receipt.postcommit_ns, observed->timestamp.load());
    EXPECT_LE(observed->timestamp.load(), returned);
    EXPECT_EQ(observed->status.load(), 0);
    EXPECT_EQ(observed->operation.load(), 101u);
    EXPECT_EQ(named_rows(reader, "operation-101"), 1u);
}

TEST_F(SyncCommitProbe, AnotherPhysicalOwnerOnSameThreadCannotFillTheReceipt) {
    TempDB left("sync_probe_left"), right("sync_probe_right");
    lattice::lattice_db writer(probe_config(left.str()));
    lattice::lattice_db other(probe_config(right.str()));
    writer.begin_transaction();
    writer.add(TestPerson{"operation-102", 102, std::nullopt});
    ASSERT_EQ(writer.sync_commit_probe_arm(102, 1), 0);
    other.add(TestPerson{"unrelated", 0, std::nullopt});
    EXPECT_EQ(probe::snapshot().status, 1);
    EXPECT_EQ(probe::snapshot().postcommit_ns, 0u);
    EXPECT_GE(probe::snapshot().ignored_owner_commits, 1u);
    EXPECT_EQ(other.sync_commit_probe_finish(102, 1).status, 3);
    writer.commit();
    EXPECT_EQ(writer.sync_commit_probe_finish(102, 1).status, 0);
}

TEST_F(SyncCommitProbe, AnotherThreadCannotFillFinishOrAdmitTheOwnerScope) {
    TempDB left("sync_probe_tls_left"), right("sync_probe_tls_right");
    lattice::lattice_db writer(probe_config(left.str()));
    lattice::lattice_db other(probe_config(right.str()));
    writer.begin_transaction();
    writer.add(TestPerson{"operation-103", 103, std::nullopt});
    ASSERT_EQ(writer.sync_commit_probe_arm(103, 1), 0);
    std::exception_ptr error;
    Receipt wrong_thread;
    int arm_result = -1;
    std::thread worker([&] {
        try {
            wrong_thread = writer.sync_commit_probe_finish(103, 1);
            arm_result = writer.sync_commit_probe_arm(103, 1);
            other.add(TestPerson{"other-thread", 0, std::nullopt});
        } catch (...) { error = std::current_exception(); }
    });
    worker.join();
    EXPECT_EQ(error, std::exception_ptr{});
    EXPECT_EQ(wrong_thread.status, 2);
    EXPECT_EQ(arm_result, 4);
    EXPECT_EQ(probe::snapshot().status, 1);
    EXPECT_EQ(probe::snapshot().ignored_owner_commits, 0u); // isolated TLS
    writer.commit();
    EXPECT_EQ(writer.sync_commit_probe_finish(103, 1).status, 0);
}

TEST_F(SyncCommitProbe, SamePathReentrantSuccessorCannotRearmOrReplaceTheOrigin) {
    TempDB path("sync_probe_reentrant");
    lattice::lattice_db writer(probe_config(path.str()));
    lattice::lattice_db sibling(probe_config(path.str()));
    bool entered = false;
    Receipt before, after;
    int rearm_result = -1;
    std::exception_ptr error;
    const auto hook = writer.add_table_observer("TestPerson", [&](const auto&) {
        if (probe::snapshot().operation_id != 104 || entered) return;
        entered = true;
        before = probe::snapshot();
        rearm_result = writer.sync_commit_probe_arm(999, 1);
        try { sibling.add(TestPerson{"successor-104", 0, std::nullopt}); }
        catch (...) { error = std::current_exception(); }
        after = probe::snapshot();
    });
    writer.begin_transaction();
    writer.add(TestPerson{"operation-104", 104, std::nullopt});
    ASSERT_EQ(writer.sync_commit_probe_arm(104, 1), 0);
    writer.commit();
    const auto receipt = writer.sync_commit_probe_finish(104, 1);
    writer.remove_table_observer("TestPerson", hook);
    EXPECT_TRUE(entered);
    EXPECT_EQ(error, std::exception_ptr{});
    EXPECT_EQ(rearm_result, 2);
    EXPECT_EQ(before.status, 0);
    EXPECT_EQ(before.postcommit_ns, after.postcommit_ns);
    EXPECT_EQ(receipt.postcommit_ns, before.postcommit_ns);
    EXPECT_EQ(receipt.operation_id, 104u);
    EXPECT_EQ(named_rows(sibling, "operation-104"), 1u);
    EXPECT_EQ(named_rows(sibling, "successor-104"), 1u);
}

TEST_F(SyncCommitProbe, RollbackDisarmsBeforeAnUnrelatedLaterCommit) {
    TempDB path("sync_probe_rollback");
    lattice::lattice_db writer(probe_config(path.str()));
    writer.begin_transaction();
    writer.add(TestPerson{"rolled-back-105", 105, std::nullopt});
    ASSERT_EQ(writer.sync_commit_probe_arm(105, 1), 0);
    writer.rollback();
    // Even before finally/defer finishes the failed scope, a later write must
    // not be misreported as its commit. It must not implicitly rearm either.
    writer.add(TestPerson{"unrelated-after-rollback", 0, std::nullopt});
    const auto rejected = writer.sync_commit_probe_finish(105, 1);
    EXPECT_EQ(rejected.status, 1);
    EXPECT_EQ(rejected.postcommit_ns, 0u);
    EXPECT_EQ(named_rows(writer, "rolled-back-105"), 0u);
    writer.begin_transaction();
    writer.add(TestPerson{"retry-105", 105, std::nullopt});
    ASSERT_EQ(writer.sync_commit_probe_arm(105, 2), 0);
    writer.commit();
    const auto retry = writer.sync_commit_probe_finish(105, 2);
    EXPECT_EQ(retry.status, 0);
    EXPECT_EQ(retry.attempt_id, 2u);
    EXPECT_EQ(named_rows(writer, "retry-105"), 1u);
}

TEST_F(SyncCommitProbe, RealDeferredConstraintCommitFailureHasNoPostcommitRecord) {
    TempDB path("sync_probe_failed_commit");
    lattice::lattice_db writer(probe_config(path.str()));
    writer.db().execute("CREATE TABLE _probe_parent(id INTEGER PRIMARY KEY)");
    writer.db().execute("CREATE TABLE _probe_child(id INTEGER PRIMARY KEY, parent INTEGER "
                        "REFERENCES _probe_parent(id) DEFERRABLE INITIALLY DEFERRED)");
    writer.begin_transaction();
    writer.db().execute("INSERT INTO _probe_child VALUES(1, 99)");
    ASSERT_EQ(writer.sync_commit_probe_arm(106, 1), 0);
    EXPECT_THROW(writer.commit(), lattice::db_error);
    EXPECT_TRUE(writer.db().is_in_transaction());
    EXPECT_EQ(probe::snapshot().status, 1);
    EXPECT_EQ(probe::snapshot().postcommit_ns, 0u);
    writer.rollback();
    EXPECT_EQ(writer.sync_commit_probe_finish(106, 1).status, 1);
    writer.begin_transaction();
    writer.db().execute("INSERT INTO _probe_parent VALUES(99)");
    writer.db().execute("INSERT INTO _probe_child VALUES(1, 99)");
    ASSERT_EQ(writer.sync_commit_probe_arm(106, 2), 0);
    writer.commit();
    const auto retry = writer.sync_commit_probe_finish(106, 2);
    EXPECT_EQ(retry.status, 0);
    EXPECT_EQ(retry.attempt_id, 2u);
    EXPECT_EQ(writer.query_read("SELECT id FROM _probe_child").size(), 1u);
}

TEST_F(SyncCommitProbe, WrongFinishCannotStealScopeAndFinishCannotBeReused) {
    TempDB path("sync_probe_attempt");
    lattice::lattice_db writer(probe_config(path.str()));
    writer.begin_transaction();
    writer.add(TestPerson{"operation-107", 107, std::nullopt});
    ASSERT_EQ(writer.sync_commit_probe_arm(107, 1), 0);
    EXPECT_EQ(writer.sync_commit_probe_arm(108, 1), 2);
    EXPECT_EQ(writer.sync_commit_probe_finish(107, 2).status, 3);
    EXPECT_EQ(writer.sync_commit_probe_finish(108, 1).status, 3);
    EXPECT_EQ(probe::snapshot().operation_id, 107u);
    writer.commit();
    EXPECT_EQ(writer.sync_commit_probe_finish(107, 1).status, 0);
    writer.add(TestPerson{"unarmed-later", 0, std::nullopt});
    EXPECT_EQ(writer.sync_commit_probe_finish(107, 1).status, 2);
    EXPECT_EQ(probe::snapshot().postcommit_ns, 0u);
}

TEST_F(SyncCommitProbe, UnsupportedClosedAndUnownedTransactionsAreRefused) {
    lattice::lattice_db memory;
    EXPECT_EQ(memory.sync_commit_probe_arm(108, 1), 3);
    TempDB path("sync_probe_admission");
    lattice::lattice_db writer(probe_config(path.str()));
    EXPECT_EQ(writer.sync_commit_probe_arm(0, 1), 1);
    EXPECT_EQ(writer.sync_commit_probe_arm(108, 0), 1);
    EXPECT_EQ(writer.sync_commit_probe_arm(108, 1), 4);
    // A raw database transaction has no lattice_db thread owner. The test
    // adapter must not borrow it, even though SQLite reports a write txn.
    writer.db().begin_transaction();
    writer.db().execute("UPDATE _SyncControl SET disabled=disabled WHERE id=1");
    EXPECT_EQ(writer.sync_commit_probe_arm(108, 1), 4);
    writer.db().rollback();
    writer.close();
    EXPECT_EQ(writer.sync_commit_probe_arm(108, 1), 3);
    EXPECT_EQ(writer.sync_commit_probe_finish(108, 1).status, 2);
}

TEST_F(SyncCommitProbe, NoOpCommitDoesNotBecomeAnInsertSample) {
    TempDB path("sync_probe_noop");
    lattice::lattice_db writer(probe_config(path.str()));
    writer.begin_transaction(); // BEGIN IMMEDIATE owns a write txn, no changes
    ASSERT_EQ(writer.sync_commit_probe_arm(109, 1), 0);
    writer.commit();
    const auto receipt = writer.sync_commit_probe_finish(109, 1);
    EXPECT_EQ(receipt.status, 1);
    EXPECT_EQ(receipt.postcommit_ns, 0u);
}

TEST_F(SyncCommitProbe, FailedBeginCannotAdmitASampleAndNextAttemptCanCommit) {
    TempDB path("sync_probe_failed_begin");
    lattice::lattice_db holder(probe_config(path.str()));
    lattice::lattice_db writer(probe_config(path.str()));
    holder.begin_transaction();
    holder.add(TestPerson{"held-uncommitted", 0, std::nullopt});
    // The explicit 100 ms SQLite busy budget bounds this real admission fault.
    EXPECT_THROW(writer.begin_transaction(), lattice::db_error);
    EXPECT_FALSE(writer.db().is_in_transaction());
    EXPECT_EQ(writer.sync_commit_probe_arm(115, 1), 4);
    EXPECT_EQ(writer.sync_commit_probe_finish(115, 1).status, 2);
    holder.rollback();
    writer.begin_transaction();
    writer.add(TestPerson{"retry-115", 115, std::nullopt});
    ASSERT_EQ(writer.sync_commit_probe_arm(115, 2), 0);
    writer.commit();
    const auto receipt = writer.sync_commit_probe_finish(115, 2);
    EXPECT_EQ(receipt.status, 0);
    EXPECT_EQ(receipt.attempt_id, 2u);
    EXPECT_EQ(named_rows(writer, "held-uncommitted"), 0u);
    EXPECT_EQ(named_rows(writer, "retry-115"), 1u);
}

TEST_F(SyncCommitProbe, ExactConnectionAndMainSchemaAreRequiredAtRecordBoundary) {
    TempDB left("sync_probe_identity_left"), right("sync_probe_identity_right");
    lattice::lattice_db writer(probe_config(left.str()));
    lattice::lattice_db other(probe_config(right.str()));
    // Obtain only the probe's diagnostic identity. Public database::handle()
    // now revokes producer provenance and must not be an instrumentation aid.
    other.begin_transaction();
    ASSERT_EQ(other.sync_commit_probe_arm(116, 1), 0);
    const auto other_scope = other.sync_commit_probe_finish(116, 1);
    other.rollback();
    ASSERT_EQ(other_scope.status, 1);
    ASSERT_NE(other_scope.connection_identity, 0u);
    writer.begin_transaction();
    writer.add(TestPerson{"operation-110", 110, std::nullopt});
    ASSERT_EQ(writer.sync_commit_probe_arm(110, 1), 0);
    const auto pending = probe::snapshot();
    auto* connection = reinterpret_cast<sqlite3*>(pending.connection_identity);
    // Negative inputs exercise guard identity only. The positive sample below
    // must come from the real SQLite COMMIT, never a direct helper invocation.
    probe::record(&writer, reinterpret_cast<sqlite3*>(other_scope.connection_identity), "main");
    probe::record(&writer, connection, "aux");
    probe::record(&writer, connection, nullptr);
    EXPECT_EQ(probe::snapshot().status, 1);
    EXPECT_EQ(probe::snapshot().ignored_owner_commits, 1u);
    EXPECT_EQ(probe::snapshot().ignored_schema_commits, 2u);
    writer.commit();
    EXPECT_EQ(writer.sync_commit_probe_finish(110, 1).status, 0);
}

TEST_F(SyncCommitProbe, QueuedOldObserverCannotMoveOrRelabelTheNativeTimestamp) {
    TempDB path("sync_probe_queued");
    auto scheduler = std::make_shared<lattice::main_thread_scheduler>();
    auto config = probe_config(path.str());
    config.sched = scheduler;
    lattice::lattice_db writer(config);
    scheduler->process_pending();
    auto observed = std::make_shared<Observation>();
    const auto hook = writer.add_table_observer("TestPerson", [observed](const auto&) {
        const auto current = probe::snapshot();
        observed->operation.store(current.operation_id);
        observed->status.store(current.status);
        observed->timestamp.store(lattice::sync_commit_probe_clock_ns());
    });
    writer.begin_transaction();
    writer.add(TestPerson{"operation-111", 111, std::nullopt});
    ASSERT_EQ(writer.sync_commit_probe_arm(111, 1), 0);
    writer.commit();
    const auto first = writer.sync_commit_probe_finish(111, 1);
    EXPECT_EQ(first.status, 0);
    EXPECT_EQ(observed->timestamp.load(), 0u);
    writer.begin_transaction();
    writer.add(TestPerson{"operation-112", 112, std::nullopt});
    ASSERT_EQ(writer.sync_commit_probe_arm(112, 1), 0);
    // Deliberate qualification misuse: drain N's queued callback while N+1 is
    // armed. A public callback/current-token heuristic would name N+1 here.
    scheduler->process_pending();
    EXPECT_EQ(observed->operation.load(), 112u);
    EXPECT_EQ(observed->status.load(), 1);
    EXPECT_EQ(probe::snapshot().postcommit_ns, 0u);
    writer.commit();
    const auto second = writer.sync_commit_probe_finish(112, 1);
    EXPECT_EQ(second.status, 0);
    EXPECT_LE(first.postcommit_ns, second.postcommit_ns);
    writer.remove_table_observer("TestPerson", hook);
    scheduler->process_pending();
}

TEST_F(SyncCommitProbe, BookkeepingAfterARecordedWriteDoesNotCreateAnotherSample) {
    TempDB path("sync_probe_bookkeeping");
    lattice::lattice_db writer(probe_config(path.str()));
    writer.begin_transaction();
    writer.add(TestPerson{"operation-113", 113, std::nullopt});
    ASSERT_EQ(writer.sync_commit_probe_arm(113, 1), 0);
    writer.commit();
    const auto first = probe::snapshot();
    const auto entries = lattice::query_audit_log(writer.db(), true);
    ASSERT_FALSE(entries.empty());
    auto callbacks = std::make_shared<std::atomic<int>>(0);
    const auto hook = writer.add_table_observer("AuditLog", [callbacks](const auto&) {
        callbacks->fetch_add(1);
    });
    lattice::mark_audit_entries_synced(writer, {entries.back().global_id});
    EXPECT_GE(callbacks->load(), 1);
    writer.db().execute("UPDATE _SyncControl SET disabled=disabled WHERE id=1");
    const auto after = writer.sync_commit_probe_finish(113, 1);
    writer.remove_table_observer("AuditLog", hook);
    EXPECT_EQ(after.status, 0);
    EXPECT_EQ(first.postcommit_ns, after.postcommit_ns);
    EXPECT_EQ(first.operation_id, after.operation_id);
}

TEST_F(SyncCommitProbe, SwiftBridgeAdapterUsesTheSameClockAndOwner) {
    TempDB path("sync_probe_bridge");
    lattice::swift_configuration config(path.str());
    config.audit_retention_seconds = 0;
    lattice::SchemaVector schemas;
#if LATTICE_HAS_FRT
    auto ref = std::unique_ptr<lattice::swift_lattice_ref>(
        lattice::swift_lattice_ref::create(config, schemas));
#else
    auto ref = std::make_unique<lattice::swift_lattice_ref>(
        lattice::swift_lattice_ref::create(config, schemas));
#endif
    ref->get()->db().execute("CREATE TABLE _probe_bridge(value INTEGER)");
    const auto start = lattice::sync_commit_probe_clock_ns();
    ref->get()->begin_transaction();
    ref->get()->db().execute("INSERT INTO _probe_bridge VALUES(114)");
    ASSERT_EQ(ref->sync_commit_probe_arm(114, 1), 0);
    ref->get()->commit();
    const auto receipt = ref->sync_commit_probe_finish(114, 1);
    const auto returned = lattice::sync_commit_probe_clock_ns();
    EXPECT_EQ(receipt.status, 0);
    EXPECT_EQ(receipt.owner_identity, reinterpret_cast<uintptr_t>(ref->get()));
    EXPECT_LE(start, receipt.armed_ns);
    EXPECT_LE(receipt.armed_ns, receipt.postcommit_ns);
    EXPECT_LE(receipt.postcommit_ns, returned);
    EXPECT_EQ(ref->get()->query_read("SELECT value FROM _probe_bridge").size(), 1u);
}

namespace {
using namespace lattice::detail;

void probe_require_commit(const recovery_install_result& result) {
    if (result.primary_error) std::rethrow_exception(result.primary_error);
    if (result.cleanup_error) std::rethrow_exception(result.cleanup_error);
    if (result.postcommit_error) std::rethrow_exception(result.postcommit_error);
    if (result.state != recovery_install_state::committed)
        throw std::runtime_error("probe fixture's owned operation did not commit");
}

struct probe_producer_fixture {
    TempDB path{"sync_probe_enrolled"};
    // Small explicit qualification budgets; no production policy is selected.
    recovery_obligation_producer_discovery_limits limits{
        {4, 128, 128, 2 * 1024 * 1024}, {4, 128, 8192},
        {4, 128, 128, 1048576, 8 * 1024 * 1024}};
    std::shared_ptr<lattice::lattice_db> owner =
        std::make_shared<lattice::lattice_db>(probe_config(path.str()));
    recovery_obligation_address address;

    probe_producer_fixture() {
        // No outside process participates. Suppress nondeterministic file
        // notifications; native synchronous commit delivery stays enabled.
        auto* notifier = lattice::instance_registry::instance().get_or_create_notifier(path.str());
        if (notifier) notifier->stop_listening();
        probe_require_commit(recovery_writer_access::install(owner, [&](auto&) {
            receive_install_store receiver(owner, limits.installations);
            receiver.initialize();
            receive_install_binding binding{
                "probe-contribution", "authority", "source", "epoch", "scope", "schema"};
            receiver.bind(binding);
            recovery_obligation_store journal(owner, limits.obligations, limits.installations);
            journal.initialize();
            address = journal.bind({binding, "grant", "receipts"}).address;
        }));
        probe_require_commit(recovery_local_producer_adapter::enroll_for_qualification(
            owner, {address, {"ProbeProducerRow"}, {'g'}}, limits));
    }

    auto audit(const std::string& target) {
        return owner->db().query(
            "SELECT * FROM AuditLog WHERE tableName='ProbeProducerRow' AND globalRowId=? ORDER BY id",
            {target});
    }
    auto stamp(const std::string& original) {
        std::optional<recovery_obligation_producer_stamp> result;
        probe_require_commit(recovery_writer_access::install(owner, [&](auto&) {
            result = recovery_obligation_producer_store::read_stamp(owner, limits.obligations,
                limits.installations, limits.producers, address, original);
        }));
        return result;
    }
    auto entry(const std::string& original) {
        std::optional<recovery_obligation_entry> result;
        probe_require_commit(recovery_writer_access::install(owner, [&](auto&) {
            recovery_obligation_store journal(owner, limits.obligations, limits.installations);
            result = journal.find(address, original);
        }));
        return result;
    }
};

struct probe_invalidation_registration {
    std::shared_ptr<lattice::lattice_db> owner;
    uint64_t token;
    void reset() {
        if (token) owner->remove_invalidation_hook(std::exchange(token, 0));
    }
    ~probe_invalidation_registration() { reset(); }
};
} // namespace

TEST_F(SyncCommitProbe, EnrolledGeneratedWriteAndSuccessorPreserveProducerProvenance) {
    probe_producer_fixture fixture;
    auto& owner = *fixture.owner;
    owner.begin_transaction();
    auto first = owner.add(ProbeProducerRow{"tagged-generated"});
    ASSERT_EQ(owner.sync_commit_probe_arm(201, 1), 0);
    owner.commit();
    const auto origin = owner.sync_commit_probe_finish(201, 1);
    ASSERT_EQ(origin.status, 0);
    EXPECT_EQ(origin.owner_identity, reinterpret_cast<uintptr_t>(fixture.owner.get()));
    EXPECT_GT(origin.postcommit_ns, 0u);
    const auto first_audit = fixture.audit(first.global_id());
    ASSERT_EQ(first_audit.size(), 1u);
    const auto original = std::get<std::string>(first_audit[0].at("globalId"));
    const auto first_stamp = fixture.stamp(original);
    ASSERT_TRUE(first_stamp);
    EXPECT_EQ(first_stamp->audit_id, std::get<int64_t>(first_audit[0].at("id")));
    EXPECT_EQ(first_stamp->contribution_incarnation, fixture.address.incarnation);

    // A raw-handle escape in arm/finish would revoke this actual generated
    // successor (and the checked stamp read), not merely change a flag oracle.
    auto second = owner.add(ProbeProducerRow{"successor-generated"});
    const auto second_audit = fixture.audit(second.global_id());
    ASSERT_EQ(second_audit.size(), 1u);
    const auto second_original = std::get<std::string>(second_audit[0].at("globalId"));
    EXPECT_NE(second_original, original);
    const auto second_stamp = fixture.stamp(second_original);
    ASSERT_TRUE(second_stamp);
    EXPECT_EQ(second_stamp->audit_id, std::get<int64_t>(second_audit[0].at("id")));
    EXPECT_EQ(second_stamp->program_digest, first_stamp->program_digest);
    EXPECT_EQ(fixture.audit(first.global_id()), first_audit);
    EXPECT_EQ(probe::snapshot().status, 2);
}

TEST_F(SyncCommitProbe, ActualProtectedClaimAndDetachedCallbackCannotReplaceConsumedOrigin) {
    probe_producer_fixture fixture;
    auto& owner = *fixture.owner;
    owner.begin_transaction();
    auto row = owner.add(ProbeProducerRow{"claim-after-app-commit"});
    ASSERT_EQ(owner.sync_commit_probe_arm(202, 1), 0);
    owner.commit();
    const auto origin = probe::snapshot();
    ASSERT_EQ(origin.status, 0);
    const auto before = fixture.audit(row.global_id());
    ASSERT_EQ(before.size(), 1u);
    const auto original = std::get<std::string>(before[0].at("globalId"));
    struct callback_state { int calls = 0, rearm = -1; Receipt observed; };
    auto callback = std::make_shared<callback_state>();
    std::weak_ptr<lattice::lattice_db> weak_owner = fixture.owner;
    probe_invalidation_registration registration{fixture.owner,
        owner.add_invalidation_hook_detailed([callback, weak_owner](const auto&, auto reason) {
            if (reason != lattice::lattice_db::invalidation_reason::commit) return;
            ++callback->calls;
            callback->observed = probe::snapshot();
            if (auto retained = weak_owner.lock())
                callback->rearm = retained->sync_commit_probe_arm(999, 1);
        })};

    // This is the actual sender's owned metadata preparation/COMMIT. The
    // retained scope is deliberately not finished until its detached tail.
    auto prepared = recovery_export_adapter::prepare_pending(
        fixture.owner, "probe-route", 1, 8, {}, false);
    ASSERT_TRUE(prepared.protected_store);
    ASSERT_TRUE(prepared.frame);
    ASSERT_EQ(prepared.frame->entries().size(), 1u);
    EXPECT_EQ(prepared.frame->entries()[0].global_id, original);
    EXPECT_EQ(callback->calls, 1);
    EXPECT_EQ(callback->rearm, 2);
    EXPECT_EQ(callback->observed.status, 0);
    EXPECT_EQ(callback->observed.operation_id, 202u);
    EXPECT_EQ(callback->observed.postcommit_ns, origin.postcommit_ns);
    const auto finished = owner.sync_commit_probe_finish(202, 1);
    EXPECT_EQ(finished.status, 0);
    EXPECT_EQ(finished.postcommit_ns, origin.postcommit_ns);
    // Remove before the following independently committed verification reads.
    registration.reset();
    const auto obligation = fixture.entry(original);
    ASSERT_TRUE(obligation);
    ASSERT_TRUE(obligation->first_export_claim);
    EXPECT_GT(*obligation->first_export_claim, 0);
    EXPECT_EQ(obligation->stage, recovery_obligation_stage::open);
    EXPECT_EQ(fixture.audit(row.global_id()), before);
}

TEST_F(SyncCommitProbe, PrivateInstallCannotArmAndRollbackDoesNotContaminatePublicSuccessor) {
    probe_producer_fixture fixture;
    auto& owner = *fixture.owner;
    struct callback_state { int commits = 0, rollbacks = 0; };
    auto callback = std::make_shared<callback_state>();
    probe_invalidation_registration registration{fixture.owner,
        owner.add_invalidation_hook_detailed([callback](const auto&, auto reason) {
            if (reason == lattice::lattice_db::invalidation_reason::commit) ++callback->commits;
            if (reason == lattice::lattice_db::invalidation_reason::rollback) ++callback->rollbacks;
        })};
    int rejected_arm = -1;
    const auto rolled = recovery_writer_access::install(fixture.owner, [&](auto& writer) {
        rejected_arm = owner.sync_commit_probe_arm(203, 1);
        writer.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        writer.execute("INSERT INTO ProbeProducerRow(value) VALUES('private-rollback')");
        writer.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
        throw std::runtime_error("probe fixture body rollback");
    });
    EXPECT_EQ(rejected_arm, 4);
    EXPECT_EQ(rolled.state, recovery_install_state::rolled_back);
    ASSERT_TRUE(rolled.primary_error);
    EXPECT_EQ(rolled.cleanup_error, std::exception_ptr{});
    EXPECT_EQ(rolled.postcommit_error, std::exception_ptr{});
    EXPECT_EQ(callback->commits, 0);
    EXPECT_EQ(callback->rollbacks, 1);
    EXPECT_TRUE(owner.query_read("SELECT id FROM ProbeProducerRow").empty());
    EXPECT_EQ(owner.sync_commit_probe_finish(203, 1).status, 2);

    int committed_arm = -1;
    probe_require_commit(recovery_writer_access::install(fixture.owner, [&](auto& writer) {
        committed_arm = owner.sync_commit_probe_arm(203, 2);
        writer.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        writer.execute("INSERT INTO ProbeProducerRow(value) VALUES('private-committed')");
        writer.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
    }));
    EXPECT_EQ(committed_arm, 4);
    EXPECT_EQ(callback->commits, 1);
    EXPECT_EQ(owner.query_read("SELECT id FROM ProbeProducerRow WHERE value='private-committed'").size(), 1u);
    EXPECT_TRUE(owner.db().query("SELECT id FROM AuditLog WHERE tableName='ProbeProducerRow'").empty());
    EXPECT_EQ(owner.sync_commit_probe_finish(203, 2).status, 2);

    owner.begin_transaction();
    auto successor = owner.add(ProbeProducerRow{"public-after-private"});
    ASSERT_EQ(owner.sync_commit_probe_arm(203, 3), 0);
    owner.commit();
    const auto origin = owner.sync_commit_probe_finish(203, 3);
    EXPECT_EQ(origin.status, 0);
    EXPECT_EQ(origin.attempt_id, 3u);
    EXPECT_GT(origin.postcommit_ns, 0u);
    EXPECT_EQ(callback->commits, 2);
    registration.reset();
    const auto audit = fixture.audit(successor.global_id());
    ASSERT_EQ(audit.size(), 1u);
    ASSERT_TRUE(fixture.stamp(std::get<std::string>(audit[0].at("globalId"))));
}
#endif
