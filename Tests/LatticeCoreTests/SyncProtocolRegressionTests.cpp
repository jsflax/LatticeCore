#include "TestHelpers.hpp"
#include <lattice/sync.hpp>
#include <algorithm>
#include <optional>
#include <string>
#include <vector>

namespace {

constexpr const char* receive_channel = "protocol-receive";
constexpr const char* bad_operation_id = "00000000-0000-4000-8000-000000000101";
constexpr const char* good_operation_id = "00000000-0000-4000-8000-000000000102";

lattice::configuration protocol_config(const TempDB& file) {
    lattice::configuration config(file.str());
    config.audit_retention_seconds = 0;
    config.busy_timeout_ms = 100;
    return config;
}

int64_t scalar(lattice::database& db, const std::string& sql,
               const std::vector<lattice::column_value_t>& params = {}) {
    return std::get<int64_t>(db.query(sql, params).at(0).at("n"));
}

std::string row_id(lattice::lattice_db& db, const std::string& name) {
    return std::get<std::string>(db.db().query(
        "SELECT globalId FROM TestPerson WHERE name = ?", {name}).at(0).at("globalId"));
}

std::string row_name(lattice::lattice_db& db, const std::string& id) {
    return std::get<std::string>(db.db().query(
        "SELECT name FROM TestPerson WHERE globalId = ?", {id}).at(0).at("name"));
}

int64_t audit_count(lattice::lattice_db& db, const std::string& id) {
    return scalar(db.db(), "SELECT COUNT(*) AS n FROM AuditLog WHERE globalId = ?", {id});
}

int64_t receipt_count(lattice::lattice_db& db, const std::string& id) {
    return scalar(db.db(),
        "SELECT COUNT(*) AS n FROM _lattice_applied_receipts WHERE globalId = ?", {id});
}

int64_t receiving_state_count(lattice::lattice_db& db, const std::string& id) {
    return scalar(db.db(),
        "SELECT COUNT(*) AS n FROM _lattice_sync_state AS s "
        "JOIN AuditLog AS a ON a.id = s.audit_entry_id "
        "WHERE a.globalId = ? AND s.sync_id = ? AND s.is_synchronized = 1",
        {id, std::string(receive_channel)});
}

int64_t effect_count(lattice::lattice_db& db, const std::string& id) {
    return scalar(db.db(),
        "SELECT COUNT(*) AS n FROM _protocol_update_effects WHERE rowGlobalId = ?", {id});
}

std::optional<std::string> receive_cursor(lattice::lattice_db& db) {
    const auto rows = db.db().query(
        "SELECT last_received_event_id AS cursor FROM _lattice_replication_slots WHERE sync_id = ?",
        {std::string(receive_channel)});
    if (rows.empty() || std::holds_alternative<std::nullptr_t>(rows.at(0).at("cursor"))) {
        return std::nullopt;
    }
    return std::get<std::string>(rows.at(0).at("cursor"));
}

void register_receiver(lattice::lattice_db& db) {
    lattice::ensure_cursor_column(db.db());
    lattice::register_replication_slot(db.db(), receive_channel);
}

void expect_settled(lattice::lattice_db& db) {
    EXPECT_FALSE(db.db().is_in_transaction());
    EXPECT_EQ(scalar(db.db(), "SELECT disabled AS n FROM _SyncControl WHERE id = 1"), 0);
}

lattice::audit_log_entry update_name(const std::string& operation_id,
                                    const std::string& object_id,
                                    const std::string& value) {
    lattice::audit_log_entry entry;
    entry.global_id = operation_id;
    entry.table_name = "TestPerson";
    entry.operation = "UPDATE";
    entry.global_row_id = object_id;
    entry.changed_fields_names = {"name"};
    entry.changed_fields = {{"name", lattice::any_property(value)}};
    entry.timestamp = "1789819200.0";
    return entry;
}

// A non-transactional hit counter proves that the intended failure stage was
// reached even when the correct rollback removes all its database effects.
// No threads, transport, clock waits, or production fault hooks are required.
struct FailureStageCounter {
    sqlite3* handle;
    int hits = 0;
    int registration_result;

    explicit FailureStageCounter(lattice::database& db) : handle(db.handle()) {
        registration_result = sqlite3_create_function_v2(
            handle, "protocol_failure_stage", 0, SQLITE_UTF8, &hits,
            [](sqlite3_context* context, int, sqlite3_value**) noexcept {
                ++*static_cast<int*>(sqlite3_user_data(context));
                sqlite3_result_int(context, 1);
            }, nullptr, nullptr, nullptr);
    }
    ~FailureStageCounter() {
        sqlite3_create_function_v2(handle, "protocol_failure_stage", 0, SQLITE_UTF8,
                                   nullptr, nullptr, nullptr, nullptr, nullptr);
    }
    FailureStageCounter(const FailureStageCounter&) = delete;
    FailureStageCounter& operator=(const FailureStageCounter&) = delete;
};

enum class BookkeepingStage { audit_insert, receiving_sync_state };

void assert_entry_bookkeeping_atomicity(BookkeepingStage stage) {
    TempDB file{stage == BookkeepingStage::audit_insert ? "protocol_audit_atomic" : "protocol_state_atomic"};
    lattice::lattice_db db{protocol_config(file)};
    db.add(TestPerson{"bad-initial", 1, std::nullopt});
    db.add(TestPerson{"good-initial", 2, std::nullopt});
    const auto bad_row = row_id(db, "bad-initial");
    const auto good_row = row_id(db, "good-initial");
    register_receiver(db);

    // A model trigger's side effects participate in the entry's atomic unit,
    // just as model-maintenance triggers must. Seed writes precede this trigger.
    db.db().execute("CREATE TABLE _protocol_update_effects(rowGlobalId TEXT NOT NULL, value TEXT NOT NULL)");
    db.db().execute(R"(
        CREATE TRIGGER protocol_update_effect AFTER UPDATE ON TestPerson
        BEGIN
            INSERT INTO _protocol_update_effects(rowGlobalId, value) VALUES(NEW.globalId, NEW.name);
        END
    )");
    FailureStageCounter failure(db.db());
    ASSERT_EQ(failure.registration_result, SQLITE_OK);
    if (stage == BookkeepingStage::audit_insert) {
        db.db().execute(R"(
            CREATE TRIGGER protocol_reject_bookkeeping BEFORE INSERT ON AuditLog
            WHEN NEW.globalId = '00000000-0000-4000-8000-000000000101'
            BEGIN
                SELECT protocol_failure_stage();
                SELECT RAISE(ABORT, 'protocol audit insertion rejected');
            END
        )");
    } else {
        db.db().execute(R"(
            CREATE TRIGGER protocol_reject_bookkeeping BEFORE INSERT ON _lattice_sync_state
            WHEN NEW.sync_id = 'protocol-receive' AND EXISTS (
                SELECT 1 FROM AuditLog WHERE id = NEW.audit_entry_id
                AND globalId = '00000000-0000-4000-8000-000000000101')
            BEGIN
                SELECT protocol_failure_stage();
                SELECT RAISE(ABORT, 'protocol receiving state insertion rejected');
            END
        )");
    }

    const auto bad = update_name(bad_operation_id, bad_row, "bad-after");
    const auto good = update_name(good_operation_id, good_row, "good-after");
    const auto applied = lattice::apply_remote_changes_for(db, {bad, good}, receive_channel);
    EXPECT_GT(failure.hits, 0) << "the named post-model-SQL bookkeeping stage must be reached";
    EXPECT_EQ(applied, (std::vector<std::string>{good_operation_id}));
    EXPECT_EQ(row_name(db, bad_row), "bad-initial") << "failed bookkeeping must roll back model SQL";
    EXPECT_EQ(effect_count(db, bad_row), 0) << "failed bookkeeping must roll back model trigger effects";
    EXPECT_EQ(audit_count(db, bad.global_id), 0);
    EXPECT_EQ(receipt_count(db, bad.global_id), 0);
    EXPECT_EQ(receiving_state_count(db, bad.global_id), 0);
    EXPECT_EQ(row_name(db, good_row), "good-after") << "a later independent entry still commits";
    EXPECT_EQ(effect_count(db, good_row), 1);
    EXPECT_EQ(audit_count(db, good.global_id), 1);
    EXPECT_EQ(receiving_state_count(db, good.global_id), 1);
    expect_settled(db);

    // Retry the IDENTICAL operation after removing the receiver-side fault.
    // Its first durable success must include every piece of loop-prevention
    // bookkeeping, then duplicate delivery must have no additional row effects.
    db.db().execute("DROP TRIGGER protocol_reject_bookkeeping");
    EXPECT_EQ(lattice::apply_remote_changes_for(db, {bad}, receive_channel),
              (std::vector<std::string>{bad_operation_id}));
    EXPECT_EQ(row_name(db, bad_row), "bad-after");
    EXPECT_EQ(effect_count(db, bad_row), 1);
    EXPECT_EQ(audit_count(db, bad.global_id), 1);
    EXPECT_EQ(receiving_state_count(db, bad.global_id), 1);
    EXPECT_EQ(receipt_count(db, bad.global_id), 0);
    EXPECT_EQ(lattice::apply_remote_changes_for(db, {bad, good}, receive_channel),
              (std::vector<std::string>{bad_operation_id, good_operation_id}));
    EXPECT_EQ(effect_count(db, bad_row), 1);
    EXPECT_EQ(effect_count(db, good_row), 1);
    EXPECT_EQ(audit_count(db, bad.global_id), 1);
    EXPECT_EQ(audit_count(db, good.global_id), 1);
    EXPECT_EQ(receiving_state_count(db, bad.global_id), 1);
    expect_settled(db);
}

} // namespace

TEST(SyncProtocolRegression, AuditInsertFailureRollsBackOnlyItsEntry) {
    assert_entry_bookkeeping_atomicity(BookkeepingStage::audit_insert);
}

TEST(SyncProtocolRegression, ReceivingStateFailureRollsBackOnlyItsEntry) {
    assert_entry_bookkeeping_atomicity(BookkeepingStage::receiving_sync_state);
}

TEST(SyncProtocolRegression, LaterDeliveryCannotSkipEarlierUnappliedDownload) {
    TempDB file{"protocol_download_gap"};
    const std::string first_id = "00000000-0000-4000-8000-000000000201";
    const std::string failed_id = "00000000-0000-4000-8000-000000000202";
    const std::string later_id = "00000000-0000-4000-8000-000000000203";
    std::string failed_row;
    {
        lattice::lattice_db db{protocol_config(file)};
        db.add(TestPerson{"first-initial", 1, std::nullopt});
        db.add(TestPerson{"failed-initial", 2, std::nullopt});
        db.add(TestPerson{"later-initial", 3, std::nullopt});
        register_receiver(db);
        ASSERT_EQ(scalar(db.db(),
            "SELECT \"notnull\" AS n FROM pragma_table_info('TestPerson') WHERE name = 'name'"), 1)
            << "the failure must be the known name column's NOT NULL constraint";
        const auto first = update_name(first_id, row_id(db, "first-initial"), "first-after");
        failed_row = row_id(db, "failed-initial");
        auto failed = update_name(failed_id, failed_row, "unused");
        // A KNOWN table and column: the receiver's NOT NULL constraint must
        // reject this SQL. Unknown-table skip-and-ACK is a different contract.
        failed.changed_fields["name"] = lattice::any_property(nullptr);
        const auto later = update_name(later_id, row_id(db, "later-initial"), "later-after");

        EXPECT_EQ(lattice::apply_remote_changes_for(db, {first, failed}, receive_channel),
                  (std::vector<std::string>{first_id}));
        EXPECT_EQ(row_name(db, first.global_row_id), "first-after");
        EXPECT_EQ(receive_cursor(db), std::optional<std::string>(first_id));
        EXPECT_EQ(row_name(db, failed_row), "failed-initial");
        EXPECT_EQ(audit_count(db, failed_id), 0);
        EXPECT_EQ(receipt_count(db, failed_id), 0);

        EXPECT_EQ(lattice::apply_remote_changes_for(db, {later}, receive_channel),
                  (std::vector<std::string>{later_id}));
        EXPECT_EQ(row_name(db, later.global_row_id), "later-after");
        EXPECT_EQ(receive_cursor(db), std::optional<std::string>(first_id))
            << "a later frame cannot acknowledge away an earlier receive gap";
        EXPECT_EQ(audit_count(db, failed_id), 0);
        EXPECT_EQ(receiving_state_count(db, failed_id), 0);
        expect_settled(db);
    }
    {
        lattice::lattice_db reopened{protocol_config(file)};
        EXPECT_EQ(receive_cursor(reopened), std::optional<std::string>(first_id))
            << "the safe download frontier must survive reopening";
        EXPECT_EQ(row_name(reopened, failed_row), "failed-initial");
        EXPECT_EQ(audit_count(reopened, failed_id), 0);
        expect_settled(reopened);
    }
}

TEST(SyncProtocolRegression, TwoWritersConvergeAfterCanonicalReplayAndReopen) {
    TempDB relay_file{"protocol_converge_relay"};
    TempDB a_file{"protocol_converge_a"};
    TempDB b_file{"protocol_converge_b"};
    std::string shared_row;
    std::string a_operation_id;
    std::string b_operation_id;
    {
        lattice::lattice_db relay{protocol_config(relay_file)};
        lattice::lattice_db a{protocol_config(a_file)};
        lattice::lattice_db b{protocol_config(b_file)};
        relay.add(TestPerson{"shared-initial", 10, std::nullopt});
        shared_row = row_id(relay, "shared-initial");
        const auto initial = lattice::events_after(relay.db(), std::nullopt);
        ASSERT_EQ(initial.size(), 1u);
        register_receiver(a);
        register_receiver(b);
        ASSERT_EQ(lattice::apply_remote_changes_for(a, initial, receive_channel),
                  (std::vector<std::string>{initial.front().global_id}));
        ASSERT_EQ(lattice::apply_remote_changes_for(b, initial, receive_channel),
                  (std::vector<std::string>{initial.front().global_id}));

        // Both local writes exist BEFORE either peer sees the other's write.
        // Capture actual trigger-created audit identities and payloads, not
        // invented remote entries. 'name' is an ordinary history-bearing field.
        a.db().execute("UPDATE TestPerson SET name = 'writer-A' WHERE globalId = ?", {shared_row});
        b.db().execute("UPDATE TestPerson SET name = 'writer-B' WHERE globalId = ?", {shared_row});
        const auto local_a = lattice::query_audit_log(a.db(), true, std::nullopt);
        const auto local_b = lattice::query_audit_log(b.db(), true, std::nullopt);
        ASSERT_EQ(local_a.size(), 1u);
        ASSERT_EQ(local_b.size(), 1u);
        const auto& op_a = local_a.front();
        const auto& op_b = local_b.front();
        a_operation_id = op_a.global_id;
        b_operation_id = op_b.global_id;
        ASSERT_NE(a_operation_id, b_operation_id);
        ASSERT_EQ(op_a.operation, "UPDATE");
        ASSERT_EQ(op_b.operation, "UPDATE");
        ASSERT_EQ(op_a.global_row_id, shared_row);
        ASSERT_EQ(op_b.global_row_id, shared_row);
        ASSERT_EQ(std::get<std::string>(op_a.changed_fields.at("name").value), "writer-A");
        ASSERT_EQ(std::get<std::string>(op_b.changed_fields.at("name").value), "writer-B");

        // A canonical relay order is insufficient if a peer subsequently
        // deduplicates its own optimistic operation without reconciling it.
        ASSERT_EQ(lattice::apply_remote_changes(relay, {op_a}),
                  (std::vector<std::string>{a_operation_id}));
        ASSERT_EQ(lattice::apply_remote_changes_for(b, {op_a}, receive_channel),
                  (std::vector<std::string>{a_operation_id}));
        ASSERT_EQ(lattice::apply_remote_changes(relay, {op_b}),
                  (std::vector<std::string>{b_operation_id}));
        ASSERT_EQ(lattice::apply_remote_changes_for(a, {op_b}, receive_channel),
                  (std::vector<std::string>{b_operation_id}));
        const auto canonical = lattice::events_after(relay.db(), std::nullopt);
        ASSERT_EQ(canonical.size(), 3u);
        ASSERT_EQ(canonical.at(1).global_id, a_operation_id);
        ASSERT_EQ(canonical.at(2).global_id, b_operation_id);
        const std::vector<std::string> canonical_ids{
            initial.front().global_id, a_operation_id, b_operation_id};
        for (int replay = 0; replay < 2; ++replay) {
            EXPECT_EQ(lattice::apply_remote_changes_for(a, canonical, receive_channel), canonical_ids);
            EXPECT_EQ(lattice::apply_remote_changes_for(b, canonical, receive_channel), canonical_ids);
        }

        // This test deliberately does not choose A or B as the winner. It
        // requires a common result after the same complete canonical history.
        EXPECT_EQ(row_name(a, shared_row), row_name(b, shared_row));
        EXPECT_EQ(row_name(a, shared_row), row_name(relay, shared_row));
        for (auto* peer : {&a, &b, &relay}) {
            EXPECT_EQ(audit_count(*peer, a_operation_id), 1);
            EXPECT_EQ(audit_count(*peer, b_operation_id), 1);
            expect_settled(*peer);
        }
    }
    {
        lattice::lattice_db relay{protocol_config(relay_file)};
        lattice::lattice_db a{protocol_config(a_file)};
        lattice::lattice_db b{protocol_config(b_file)};
        EXPECT_EQ(row_name(a, shared_row), row_name(b, shared_row));
        EXPECT_EQ(row_name(a, shared_row), row_name(relay, shared_row));
        for (auto* peer : {&a, &b, &relay}) {
            EXPECT_EQ(audit_count(*peer, a_operation_id), 1);
            EXPECT_EQ(audit_count(*peer, b_operation_id), 1);
            expect_settled(*peer);
        }
    }
}
