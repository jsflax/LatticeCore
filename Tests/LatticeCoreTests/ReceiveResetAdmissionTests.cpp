#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"
#include <lattice/sync.hpp>
#include <array>
#include <functional>
#include <memory>

namespace {
using namespace lattice;
namespace hooks = detail::recovery_channel_reset_test_hooks;
constexpr const char* reset_channel = "reset-successor-channel";

thread_local std::function<void()> admission_action;
struct admission_action_scope {
    void (*previous_hook)();
    std::function<void()> previous_action;
    explicit admission_action_scope(std::function<void()> action)
        : previous_hook(hooks::after_write_admission),
          previous_action(std::move(admission_action)) {
        admission_action = std::move(action);
        hooks::after_write_admission = [] { admission_action(); };
    }
    ~admission_action_scope() {
        hooks::after_write_admission = previous_hook;
        admission_action = std::move(previous_action);
    }
};

// The fixture owns both explicit transactions. Always settle its remaining
// transaction on an assertion exit; the production reset must never do so.
struct fixture_rollback {
    lattice_db& owner;
    ~fixture_rollback() {
        try { if (owner.db().is_in_transaction()) owner.rollback(); }
        catch (...) {}
    }
};

int64_t scalar(database& writer, const std::string& sql) {
    return std::get<int64_t>(writer.query(sql).at(0).at("n"));
}

auto channel_rows(database& writer) {
    const std::vector<column_value_t> args{std::string(reset_channel)};
    return std::array<std::vector<database::row_t>, 3>{
        writer.query("SELECT * FROM _lattice_sync_state WHERE sync_id=? ORDER BY audit_entry_id", args),
        writer.query("SELECT * FROM _lattice_sync_set WHERE sync_id=? ORDER BY table_name,global_row_id", args),
        writer.query("SELECT * FROM _lattice_replication_slots WHERE sync_id=?", args)};
}

void successor_case(const std::string& path, bool commit_prior, bool write_prior) {
    SCOPED_TRACE(::testing::Message() << "path=" << path
        << " commit_prior=" << commit_prior << " write_prior=" << write_prior);
    configuration config(path);
    config.audit_retention_seconds = 0;
    config.busy_timeout_ms = 100;
    auto owner = std::make_shared<lattice_db>(config);
    if (!config.is_in_memory()) {
        auto* notifier = instance_registry::instance().get_or_create_notifier(path);
        if (notifier) notifier->stop_listening();
    }
    owner->add(TestPerson{"reset-owner-seed", 1, std::nullopt});
    auto& writer = owner->db();
    const auto seed = writer.query("SELECT globalId FROM TestPerson WHERE name='reset-owner-seed'");
    ASSERT_EQ(seed.size(), 1u);
    const auto target = std::get<std::string>(seed[0].at("globalId"));
    const auto audit_id = scalar(writer, "SELECT MAX(id) AS n FROM AuditLog");
    ASSERT_GT(audit_id, 0);
    writer.execute("INSERT INTO _lattice_sync_state VALUES(?,?,0)",
        {audit_id, std::string(reset_channel)});
    writer.execute("INSERT INTO _lattice_sync_set VALUES(?,'TestPerson',?)",
        {std::string(reset_channel), target});
    register_replication_slot(writer, reset_channel);
    writer.execute("UPDATE _lattice_replication_slots SET confirmed_audit_id=9,upload_floor=7 WHERE sync_id=?",
        {std::string(reset_channel)});
    // An ordinary underscore fixture table has no model/audit semantics.
    // It is created before the prior transaction: no-write COMMIT really has
    // no writes and therefore need not produce a file WAL callback.
    writer.execute("CREATE TABLE _receive_reset_successor (key TEXT PRIMARY KEY,value INTEGER NOT NULL) WITHOUT ROWID");
    const auto before = channel_rows(writer);
    ASSERT_EQ(before[0].size(), 1u);
    ASSERT_EQ(before[1].size(), 1u);
    ASSERT_EQ(before[2].size(), 1u);

    owner->begin_transaction();
    fixture_rollback cleanup{*owner};
    if (write_prior) writer.execute("UPDATE TestPerson SET age=2 WHERE globalId=?", {target});
    int callback_stage = 0;
    int callback_count = 0;
    {
        admission_action_scope action([&] {
            ++callback_count;
            callback_stage = 1;
            if (commit_prior) owner->commit();
            else owner->rollback();
            callback_stage = 2;
            owner->begin_transaction();
            writer.execute("INSERT INTO _receive_reset_successor VALUES('pending',1)");
            callback_stage = 3;
        });
        EXPECT_THROW(owner->reset_sync_state(reset_channel), db_error);
    }
    ASSERT_EQ(callback_count, 1);
    ASSERT_EQ(callback_stage, 3) << "the real prior settlement and successor write must both complete";
    EXPECT_TRUE(writer.is_in_transaction());
    EXPECT_EQ(detail::recovery_writer_access::active_writer(*owner), &writer);
    EXPECT_EQ(scalar(writer, "SELECT COUNT(*) AS n FROM _receive_reset_successor WHERE key='pending'"), 1);
    // Refusal must precede all reset mutations, including its slot floors and
    // deletion of per-channel pending state/membership in the successor.
    EXPECT_EQ(channel_rows(writer), before);
    EXPECT_EQ(scalar(writer, "SELECT age AS n FROM TestPerson WHERE name='reset-owner-seed'"),
        commit_prior && write_prior ? 2 : 1);

    ASSERT_NO_THROW(owner->rollback()); // Only the fixture settles its successor.
    EXPECT_FALSE(writer.is_in_transaction());
    EXPECT_EQ(scalar(writer, "SELECT COUNT(*) AS n FROM _receive_reset_successor"), 0);
    EXPECT_EQ(channel_rows(writer), before);
    EXPECT_EQ(scalar(writer, "SELECT age AS n FROM TestPerson WHERE name='reset-owner-seed'"),
        commit_prior && write_prior ? 2 : 1);
}
} // namespace

TEST(ReceiveResetAdmission, MemoryCommitWithPriorWriteRefusesSuccessor) {
    successor_case(":memory:", true, true);
}
TEST(ReceiveResetAdmission, MemoryCommitWithoutPriorWriteRefusesSuccessor) {
    successor_case(":memory:", true, false);
}
TEST(ReceiveResetAdmission, MemoryRollbackWithPriorWriteRefusesSuccessor) {
    successor_case(":memory:", false, true);
}
TEST(ReceiveResetAdmission, MemoryRollbackWithoutPriorWriteRefusesSuccessor) {
    successor_case(":memory:", false, false);
}
TEST(ReceiveResetAdmission, FileCommitWithPriorWriteRefusesSuccessor) {
    TempDB file{"receive_reset_commit_write"};
    successor_case(file.str(), true, true);
}
TEST(ReceiveResetAdmission, FileCommitWithoutPriorWriteRefusesSuccessorWithoutWalCommit) {
    TempDB file{"receive_reset_commit_empty"};
    successor_case(file.str(), true, false);
}
TEST(ReceiveResetAdmission, FileRollbackWithPriorWriteRefusesSuccessor) {
    TempDB file{"receive_reset_rollback_write"};
    successor_case(file.str(), false, true);
}
TEST(ReceiveResetAdmission, FileRollbackWithoutPriorWriteRefusesSuccessor) {
    TempDB file{"receive_reset_rollback_empty"};
    successor_case(file.str(), false, false);
}
