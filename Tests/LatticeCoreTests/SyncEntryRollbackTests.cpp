#include "TestHelpers.hpp"
#include <lattice/sync.hpp>
#include <stdexcept>

namespace lattice {
// Inspect only deferred, non-SQL state; production SQL and hooks remain real.
struct sync_entry_rollback_test_access {
    static size_t buffered(lattice_db& db) {
        std::lock_guard<std::mutex> lock(db.change_buffer_mutex_);
        return db.change_buffer_.size();
    }
    static bool unknown_link(lattice_db& db, const std::string& table) {
        return db.link_tables_unknown_target_.count(table) != 0;
    }
    static bool virtual_link(lattice_db& db, const std::string& table) {
        return db.virtual_link_tables_.count(table) != 0;
    }
    static int64_t cursor(lattice_db& db) { return db.last_seen_audit_id_.load(); }
};
}

namespace {
constexpr const char* channel = "entry-rollback-receiver";
constexpr const char* bad_id = "00000000-0000-4000-8000-000000000501";
constexpr const char* good_id = "00000000-0000-4000-8000-000000000502";
constexpr const char* first_id = "00000000-0000-4000-8000-000000000503";
using access = lattice::sync_entry_rollback_test_access;

lattice::configuration config(const std::string& path) {
    lattice::configuration result(path);
    result.audit_retention_seconds = 0;
    result.busy_timeout_ms = 100;
    return result;
}

int64_t scalar(lattice::database& db, const std::string& sql,
               const std::vector<lattice::column_value_t>& params = {}) {
    return std::get<int64_t>(db.query(sql, params).at(0).at("n"));
}

void stop_notifier(lattice::lattice_db& db) {
    if (db.config().is_in_memory()) return;
    auto* notifier = lattice::instance_registry::instance().get_or_create_notifier(db.config().path);
    ASSERT_NE(notifier, nullptr);
    notifier->stop_listening();
    ASSERT_FALSE(notifier->is_listening());
}

void register_receiver(lattice::lattice_db& db) {
    lattice::ensure_cursor_column(db.db());
    lattice::register_replication_slot(db.db(), channel);
}

lattice::audit_log_entry seeded_update(lattice::lattice_db& db, const std::string& operation,
                                      const std::string& name) {
    db.add(TestPerson{name, 1, std::nullopt});
    lattice::audit_log_entry entry;
    entry.global_id = operation;
    entry.table_name = "TestPerson";
    entry.operation = "UPDATE";
    entry.global_row_id = std::get<std::string>(db.db().query(
        "SELECT globalId FROM TestPerson WHERE name = ?", {name}).at(0).at("globalId"));
    entry.changed_fields_names = {"name"};
    entry.changed_fields = {{"name", lattice::any_property(name + "-changed")}};
    entry.timestamp = "1789819200.0";
    return entry;
}

std::string name(lattice::lattice_db& db, const lattice::audit_log_entry& entry) {
    return std::get<std::string>(db.db().query("SELECT name FROM TestPerson WHERE globalId = ?",
        {entry.global_row_id}).at(0).at("name"));
}

struct Observer {
    lattice::lattice_db& db;
    std::vector<std::string> ids;
    lattice::lattice_db::observer_id token;
    explicit Observer(lattice::lattice_db& owner) : db(owner) {
        token = db.add_table_observer("TestPerson", [this](const auto& changes) {
            for (const auto& change : changes) ids.push_back(std::get<3>(change));
        });
    }
    ~Observer() { db.remove_table_observer("TestPerson", token); }
};

struct Fault {
    lattice::database& db;
    int hits = 0;
    explicit Fault(lattice::database& writer, bool whole_transaction = false, bool audit = false) : db(writer) {
        const int rc = sqlite3_create_function_v2(db.handle(), "entry_fault", 0, SQLITE_UTF8, &hits,
            [](sqlite3_context* c, int, sqlite3_value**) noexcept {
                ++*static_cast<int*>(sqlite3_user_data(c));
                sqlite3_result_int(c, 1);
            }, nullptr, nullptr, nullptr);
        if (rc != SQLITE_OK) throw std::runtime_error("register entry fault counter");
        const std::string target = audit ? "AuditLog" : "_lattice_sync_state";
        const std::string predicate = audit ? "NEW.globalId = '" + std::string(bad_id) + "'" :
            "EXISTS(SELECT 1 FROM AuditLog WHERE id = NEW.audit_entry_id AND globalId = '" +
                std::string(bad_id) + "')";
        db.execute("CREATE TRIGGER reject_entry BEFORE INSERT ON " + target + " WHEN " + predicate +
            " BEGIN SELECT entry_fault(); SELECT RAISE(" + (whole_transaction ? "ROLLBACK" : "ABORT") +
            ", 'entry bookkeeping rejected'); END");
    }
    ~Fault() {
        sqlite3_create_function_v2(db.handle(), "entry_fault", 0, SQLITE_UTF8,
                                  nullptr, nullptr, nullptr, nullptr, nullptr);
    }
};

void observer_tail_case(const std::string& path) {
    lattice::lattice_db db{config(path)};
    ASSERT_NO_FATAL_FAILURE(stop_notifier(db));
    const auto first = seeded_update(db, first_id, "first");
    const auto bad = seeded_update(db, bad_id, "bad");
    const auto good = seeded_update(db, good_id, "good");
    register_receiver(db);
    Fault fault(db.db());
    Observer observed(db);
    EXPECT_EQ(lattice::apply_remote_changes_for(db, {first, bad, good}, channel),
              (std::vector<std::string>{first_id, good_id}));
    EXPECT_EQ(fault.hits, 1);
    EXPECT_EQ(observed.ids, (std::vector<std::string>{first.global_row_id, good.global_row_id}));
    EXPECT_EQ(name(db, bad), "bad");
    EXPECT_EQ(access::buffered(db), 0u);
    EXPECT_FALSE(db.applying_remote_changes_.load());
    db.db().execute("DROP TRIGGER reject_entry");
    EXPECT_EQ(lattice::apply_remote_changes_for(db, {bad}, channel), (std::vector<std::string>{bad_id}));
    EXPECT_EQ(observed.ids, (std::vector<std::string>{first.global_row_id, good.global_row_id, bad.global_row_id}));
    EXPECT_EQ(lattice::apply_remote_changes_for(db, {bad}, channel), (std::vector<std::string>{bad_id}));
    EXPECT_EQ(observed.ids.size(), 3u) << "dedup does not replay observers";
}

// Deny only the exact cleanup boundary being tested, through SQLite's
// authorizer; trigger faults still prove model and bookkeeping were reached.
struct DenyBoundary {
    sqlite3* handle;
    const char* savepoint_action;
    bool deny_outer_rollback;
    int savepoint_hits = 0;
    int outer_hits = 0;
    DenyBoundary(lattice::database& db, const char* action, bool deny_outer = false)
        : handle(db.handle()), savepoint_action(action), deny_outer_rollback(deny_outer) {
        const auto rc = sqlite3_set_authorizer(handle,
            [](void* p, int action, const char* first, const char* second, const char*, const char*) noexcept {
                auto& self = *static_cast<DenyBoundary*>(p);
                if (action == SQLITE_SAVEPOINT && first && second &&
                    std::strcmp(first, self.savepoint_action) == 0 &&
                    std::strcmp(second, "lattice_sync_entry") == 0) {
                    ++self.savepoint_hits;
                    return SQLITE_DENY;
                }
                if (action == SQLITE_TRANSACTION && first && std::strcmp(first, "ROLLBACK") == 0) {
                    ++self.outer_hits;
                    if (self.deny_outer_rollback) return SQLITE_DENY;
                }
                return SQLITE_OK;
            }, this);
        if (rc != SQLITE_OK) throw std::runtime_error("register authorizer");
    }
    ~DenyBoundary() { sqlite3_set_authorizer(handle, nullptr, nullptr); }
};
}

TEST(SyncEntryRollback, FileObserverTailExcludesFailedEntryAndPreservesNeighbors) {
    TempDB file{"sync_entry_observers"};
    observer_tail_case(file.str());
}

TEST(SyncEntryRollback, MemoryObserverTailExcludesFailedEntryAndPreservesNeighbors) {
    observer_tail_case(":memory:");
}

TEST(SyncEntryRollback, SoleFailedEntryDoesNotHideExternalReusedAuditIDFromEitherOwner) {
    ASSERT_EQ(std::getenv("LATTICE_DISABLE_XPROC"), nullptr);
    TempDB file{"sync_entry_cursor"};
    lattice::lattice_db db{config(file.str())};
    const auto bad = seeded_update(db, bad_id, "bad");
    register_receiver(db);
    lattice::lattice_db sibling{config(file.str())};
    ASSERT_NO_FATAL_FAILURE(stop_notifier(db));
    Fault fault(db.db());
    Observer own(db), peer(sibling);
    const auto before = scalar(db.db(), "SELECT MAX(id) AS n FROM AuditLog");
    ASSERT_EQ(access::cursor(db), before);
    ASSERT_EQ(access::cursor(sibling), before);
    EXPECT_TRUE(lattice::apply_remote_changes_for(db, {bad}, channel).empty());
    ASSERT_EQ(fault.hits, 1);
    ASSERT_EQ(scalar(db.db(), "SELECT MAX(id) AS n FROM AuditLog"), before);
    EXPECT_EQ(access::cursor(db), before);
    EXPECT_EQ(access::cursor(sibling), before);
    EXPECT_TRUE(own.ids.empty());
    EXPECT_TRUE(peer.ids.empty());
    ASSERT_EQ(access::buffered(db), 0u);

    // Independent SQLite connection, no Lattice registry/update hook. The
    // normal model trigger creates the external audit row and reuses the ID.
    sqlite3* raw = nullptr;
    const int opened = sqlite3_open_v2(file.str().c_str(), &raw,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, nullptr);
    std::unique_ptr<sqlite3, decltype(&sqlite3_close)> external(raw, sqlite3_close);
    ASSERT_EQ(opened, SQLITE_OK);
    ASSERT_EQ(sqlite3_create_function(raw, "sync_disabled", 0, SQLITE_UTF8, nullptr,
        [](sqlite3_context* c, int, sqlite3_value**) { sqlite3_result_int(c, 0); }, nullptr, nullptr), SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(raw, "INSERT INTO TestPerson(globalId, name, age) "
        "VALUES('external-reused-id', 'external', 9)", nullptr, nullptr, nullptr), SQLITE_OK)
        << sqlite3_errmsg(raw);
    ASSERT_EQ(scalar(db.db(), "SELECT MAX(id) AS n FROM AuditLog"), before + 1);
    db.handle_cross_process_notification();
    sibling.handle_cross_process_notification();
    EXPECT_EQ(own.ids, (std::vector<std::string>{"external-reused-id"}));
    EXPECT_EQ(peer.ids, (std::vector<std::string>{"external-reused-id"}));
    db.handle_cross_process_notification();
    sibling.handle_cross_process_notification();
    EXPECT_EQ(own.ids.size(), 1u);
    EXPECT_EQ(peer.ids.size(), 1u);
}

TEST(SyncEntryRollback, RollbackTriggerAbortsWholeChunkAndSameOperationsCanRetry) {
    lattice::lattice_db db{config(":memory:")};
    const auto first = seeded_update(db, first_id, "first");
    const auto bad = seeded_update(db, bad_id, "bad");
    const auto good = seeded_update(db, good_id, "good");
    register_receiver(db);
    Fault fault(db.db(), true);
    Observer observed(db);
    EXPECT_TRUE(lattice::apply_remote_changes_for(db, {first, bad, good}, channel).empty());
    EXPECT_EQ(fault.hits, 2) << "one bounded retry of the whole chunk";
    EXPECT_FALSE(db.db().is_in_transaction());
    EXPECT_EQ(name(db, first), "first");
    EXPECT_EQ(name(db, bad), "bad");
    EXPECT_EQ(name(db, good), "good") << "no successor SQL runs in autocommit after RAISE(ROLLBACK)";
    EXPECT_TRUE(observed.ids.empty());
    EXPECT_EQ(access::buffered(db), 0u);
    EXPECT_EQ(scalar(db.db(), "SELECT disabled AS n FROM _SyncControl WHERE id=1"), 0);
    db.db().execute("DROP TRIGGER reject_entry");
    EXPECT_EQ(lattice::apply_remote_changes_for(db, {first, bad, good}, channel),
              (std::vector<std::string>{first_id, bad_id, good_id}));
    EXPECT_EQ(observed.ids, (std::vector<std::string>{first.global_row_id, bad.global_row_id, good.global_row_id}));
}

TEST(SyncEntryRollback, SavepointCleanupFailureEscalatesToOwnedChunkRollback) {
    for (const char* boundary : {"ROLLBACK", "RELEASE"}) {
        SCOPED_TRACE(boundary);
        lattice::lattice_db db{config(":memory:")};
        const auto bad = seeded_update(db, bad_id, "bad");
        const auto good = seeded_update(db, good_id, "good");
        register_receiver(db);
        Fault fault(db.db());
        Observer observed(db);
        {
            DenyBoundary denied(db.db(), boundary);
            EXPECT_TRUE(lattice::apply_remote_changes_for(db, {bad, good}, channel).empty());
            EXPECT_EQ(denied.savepoint_hits, 2);
            EXPECT_EQ(denied.outer_hits, 2);
        }
        EXPECT_EQ(fault.hits, 2);
        EXPECT_FALSE(db.db().is_in_transaction());
        EXPECT_EQ(name(db, bad), "bad");
        EXPECT_EQ(name(db, good), "good");
        EXPECT_TRUE(observed.ids.empty());
        EXPECT_EQ(access::buffered(db), 0u);
        EXPECT_EQ(lattice::apply_remote_changes_for(db, {good}, channel), (std::vector<std::string>{good_id}));
    }
}

TEST(SyncEntryRollback, UnsettledChunkRollbackStopsWithoutRetryOrClosingCallerConnection) {
    lattice::lattice_db db{config(":memory:")};
    const auto bad = seeded_update(db, bad_id, "bad");
    register_receiver(db);
    Fault fault(db.db());
    {
        DenyBoundary denied(db.db(), "ROLLBACK", true);
        EXPECT_TRUE(lattice::apply_remote_changes_for(db, {bad}, channel).empty());
        EXPECT_EQ(denied.savepoint_hits, 1);
        EXPECT_EQ(denied.outer_hits, 1);
        EXPECT_EQ(fault.hits, 1);
        EXPECT_TRUE(db.db().is_in_transaction());
        EXPECT_FALSE(db.applying_remote_changes_.load());
    }
    db.db().rollback();
    EXPECT_FALSE(db.db().is_in_transaction());
    EXPECT_EQ(name(db, bad), "bad");
    EXPECT_EQ(access::buffered(db), 0u);
    db.db().execute("DROP TRIGGER reject_entry");
    EXPECT_EQ(lattice::apply_remote_changes_for(db, {bad}, channel), (std::vector<std::string>{bad_id}));
}

TEST(SyncEntryRollback, ReleaseFailureAfterSuccessfulEntryCannotAckOrCommitIt) {
    lattice::lattice_db db{config(":memory:")};
    const auto good = seeded_update(db, good_id, "good");
    Observer observed(db);
    {
        DenyBoundary denied(db.db(), "RELEASE");
        EXPECT_TRUE(lattice::apply_remote_changes(db, {good}).empty());
        EXPECT_EQ(denied.savepoint_hits, 2);
        EXPECT_EQ(denied.outer_hits, 2);
    }
    EXPECT_FALSE(db.db().is_in_transaction());
    EXPECT_EQ(name(db, good), "good");
    EXPECT_TRUE(observed.ids.empty());
    EXPECT_EQ(access::buffered(db), 0u);
    EXPECT_EQ(lattice::apply_remote_changes(db, {good}), (std::vector<std::string>{good_id}));
    EXPECT_EQ(observed.ids, (std::vector<std::string>{good.global_row_id}));
}

TEST(SyncEntryRollback, AdmissionDoesNotModifyOrRollbackCallersExistingTransaction) {
    lattice::lattice_db db{config(":memory:")};
    const auto good = seeded_update(db, good_id, "good");
    db.db().begin_transaction();
    db.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
    EXPECT_TRUE(lattice::apply_remote_changes(db, {good}).empty());
    EXPECT_TRUE(db.db().is_in_transaction());
    EXPECT_EQ(scalar(db.db(), "SELECT disabled AS n FROM _SyncControl WHERE id=1"), 1);
    EXPECT_FALSE(db.db().table_exists("_lattice_applied_receipts"));
    EXPECT_FALSE(db.applying_remote_changes_.load());
    db.db().rollback();
    EXPECT_EQ(name(db, good), "good");
}

TEST(SyncEntryRollback, ReadOnlyFacadeRefusesApplyWithoutWriterHookContext) {
    TempDB file{"sync_entry_read_only"};
    lattice::audit_log_entry good;
    {
        lattice::lattice_db writer{config(file.str())};
        ASSERT_NO_FATAL_FAILURE(stop_notifier(writer));
        good = seeded_update(writer, good_id, "good");
    }
    auto read_only = config(file.str());
    read_only.read_only = true;
    lattice::lattice_db reader{read_only};
    EXPECT_TRUE(lattice::apply_remote_changes(reader, {good}).empty());
    EXPECT_FALSE(reader.db().is_in_transaction());
    EXPECT_FALSE(reader.applying_remote_changes_.load());
    EXPECT_EQ(name(reader, good), "good");
    EXPECT_EQ(scalar(reader.db(), "SELECT COUNT(*) AS n FROM AuditLog WHERE globalId=?",
                     {std::string(good_id)}), 0);
    EXPECT_FALSE(reader.db().table_exists("_lattice_applied_receipts"));
}

TEST(SyncEntryRollback, MemoryCallbackExceptionPreservesCommitAndCallbackSuccessorTransaction) {
    lattice::lattice_db db{config(":memory:")};
    const auto good = seeded_update(db, good_id, "good");
    int calls = 0;
    const auto token = db.add_table_observer("TestPerson", [&](const auto&) {
        ++calls;
        db.db().begin_transaction();
        db.db().execute("INSERT INTO _lattice_meta(key,value) VALUES('callback-successor','pending')");
        throw std::runtime_error("postcommit observer failure");
    });
    EXPECT_EQ(lattice::apply_remote_changes(db, {good}), (std::vector<std::string>{good_id}));
    db.remove_table_observer("TestPerson", token);
    EXPECT_EQ(calls, 1);
    EXPECT_TRUE(db.db().is_in_transaction());
    EXPECT_EQ(name(db, good), "good-changed");
    EXPECT_EQ(scalar(db.db(), "SELECT COUNT(*) AS n FROM _lattice_meta WHERE key='callback-successor'"), 1);
    db.db().rollback();
    EXPECT_EQ(name(db, good), "good-changed") << "only the callback's transaction rolls back";
    EXPECT_EQ(scalar(db.db(), "SELECT COUNT(*) AS n FROM AuditLog WHERE globalId=?", {std::string(good_id)}), 1);
}

TEST(SyncEntryRollback, FileCallbackSuccessorRefusesNextChunkWithoutLosingCommittedAcks) {
    TempDB file{"sync_entry_callback_successor"};
    lattice::lattice_db db{config(file.str())};
    ASSERT_NO_FATAL_FAILURE(stop_notifier(db));
    const auto seeded = seeded_update(db, good_id, "good");
    std::vector<lattice::audit_log_entry> entries;
    std::vector<std::string> committed_ids;
    for (int i = 0; i < 51; ++i) {
        auto entry = seeded;
        entry.global_id = "entry-callback-operation-" + std::to_string(i);
        entry.changed_fields["name"] = lattice::any_property("value-" + std::to_string(i));
        entries.push_back(entry);
        if (i < 50) committed_ids.push_back(entry.global_id);
    }
    int calls = 0;
    const auto token = db.add_table_observer("TestPerson", [&](const auto&) {
        ++calls;
        db.db().begin_transaction();
        db.db().execute("INSERT INTO _lattice_meta(key,value) VALUES('file-callback-successor','pending')");
    });
    EXPECT_EQ(lattice::apply_remote_changes(db, entries), committed_ids);
    db.remove_table_observer("TestPerson", token);
    EXPECT_EQ(calls, 1);
    EXPECT_TRUE(db.db().is_in_transaction());
    EXPECT_EQ(name(db, seeded), "value-49");
    EXPECT_EQ(scalar(db.db(), "SELECT COUNT(*) AS n FROM _lattice_meta WHERE key='file-callback-successor'"), 1);
    db.db().rollback();
    EXPECT_EQ(name(db, seeded), "value-49");
    EXPECT_EQ(scalar(db.db(), "SELECT COUNT(*) AS n FROM AuditLog WHERE globalId LIKE 'entry-callback-operation-%'"), 50);
}

TEST(SyncEntryRollback, LinkDDLAndMembershipRollbackAllowSameTableSuccessorAndRetry) {
    for (bool polymorphic : {false, true}) {
        SCOPED_TRACE(polymorphic);
        lattice::lattice_db db{config(":memory:")};
        lattice::audit_log_entry bad;
        bad.global_id = bad_id;
        bad.global_row_id = "entry-link-bad";
        bad.table_name = polymorphic ? "_entry_virtual_links" : "_entry_regular_links";
        bad.operation = "INSERT";
        bad.changed_fields_names = {"lhs", "rhs"};
        bad.changed_fields = {{"lhs", lattice::any_property(std::string("parent"))},
                              {"rhs", lattice::any_property(std::string("target"))}};
        if (polymorphic) {
            bad.changed_fields_names.push_back("rhs_type");
            bad.changed_fields["rhs_type"] = lattice::any_property(std::string("TestPerson"));
        }
        Fault fault(db.db(), false, true);
        EXPECT_TRUE(lattice::apply_remote_changes(db, {bad}).empty());
        EXPECT_EQ(fault.hits, 1);
        EXPECT_FALSE(db.db().table_exists(bad.table_name));
        EXPECT_FALSE(access::unknown_link(db, bad.table_name));
        EXPECT_FALSE(access::virtual_link(db, bad.table_name));
        EXPECT_EQ(scalar(db.db(), "SELECT COUNT(*) AS n FROM _lattice_meta WHERE key=?",
                         {"internal_table:" + bad.table_name}), 0);
        auto good = bad;
        good.global_id = good_id;
        good.global_row_id = "entry-link-good";
        good.changed_fields["rhs"] = lattice::any_property(std::string("other-target"));
        EXPECT_EQ(lattice::apply_remote_changes(db, {bad, good}), (std::vector<std::string>{good_id}));
        EXPECT_EQ(fault.hits, 2);
        EXPECT_TRUE(db.db().table_exists(bad.table_name));
        EXPECT_EQ(scalar(db.db(), "SELECT COUNT(*) AS n FROM " + bad.table_name), 1);
        EXPECT_EQ(access::virtual_link(db, bad.table_name), polymorphic);
        EXPECT_EQ(access::unknown_link(db, bad.table_name), !polymorphic);
        db.db().execute("DROP TRIGGER reject_entry");
        EXPECT_EQ(lattice::apply_remote_changes(db, {bad}), (std::vector<std::string>{bad_id}));
        EXPECT_EQ(scalar(db.db(), "SELECT COUNT(*) AS n FROM " + bad.table_name), 2);
    }
}
