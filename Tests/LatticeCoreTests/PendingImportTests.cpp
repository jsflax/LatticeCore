// Synthetic actual-Core rescue/export regressions. TMPDIR owns every fixture.
#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
#include <gtest/gtest.h>
#include <lattice.hpp>
#include <lattice/sync.hpp>
#include <nlohmann/json.hpp>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <limits>
#include <thread>

namespace lattice {
struct audit_maintenance_test_access {
    static int64_t cursor(const lattice_db& db) { return db.last_seen_audit_id_.load(); }
};
} // namespace lattice

namespace {
using namespace lattice;
using json = nlohmann::json;
struct File {
    std::string path;
    File() {
        static std::atomic<unsigned> serial{0};
        path = (std::filesystem::temp_directory_path() / ("pending-import-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
            std::to_string(serial++) + ".sqlite")).string();
        if (std::filesystem::exists(path)) throw std::runtime_error("Fixture must be fresh");
    }
    ~File() { std::error_code ec; for (auto suffix : {"", "-wal", "-shm"}) std::filesystem::remove(path + suffix, ec); }
};
swift_configuration config(const File& file) {
    configuration c(file.path); c.audit_retention_seconds = 0; return swift_configuration(c);
}
SchemaVector schema() {
    swift_schema_entry s; s.table_name = "PendingRow";
    for (const auto* name : {"title", "body", "required", "amount"}) {
        property_descriptor d; d.name = name; d.kind = property_kind::primitive;
        d.type = std::string(name) == "amount" ? column_type::integer : column_type::text;
        d.nullable = std::string(name) == "body";
        d.no_history = std::string(name) == "body" || std::string(name) == "required";
        s.properties[name] = d;
    }
    return {s};
}
void seed(lattice::swift_lattice& db) {
    db.db().execute("INSERT INTO PendingRow(globalId,title,body,required,amount) VALUES('row','first','body','kept',0)");
}
int64_t scalar(database& db, const std::string& sql, const std::vector<column_value_t>& args = {}) {
    return std::get<int64_t>(db.query(sql, args).at(0).at("n"));
}
audit_log_entry change(const std::string& id, const std::string& title = "changed") {
    audit_log_entry e; e.global_id = id; e.table_name = "PendingRow"; e.global_row_id = "row";
    e.operation = "UPDATE"; e.changed_fields_names = {"title"};
    e.changed_fields = {{"title", any_property(title)}}; e.timestamp = "0.125"; return e;
}
std::string title(database& db) {
    return std::get<std::string>(db.query("SELECT title FROM PendingRow WHERE globalId='row'")[0].at("title"));
}
std::pair<int64_t, std::string> latest(database& db) {
    const auto row = db.query("SELECT id,globalId FROM AuditLog WHERE tableName='PendingRow' ORDER BY id DESC LIMIT 1")[0];
    return {std::get<int64_t>(row.at("id")), std::get<std::string>(row.at("globalId"))};
}

TEST(PendingImport, IdenticalValuesStillCreateUploadEligibleJournalWithoutSyncMarks) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    auto floor = scalar(db.db(), "SELECT max(id) AS n FROM AuditLog");
    auto e = change("rescue-identical", "first");
    auto result = apply_pending_changes(db, {e});
    EXPECT_EQ(result.imported_ids, std::vector<std::string>{e.global_id});
    EXPECT_TRUE(result.already_present_ids.empty()); EXPECT_TRUE(result.rejected_ids.empty());
    auto row = db.db().query("SELECT id,isFromRemote,isSynchronized,timestamp FROM AuditLog WHERE globalId=?", {e.global_id})[0];
    EXPECT_GT(std::get<int64_t>(row.at("id")), floor);
    EXPECT_EQ(std::get<int64_t>(row.at("isFromRemote")), 1);
    EXPECT_EQ(std::get<int64_t>(row.at("isSynchronized")), 0);
    EXPECT_EQ(scalar(db.db(), "SELECT count(*) AS n FROM _lattice_sync_state"), 0);
    EXPECT_EQ(scalar(db.db(), "SELECT count(*) AS n FROM _lattice_applied_receipts"), 0);
    auto upload = query_audit_log_for_sync(db.db(), "rescue:upload", std::nullopt, floor, 10);
    ASSERT_EQ(upload.size(), 1u); EXPECT_EQ(upload[0].global_id, e.global_id);
}

TEST(PendingImport, NativeFrameKeepsAllInt64Bits) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    int sequence = 0;
    for (const int64_t n : {std::numeric_limits<int64_t>::min(), int64_t(-9007199254740993LL),
                           int64_t(9007199254740993LL), std::numeric_limits<int64_t>::max()}) {
        auto e = change("integer-" + std::to_string(sequence++)); e.changed_fields_names = {"amount"};
        e.changed_fields = {{"amount", any_property(n)}};
        auto parsed = audit_log_entry::from_json(e.to_json()); ASSERT_TRUE(parsed);
        EXPECT_EQ(apply_pending_changes(db, {*parsed}).imported_ids.size(), 1u);
        EXPECT_EQ(scalar(db.db(), "SELECT amount AS n FROM PendingRow WHERE globalId='row'"), n);
        auto selected = query_audit_log_selected(db.db(), {latest(db.db())});
        EXPECT_EQ(std::get<int64_t>(selected[0].changed_fields.at("amount").value), n);
        EXPECT_EQ(selected[0].timestamp, "0.125");
    }
}

TEST(PendingImport, NullableClearAndLegacyRequiredGuardsShareCorePolicy) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    for (bool marked : {false, true}) {
        auto e = change(marked ? "marked" : "legacy");
        e.changed_fields_names = {"body", "required"};
        e.changed_fields = {{"body", any_property(nullptr)}, {"required", any_property(nullptr)}};
        if (marked) e.no_history_nulls = {"body", "required"};
        ASSERT_EQ(apply_pending_changes(db, {e}).imported_ids.size(), 1u);
        auto row = db.db().query("SELECT body,required FROM PendingRow WHERE globalId='row'")[0];
        EXPECT_EQ(std::get<std::string>(row.at("required")), "kept");
        EXPECT_EQ(std::holds_alternative<std::nullptr_t>(row.at("body")), marked);
    }
    const auto selected = query_audit_log_selected(db.db(), {latest(db.db())});
    EXPECT_EQ(selected[0].no_history_nulls, std::vector<std::string>{"body"});
    EXPECT_EQ(std::get<std::string>(selected[0].changed_fields.at("required").value), "kept");
}

TEST(PendingImport, ExistingJournalOrReceiptNeverReplaysStaleData) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    auto e = change("known-journal"); ASSERT_EQ(apply_pending_changes(db, {e}).imported_ids.size(), 1u);
    db.db().execute("UPDATE PendingRow SET title='newer' WHERE globalId='row'");
    EXPECT_EQ(apply_pending_changes(db, {e}).already_present_ids, std::vector<std::string>{e.global_id});
    e.global_id = "known-receipt";
    db.db().execute("INSERT INTO _lattice_applied_receipts(globalId) VALUES(?)", {e.global_id});
    EXPECT_EQ(apply_pending_changes(db, {e}).already_present_ids, std::vector<std::string>{e.global_id});
    EXPECT_EQ(title(db.db()), "newer");
}

TEST(PendingImport, DuplicatesAndMalformedOrUnsupportedEntriesHaveNoEffects) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    auto e = change("duplicate"); auto other = e; other.changed_fields["title"] = any_property(std::string("other"));
    auto result = apply_pending_changes(db, {e, other});
    EXPECT_EQ(result.rejected_ids, std::vector<std::string>{e.global_id}); EXPECT_EQ(title(db.db()), "first");
    std::vector<audit_log_entry> invalid;
    e = change("unknown-table"); e.table_name = "NoSuchModel"; invalid.push_back(e);
    e = change("unknown-column"); e.changed_fields_names.push_back("missing"); invalid.push_back(e);
    e = change("unknown-value"); e.changed_fields["missing"] = any_property(1); invalid.push_back(e);
    e = change("invalid-marker"); e.no_history_nulls = {"title"}; invalid.push_back(e);
    for (const auto& bad : invalid) {
        EXPECT_EQ(apply_pending_changes(db, {bad}).rejected_ids, std::vector<std::string>{bad.global_id});
        EXPECT_EQ(scalar(db.db(), "SELECT count(*) AS n FROM AuditLog WHERE globalId=?", {bad.global_id}), 0);
    }
    EXPECT_EQ(title(db.db()), "first");
}

TEST(PendingImport, JournalFailureRollsBackModelAndLeavesSourceForRetry) {
    File source_file, target_file;
    lattice::swift_lattice source(config(source_file), schema()), target(config(target_file), schema());
    seed(source); seed(target);
    source.db().execute("UPDATE PendingRow SET title='rescued' WHERE globalId='row'");
    const auto identity = latest(source.db());
    const auto entries = query_audit_log_selected(source.db(), {identity});
    const auto original = entries[0].to_json();
    target.db().execute("CREATE TRIGGER refuse_rescue BEFORE INSERT ON AuditLog WHEN new.globalId='" + identity.second + "' BEGIN SELECT RAISE(ABORT,'journal denied'); END");
    EXPECT_EQ(apply_pending_changes(target, entries).rejected_ids, std::vector<std::string>{identity.second});
    EXPECT_EQ(title(target.db()), "first");
    EXPECT_EQ(scalar(target.db(), "SELECT count(*) AS n FROM AuditLog WHERE globalId=?", {identity.second}), 0);
    EXPECT_EQ(query_audit_log_selected(source.db(), {identity})[0].to_json(), original);
    target.db().execute("DROP TRIGGER refuse_rescue");
    EXPECT_EQ(apply_pending_changes(target, entries).imported_ids.size(), 1u);
    EXPECT_EQ(title(target.db()), "rescued");
}

TEST(PendingImport, FailedAfterJournalInsertDoesNotLeakModelOrAuditObservers) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    // A second same-path owner must receive only committed notifications too.
    lattice::swift_lattice peer(config(file), schema());
    std::vector<lattice_db::change_event> model_events, audit_events, peer_events;
    auto model = db.lattice_db::add_table_observer("PendingRow", [&](const auto& events) { model_events.insert(model_events.end(), events.begin(), events.end()); });
    auto audit = db.lattice_db::add_table_observer("AuditLog", [&](const auto& events) { audit_events.insert(audit_events.end(), events.begin(), events.end()); });
    auto other = peer.lattice_db::add_table_observer("PendingRow", [&](const auto& events) { peer_events.insert(peer_events.end(), events.begin(), events.end()); });
    struct Cleanup { std::function<void()> remove; ~Cleanup() { remove(); } } cleanup{[&] {
        db.remove_table_observer("PendingRow", model); db.remove_table_observer("AuditLog", audit);
        peer.remove_table_observer("PendingRow", other);
    }};
    db.db().execute("CREATE TRIGGER refuse_after_journal AFTER INSERT ON AuditLog WHEN new.globalId='failed-delete' BEGIN SELECT RAISE(ABORT,'after journal denied'); END");
    auto bad = change("failed-delete"); bad.operation = "DELETE"; bad.changed_fields.clear(); bad.changed_fields_names.clear();
    auto good = change("accepted-after-failure", "accepted");
    const auto owner_cursor = audit_maintenance_test_access::cursor(db);
    const auto peer_cursor = audit_maintenance_test_access::cursor(peer);
    EXPECT_EQ(apply_pending_changes(db, {bad}).rejected_ids, std::vector<std::string>{bad.global_id});
    EXPECT_TRUE(model_events.empty()); EXPECT_TRUE(audit_events.empty()); EXPECT_TRUE(peer_events.empty());
    EXPECT_EQ(audit_maintenance_test_access::cursor(db), owner_cursor);
    EXPECT_EQ(audit_maintenance_test_access::cursor(peer), peer_cursor);
    auto result = apply_pending_changes(db, {bad, good});
    EXPECT_EQ(result.rejected_ids, std::vector<std::string>{bad.global_id});
    EXPECT_EQ(result.imported_ids, std::vector<std::string>{good.global_id});
    EXPECT_EQ(title(db.db()), "accepted");
    ASSERT_EQ(model_events.size(), 1u); EXPECT_EQ(std::get<1>(model_events[0]), "UPDATE");
    ASSERT_EQ(peer_events.size(), 1u); EXPECT_EQ(std::get<1>(peer_events[0]), "UPDATE");
    ASSERT_EQ(audit_events.size(), 1u); EXPECT_EQ(std::get<1>(audit_events[0]), "INSERT");
    EXPECT_EQ(std::get<3>(audit_events[0]), good.global_id);
    EXPECT_FALSE(db.defer_early_audit_cursor_.load());
    EXPECT_EQ(scalar(db.db(), "SELECT count(*) AS n FROM AuditLog WHERE globalId='failed-delete'"), 0);
    const auto committed_id = scalar(db.db(), "SELECT max(id) AS n FROM AuditLog");
    EXPECT_EQ(audit_maintenance_test_access::cursor(db), committed_id);
    EXPECT_EQ(audit_maintenance_test_access::cursor(peer), committed_id);
}

TEST(PendingImport, FailedDependencyRemainsRetryableAfterASeparateEntrySucceeds) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    db.db().execute("CREATE TRIGGER require_parent BEFORE UPDATE OF title ON PendingRow WHEN new.title='child' AND NOT EXISTS(SELECT 1 FROM PendingRow WHERE globalId='parent') BEGIN SELECT RAISE(ABORT,'missing parent'); END");
    auto child = change("child-write", "child");
    auto parent = change("parent-write", "parent"); parent.operation = "INSERT"; parent.global_row_id = "parent";
    parent.changed_fields_names = {"title", "body", "required", "amount"};
    parent.changed_fields = {{"title", any_property(std::string("parent"))}, {"body", any_property(nullptr)},
                            {"required", any_property(std::string("kept"))}, {"amount", any_property(int64_t(0))}};
    auto result = apply_pending_changes(db, {child, parent});
    EXPECT_EQ(result.rejected_ids, std::vector<std::string>{child.global_id});
    EXPECT_EQ(result.imported_ids, std::vector<std::string>{parent.global_id});
    EXPECT_EQ(apply_pending_changes(db, {child}).imported_ids.size(), 1u);
    EXPECT_EQ(title(db.db()), "child");
}

TEST(PendingImport, SynthesizedExistingRowsAndFilterRemovalKeepTheirPolicies) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    auto e = change("snapshot", "stale"); e.operation = "INSERT"; e.synthesized = true;
    ASSERT_EQ(apply_pending_changes(db, {e}).imported_ids.size(), 1u);
    EXPECT_EQ(title(db.db()), "first");
    EXPECT_TRUE(query_audit_log_selected(db.db(), {latest(db.db())})[0].synthesized);
    e = change("filter-removal"); e.operation = "DELETE"; e.changed_fields.clear(); e.changed_fields_names = {"__lattice_filter_removal"};
    ASSERT_EQ(apply_pending_changes(db, {e}).imported_ids.size(), 1u);
    EXPECT_EQ(scalar(db.db(), "SELECT count(*) AS n FROM PendingRow"), 0);
    EXPECT_EQ(scalar(db.db(), "SELECT isSynchronized AS n FROM AuditLog WHERE globalId='filter-removal'"), 1);
}

TEST(PendingImport, MissingDestinationUpdateIsJournaledForUpstreamWithoutInventingARow) {
    File rescue_file, upstream_file;
    lattice::swift_lattice rescue(config(rescue_file), schema()), upstream(config(upstream_file), schema());
    seed(upstream);
    auto e = change("missing-local-update", "event survives");
    e.changed_fields_names.push_back("amount"); e.changed_fields["amount"] = any_property(int64_t(9007199254740993LL));
    ASSERT_EQ(apply_pending_changes(rescue, {e}).imported_ids, std::vector<std::string>{e.global_id});
    EXPECT_EQ(scalar(rescue.db(), "SELECT count(*) AS n FROM PendingRow"), 0);
    const auto exported = query_audit_log_selected(rescue.db(), {latest(rescue.db())});
    ASSERT_EQ(exported.size(), 1u);
    EXPECT_EQ(exported[0].changed_fields_to_json(), e.changed_fields_to_json());
    const auto parsed = audit_log_entry::from_json(exported[0].to_json()); ASSERT_TRUE(parsed);
    ASSERT_EQ(apply_remote_changes(upstream, {*parsed}).size(), 1u);
    EXPECT_EQ(title(upstream.db()), "event survives");
    EXPECT_EQ(scalar(upstream.db(), "SELECT amount AS n FROM PendingRow WHERE globalId='row'"), int64_t(9007199254740993LL));
}

TEST(PendingImport, CallerTransactionIsRefusedWithoutRollbackOrCommit) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    db.db().execute("BEGIN");
    db.db().execute("UPDATE PendingRow SET title='caller draft' WHERE globalId='row'");
    EXPECT_THROW(apply_pending_changes(db, {change("not-owned")}), std::invalid_argument);
    EXPECT_TRUE(db.db().is_in_transaction()); EXPECT_EQ(title(db.db()), "caller draft");
    db.db().execute("ROLLBACK"); EXPECT_EQ(title(db.db()), "first");
}

TEST(PendingImport, FailedChunkReportsOnlyCommittedEntries) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    struct Reset { ~Reset() { test_hooks::apply_chunk_begin = nullptr; } } reset;
    test_hooks::apply_chunk_begin = [](size_t start) { if (start >= 50) throw std::runtime_error("second chunk fails"); };
    std::vector<audit_log_entry> entries;
    for (int i = 0; i < 51; ++i) entries.push_back(change("chunk-" + std::to_string(i), std::to_string(i)));
    auto result = apply_pending_changes(db, entries);
    EXPECT_EQ(result.imported_ids.size(), 50u);
    EXPECT_EQ(result.rejected_ids, std::vector<std::string>{"chunk-50"}); EXPECT_EQ(title(db.db()), "49");
    EXPECT_EQ(scalar(db.db(), "SELECT count(*) AS n FROM AuditLog WHERE globalId LIKE 'chunk-%'"), 50);
}

TEST(SelectedAudit, ExactIdentityMalformedRowsAndDuplicateSelectionsRefuse) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    const auto identity = latest(db.db());
    EXPECT_THROW(query_audit_log_selected(db.db(), {{identity.first, "wrong"}}), std::invalid_argument);
    EXPECT_THROW(query_audit_log_selected(db.db(), {{identity.first + 1, identity.second}}), std::invalid_argument);
    EXPECT_THROW(query_audit_log_selected(db.db(), {identity, identity}), std::invalid_argument);
    EXPECT_THROW(query_audit_log_selected(db.db(), {{1, std::string(40, 'a')}, {2, std::string(40, 'b')}}, 64), std::length_error);
    db.db().execute("UPDATE AuditLog SET changedFields='[' WHERE id=?", {identity.first});
    EXPECT_THROW(query_audit_log_selected(db.db(), {identity}), std::invalid_argument);
    for (const auto* malformed : {R"({"title":{}})", R"({"title":{"kind":999,"value":"x"}})", R"({"amount":{"kind":5,"value":1e100}})", R"({"amount":{"kind":0,"value":9007199254740993}})"}) {
        db.db().execute("UPDATE AuditLog SET changedFields=? WHERE id=?", {std::string(malformed), identity.first});
        EXPECT_THROW(query_audit_log_selected(db.db(), {identity}), std::invalid_argument);
    }
    EXPECT_FALSE(db.db().is_in_transaction());
}

TEST(SelectedAudit, RawAndExpandedLimitsRefuseBeforeRetainingOversizedRows) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    const auto identity = latest(db.db());
    EXPECT_THROW(query_audit_log_selected(db.db(), {identity}, 1), std::length_error);
    EXPECT_THROW(query_audit_log_selected(db.db(), {identity}, 0), std::length_error);
    EXPECT_THROW(query_audit_log_selected(db.db(), {identity}, 16 * 1024 * 1024 + 1), std::length_error);
    db.db().execute("UPDATE PendingRow SET body=? WHERE globalId='row'", {std::string(8192, 'x')});
    const auto expanded = latest(db.db());
    EXPECT_THROW(query_audit_log_selected(db.db(), {expanded}, 4096), std::length_error);
    const auto entry = query_audit_log_selected(db.db(), {expanded}, 16384)[0];
    EXPECT_EQ(std::get<std::string>(entry.changed_fields.at("body").value).size(), 8192u);
    auto two = std::vector<std::pair<int64_t, std::string>>{identity, expanded};
    EXPECT_THROW(query_audit_log_selected(db.db(), two, entry.to_json().size()), std::length_error);
    EXPECT_THROW(query_audit_log_selected(db.db(), std::vector<std::pair<int64_t, std::string>>(4097)), std::length_error);
}

TEST(SelectedAudit, InsertAndUpdateMetadataAreBoundedBeforeLateBinding) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    const auto insert = latest(db.db());
    db.db().execute("UPDATE PendingRow SET body='next' WHERE globalId='row'");
    const auto update = latest(db.db());
    db.db().execute("UPDATE _lattice_meta SET value=? WHERE key='trigger_flags:PendingRow'", {std::string(8192, 'x')});
    for (const auto& identity : {insert, update})
        EXPECT_THROW(query_audit_log_selected(db.db(), {identity}, 4096), std::length_error);
    db.db().execute("UPDATE _lattice_meta SET value=? WHERE key='trigger_flags:PendingRow'", {std::string(1025, 'x')});
    EXPECT_THROW(query_audit_log_selected(db.db(), {insert}, 4096), std::length_error);
    db.db().execute("UPDATE _lattice_meta SET value='body;invalid' WHERE key='trigger_flags:PendingRow'");
    EXPECT_THROW(query_audit_log_selected(db.db(), {insert}, 4096), std::invalid_argument);
    // BLOB bypasses the metadata table's TEXT affinity.
    db.db().execute("UPDATE _lattice_meta SET value=X'626f6479' WHERE key='trigger_flags:PendingRow'");
    EXPECT_THROW(query_audit_log_selected(db.db(), {insert}, 4096), std::invalid_argument);
}

TEST(SelectedAudit, AggregateRawAndMetadataWorkIsBoundedEvenWhenOutputIsSmall) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    const auto first = latest(db.db());
    db.db().execute("UPDATE PendingRow SET title='next' WHERE globalId='row'");
    const auto second = latest(db.db());
    const std::string padded = std::string(2000, ' ') + "{}";
    db.db().execute("UPDATE AuditLog SET changedFields=? WHERE id IN (?,?)", {padded, first.first, second.first});
    EXPECT_THROW(query_audit_log_selected(db.db(), {first, second}, 4096), std::length_error);
    db.db().execute("UPDATE AuditLog SET changedFields='{}' WHERE id IN (?,?)", {first.first, second.first});
    std::string csv;
    for (int i = 0; i < 4097; ++i) csv += "a,";
    db.db().execute("UPDATE _lattice_meta SET value=? WHERE key='trigger_flags:PendingRow'", {csv});
    EXPECT_THROW(query_audit_log_selected(db.db(), {first}), std::length_error);
    csv = std::string(1000, 'a');
    db.db().execute("UPDATE _lattice_meta SET value=? WHERE key='trigger_flags:PendingRow'", {csv});
    EXPECT_THROW(query_audit_log_selected(db.db(), {first, second}, 4096), std::length_error);
}

TEST(SelectedAudit, ReservedNoHistoryColumnReadsExactlyAndUnavailableColumnRefuses) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    db.db().execute("ALTER TABLE PendingRow ADD COLUMN \"select\" TEXT");
    db.db().execute("UPDATE PendingRow SET \"select\"='keyword value',title='keyword' WHERE globalId='row'");
    const auto identity = latest(db.db());
    db.db().execute("UPDATE _lattice_meta SET value='select' WHERE key='trigger_flags:PendingRow'");
    db.db().execute("UPDATE AuditLog SET changedFields='{\"select\":null}',changedFieldsNames='[\"select\"]' WHERE id=?", {identity.first});
    const auto selected = query_audit_log_selected(db.db(), {identity});
    EXPECT_EQ(std::get<std::string>(selected[0].changed_fields.at("select").value), "keyword value");
    db.db().execute("UPDATE _lattice_meta SET value='missing' WHERE key='trigger_flags:PendingRow'");
    db.db().execute("UPDATE AuditLog SET changedFields='{\"missing\":null}',changedFieldsNames='[\"missing\"]' WHERE id=?", {identity.first});
    EXPECT_THROW(query_audit_log_selected(db.db(), {identity}), db_error);
}

TEST(SelectedAudit, DeferredNotificationRunsOnlyAfterCaptureUnlocks) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    const auto identity = latest(db.db());
    int deliveries = 0, mutex_result = -1;
    db.db().set_txn_hooks([&] {
        ++deliveries;
        std::thread probe([&] {
            auto* mutex = sqlite3_db_mutex(db.db().handle());
            mutex_result = sqlite3_mutex_try(mutex);
            if (mutex_result == SQLITE_OK) sqlite3_mutex_leave(mutex);
        });
        probe.join();
    }, [] {});
    db.db().mark_txn_dirty(); // Previously completed work waiting for delivery.
    ASSERT_EQ(query_audit_log_selected(db.db(), {identity}).size(), 1u);
    EXPECT_EQ(deliveries, 1); EXPECT_EQ(mutex_result, SQLITE_OK);
    db.db().set_txn_hooks(nullptr, nullptr);
}

TEST(SelectedAudit, ReadOnlyNestedSnapshotIsSparseAndLeavesStoreUnchanged) {
    File file; lattice::swift_lattice db(config(file), schema()); seed(db);
    const auto earlier = latest(db.db());
    db.db().execute("UPDATE PendingRow SET body=NULL,title='later' WHERE globalId='row'");
    const auto identity = latest(db.db());
    db.db().execute("UPDATE AuditLog SET changedFields='broken unselected row' WHERE id=?", {earlier.first});
    auto raw = std::get<std::string>(db.db().query("SELECT changedFields FROM AuditLog WHERE id=?", {identity.first})[0].at("changedFields"));
    database reader(file.path, database::open_mode::read_only);
    reader.execute("BEGIN");
    const auto selected = query_audit_log_selected(reader, {identity});
    ASSERT_EQ(selected.size(), 1u); EXPECT_EQ(selected[0].no_history_nulls, std::vector<std::string>{"body"});
    EXPECT_TRUE(reader.is_in_transaction()); reader.execute("ROLLBACK");
    EXPECT_EQ(std::get<std::string>(db.db().query("SELECT changedFields FROM AuditLog WHERE id=?", {identity.first})[0].at("changedFields")), raw);
}
} // namespace
