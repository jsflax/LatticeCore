// Actual-Core nullable no-history regressions. Synthetic files only; no
// TestHelpers global registry/log environment. Set TMPDIR for fixture output.
#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
#include <gtest/gtest.h>
#include <lattice.hpp>
#include <lattice/sync.hpp>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>

namespace {
using namespace lattice;
using json = nlohmann::json;

struct File {
    std::string path;
    File() {
        static std::atomic<unsigned> sequence{0};
        path = (std::filesystem::temp_directory_path() / ("nh-null-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
            std::to_string(sequence++) + ".sqlite")).string();
        if (std::filesystem::exists(path)) throw std::runtime_error("fixture must be fresh");
    }
    ~File() {
        std::error_code ignored;
        for (const auto* suffix : {"", "-wal", "-shm"}) std::filesystem::remove(path + suffix, ignored);
    }
};

swift_configuration config(const File& file) {
    configuration c(file.path);
    c.audit_retention_seconds = 0;
    return swift_configuration(c);
}

property_descriptor text(const std::string& name, bool nullable, bool no_history) {
    property_descriptor d;
    d.name = name; d.type = column_type::text; d.kind = property_kind::primitive;
    d.nullable = nullable; d.no_history = no_history;
    return d;
}

SchemaVector schema() {
    swift_schema_entry s;
    s.table_name = "NhNull";
    s.properties = {{"title", text("title", false, false)},
                    {"body", text("body", true, true)},
                    {"required", text("required", false, true)}};
    return {s};
}

void seed(lattice::swift_lattice& db, const char* body = "first") {
    db.db().execute("INSERT INTO NhNull(globalId,title,body,required) VALUES('row','initial',?,'kept')",
                    {body ? column_value_t(std::string(body)) : column_value_t(nullptr)});
}

void expect_body(lattice::swift_lattice& db, const char* expected) {
    auto rows = db.db().query("SELECT body, required FROM NhNull WHERE globalId='row'");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("required")), "kept");
    if (!expected) EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(rows[0].at("body")));
    else {
        ASSERT_TRUE(std::holds_alternative<std::string>(rows[0].at("body")));
        EXPECT_EQ(std::get<std::string>(rows[0].at("body")), expected);
    }
}

std::vector<audit_log_entry> outbound(database& db, bool upload) {
    auto entries = upload ? query_audit_log_for_sync(db, "test:nohistory", std::nullopt, 0, 100)
                          : events_after(db, std::nullopt);
    entries.erase(std::remove_if(entries.begin(), entries.end(), [](const auto& e) {
        return e.table_name != "NhNull";
    }), entries.end());
    return entries;
}

audit_log_entry clear(const std::string& id = "clear") {
    audit_log_entry e;
    e.global_id = id; e.table_name = "NhNull"; e.operation = "UPDATE"; e.global_row_id = "row";
    e.changed_fields_names = {"body", "title"};
    e.changed_fields = {{"body", any_property(nullptr)}, {"title", any_property(std::string("changed"))}};
    e.no_history_nulls = {"body"};
    return e;
}

class NoHistoryNullRoute : public ::testing::TestWithParam<bool> {};
TEST_P(NoHistoryNullRoute, RealUpdateRoundTripClearsAndLeavesRawHistoryUnchanged) {
    File source_file, target_file;
    lattice::swift_lattice source(config(source_file), schema()), target(config(target_file), schema());
    seed(source);
    auto initial = outbound(source.db(), GetParam());
    ASSERT_EQ(initial.size(), 1u);
    ASSERT_EQ(apply_remote_changes(target, initial).size(), 1u);
    source.db().execute("UPDATE NhNull SET body='second' WHERE globalId='row'");
    auto nonnull = outbound(source.db(), GetParam()).back();
    EXPECT_TRUE(nonnull.no_history_nulls.empty());
    auto decoded = audit_log_entry::from_json(nonnull.to_json());
    ASSERT_TRUE(decoded);
    ASSERT_EQ(apply_remote_changes(target, {*decoded}).size(), 1u);
    expect_body(target, "second");

    source.db().execute("UPDATE NhNull SET title='cleared',body=NULL WHERE globalId='row'");
    auto entry = outbound(source.db(), GetParam()).back();
    EXPECT_EQ(entry.no_history_nulls, std::vector<std::string>{"body"});
    auto raw = source.db().query("SELECT changedFields FROM AuditLog WHERE globalId=?", {entry.global_id});
    ASSERT_EQ(raw.size(), 1u);
    const auto raw_text = std::get<std::string>(raw[0].at("changedFields"));
    EXPECT_EQ(json::parse(raw_text)["body"], nullptr);
    EXPECT_EQ(raw_text.find("noHistoryNulls"), std::string::npos);
    decoded = audit_log_entry::from_json(entry.to_json());
    ASSERT_TRUE(decoded);
    ASSERT_EQ(decoded->no_history_nulls, entry.no_history_nulls);
    ASSERT_EQ(apply_remote_changes(target, {*decoded}), std::vector<std::string>{entry.global_id});
    expect_body(target, nullptr);
    auto after = source.db().query("SELECT changedFields FROM AuditLog WHERE globalId=?", {entry.global_id});
    EXPECT_EQ(std::get<std::string>(after[0].at("changedFields")), raw_text);
}
INSTANTIATE_TEST_SUITE_P(Export, NoHistoryNullRoute, ::testing::Values(true, false),
    [](const auto& p) { return p.param ? "Upload" : "EventsAfter"; });

TEST(NoHistoryNull, LegacyPlaceholderAndMarkedRequiredNullKeepValues) {
    File file;
    lattice::swift_lattice db(config(file), schema());
    seed(db);
    for (bool marked : {false, true}) {
        auto e = clear(marked ? "marked" : "legacy");
        e.changed_fields_names.push_back("required");
        e.changed_fields["required"] = any_property(nullptr);
        e.no_history_nulls = marked ? std::vector<std::string>{"body", "required"} : std::vector<std::string>{};
        auto decoded = audit_log_entry::from_json(e.to_json());
        ASSERT_TRUE(decoded);
        ASSERT_EQ(apply_remote_changes(db, {*decoded}).size(), 1u);
        expect_body(db, marked ? nullptr : "first");
    }
}

TEST(NoHistoryNull, MissingAndInconsistentMetadataFailBeforeTypedDedup) {
    File file;
    lattice::swift_lattice db(config(file), schema());
    seed(db);
    auto valid = clear();
    ASSERT_EQ(apply_remote_changes(db, {valid}).size(), 1u);
    std::vector<audit_log_entry> invalid;
    auto e = valid; e.no_history_nulls = {"body", "body"}; invalid.push_back(e);
    e = valid; e.no_history_nulls = {""}; invalid.push_back(e);
    e = valid; e.changed_fields.erase("body"); invalid.push_back(e);
    e = valid; e.changed_fields_names = {"title"}; invalid.push_back(e);
    e = valid; e.changed_fields["body"] = any_property(std::string("not null")); invalid.push_back(e);
    e = valid; e.changed_fields["body"].value = int64_t(9); invalid.push_back(e);
    e = valid; e.operation = "DELETE"; invalid.push_back(e);
    // Same globalId as the already-applied entry: validation must precede dedup ACK.
    for (const auto& bad : invalid) {
        EXPECT_TRUE(apply_remote_changes(db, {bad}).empty());
        EXPECT_THROW(bad.to_json(), std::invalid_argument);
    }
}

TEST(NoHistoryNull, WireMetadataIsStrictButAbsentLegacyKeyAndFutureKeysRemainCompatible) {
    const auto base = json::parse(clear().to_json());
    std::vector<json> malformed;
    for (const auto& value : {json(nullptr), json("body"), json::array({nullptr}),
                              json::array({"body", "body"}), json::array({"unknown"})}) {
        auto j = base; j["noHistoryNulls"] = value; malformed.push_back(j);
    }
    for (const auto& value : {json::object(), json{{"kind", 4}}, json{{"kind", 4}, {"value", 7}},
                              json{{"kind", 2}, {"value", nullptr}}, json::array()}) {
        auto j = base; j["changedFields"]["body"] = value; malformed.push_back(j);
    }
    for (const auto& j : malformed) EXPECT_FALSE(audit_log_entry::from_json(j.dump())) << j.dump();
    auto legacy = base; legacy.erase("noHistoryNulls"); legacy["futureMetadata"] = true;
    auto decoded = audit_log_entry::from_json(legacy.dump());
    ASSERT_TRUE(decoded); EXPECT_TRUE(decoded->no_history_nulls.empty());
    auto flat = base; flat["changedFields"]["body"] = nullptr;
    EXPECT_TRUE(audit_log_entry::from_json(flat.dump()));
    auto string_fields = base;
    string_fields["changedFields"] = base["changedFields"].dump();
    string_fields["changedFieldsNames"] = base["changedFieldsNames"].dump();
    EXPECT_TRUE(audit_log_entry::from_json(string_fields.dump()));
}

TEST(NoHistoryNull, BinderRecomputesMarkerAndDropsGoneRows) {
    File file;
    lattice::swift_lattice db(config(file), schema());
    seed(db, nullptr);
    std::vector<audit_log_entry> entries{clear()};
    late_bind_no_history(db.db(), entries);
    ASSERT_EQ(entries[0].no_history_nulls, std::vector<std::string>{"body"});
    db.db().execute("UPDATE NhNull SET body='again' WHERE globalId='row'");
    late_bind_no_history(db.db(), entries);
    EXPECT_TRUE(entries[0].no_history_nulls.empty());
    EXPECT_EQ(std::get<std::string>(entries[0].changed_fields.at("body").value), "again");
    entries = {clear()};
    db.db().execute("DELETE FROM NhNull WHERE globalId='row'");
    late_bind_no_history(db.db(), entries);
    EXPECT_TRUE(entries[0].no_history_nulls.empty());
    EXPECT_EQ(entries[0].changed_fields.count("body"), 0u);
    EXPECT_EQ(std::find(entries[0].changed_fields_names.begin(), entries[0].changed_fields_names.end(), "body"),
              entries[0].changed_fields_names.end());
}

TEST(NoHistoryNull, InsertNullIsExplicitAndRelayReexportsItAfterReopen) {
    File source_file, relay_file, target_file;
    lattice::swift_lattice source(config(source_file), schema()), target(config(target_file), schema());
    seed(source, nullptr);
    auto entries = outbound(source.db(), false);
    ASSERT_EQ(entries.size(), 1u);
    ASSERT_EQ(entries[0].operation, "INSERT");
    EXPECT_EQ(entries[0].no_history_nulls, std::vector<std::string>{"body"});
    {
        lattice::swift_lattice relay(config(relay_file), schema());
        seed(relay); // An ordinary INSERT upsert must clear an existing value, too.
        ASSERT_EQ(apply_remote_changes(relay, entries).size(), 1u);
        expect_body(relay, nullptr);
        auto update = clear("relayed-update");
        ASSERT_EQ(apply_remote_changes(relay, {update}).size(), 1u);
    }
    lattice::swift_lattice reopened(config(relay_file), schema());
    auto sent = outbound(reopened.db(), false);
    auto found = std::find_if(sent.begin(), sent.end(), [](const auto& e) { return e.global_id == "relayed-update"; });
    ASSERT_NE(found, sent.end());
    EXPECT_EQ(found->no_history_nulls, std::vector<std::string>{"body"});
    seed(target);
    ASSERT_EQ(apply_remote_changes(target, {*found}).size(), 1u);
    expect_body(target, nullptr);
    auto columns = reopened.db().query("PRAGMA table_info(AuditLog)");
    for (const auto& row : columns) EXPECT_NE(std::get<std::string>(row.at("name")), "noHistoryNulls");
}

class SnapshotExporter : public synchronizer_base {
public:
    explicit SnapshotExporter(lattice_db& value) {
        db_ptr_ = &value;
        sync_config c; c.sync_id = "test:nohistory-snapshot";
        init_sync(c, std::make_shared<immediate_scheduler>());
    }
    using synchronizer_base::build_insert_entry_from_current_row;
};

TEST(NoHistoryNull, SynthesizedSnapshotMarksNullButStillDoesNotReplaceExistingRows) {
    File source_file, target_file;
    lattice::swift_lattice source(config(source_file), schema()), target(config(target_file), schema());
    seed(source, nullptr); seed(target);
    SnapshotExporter exporter(source);
    auto e = exporter.build_insert_entry_from_current_row("NhNull", "row");
    ASSERT_TRUE(e); EXPECT_TRUE(e->synthesized);
    EXPECT_EQ(e->no_history_nulls, std::vector<std::string>{"body"});
    ASSERT_EQ(apply_remote_changes(target, {*e}).size(), 1u);
    expect_body(target, "first"); // Reconciliation is not repair of a previously missed clear.
}

TEST(NoHistoryNull, AlreadyAckedLegacyClearIsNotRepairedByReplayingWithMarker) {
    File file;
    lattice::swift_lattice db(config(file), schema()); seed(db);
    auto e = clear(); e.no_history_nulls.clear();
    ASSERT_EQ(apply_remote_changes(db, {e}).size(), 1u);
    e.no_history_nulls = {"body"};
    ASSERT_EQ(apply_remote_changes(db, {e}).size(), 1u);
    expect_body(db, "first"); // Same audit identity was already ACKed; cutover needs explicit repair.
}

TEST(NoHistoryNull, ChunkRetryRefreshesPhysicalNullabilityAfterSchemaChange) {
    File file;
    lattice::swift_lattice db(config(file), schema());
    seed(db);
    auto entry = clear("schema-retry");
    entry.changed_fields_names.push_back("future");
    entry.changed_fields["future"] = any_property(nullptr);
    entry.no_history_nulls.push_back("future");
    // The first attempt populates its schema/nullability caches, then fails
    // while restoring the trigger gate. No row change or receipt may survive.
    db.db().execute("CREATE TRIGGER fail_chunk_restore BEFORE UPDATE OF disabled ON _SyncControl "
                    "WHEN NEW.disabled=0 BEGIN SELECT RAISE(ABORT,'retry fixture'); END");
    struct ResetHook {
        ~ResetHook() { test_hooks::apply_chunk_begin = nullptr; }
    } reset;
    int attempts = 0;
    test_hooks::apply_chunk_begin = [&](size_t chunk_start) {
        EXPECT_EQ(chunk_start, 0u);
        if (++attempts != 2) return;
        const auto before = db.db().query("SELECT title FROM NhNull WHERE globalId='row'");
        ASSERT_EQ(before.size(), 1u);
        EXPECT_EQ(std::get<std::string>(before[0].at("title")), "initial");
        EXPECT_TRUE(db.db().query("SELECT id FROM AuditLog WHERE globalId='schema-retry'").empty());
        // Simulate a committed physical schema/NoHistory metadata addition
        // between the rolled-back attempt and its retry, using real SQLite.
        db.db().execute("DROP TRIGGER fail_chunk_restore");
        db.db().execute("ALTER TABLE NhNull ADD COLUMN future TEXT DEFAULT 'prior'");
        db.db().execute("UPDATE _lattice_meta SET value='body,future,required' "
                        "WHERE key='trigger_flags:NhNull'");
    };
    EXPECT_EQ(apply_remote_changes(db, {entry}), std::vector<std::string>{entry.global_id});
    EXPECT_EQ(attempts, 2);
    const auto after = db.db().query("SELECT future FROM NhNull WHERE globalId='row'");
    ASSERT_EQ(after.size(), 1u);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(after[0].at("future")));
    EXPECT_EQ(db.db().query("SELECT id FROM AuditLog WHERE globalId='schema-retry'").size(), 1u);
}
} // namespace
