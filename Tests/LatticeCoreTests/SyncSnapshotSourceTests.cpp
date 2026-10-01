#include "TestHelpers.hpp"
#include <lattice.hpp>
#include <nlohmann/json.hpp>
#include "../../Sources/LatticeCore/src/sync_snapshot_source.hpp"
#include "../../Sources/LatticeCore/src/sync_recovery_values.hpp"

namespace sr = lattice::detail::sync_recovery;
namespace {
using lattice::configuration;
using lattice::lattice_db;
using json = nlohmann::json;

sr::source_limits source_budget() { return {{65536, 16, 4096, 8192, 2, 32, 64, 524288}, 8, 16, 16}; }
std::vector<sr::source_relation> raw_scope() { return {{"_source_rows", sr::relation_kind::model, true}}; }
void create_raw(lattice_db& db) {
    // An underscore-prefixed fixture avoids asking the ordinary model update
    // hook to resolve an intentionally unregistered dynamic test schema.
    db.db().execute("CREATE TABLE _source_rows(id INTEGER PRIMARY KEY, globalId TEXT UNIQUE, "
                    "body TEXT, amount REAL, bytes BLOB, extra INTEGER)");
}
void insert_raw(lattice_db& db, const std::string& id, const std::string& text = "body") {
    db.db().execute("INSERT INTO _source_rows(globalId,body,amount,bytes,extra) VALUES(?,?,1.25,X'0001ff',NULL)", {id, text});
}
std::vector<sr::row> flatten(const sr::unsealed_materialization& captured) {
    std::vector<sr::row> rows;
    for (const auto& p : captured.pages) rows.insert(rows.end(), p.begin(), p.end());
    return rows;
}
int64_t scalar(lattice_db& db, const std::string& sql) {
    const auto values = db.db().query(sql, {});
    if (values.size() != 1) throw std::runtime_error("invalid test scalar");
    return std::get<int64_t>(values.front().at("v"));
}
template<class F> void refuses(F&& f, const char* reason) {
    try { f(); FAIL() << "source accepted a refused capture"; }
    catch (const sr::protocol_error& e) { EXPECT_STREQ(e.what(), reason); }
}
lattice::SchemaVector doc_schema() {
    lattice::swift_schema_entry entry; entry.table_name = "SnapshotDoc";
    for (const auto* name : {"title", "body"}) {
        lattice::property_descriptor p;
        p.name = name; p.type = lattice::column_type::text; p.kind = lattice::property_kind::primitive;
        p.no_history = std::string(name) == "body";
        entry.properties[name] = p;
    }
    return {entry};
}
}

TEST(SyncSnapshotSource, PinsCurrentNoHistoryValuesAndCandidateHeadAcrossPages) {
    TempDB temp{"source_same_view"};
    lattice::swift_lattice db{lattice::swift_configuration(temp.str()), doc_schema()};
    for (const auto* id : {"a", "b", "c"})
        db.db().execute("INSERT INTO SnapshotDoc(globalId,title,body) VALUES(?,?,?)", {std::string(id), std::string("title"), std::string(id)});
    db.db().execute("UPDATE SnapshotDoc SET body='b-current' WHERE globalId='b'");
    const auto audit = db.db().query("SELECT changedFields FROM AuditLog WHERE tableName='SnapshotDoc' AND operation='UPDATE' ORDER BY id DESC LIMIT 1", {});
    ASSERT_EQ(audit.size(), 1u);
    EXPECT_TRUE(json::parse(std::get<std::string>(audit.front().at("changedFields"))).at("body").is_null());
    const auto head = scalar(db, "SELECT MAX(id) AS v FROM AuditLog");
    auto budget = source_budget(); budget.wire.rows_per_page = 1;
    const std::vector<sr::source_relation> scope{{"SnapshotDoc", sr::relation_kind::model, true}};
    size_t interleavings = 0;
    const auto captured = sr::source_test_hooks::materialize(db, scope, budget, [&](size_t page_count, uint64_t generation) {
        EXPECT_NE(generation, 0u);
        if (page_count != 1) return;
        ++interleavings;
        db.db().execute("UPDATE SnapshotDoc SET body='b-new' WHERE globalId='b'");
        db.db().execute("DELETE FROM SnapshotDoc WHERE globalId='c'");
        db.db().execute("INSERT INTO SnapshotDoc(globalId,title,body) VALUES('d','title','d-new')");
    });
    ASSERT_EQ(interleavings, 1u);
    EXPECT_EQ(captured.audit_head_candidate, static_cast<uint64_t>(head));
    const auto before = flatten(captured); ASSERT_EQ(before.size(), 3u);
    EXPECT_EQ(before[0].global_id, "a"); EXPECT_EQ(before[1].global_id, "b"); EXPECT_EQ(before[2].global_id, "c");
    EXPECT_EQ(json::parse(before[1].payload).at("body").at("value"), "b-current");
    EXPECT_EQ(json::parse(before[2].payload).at("body").at("value"), "c");
    const auto fresh = sr::materialize_source(db, scope, budget);
    EXPECT_GT(fresh.audit_head_candidate, captured.audit_head_candidate);
    const auto after = flatten(fresh); ASSERT_EQ(after.size(), 3u);
    EXPECT_EQ(after[2].global_id, "d");
    EXPECT_EQ(json::parse(after[1].payload).at("body").at("value"), "b-new");
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
}

TEST(SyncSnapshotSource, PreservesEmbeddedNulTextBlobAndScalarTypesWithStableBytes) {
    TempDB temp{"source_bytes"}; lattice_db db{configuration(temp.str())}; create_raw(db);
    const std::string text("left\0right", 10);
    // The inherited TEXT binder also uses length -1. Construct exact stored
    // bytes in SQL, so the new reader is tested against a real embedded NUL.
    db.db().execute("INSERT INTO _source_rows(globalId,body,amount,bytes,extra) "
                    "VALUES('row-a',CAST(X'6c656674007269676874' AS TEXT),1.25,X'0001ff',NULL)");
    const auto changes = scalar(db, "SELECT total_changes() AS v");
    const auto first = sr::materialize_source(db, raw_scope(), source_budget());
    const auto second = sr::materialize_source(db, raw_scope(), source_budget());
    EXPECT_EQ(first.pages, second.pages);
    const auto rows = flatten(first); ASSERT_EQ(rows.size(), 1u);
    const auto payload = json::parse(rows.front().payload);
    EXPECT_EQ(payload.at("body").at("value").get<std::string>(), text);
    EXPECT_EQ(payload.at("body").at("kind"), 2);
    EXPECT_EQ(payload.at("bytes").at("value"), "0001ff"); EXPECT_EQ(payload.at("bytes").at("kind"), 6);
    EXPECT_EQ(payload.at("amount").at("value"), 1.25); EXPECT_EQ(payload.at("amount").at("kind"), 7);
    EXPECT_TRUE(payload.at("extra").at("value").is_null()); EXPECT_EQ(payload.at("extra").at("kind"), 4);
    EXPECT_FALSE(payload.contains("id")); EXPECT_FALSE(payload.contains("globalId"));
    EXPECT_EQ(first.content_bytes, sr::canonical_row_bytes(rows.front()));
    EXPECT_EQ(scalar(db, "SELECT total_changes() AS v"), changes);
    db.db().execute("UPDATE _source_rows SET extra=9223372036854775807");
    const auto integer_rows = flatten(sr::materialize_source(db, raw_scope(), source_budget()));
    ASSERT_EQ(integer_rows.size(), 1u);
    const auto whole = json::parse(integer_rows.front().payload).at("extra");
    EXPECT_EQ(whole.at("kind"), 1); EXPECT_EQ(whole.at("value").get<int64_t>(), INT64_MAX);
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
}

TEST(SyncSnapshotSource, MaterializedPayloadUsesStrictInstallerScalarGrammar) {
    TempDB temp{"source_installer_values"}; lattice_db db{configuration(temp.str())}; create_raw(db);
    db.db().execute("INSERT INTO _source_rows(globalId,body,amount,bytes,extra) "
                    "VALUES('a',CAST(X'c3a9007461696c00' AS TEXT),1.25,X'00ff00',9223372036854775807),"
                    "('b','',-1.25,X'',NULL)");
    const auto captured = flatten(sr::materialize_source(db, raw_scope(), source_budget()));
    ASSERT_EQ(captured.size(), 2u);
    const sr::value_limits budget{8192, 16, 256, 8192, 8192};
    const sr::row_values first{{"body", std::string("\xc3\xa9\0tail\0", 8)},
        {"amount", 1.25}, {"bytes", std::vector<uint8_t>{0,255,0}}, {"extra", int64_t{INT64_MAX}}};
    const sr::row_values second{{"body", std::string{}}, {"amount", -1.25},
        {"bytes", std::vector<uint8_t>{}}, {"extra", nullptr}};
    EXPECT_EQ(sr::decode_values(captured[0].payload, budget), first);
    EXPECT_EQ(sr::decode_values(captured[1].payload, budget), second);
    EXPECT_EQ(captured[0].payload, sr::encode_values(first, budget));
    EXPECT_EQ(captured[1].payload, sr::encode_values(second, budget));

    db.db().execute("UPDATE _source_rows SET body=CAST(X'ff' AS TEXT) WHERE globalId='a'");
    EXPECT_THROW((void)sr::materialize_source(db, raw_scope(), source_budget()), sr::protocol_error);
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
}

TEST(SyncSnapshotSource, CopiesOrdinaryAndPolymorphicLinksInCanonicalTableOrder) {
    TempDB temp{"source_links"}; lattice_db db{configuration(temp.str())};
    db.add(TestPerson{"person", 20, std::nullopt});
    const auto person = db.db().query("SELECT globalId FROM TestPerson LIMIT 1", {});
    ASSERT_EQ(person.size(), 1u); const auto id = std::get<std::string>(person.front().at("globalId"));
    db.ensure_link_table("_source_links", "TestPerson", "TestPerson");
    db.ensure_virtual_link_table("_source_virtuals", "TestPerson");
    db.db().execute("INSERT INTO _source_links(globalId,lhs,rhs) VALUES('link-a',?,?)", {id, id});
    db.db().execute("INSERT INTO _source_virtuals(globalId,lhs,rhs,rhs_type) VALUES('link-b',?,?,'TestPerson')", {id, id});
    const std::vector<sr::source_relation> scope{{"_source_virtuals", sr::relation_kind::link, true},
        {"TestPerson", sr::relation_kind::model, true}, {"_source_links", sr::relation_kind::link, true}};
    const auto captured = sr::materialize_source(db, scope, source_budget());
    const auto rows = flatten(captured); ASSERT_EQ(rows.size(), 3u); ASSERT_EQ(captured.layouts.size(), 3u);
    EXPECT_EQ(rows[0].table, "TestPerson"); EXPECT_EQ(rows[1].table, "_source_links"); EXPECT_EQ(rows[2].table, "_source_virtuals");
    EXPECT_EQ(json::parse(rows[1].payload).at("lhs").at("value"), id);
    EXPECT_EQ(json::parse(rows[1].payload).at("rhs").at("value"), id);
    EXPECT_EQ(json::parse(rows[2].payload).at("rhs_type").at("value"), "TestPerson");
    EXPECT_EQ(captured.layouts[1].kind, sr::relation_kind::link);
    EXPECT_EQ(captured.layouts[1].identity_collation, "NOCASE");
    db.db().execute("UPDATE _source_links SET globalId='UPPER' WHERE globalId='link-a'");
    const auto uppercase = flatten(sr::materialize_source(db, scope, source_budget()));
    ASSERT_EQ(uppercase.size(), 3u); EXPECT_EQ(uppercase[1].global_id, "UPPER");
}

TEST(SyncSnapshotSource, UsesNoCaseIndexThenSortsBoundedMixedCaseRowsForTheCodec) {
    TempDB temp{"source_nocase"}; lattice_db db{configuration(temp.str())};
    db.db().execute("CREATE TABLE _source_nocase(id INTEGER PRIMARY KEY,globalId TEXT UNIQUE COLLATE NOCASE,body TEXT)");
    db.db().execute("INSERT INTO _source_nocase(globalId,body) VALUES('a','one'),('B','two'),('c','three')");
    auto budget = source_budget(); budget.wire.rows_per_page = 1;
    size_t batches = 0;
    const auto captured = sr::source_test_hooks::materialize(db,
        {{"_source_nocase", sr::relation_kind::model, true}}, budget, [&](size_t n, uint64_t) { batches = n; });
    EXPECT_EQ(batches, 3u); EXPECT_EQ(captured.pages.size(), 3u);
    const auto rows = flatten(captured); ASSERT_EQ(rows.size(), 3u);
    EXPECT_EQ(rows[0].global_id, "B"); EXPECT_EQ(rows[1].global_id, "a"); EXPECT_EQ(rows[2].global_id, "c");
    EXPECT_EQ(captured.layouts.front().identity_collation, "NOCASE");
}

TEST(SyncSnapshotSource, QuotesDurableTableAndColumnNamesWithoutInterpretingSql) {
    TempDB temp{"source_quoted"}; lattice_db db{configuration(temp.str())};
    db.db().execute(R"SQL(CREATE TABLE "_source""table"(id INTEGER PRIMARY KEY, globalId TEXT UNIQUE, "body""; --" TEXT))SQL");
    db.db().execute(R"SQL(INSERT INTO "_source""table"(globalId,"body""; --") VALUES('a','literal'))SQL");
    const auto captured = sr::materialize_source(db, {{"_source\"table", sr::relation_kind::model, true}}, source_budget());
    const auto rows = flatten(captured); ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows.front().table, "_source\"table");
    EXPECT_EQ(json::parse(rows.front().payload).at("body\"; --").at("value"), "literal");
    EXPECT_EQ(scalar(db, "SELECT count(*) AS v FROM AuditLog"), 0);
}

TEST(SyncSnapshotSource, RejectsPartialScopesUnsupportedLayoutsAndUnprovenIdentity) {
    TempDB temp{"source_schema"}; lattice_db db{configuration(temp.str())}; create_raw(db);
    const auto budget = source_budget();
    refuses([&] { (void)sr::materialize_source(db, {{"_source_rows", sr::relation_kind::model, false}}, budget); }, "filtered or undeclared source scope refused");
    refuses([&] { (void)sr::materialize_source(db, {{"AuditLog", sr::relation_kind::model, true}}, budget); }, "internal source relation refused");
    refuses([&] { (void)sr::materialize_source(db, {raw_scope().front(), raw_scope().front()}, budget); }, "duplicate source relation");
    db.db().execute("CREATE TABLE _source_no_unique(id INTEGER PRIMARY KEY, globalId TEXT, body TEXT)");
    refuses([&] { (void)sr::materialize_source(db, {{"_source_no_unique", sr::relation_kind::model, true}}, budget); }, "source globalId uniqueness not proven");
    db.db().execute("CREATE TABLE _source_generated(id INTEGER PRIMARY KEY, globalId TEXT UNIQUE, body TEXT, derived TEXT GENERATED ALWAYS AS(body) STORED)");
    refuses([&] { (void)sr::materialize_source(db, {{"_source_generated", sr::relation_kind::model, true}}, budget); }, "generated/hidden source column refused");
    db.db().execute("CREATE VIEW _source_view AS SELECT * FROM _source_rows");
    refuses([&] { (void)sr::materialize_source(db, {{"_source_view", sr::relation_kind::model, true}}, budget); }, "source must be a concrete main table");
    db.db().execute("CREATE TABLE _source_link_extra(lhs TEXT,rhs TEXT,globalId TEXT UNIQUE,extra TEXT)");
    refuses([&] { (void)sr::materialize_source(db, {{"_source_link_extra", sr::relation_kind::link, true}}, budget); }, "unsupported link identity schema");
    db.db().execute("CREATE TABLE _source_link_bad_key(lhs TEXT,rhs TEXT,globalId TEXT UNIQUE)");
    refuses([&] { (void)sr::materialize_source(db, {{"_source_link_bad_key", sr::relation_kind::link, true}}, budget); }, "unsupported link primary key schema");
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
}

TEST(SyncSnapshotSource, RejectsCaseAliasesAndInvalidIdentitiesWithoutCollapsingThem) {
    TempDB temp{"source_identity"}; lattice_db db{configuration(temp.str())}; create_raw(db);
    insert_raw(db, "a"); insert_raw(db, "B");
    auto budget = source_budget(); budget.wire.rows_per_page = 1;
    const auto ordered = flatten(sr::materialize_source(db, raw_scope(), budget)); ASSERT_EQ(ordered.size(), 2u);
    EXPECT_EQ(ordered[0].global_id, "B"); EXPECT_EQ(ordered[1].global_id, "a");
    insert_raw(db, "A");
    refuses([&] { (void)sr::materialize_source(db, raw_scope(), budget); }, "case-aliased source identity refused");
    db.db().execute("DELETE FROM _source_rows");
    db.db().execute("INSERT INTO _source_rows(globalId,body) VALUES(CAST(X'61007461696c' AS TEXT),'body')");
    refuses([&] { (void)sr::materialize_source(db, raw_scope(), budget); }, "source identifier control byte refused");
    db.db().execute("DELETE FROM _source_rows"); insert_raw(db, std::string(257, 'a'));
    refuses([&] { (void)sr::materialize_source(db, raw_scope(), budget); }, "invalid source byte string");
    db.db().execute("DELETE FROM _source_rows");
    db.db().execute("INSERT INTO _source_rows(globalId,body) VALUES(NULL,'null-id')");
    refuses([&] { (void)sr::materialize_source(db, raw_scope(), budget); }, "invalid source byte string");
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
}

TEST(SyncSnapshotSource, EnforcesRowsPagesAndSchemaBudgetsWithExactFailureStages) {
    TempDB temp{"source_caps"}; lattice_db db{configuration(temp.str())}; create_raw(db);
    insert_raw(db, "a"); insert_raw(db, "b");
    const auto baseline = sr::materialize_source(db, raw_scope(), source_budget()); ASSERT_EQ(baseline.rows, 2u);
    auto budget = source_budget(); budget.wire.rows_per_page = 1; budget.wire.total_rows = 1;
    refuses([&] { (void)sr::materialize_source(db, raw_scope(), budget); }, "source row budget exceeded");
    budget = source_budget(); budget.wire.rows_per_page = 1; budget.wire.pages = 1;
    refuses([&] { (void)sr::materialize_source(db, raw_scope(), budget); }, "source page budget exceeded");
    budget = source_budget(); budget.wire.total_bytes = 1;
    refuses([&] { (void)sr::materialize_source(db, raw_scope(), budget); }, "source content budget exceeded");
    budget = source_budget(); budget.columns_per_table = 1;
    refuses([&] { (void)sr::materialize_source(db, raw_scope(), budget); }, "source column budget exceeded");
    db.db().execute("CREATE INDEX _source_extra_index ON _source_rows(body)");
    budget = source_budget(); budget.indexes_per_table = 1;
    refuses([&] { (void)sr::materialize_source(db, raw_scope(), budget); }, "source index budget exceeded");
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
}

TEST(SyncSnapshotSource, RefusesLargeValueAtBytePreflightBeforeCopyingAPage) {
    TempDB temp{"source_large_value"}; lattice_db db{configuration(temp.str())}; create_raw(db);
    insert_raw(db, "a", std::string(4096, 'x'));
    auto budget = source_budget(); budget.wire.string_bytes = 1024;
    size_t copied_pages = 0;
    // The exact error distinguishes the typeof/length preflight from a later
    // encoded-payload/page rejection. A generous budget is the positive oracle.
    auto sufficient = source_budget(); sufficient.wire.string_bytes = 32768;
    EXPECT_EQ(sr::materialize_source(db, raw_scope(), sufficient).rows, 1u);
    refuses([&] { (void)sr::source_test_hooks::materialize(db, raw_scope(), budget,
        [&](size_t, uint64_t) { ++copied_pages; }); }, "source value exceeds allocation budget");
    EXPECT_EQ(copied_pages, 0u); EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
}

TEST(SyncSnapshotSource, RefusesRetiredViewInsteadOfReturningMixedGenerations) {
    TempDB temp{"source_retired"}; lattice_db db{configuration(temp.str())}; create_raw(db);
    insert_raw(db, "a", "before"); insert_raw(db, "b", "before");
    auto budget = source_budget(); budget.wire.rows_per_page = 1;
    size_t pages = 0;
    refuses([&] { (void)sr::source_test_hooks::materialize(db, raw_scope(), budget, [&](size_t page, uint64_t) {
        ++pages;
        if (page == 1) { db.retire_all_read_generations(); db.db().execute("UPDATE _source_rows SET body='after'"); }
    }); }, "source view retired or read failed");
    EXPECT_EQ(pages, 1u); EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
    const auto fresh = flatten(sr::materialize_source(db, raw_scope(), budget)); ASSERT_EQ(fresh.size(), 2u);
    for (const auto& value : fresh) EXPECT_EQ(json::parse(value.payload).at("body").at("value"), "after");
}

TEST(SyncSnapshotSource, ReleasesLeaseWhenThePerCallTestSeamThrows) {
    TempDB temp{"source_throw"}; lattice_db db{configuration(temp.str())}; create_raw(db); insert_raw(db, "a");
    struct seam_failure {};
    EXPECT_THROW((void)sr::source_test_hooks::materialize(db, raw_scope(), source_budget(),
        [](size_t, uint64_t) { throw seam_failure{}; }), seam_failure);
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
    EXPECT_EQ(sr::materialize_source(db, raw_scope(), source_budget()).rows, 1u);
}

TEST(SyncSnapshotSource, RefusesMemorySourcesAndMalformedFinitePolicy) {
    lattice_db memory;
    refuses([&] { (void)sr::materialize_source(memory, raw_scope(), source_budget()); }, "source view unavailable");
    auto budget = source_budget(); budget.wire.total_rows = 0;
    refuses([&] { (void)sr::materialize_source(memory, raw_scope(), budget); }, "invalid source wire budget");
    budget = source_budget(); budget.tables = 0;
    refuses([&] { (void)sr::materialize_source(memory, raw_scope(), budget); }, "invalid source schema budget");
    EXPECT_EQ(memory.local_read_generations_outstanding(), 0u);
}
