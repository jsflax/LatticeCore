#include "TestHelpers.hpp"
#include <lattice.hpp>          // swift_lattice, swift_lattice_ref (the bridge Swift uses)
#include <lattice/sync.hpp>
#include <filesystem>

// ============================================================================
// 1.5.0 — history regeneration covers link tables; disk space actually comes
// back and the bridge says what happened.
//
// Link rows: generate_history() excluded every underscore-prefixed table, so a
// force-compaction regenerated every model row DETACHED from its relationships
// (`_Parent_Child_prop` link tables are real synced tables with real audit
// rows). A fresh peer catching up from that history had the rows and none of
// the links.
//
// Space: in WAL mode VACUUM writes the rebuilt image into the WAL; the main
// file shrinks only at the NEXT checkpoint. A caller that checkpointed first
// and vacuumed second saw a 17 GB file stay 17 GB — and the sealed bridge
// wrapper reported nothing either way.
// ============================================================================

namespace {

lattice::swift_schema_entry make_schema(
    const std::string& table_name,
    std::initializer_list<std::pair<std::string, lattice::property_descriptor>> props) {
    lattice::swift_schema_entry entry;
    entry.table_name = table_name;
    for (const auto& [name, desc] : props) entry.properties[name] = desc;
    return entry;
}

lattice::property_descriptor text_prop(const std::string& name) {
    lattice::property_descriptor d;
    d.name = name; d.type = lattice::column_type::text; d.kind = lattice::property_kind::primitive;
    return d;
}

lattice::property_descriptor int_prop(const std::string& name) {
    lattice::property_descriptor d;
    d.name = name; d.type = lattice::column_type::integer; d.kind = lattice::property_kind::primitive;
    return d;
}

lattice::property_descriptor list_prop(const std::string& name, const std::string& target) {
    lattice::property_descriptor d;
    d.name = name; d.kind = lattice::property_kind::list; d.target_table = target;
    d.type = lattice::column_type::text;
    return d;
}

lattice::SchemaVector band_schemas() {
    return {
        make_schema("HxBand", {{"name", text_prop("name")}, {"albums", list_prop("albums", "HxAlbum")}}),
        make_schema("HxAlbum", {{"title", text_prop("title")}, {"year", int_prop("year")}}),
    };
}

int64_t count(lattice::database& db, const std::string& sql) {
    auto rows = db.query(sql, {});
    return rows.empty() ? -1 : std::get<int64_t>(rows[0].begin()->second);
}

int64_t page_count(lattice::database& db) { return count(db, "PRAGMA page_count"); }

uintmax_t file_bytes(const std::string& p) {
    std::error_code ec;
    auto n = std::filesystem::file_size(p, ec);
    return ec ? 0 : n;
}

} // namespace

// ---------------------------------------------------------------------------
// generate_history / force_compact regenerate link rows in the live shape.
// ---------------------------------------------------------------------------
TEST(HistoryRegeneration, ForceCompactRegeneratesLinkRows) {
    TempDB tmp{"history_links"};
    auto schemas = band_schemas();
    auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *ref->get();

    db.db().execute("INSERT INTO HxBand(globalId, name) VALUES('band-1', 'Zeppelin')");
    db.db().execute("INSERT INTO HxAlbum(globalId, title, year) VALUES('al-1', 'IV', 1971)");
    db.db().execute("INSERT INTO HxAlbum(globalId, title, year) VALUES('al-2', 'Houses', 1973)");
    db.db().execute("INSERT INTO _HxBand_HxAlbum_albums(lhs, rhs) VALUES('band-1', 'al-1')");
    db.db().execute("INSERT INTO _HxBand_HxAlbum_albums(lhs, rhs) VALUES('band-1', 'al-2')");
    ASSERT_EQ(count(db.db(), "SELECT COUNT(*) FROM AuditLog WHERE tableName = '_HxBand_HxAlbum_albums'"), 2)
        << "test premise: live link triggers mint audit rows";
    const int64_t max_before = count(db.db(), "SELECT COALESCE(MAX(id),0) FROM AuditLog");

    const int64_t regenerated = db.force_compact_audit_log();
    EXPECT_EQ(regenerated, 5) << "1 band + 2 albums + 2 links";

    // Link snapshots exist, in exactly the live trigger's shape.
    auto links = db.db().query(
        "SELECT rowId, globalRowId, changedFields, changedFieldsNames, synthesized "
        "FROM AuditLog WHERE tableName = '_HxBand_HxAlbum_albums' ORDER BY id", {});
    ASSERT_EQ(links.size(), 2u);
    for (const auto& row : links) {
        EXPECT_EQ(std::get<int64_t>(row.at("rowId")), 0);
        EXPECT_FALSE(std::get<std::string>(row.at("globalRowId")).empty());
        const auto fields = std::get<std::string>(row.at("changedFields"));
        EXPECT_NE(fields.find("\"lhs\":\"band-1\""), std::string::npos) << fields;
        EXPECT_NE(fields.find("\"rhs\":\"al-"), std::string::npos) << fields;
        EXPECT_EQ(std::get<std::string>(row.at("changedFieldsNames")), "[\"lhs\",\"rhs\"]");
        EXPECT_EQ(std::get<int64_t>(row.at("synthesized")), 1);
    }
    // And the sequence kept counting (A2) — nothing was renumbered.
    EXPECT_GT(count(db.db(), "SELECT MIN(id) FROM AuditLog"), max_before);

    // A fresh peer catching up from the regenerated history receives the links.
    TempDB peer_tmp{"history_links_peer"};
    auto peer_schemas = band_schemas();
    auto* peer_ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(peer_tmp.str()), peer_schemas);
    auto& peer = *peer_ref->get();
    auto entries = lattice::events_after(db.db(), std::nullopt);
    ASSERT_EQ(entries.size(), 5u);
    auto applied = lattice::apply_remote_changes(peer, entries);
    EXPECT_EQ(applied.size(), 5u);
    EXPECT_EQ(count(peer.db(), "SELECT COUNT(*) FROM _HxBand_HxAlbum_albums WHERE lhs = 'band-1'"), 2)
        << "the relationship must survive a compaction round-trip";
    EXPECT_EQ(count(peer.db(), "SELECT COUNT(*) FROM HxAlbum"), 2);
}

TEST(HistoryRegeneration, ShadowAndInternalTablesAreSkippedByShape) {
    TempDB tmp{"history_shape"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"p", 1, std::nullopt});
    // A table with no audit shape (no id/globalId, not a link) must be skipped, not fail the pass.
    db.db().execute("CREATE TABLE _weird_side_table (k TEXT, v TEXT)");
    db.db().execute("INSERT INTO _weird_side_table VALUES('a', 'b')");
    EXPECT_NO_THROW(db.force_compact_audit_log());
    EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM AuditLog WHERE tableName = '_weird_side_table'"), 0);
    EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM AuditLog WHERE tableName = 'TestPerson'"), 1);
}

// ---------------------------------------------------------------------------
// reclaim_space: the file shrinks in ONE pass; the old order did not shrink it.
// ---------------------------------------------------------------------------
namespace {

/// 200 rows × 64 KB, each also mirrored into an audit row → tens of MB on disk.
void bloat(lattice::swift_lattice& db) {
    const std::string blob(64 * 1024, 'x');
    for (int i = 0; i < 200; ++i) {
        db.db().execute("INSERT INTO BigRow(globalId, payload) VALUES(?, ?)",
                        {std::string("g-") + std::to_string(i), blob});
    }
}

lattice::SchemaVector bigrow_schema() {
    return { make_schema("BigRow", {{"payload", text_prop("payload")}}) };
}

} // namespace

TEST(Reclaim, ReclaimSpaceShrinksFileInOnePass) {
    TempDB tmp{"reclaim_one_pass"};
    auto schemas = bigrow_schema();
    auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *ref->get();
    bloat(db);
    db.checkpoint();   // land the bloat in the main file so the peak is real
    const auto peak_pages = page_count(db.db());
    const auto peak_bytes = file_bytes(tmp.str());
    ASSERT_GT(peak_bytes, 10u * 1024 * 1024) << "test premise: the file is tens of MB";

    db.db().execute("DELETE FROM BigRow");
    db.db().execute("DELETE FROM AuditLog");

    auto r = ref->reclaim_space();
    EXPECT_TRUE(r.ok) << r.error;
    EXPECT_EQ(r.passes, 1);
    EXPECT_GE(r.pages_before, peak_pages) << "the two DELETEs above can only add pages before reclaim measures";
    EXPECT_LT(r.pages_after, peak_pages / 10) << "live data is a few pages; the file must follow";
    EXPECT_LT(file_bytes(tmp.str()), peak_bytes / 10) << "the MAIN file shrank, not just the WAL view";
    EXPECT_LT(r.wal_bytes_after, 64 * 1024) << "TRUNCATE checkpoint zeroed the -wal";
    EXPECT_TRUE(lattice::last_bridge_error().empty());
}

TEST(Reclaim, CheckpointThenVacuumLeavesTheFileLarge) {
    // Pins the ordering bug: the old CLI recipe (checkpoint → vacuum → exit)
    // rebuilt the image into the WAL and never folded it back.
    TempDB tmp{"reclaim_wrong_order"};
    auto schemas = bigrow_schema();
    auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *ref->get();
    bloat(db);
    db.checkpoint();
    const auto peak_bytes = file_bytes(tmp.str());
    db.db().execute("DELETE FROM BigRow");
    db.db().execute("DELETE FROM AuditLog");

    db.checkpoint();
    EXPECT_TRUE(ref->vacuum());
    EXPECT_GT(file_bytes(tmp.str()), peak_bytes / 2)
        << "after VACUUM alone the main file is still at its peak — the rebuilt image sits in the WAL";

    auto ck = ref->checkpoint();
    EXPECT_TRUE(ck.complete) << "busy=" << ck.busy << " log=" << ck.log_frames << " ckpt=" << ck.checkpointed;
    EXPECT_LT(file_bytes(tmp.str()), peak_bytes / 10) << "the checkpoint AFTER vacuum is what shrinks the file";
}

TEST(Reclaim, VacuumReportsFailureInsteadOfPretending) {
    TempDB tmp{"reclaim_busy"};
    auto schemas = bigrow_schema();
    lattice::swift_configuration cfg(tmp.str());
    cfg.busy_timeout_ms = 200;   // don't wait 30 s for the lock below
    auto* ref = lattice::swift_lattice_ref::create(cfg, schemas);
    auto& db = *ref->get();
    db.db().execute("INSERT INTO BigRow(globalId, payload) VALUES('g', 'p')");

    // Another connection holds the WRITE lock (BEGIN IMMEDIATE takes it without
    // touching a table — a raw connection has no sync_disabled() for the audit
    // triggers): VACUUM cannot run.
    lattice::database other(tmp.str());
    other.execute("BEGIN IMMEDIATE");

    EXPECT_FALSE(ref->vacuum());
    EXPECT_FALSE(lattice::last_bridge_error().empty()) << "the sealed tier must surface WHY";
    auto r = ref->reclaim_space();
    EXPECT_FALSE(r.ok);
    EXPECT_FALSE(r.error.empty());

    other.execute("ROLLBACK");
    EXPECT_TRUE(ref->vacuum()) << "and succeed once the lock is gone";
    EXPECT_TRUE(lattice::last_bridge_error().empty());

    // checkpoint_bounded: -2 distinguishes "threw" from a legitimate 0.
    EXPECT_GE(ref->checkpoint_bounded(250), 0);
}
