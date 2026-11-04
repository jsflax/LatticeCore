#include "TestHelpers.hpp"
#include <lattice.hpp>          // swift_lattice, swift_lattice_ref (the bridge Swift uses)
#include <lattice/sync.hpp>
#include <filesystem>
#include <thread>

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

TEST(CurrentHistory, PrunedInsertTailBootstrapsTwoFreshPeersWithLinksAndPreservesHistory) {
    TempDB source_tmp{"current_history"};
    auto* source_ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(source_tmp.str()), band_schemas());
    auto& source = *source_ref->get();
    source.db().execute("INSERT INTO HxBand(globalId,name) VALUES('band-1','before')");
    source.db().execute("INSERT INTO HxAlbum(globalId,title,year) VALUES('album-1','One',2001),('album-2','Two',2002),('deleted','Gone',2000)");
    source.db().execute("INSERT INTO _HxBand_HxAlbum_albums(lhs,rhs) VALUES('band-1','album-1'),('band-1','album-2')");
    source.ensure_virtual_link_table("_HxBand_favorites");
    source.db().execute("INSERT INTO _HxBand_favorites(lhs,rhs,rhs_type) VALUES('band-1','album-2','HxAlbum')");
    source.db().execute("UPDATE HxBand SET name='current' WHERE globalId='band-1'");
    source.db().execute("DELETE FROM HxAlbum WHERE globalId='deleted'");
    source.db().execute("DELETE FROM AuditLog WHERE operation='INSERT'");
    source.db().execute("INSERT INTO _lattice_replication_slots(sync_id,confirmed_audit_id,upload_floor) VALUES('existing-peer',4,3)");
    const auto retained = source.db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto slots = source.db().query("SELECT * FROM _lattice_replication_slots ORDER BY sync_id");
    ASSERT_EQ(retained.size(), 2u); // UPDATE of a live row + DELETE of a removed row.
    const auto old_max = count(source.db(), "SELECT MAX(id) FROM AuditLog");
    EXPECT_THROW(source.ensure_current_history(), std::runtime_error);

    for (int peer_number = 0; peer_number < 2; ++peer_number) {
        source.begin_transaction();
        EXPECT_EQ(source.ensure_current_history(), 6); // 3 models + 2 list links + 1 virtual link.
        EXPECT_EQ(source.ensure_current_history(), 0);
        source.commit();
        EXPECT_EQ(source.db().query("SELECT * FROM AuditLog WHERE id<=? ORDER BY id", {old_max}), retained);
        EXPECT_EQ(source.db().query("SELECT * FROM _lattice_replication_slots ORDER BY sync_id"), slots);
        EXPECT_EQ(count(source.db(), "SELECT COUNT(*) FROM AuditLog WHERE operation='INSERT' AND synthesized=0 AND isFromRemote=0"), 6);
        EXPECT_GT(count(source.db(), "SELECT MIN(id) FROM AuditLog WHERE operation='INSERT'"), old_max);
        EXPECT_EQ(count(source.db(), "SELECT COUNT(*) FROM sqlite_master WHERE name='idx_lattice_current_history_baseline'"), 0);

        TempDB peer_tmp{"current_history_peer"};
        auto* peer_ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(peer_tmp.str()), band_schemas());
        auto& peer = *peer_ref->get();
        peer.ensure_virtual_link_table("_HxBand_favorites");
        const auto events = lattice::events_after(source.db(), std::nullopt);
        ASSERT_EQ(events.size(), 8u);
        EXPECT_NO_THROW(lattice::apply_remote_changes(peer, events));
        EXPECT_EQ(std::get<std::string>(peer.db().query("SELECT name FROM HxBand")[0].at("name")), "current");
        EXPECT_EQ(count(peer.db(), "SELECT COUNT(*) FROM HxAlbum"), 2);
        EXPECT_EQ(count(peer.db(), "SELECT COUNT(*) FROM HxAlbum WHERE globalId='deleted'"), 0);
        EXPECT_EQ(peer.db().query("SELECT * FROM _HxBand_HxAlbum_albums ORDER BY rhs"), source.db().query("SELECT * FROM _HxBand_HxAlbum_albums ORDER BY rhs"));
        EXPECT_EQ(peer.db().query("SELECT * FROM _HxBand_favorites"), source.db().query("SELECT * FROM _HxBand_favorites"));
        source.begin_transaction();
        const auto source_identity = source.current_identity_snapshot();
        source.commit();
        peer.begin_transaction();
        EXPECT_EQ(peer.current_identity_snapshot(), source_identity);
        peer.commit();
        // A later fresh device needs another baseline after retention runs again.
        if (peer_number == 0) source.db().execute("DELETE FROM AuditLog WHERE operation='INSERT'");
    }
}

TEST(CurrentHistory, RollbackAndLinkInsertFailureCannotCommitPartialRepair) {
    TempDB tmp{"current_history_atomic"};
    auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), band_schemas());
    auto& db = *ref->get();
    db.db().execute("INSERT INTO HxBand(globalId,name) VALUES('band','Band')");
    db.db().execute("INSERT INTO HxAlbum(globalId,title,year) VALUES('album','Album',2000)");
    db.db().execute("INSERT INTO _HxBand_HxAlbum_albums(lhs,rhs) VALUES('band','album')");
    db.db().execute("DELETE FROM AuditLog");
    db.begin_transaction();
    EXPECT_EQ(db.ensure_current_history(), 3);
    db.rollback();
    EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM AuditLog"), 0);

    db.db().execute("CREATE TRIGGER reject_history_link BEFORE INSERT ON AuditLog WHEN NEW.tableName='_HxBand_HxAlbum_albums' BEGIN SELECT RAISE(ABORT,'blocked link baseline'); END");
    db.begin_transaction();
    EXPECT_THROW(db.ensure_current_history(), lattice::db_error);
    EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM AuditLog"), 2); // The caller still owns the transaction.
    db.rollback();
    EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM AuditLog"), 0);
    EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM sqlite_master WHERE name='idx_lattice_current_history_baseline'"), 0);
    db.db().execute("DROP TRIGGER reject_history_link");
    db.begin_transaction();
    EXPECT_EQ(db.ensure_current_history(), 3);
    db.commit();
}

TEST(CurrentHistory, CompleteScalarPayloadKeepsRealPrecisionAndNullableBlob) {
    TempDB tmp{"current_history_scalars"};
    auto real = text_prop("exact"); real.type = lattice::column_type::real;
    auto blob = text_prop("bytes"); blob.type = lattice::column_type::blob; blob.nullable = true;
    auto schemas = lattice::SchemaVector{make_schema("HxScalar", {{"exact", real}, {"bytes", blob}})};
    auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *ref->get();
    const double precise = 1788921763.083822;
    db.db().execute("INSERT INTO HxScalar(globalId,exact,bytes) VALUES('one',?,X'000AFF'),('two',?,NULL)", {precise, precise});
    db.db().execute("DELETE FROM AuditLog");
    db.db().execute("CREATE TABLE _unrelated(k TEXT,v TEXT)");
    db.begin_transaction();
    EXPECT_EQ(db.ensure_current_history(), 2);
    db.commit();
    auto entries = lattice::events_after(db.db(), std::nullopt);
    ASSERT_EQ(entries.size(), 2u);
    EXPECT_EQ(std::get<double>(entries[0].changed_fields.at("exact").to_column_value()), precise);
    EXPECT_EQ(std::get<std::string>(entries[0].changed_fields.at("bytes").to_column_value()), "000AFF");
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(entries[1].changed_fields.at("bytes").to_column_value()));
}

TEST(CurrentHistory, IdentitySnapshotDetectsEqualCountReplacementAndAllLinkKindsWithoutText) {
    TempDB tmp{"current_history_identity"};
    auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), band_schemas());
    auto& db = *ref->get();
    db.db().execute("INSERT INTO HxBand(globalId,name) VALUES('band','PRIVATE BAND TEXT')");
    db.db().execute("INSERT INTO HxAlbum(globalId,title,year) VALUES('album','PRIVATE ALBUM TEXT',2000)");
    db.db().execute("INSERT INTO _HxBand_HxAlbum_albums(lhs,rhs) VALUES('band','album')");
    db.ensure_virtual_link_table("_HxBand_favorites");
    db.db().execute("INSERT INTO _HxBand_favorites(lhs,rhs,rhs_type) VALUES('band','album','HxAlbum')");
    EXPECT_THROW(db.current_identity_snapshot(), std::runtime_error);
    db.begin_transaction();
    const auto original = db.current_identity_snapshot();
    EXPECT_EQ(original.find("PRIVATE"), std::string::npos);
    EXPECT_EQ(original.find("AuditLog"), std::string::npos);
    EXPECT_NE(original.find("\"album\",\"HxAlbum\""), std::string::npos); // Polymorphic type is represented positionally.
    db.db().execute("UPDATE HxBand SET name='changed text'");
    EXPECT_EQ(db.current_identity_snapshot(), original);
    db.rollback();

    db.begin_transaction();
    db.db().execute("UPDATE HxAlbum SET globalId='replacement' WHERE globalId='album'");
    EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM HxAlbum"), 1);
    EXPECT_NE(db.current_identity_snapshot(), original);
    db.rollback();
    db.begin_transaction();
    db.db().execute("DELETE FROM _HxBand_HxAlbum_albums");
    EXPECT_NE(db.current_identity_snapshot(), original);
    db.rollback();
    db.begin_transaction();
    db.db().execute("DELETE FROM _HxBand_favorites");
    EXPECT_NE(db.current_identity_snapshot(), original);
    db.rollback();
    db.begin_transaction();
    db.db().execute("UPDATE _HxBand_favorites SET rhs_type='DifferentModel'");
    EXPECT_NE(db.current_identity_snapshot(), original);
    db.rollback();
    db.begin_transaction();
    EXPECT_EQ(db.current_identity_snapshot(), original);
    db.commit();
}

TEST(CurrentHistory, OwnedUnionLiveAndPrunedBaselineSyncKeepCasePayloadAndIdentity) {
    auto schemas = lattice::SchemaVector{make_schema("HxUnionOwner", {{"state", text_prop("state")}})};
    lattice::union_descriptor descriptor{"_HxSyncState", {
        {"none", {}}, {"waiting", {{"reason", lattice::column_type::text}}}}};
    TempDB source_tmp{"history_union_source"}, peer_tmp{"history_union_peer"}, fresh_tmp{"history_union_fresh"};
    auto* source_ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(source_tmp.str()), schemas);
    auto* peer_ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(peer_tmp.str()), schemas);
    auto* fresh_ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(fresh_tmp.str()), schemas);
    auto& source = *source_ref->get(); auto& peer = *peer_ref->get(); auto& fresh = *fresh_ref->get();
    for (auto* store : {&source, &peer, &fresh}) store->ensure_union_table("_HxSyncState", descriptor, "HxUnionOwner", "state");
    source.db().execute("INSERT INTO _HxSyncState(globalId,\"case\",waiting__reason) VALUES('state','waiting','First reason')");
    source.db().execute("INSERT INTO HxUnionOwner(globalId,state) VALUES('owner','state')");
    auto events = lattice::events_after(source.db(), std::nullopt);
    EXPECT_EQ(lattice::apply_remote_changes(peer, events).size(), events.size());
    ASSERT_EQ(count(peer.db(), "SELECT COUNT(*) FROM _HxSyncState"), 1);
    EXPECT_EQ(std::get<std::string>(peer.db().query("SELECT waiting__reason FROM _HxSyncState")[0].at("waiting__reason")), "First reason");

    source.db().execute("UPDATE _HxSyncState SET \"case\"='none',waiting__reason=NULL WHERE globalId='state'");
    events = lattice::events_after(source.db(), std::nullopt);
    EXPECT_EQ(lattice::apply_remote_changes(peer, events).size(), events.size());
    EXPECT_EQ(std::get<std::string>(peer.db().query("SELECT \"case\" FROM _HxSyncState")[0].at("case")), "none");

    source.db().execute("UPDATE _HxSyncState SET \"case\"='waiting',waiting__reason='Retained reason' WHERE globalId='state'");
    source.db().execute("DELETE FROM AuditLog WHERE operation='INSERT'");
    source.begin_transaction();
    EXPECT_EQ(source.ensure_current_history(), 2);
    const auto identity = source.current_identity_snapshot();
    source.commit();
    events = lattice::events_after(source.db(), std::nullopt);
    EXPECT_EQ(lattice::apply_remote_changes(fresh, events).size(), events.size());
    ASSERT_EQ(count(fresh.db(), "SELECT COUNT(*) FROM _HxSyncState"), 1);
    EXPECT_EQ(std::get<std::string>(fresh.db().query("SELECT waiting__reason FROM _HxSyncState")[0].at("waiting__reason")), "Retained reason");
    fresh.begin_transaction();
    EXPECT_EQ(fresh.current_identity_snapshot(), identity);
    fresh.commit();

    // Existing private tables with the same physical shape are not union
    // storage unless configured ownership metadata establishes that role.
    peer.db().execute("CREATE TABLE _PrivateState(id INTEGER PRIMARY KEY,globalId TEXT UNIQUE,\"case\" TEXT)");
    lattice::audit_log_entry private_entry;
    private_entry.global_id = "private-event"; private_entry.global_row_id = "private-row";
    private_entry.table_name = "_PrivateState"; private_entry.operation = "INSERT";
    private_entry.changed_fields_names = {"case"}; private_entry.changed_fields = {{"case", std::string("forged")}};
    EXPECT_TRUE(lattice::apply_remote_changes(peer, {private_entry}).empty());
    EXPECT_EQ(count(peer.db(), "SELECT COUNT(*) FROM _PrivateState"), 0);
}

TEST(CurrentHistory, OwnedUnionRejectsAmbiguousDiscriminatorBeforeDedupAndEffects) {
    auto schemas = lattice::SchemaVector{make_schema("HxAmbiguousOwner", {{"state", text_prop("state")}})};
    lattice::union_descriptor descriptor{"_HxAmbiguousState", {
        {"none", {}}, {"waiting", {{"reason", lattice::column_type::text}}}}};
    TempDB tmp{"history_union_ambiguous"};
    auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *ref->get();
    db.ensure_union_table("_HxAmbiguousState", descriptor, "HxAmbiguousOwner", "state");
    lattice::audit_log_entry accepted;
    accepted.global_id = "accepted-union-event"; accepted.global_row_id = "state";
    accepted.table_name = "_HxAmbiguousState"; accepted.operation = "INSERT";
    accepted.changed_fields_names = {"case", "waiting__reason"};
    accepted.changed_fields = {{"case", std::string("waiting")}, {"waiting__reason", std::string("Kept")}};
    ASSERT_EQ(lattice::apply_remote_changes(db, {accepted}), std::vector<std::string>{accepted.global_id});
    const auto audits_before = count(db.db(), "SELECT COUNT(*) FROM AuditLog");
    const auto receipts_before = count(db.db(), "SELECT COUNT(*) FROM _lattice_applied_receipts");

    // Both the names and value map participate in ambiguity detection. Reject
    // even equal values, and reject before the already-applied ACK shortcut.
    for (int spelling = 0; spelling < 3; ++spelling) {
        for (bool retained_identity : {false, true}) {
            auto ambiguous = accepted;
            if (!retained_identity) {
                ambiguous.global_id = "new-ambiguous-" + std::to_string(spelling);
                ambiguous.global_row_id = "new-state-" + std::to_string(spelling);
            }
            if (spelling != 1) ambiguous.changed_fields_names.push_back("\"case\"");
            if (spelling != 2) ambiguous.changed_fields["\"case\""] = std::string("waiting");
            EXPECT_TRUE(lattice::apply_remote_changes(db, {ambiguous}).empty());
            EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM _HxAmbiguousState"), 1);
            EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM AuditLog"), audits_before);
            EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM _lattice_applied_receipts"), receipts_before);
        }
    }
    EXPECT_EQ(std::get<std::string>(db.db().query("SELECT waiting__reason FROM _HxAmbiguousState")[0].at("waiting__reason")), "Kept");
}

TEST(CurrentHistory, OwnedUnionPendingImportAcceptsCanonicalAndLegacyNamesWithoutChangingWireHistory) {
    auto schemas = lattice::SchemaVector{make_schema("HxPendingOwner", {{"state", text_prop("state")}})};
    lattice::union_descriptor descriptor{"_HxPendingState", {{"waiting", {{"reason", lattice::column_type::text}}}}};
    TempDB tmp{"history_union_pending"};
    auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *ref->get();
    db.ensure_union_table("_HxPendingState", descriptor, "HxPendingOwner", "state");
    for (bool legacy : {false, true}) {
        const std::string field = legacy ? "\"case\"" : "case";
        lattice::audit_log_entry entry;
        entry.global_id = legacy ? "pending-legacy" : "pending-canonical";
        entry.global_row_id = legacy ? "legacy-row" : "canonical-row";
        entry.table_name = "_HxPendingState"; entry.operation = "INSERT";
        entry.changed_fields_names = {field, "waiting__reason"};
        entry.changed_fields = {{field, std::string("waiting")}, {"waiting__reason", std::string("Pending value")}};
        const auto outcome = lattice::apply_pending_changes(db, {entry});
        EXPECT_EQ(outcome.imported_ids, std::vector<std::string>{entry.global_id});
        EXPECT_TRUE(outcome.rejected_ids.empty());
        EXPECT_TRUE(outcome.already_present_ids.empty());
        const auto rows = db.db().query("SELECT \"case\",waiting__reason FROM _HxPendingState WHERE globalId=?", {entry.global_row_id});
        ASSERT_EQ(rows.size(), 1u);
        EXPECT_EQ(std::get<std::string>(rows[0].at("case")), "waiting");
        EXPECT_EQ(std::get<std::string>(rows[0].at("waiting__reason")), "Pending value");
        const auto events = lattice::events_after(db.db(), std::nullopt);
        const auto found = std::find_if(events.begin(), events.end(), [&](const auto& event) { return event.global_id == entry.global_id; });
        ASSERT_NE(found, events.end());
        EXPECT_EQ(found->changed_fields_names, entry.changed_fields_names);
        EXPECT_TRUE(found->changed_fields.count(field));
        EXPECT_FALSE(found->changed_fields.count(legacy ? "case" : "\"case\""));
        const auto replay = lattice::apply_pending_changes(db, {entry});
        EXPECT_EQ(replay.already_present_ids, std::vector<std::string>{entry.global_id});
        EXPECT_TRUE(replay.imported_ids.empty());
        EXPECT_TRUE(replay.rejected_ids.empty());
    }
    EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM _HxPendingState"), 2);
}

TEST(CurrentHistory, RejectsUncheckedAndOtherThreadTransactionsBeforeAnyWork) {
    TempDB tmp{"history_checked_owner"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"owned", 1, std::nullopt});
    db.db().execute("DELETE FROM AuditLog");
    EXPECT_THROW(db.ensure_current_history(), std::runtime_error);
    EXPECT_THROW(db.current_identity_snapshot(), std::runtime_error);
    EXPECT_THROW(db.append_current_snapshots({"TestPerson"}, "unchecked"), std::runtime_error);
    // A raw SQLite transaction is not an admitted Lattice transaction owner.
    db.db().execute("BEGIN");
    EXPECT_THROW(db.ensure_current_history(), std::runtime_error);
    EXPECT_THROW(db.current_identity_snapshot(), std::runtime_error);
    EXPECT_THROW(db.append_current_snapshots({"TestPerson"}, "unchecked"), std::runtime_error);
    EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM AuditLog"), 0);
    db.db().execute("ROLLBACK");
    db.begin_transaction();
    int refused = 0;
    std::thread other([&] {
        try { (void)db.ensure_current_history(); } catch (const std::runtime_error&) { ++refused; }
        try { (void)db.current_identity_snapshot(); } catch (const std::runtime_error&) { ++refused; }
        try { (void)db.append_current_snapshots({"TestPerson"}, "other-thread"); } catch (const std::runtime_error&) { ++refused; }
    });
    other.join();
    EXPECT_EQ(refused, 3);
    EXPECT_TRUE(db.db().is_in_transaction());
    EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM AuditLog"), 0);
    // Positive control: the original owner can still perform and roll back.
    EXPECT_EQ(db.ensure_current_history(), 1);
    EXPECT_FALSE(db.current_identity_snapshot().empty());
    EXPECT_EQ(db.append_current_snapshots({"TestPerson"}, "owner"), 1);
    db.rollback();
    EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM AuditLog"), 0);
}


namespace {

// Reinstall the exact pre-precision value expressions while preserving the
// trigger gates and all other generated SQL. This models a real existing store.
void install_legacy_real_audit_templates(lattice::database& db, const std::string& table,
                                        const std::vector<std::string>& real_columns) {
    const auto triggers = db.query(
        "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND name IN (?,?,?)",
        {"Audit" + table + "Insert", "AuditLog_Update_" + table, "Audit" + table + "Delete"});
    ASSERT_EQ(triggers.size(), 3u);
    for (const auto& row : triggers) {
        auto sql = std::get<std::string>(row.at("sql"));
        size_t replacements = 0;
        for (const auto& column : real_columns) {
            for (const char* prefix : {"NEW", "OLD"}) {
                const auto value = std::string(prefix) + "." + column;
                const auto precise = "CASE WHEN " + value + " IS NULL THEN NULL ELSE json(printf('%!.17g', " + value + ")) END";
                size_t offset = 0;
                while ((offset = sql.find(precise, offset)) != std::string::npos) {
                    sql.replace(offset, precise.size(), value);
                    offset += value.size();
                    ++replacements;
                }
            }
        }
        ASSERT_GT(replacements, 0u);
        db.execute("DROP TRIGGER " + std::get<std::string>(row.at("name")));
        db.execute(sql);
    }
}

// Serialize this one-table fixture using the published ffb84 Swift fingerprint
// format. The property serializer is unchanged; the old format had no audit
// template revision. A current schema cookie must not admit these old triggers.
struct LegacyRealAuditFingerprint : lattice::lattice_db {
    static std::string key(const lattice::swift_schema_entry& entry) {
        std::ostringstream out;
        out << "swift\nepoch:9\ntarget_schema_version:1\ntable:" << entry.table_name << '\n';
        std::vector<std::string> names;
        for (const auto& item : entry.properties) names.push_back(item.first);
        std::sort(names.begin(), names.end());
        for (const auto& name : names) {
            out << "prop:" << name << '\x01';
            serialize_property_for_fingerprint(out, entry.properties.at(name));
        }
        std::ostringstream hex;
        hex << std::hex << std::setfill('0') << std::setw(16) << fnv1a_hash(out.str());
        return "schema_fingerprint:" + hex.str();
    }
};

} // namespace

TEST(AuditPrecision, LiveRealInsertUpdateDeletePreserveExactValuesAndNull) {
    TempDB tmp{"live_audit_real"};
    auto exact = text_prop("exact"); exact.type = lattice::column_type::real;
    auto optional = exact; optional.name = "optional"; optional.nullable = true;
    const auto schemas = lattice::SchemaVector{make_schema("HxLiveReal", {{"exact", exact}, {"optional", optional}})};
    auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *ref->get();
    const double first = 1788921763.083822;
    const double next = 1788921763.083823;
    db.db().execute("INSERT INTO HxLiveReal(globalId,exact,optional) VALUES('one',?,NULL)", {first});
    db.db().execute("UPDATE HxLiveReal SET exact=?,optional=? WHERE globalId='one'", {next, first});
    db.db().execute("UPDATE HxLiveReal SET optional=NULL WHERE globalId='one'");
    db.db().execute("DELETE FROM HxLiveReal WHERE globalId='one'");
    const auto events = lattice::events_after(db.db(), std::nullopt);
    ASSERT_EQ(events.size(), 4u);
    EXPECT_EQ(events[0].operation, "INSERT");
    EXPECT_EQ(std::get<double>(events[0].changed_fields.at("exact").to_column_value()), first);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(events[0].changed_fields.at("optional").to_column_value()));
    EXPECT_EQ(events[1].operation, "UPDATE");
    EXPECT_EQ(std::get<double>(events[1].changed_fields.at("exact").to_column_value()), next);
    EXPECT_EQ(std::get<double>(events[1].changed_fields.at("optional").to_column_value()), first);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(events[2].changed_fields.at("optional").to_column_value()));
    EXPECT_EQ(events[3].operation, "DELETE");
    EXPECT_EQ(std::get<double>(events[3].changed_fields.at("exact").to_column_value()), next);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(events[3].changed_fields.at("optional").to_column_value()));
    db.close();
}

TEST(AuditPrecision, ExistingFingerprintUpgradesRealTriggersOnceWithoutChangingWriterEpoch) {
    TempDB tmp{"audit_real_upgrade"};
    auto exact = text_prop("exact"); exact.type = lattice::column_type::real;
    const auto entry = make_schema("HxUpgradeReal", {{"exact", exact}});
    const auto schemas = lattice::SchemaVector{entry};
    auto config = lattice::swift_configuration(tmp.str()); config.target_schema_version = 1;
    const double precise = 1788921763.083822;
    int64_t audit_before = 0;
    {
        auto* ref = lattice::swift_lattice_ref::create(config, schemas);
        auto& db = *ref->get();
        db.db().execute("INSERT INTO HxUpgradeReal(globalId,exact) VALUES('one',?)", {precise});
        audit_before = count(db.db(), "SELECT COUNT(*) FROM AuditLog");
        db.close();
    }
    {
        // Stamp after close's possible PRAGMA optimize DDL. This is an owned
        // current writer constructing the old trigger/fingerprint fixture.
        lattice::database raw(tmp.str());
        lattice::detail::register_sync_storage_writer(raw);
        const auto receipt = raw.query("SELECT value FROM _lattice_meta WHERE key LIKE 'schema_structure:%' LIMIT 1");
        ASSERT_EQ(receipt.size(), 1u);
        install_legacy_real_audit_templates(raw, "HxUpgradeReal", {"exact"});
        const auto cookie = std::to_string(count(raw, "PRAGMA schema_version"));
        raw.execute("DELETE FROM _lattice_meta WHERE key LIKE 'schema_fingerprint:%' OR key LIKE 'schema_structure:%'");
        const auto old_key = LegacyRealAuditFingerprint::key(entry);
        raw.execute("INSERT INTO _lattice_meta(key,value) VALUES(?,?)", {old_key, cookie});
        raw.execute("INSERT INTO _lattice_meta(key,value) VALUES(?,?)",
                    {"schema_structure:" + old_key, std::get<std::string>(receipt[0].at("value"))});
    }
    {
        lattice::database observer(tmp.str(), lattice::database::open_mode::read_only);
        const auto markers = observer.query("SELECT value FROM _lattice_meta WHERE key=?", {LegacyRealAuditFingerprint::key(entry)});
        ASSERT_EQ(markers.size(), 1u);
        EXPECT_EQ(std::get<std::string>(markers[0].at("value")), std::to_string(count(observer, "PRAGMA schema_version")));
        const auto old_sql = observer.query("SELECT sql FROM sqlite_master WHERE name='AuditHxUpgradeRealInsert'");
        ASSERT_EQ(old_sql.size(), 1u);
        EXPECT_EQ(std::get<std::string>(old_sql[0].at("sql")).find("json(printf('%!.17g', "), std::string::npos);
    }
    int64_t upgraded_cookie = 0;
    std::vector<lattice::database::row_t> upgraded_triggers;
    {
        auto* ref = lattice::swift_lattice_ref::create(config, schemas);
        auto& db = *ref->get();
        EXPECT_EQ(lattice::lattice_db::schema_format_epoch(), 9);
        EXPECT_EQ(count(db.db(), "SELECT CAST(value AS INTEGER) FROM _lattice_meta WHERE key='schema_version'"), 1);
        EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM AuditLog"), audit_before);
        EXPECT_EQ(std::get<double>(db.db().query("SELECT exact FROM HxUpgradeReal")[0].at("exact")), precise);
        upgraded_triggers = db.db().query(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name='HxUpgradeReal' AND name LIKE 'Audit%' ORDER BY name");
        ASSERT_EQ(upgraded_triggers.size(), 3u);
        for (const auto& row : upgraded_triggers) {
            EXPECT_NE(std::get<std::string>(row.at("sql")).find("json(printf('%!.17g', "), std::string::npos);
        }
        db.db().execute("INSERT INTO HxUpgradeReal(globalId,exact) VALUES('two',?)", {precise});
        const auto events = lattice::events_after(db.db(), std::nullopt);
        ASSERT_FALSE(events.empty());
        EXPECT_EQ(std::get<double>(events.back().changed_fields.at("exact").to_column_value()), precise);
        upgraded_cookie = count(db.db(), "PRAGMA schema_version");
        db.close();
    }
    {
        auto* ref = lattice::swift_lattice_ref::create(config, schemas);
        auto& db = *ref->get();
        EXPECT_EQ(count(db.db(), "PRAGMA schema_version"), upgraded_cookie);
        EXPECT_EQ(db.db().query(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name='HxUpgradeReal' AND name LIKE 'Audit%' ORDER BY name"), upgraded_triggers);
        db.close();
    }
}

TEST(AuditPrecision, ExistingOwnedUnionRefreshesRealTriggersWithoutRebuildingRows) {
    TempDB tmp{"audit_union_real_upgrade"};
    const auto schemas = lattice::SchemaVector{make_schema("HxRealOwner", {{"state", text_prop("state")}})};
    const lattice::union_descriptor descriptor{"_HxRealState", {{"none", {}}, {"value", {{"amount", lattice::column_type::real}}}}};
    auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *ref->get();
    db.ensure_union_table("_HxRealState", descriptor, "HxRealOwner", "state");
    const double precise = 1788921763.083822;
    db.db().execute("INSERT INTO _HxRealState(globalId,\"case\",value__amount) VALUES('state','value',?)", {precise});
    const auto rows = db.db().query("SELECT * FROM _HxRealState");
    const auto audits = count(db.db(), "SELECT COUNT(*) FROM AuditLog");
    install_legacy_real_audit_templates(db.db(), "_HxRealState", {"value__amount"});
    db.ensure_union_table("_HxRealState", descriptor, "HxRealOwner", "state");
    EXPECT_EQ(db.db().query("SELECT * FROM _HxRealState"), rows);
    EXPECT_EQ(count(db.db(), "SELECT COUNT(*) FROM AuditLog"), audits);
    const auto cookie = count(db.db(), "PRAGMA schema_version");
    db.ensure_union_table("_HxRealState", descriptor, "HxRealOwner", "state");
    EXPECT_EQ(count(db.db(), "PRAGMA schema_version"), cookie);
    db.db().execute("INSERT INTO _HxRealState(globalId,\"case\",value__amount) VALUES('new','value',?)", {precise});
    const auto events = lattice::events_after(db.db(), std::nullopt);
    ASSERT_FALSE(events.empty());
    EXPECT_EQ(std::get<double>(events.back().changed_fields.at("value__amount").to_column_value()), precise);
    db.close();
}
