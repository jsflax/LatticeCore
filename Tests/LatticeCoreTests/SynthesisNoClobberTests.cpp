#include "TestHelpers.hpp"

// ============================================================================
// A5 — synthesized full-row snapshots are insert-if-absent
//
// Reconcile Phase-2 additions, classify's UPDATE→INSERT conversion, and
// nuclear-compact history regeneration all synthesize full-row INSERT
// entries from LOCAL state. Applied as unconditional every-column upserts
// (there is no timestamp LWW anywhere — "last write wins" is really
// last-ARRIVAL wins), a stale snapshot would revert a peer's newer edits
// and resurrect tombstones on re-expose/re-join/nuclear-compact. Marked
// synthesized, they apply only when the row is absent.
// ============================================================================

namespace {

lattice::audit_log_entry synth_insert(const std::string& gid,
                                      const std::string& row_gid,
                                      const std::string& name,
                                      int64_t age) {
    lattice::audit_log_entry e;
    e.global_id = gid;
    e.table_name = "TestPerson";
    e.operation = "INSERT";
    e.global_row_id = row_gid;
    e.timestamp = "1700000000.0";
    e.synthesized = true;
    e.changed_fields["name"] = lattice::any_property(name);
    e.changed_fields_names.push_back("name");
    e.changed_fields["age"] = lattice::any_property(age);
    e.changed_fields_names.push_back("age");
    return e;
}

} // namespace

TEST(SynthesisNoClobber, SynthesizedInsertDoesNotOverwriteExistingRow) {
    TempDB tmp{"synth_clobber"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"NewerEdit", 40, std::nullopt});
    auto gid_rows = db.db().query("SELECT globalId FROM TestPerson WHERE name = 'NewerEdit'");
    ASSERT_EQ(gid_rows.size(), 1u);
    const std::string row_gid = std::get<std::string>(gid_rows[0].at("globalId"));

    // A stale peer re-joins and synthesizes its old copy of the row.
    auto applied = lattice::apply_remote_changes(
        db, {synth_insert("synth-1", row_gid, "StaleSnapshot", 4)});
    ASSERT_EQ(applied.size(), 1u) << "the synthesized entry still acks";

    auto rows = db.db().query("SELECT name, age FROM TestPerson WHERE globalId = ?", {row_gid});
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("name")), "NewerEdit")
        << "a synthesized snapshot must never overwrite an existing row";
    EXPECT_EQ(std::get<int64_t>(rows[0].at("age")), 40);

    // Provenance survives into the receiver's bookkeeping for onward hops.
    auto audit = db.db().query(
        "SELECT synthesized FROM AuditLog WHERE globalId = 'synth-1'");
    ASSERT_EQ(audit.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(audit[0].at("synthesized")), 1);
}

TEST(SynthesisNoClobber, SynthesizedInsertAppliesWhenAbsent) {
    TempDB tmp{"synth_absent"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"Seed", 1, std::nullopt});

    auto applied = lattice::apply_remote_changes(
        db, {synth_insert("synth-2", "brand-new-row", "Hydrated", 7)});
    ASSERT_EQ(applied.size(), 1u);

    auto rows = db.db().query(
        "SELECT name FROM TestPerson WHERE globalId = 'brand-new-row'");
    ASSERT_EQ(rows.size(), 1u) << "absent rows must still hydrate from snapshots";
    EXPECT_EQ(std::get<std::string>(rows[0].at("name")), "Hydrated");
}

TEST(SynthesisNoClobber, WireRoundTripPreservesFlag) {
    auto e = synth_insert("synth-3", "row-3", "X", 1);
    auto parsed = lattice::audit_log_entry::from_json(e.to_json());
    ASSERT_TRUE(parsed.has_value());
    EXPECT_TRUE(parsed->synthesized);

    // Absent key (an old peer's entry) parses as false.
    lattice::audit_log_entry plain;
    plain.global_id = "plain-1";
    plain.table_name = "TestPerson";
    plain.operation = "INSERT";
    plain.global_row_id = "row-p";
    auto parsed_plain = lattice::audit_log_entry::from_json(plain.to_json());
    ASSERT_TRUE(parsed_plain.has_value());
    EXPECT_FALSE(parsed_plain->synthesized);
}

TEST(SynthesisNoClobber, NuclearCompactMarksRegeneratedHistory) {
    TempDB tmp{"synth_nuclear"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"A", 1, std::nullopt});
    db.add(TestPerson{"B", 2, std::nullopt});

    auto created = db.force_compact_audit_log();
    ASSERT_GE(created, 2);

    auto rows = db.db().query(
        "SELECT COUNT(*) AS n FROM AuditLog WHERE operation = 'INSERT' AND synthesized = 1");
    EXPECT_GE(std::get<int64_t>(rows[0].at("n")), 2)
        << "nuclear-compact regeneration is synthesized-from-local-state";
}
