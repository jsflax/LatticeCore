#include "TestHelpers.hpp"

// ============================================================================
// A3 — schema-skew tolerance tests
//
// A NEWER peer's audit entries may reference tables/columns this build does
// not have. Old behavior: the generated SQL named them, the execute failed,
// the entry was skipped WITHOUT ack — the sender re-sent the same page
// forever (silent sync wedge; the live web client's 10-day stall class) and
// the receiver lost the entire row. New behavior: drop unknown columns
// loudly and apply the remainder; skip-and-ack unknown tables; an UPDATE
// whose fields are all unknown acks as a no-op.
// ============================================================================

namespace {

lattice::audit_log_entry make_entry(const std::string& gid,
                                    const std::string& table,
                                    const std::string& op,
                                    const std::string& row_gid) {
    lattice::audit_log_entry e;
    e.global_id = gid;
    e.table_name = table;
    e.operation = op;
    e.global_row_id = row_gid;
    e.timestamp = "1700000000.0";
    return e;
}

void set_field(lattice::audit_log_entry& e, const std::string& name,
               const std::string& value) {
    e.changed_fields[name] = lattice::any_property(value);
    e.changed_fields_names.push_back(name);
}

void set_field(lattice::audit_log_entry& e, const std::string& name, int64_t value) {
    e.changed_fields[name] = lattice::any_property(value);
    e.changed_fields_names.push_back(name);
}

} // namespace

TEST(SkewTolerance, UnknownColumnDroppedAndAcked) {
    TempDB tmp{"skew_col"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"Seed", 1, std::nullopt});  // ensures the table exists

    auto e = make_entry("skew-entry-1", "TestPerson", "INSERT", "skew-row-1");
    set_field(e, "name", std::string("FromNewerPeer"));
    set_field(e, "age", int64_t{33});
    set_field(e, "quantum_flux", std::string("v3-only-column"));

    auto applied = lattice::apply_remote_changes(db, {e});
    ASSERT_EQ(applied.size(), 1u) << "entry with an unknown column must still ack";
    EXPECT_EQ(applied[0], "skew-entry-1");

    auto rows = db.db().query(
        "SELECT name, age FROM TestPerson WHERE globalId = 'skew-row-1'");
    ASSERT_EQ(rows.size(), 1u) << "the known columns must still apply";
    EXPECT_EQ(std::get<std::string>(rows[0].at("name")), "FromNewerPeer");
    EXPECT_EQ(std::get<int64_t>(rows[0].at("age")), 33);
}

TEST(SkewTolerance, UnknownOnlyUpdateAcksAsNoOp) {
    TempDB tmp{"skew_noop"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"Stable", 50, std::nullopt});
    auto gid_rows = db.db().query("SELECT globalId FROM TestPerson WHERE name = 'Stable'");
    ASSERT_EQ(gid_rows.size(), 1u);
    const std::string row_gid = std::get<std::string>(gid_rows[0].at("globalId"));

    auto e = make_entry("skew-entry-2", "TestPerson", "UPDATE", row_gid);
    set_field(e, "deletion_epoch", std::string("v3-tombstone-ish"));

    auto applied = lattice::apply_remote_changes(db, {e});
    ASSERT_EQ(applied.size(), 1u) << "unknown-only UPDATE must ack (documented no-op skew semantic)";

    auto rows = db.db().query("SELECT name, age FROM TestPerson WHERE globalId = ?", {row_gid});
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("name")), "Stable") << "row must be untouched";
    EXPECT_EQ(std::get<int64_t>(rows[0].at("age")), 50);
}

TEST(SkewTolerance, UnknownTableSkippedAndAcked) {
    TempDB tmp{"skew_table"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"Anchor", 9, std::nullopt});

    auto e = make_entry("skew-entry-3", "GroupProjectMapFromTheFuture", "INSERT", "future-row");
    set_field(e, "memberUserId", std::string("u-1"));

    auto applied = lattice::apply_remote_changes(db, {e});
    ASSERT_EQ(applied.size(), 1u)
        << "unknown-table entry must ack — this build can never apply it, and "
           "an unacked skip means the sender re-sends the page forever";

    auto tbl = db.db().query(
        "SELECT COUNT(*) AS n FROM sqlite_master WHERE name = 'GroupProjectMapFromTheFuture'");
    EXPECT_EQ(std::get<int64_t>(tbl[0].at("n")), 0) << "no phantom table may be created";
}
