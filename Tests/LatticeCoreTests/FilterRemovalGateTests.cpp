#include "TestHelpers.hpp"

// ============================================================================
// A4 — narrowing must never delete beyond its own bookkeeping
//
// (i) A marked filter-removal DELETE that arrives over IPC clears the local
//     mirror and STOPS DEAD: it is recorded fully-synchronized so no other
//     synchronizer on the receiving database (in particular the spoke's
//     unfiltered WSS uplink) ever enumerates it as pending — un-exposing a
//     project must not delete rows from a shared server DB.
// (ii) A channel configured with narrowing_emits_removals=false narrows as
//      BOOKKEEPING ONLY: rows leave its sync set, no DELETE is synthesized,
//      the paired spoke keeps its mirror (group-channel semantics).
// ============================================================================

namespace {

lattice::audit_log_entry marked_removal(const std::string& gid,
                                        const std::string& row_gid) {
    lattice::audit_log_entry e;
    e.global_id = gid;
    e.table_name = "TestPerson";
    e.operation = "DELETE";
    e.global_row_id = row_gid;
    e.timestamp = "1700000000.0";
    e.changed_fields_names.push_back("__lattice_filter_removal");
    return e;
}

int64_t scalar(lattice::lattice_db& db, const std::string& sql) {
    auto rows = db.db().query(sql);
    if (rows.empty()) return -1;
    auto& val = rows[0].begin()->second;
    return std::holds_alternative<int64_t>(val) ? std::get<int64_t>(val) : -1;
}

} // namespace

TEST(FilterRemovalGate, MarkedRemovalRecordedFullySynced) {
    TempDB tmp{"a4_marked"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"Mirrored", 30, std::nullopt});
    auto gid_rows = db.db().query("SELECT globalId FROM TestPerson WHERE name = 'Mirrored'");
    const std::string row_gid = std::get<std::string>(gid_rows[0].at("globalId"));

    // Per-sync relay mode (IPC receive path): a normal remote entry is
    // recorded isSynchronized=0 so OTHER synchronizers relay it onward.
    lattice::audit_log_entry normal;
    normal.global_id = "a4-normal";
    normal.table_name = "TestPerson";
    normal.operation = "UPDATE";
    normal.global_row_id = row_gid;
    normal.timestamp = "1700000000.0";
    normal.changed_fields["age"] = lattice::any_property(int64_t{31});
    normal.changed_fields_names.push_back("age");
    lattice::apply_remote_changes_for(db, {normal}, "ipc:test-channel");
    EXPECT_EQ(scalar(db,
        "SELECT isSynchronized FROM AuditLog WHERE globalId = 'a4-normal'"), 0)
        << "control: normal per-sync entries stay pending for other channels";

    // A marked filter-removal DELETE must be recorded fully-synchronized —
    // it clears this mirror and never relays (the spoke's unfiltered WSS
    // would otherwise upload it and delete shared server rows).
    lattice::apply_remote_changes_for(db, {marked_removal("a4-marked", row_gid)},
                                      "ipc:test-channel");
    EXPECT_EQ(scalar(db,
        "SELECT isSynchronized FROM AuditLog WHERE globalId = 'a4-marked'"), 1)
        << "marked removals must stop dead at the receiving database";

    EXPECT_EQ(scalar(db, "SELECT COUNT(*) FROM TestPerson WHERE globalId = '" + row_gid + "'"), 0)
        << "the mirror row itself is deleted (that part is the point)";
}

TEST(FilterRemovalGate, BookkeepingOnlyNarrowingEmitsNoDeletes) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"a4_narrow"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    db->add(TestPerson{"Shared", 30, std::nullopt});
    lattice::lattice_db reader(lattice::configuration(tmp.str()));
    const std::string row_gid = std::get<std::string>(
        reader.db().query("SELECT globalId FROM TestPerson WHERE name = 'Shared'")[0]
            .at("globalId"));

    lattice::sync_config cfg;
    cfg.websocket_url = "ws://localhost:8080/sync";
    cfg.authorization_token = "t";
    cfg.sync_id = "group-chan";
    cfg.all_active_sync_ids = {"group-chan"};
    cfg.sync_filter = std::vector<lattice::sync_filter_entry>{
        {"TestPerson", std::string("age < 100")}};
    cfg.narrowing_emits_removals = false;   // group-channel semantics
    lattice::synchronizer sync(std::move(db), cfg);
    auto* ws = mock_factory->last_websocket();
    sync.connect();
    sync.sync_now();

    ASSERT_EQ(scalar(reader,
        "SELECT COUNT(*) FROM _lattice_sync_set WHERE sync_id = 'group-chan'"), 1)
        << "precondition: the row entered the channel's sync set";

    // Narrow the filter so the row leaves it (an un-expose).
    sync.update_sync_filter({{"TestPerson", std::string("name = 'NOTHING_MATCHES'")}});

    EXPECT_EQ(scalar(reader,
        "SELECT COUNT(*) FROM AuditLog WHERE changedFieldsNames LIKE '%__lattice_filter_removal%'"), 0)
        << "bookkeeping-only narrowing must synthesize NO removal deletes";
    EXPECT_EQ(scalar(reader,
        "SELECT COUNT(*) FROM AuditLog WHERE operation = 'DELETE'"), 0)
        << "no DELETE of any kind may be emitted by narrowing";
    EXPECT_EQ(scalar(reader,
        "SELECT COUNT(*) FROM _lattice_sync_set WHERE sync_id = 'group-chan'"), 0)
        << "the sync-set bookkeeping itself must still narrow";
    for (const auto& msg : ws->get_sent_messages()) {
        EXPECT_EQ(msg.as_string().find("\"DELETE\""), std::string::npos)
            << "no DELETE frame may reach the wire on narrowing";
    }

    sync.disconnect();
}

TEST(FilterRemovalGate, DefaultNarrowingStillEmitsRemovals) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"a4_default"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    db->add(TestPerson{"Personal", 30, std::nullopt});
    lattice::lattice_db reader(lattice::configuration(tmp.str()));

    lattice::sync_config cfg;
    cfg.websocket_url = "ws://localhost:8080/sync";
    cfg.authorization_token = "t";
    cfg.sync_id = "personal-chan";
    cfg.all_active_sync_ids = {"personal-chan"};
    cfg.sync_filter = std::vector<lattice::sync_filter_entry>{
        {"TestPerson", std::string("age < 100")}};
    // narrowing_emits_removals defaults to true (personal-sync semantics).
    lattice::synchronizer sync(std::move(db), cfg);
    sync.connect();
    sync.sync_now();
    sync.update_sync_filter({{"TestPerson", std::string("name = 'NOTHING'")}});

    EXPECT_EQ(scalar(reader,
        "SELECT COUNT(*) FROM AuditLog WHERE changedFieldsNames LIKE '%__lattice_filter_removal%'"), 1)
        << "default narrowing keeps the mirror-clear behavior";

    sync.disconnect();
}
