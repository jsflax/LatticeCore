#include "TestHelpers.hpp"
#include <algorithm>
#include <stdexcept>

namespace {

// The two-entry send window is filled deliberately. Delivery/ACK is driven
// synchronously by the test, so no sleeps or watchdog timing select the race.
struct filtered_window_fixture {
    TempDB source_path{"filtered_window_source"};
    TempDB peer_path{"filtered_window_peer"};
    lattice::lattice_db peer{lattice::configuration(peer_path.str())};
    lattice::sync_config config;
    lattice::lattice_db* source = nullptr;
    lattice::mock_sync_transport* wire = nullptr;
    std::unique_ptr<lattice::synchronizer> sync;
    std::string target_gid;
    size_t delivered_messages = 0;

    explicit filtered_window_fixture(bool connect = true) {
        auto owned = std::make_unique<lattice::lattice_db>(
            lattice::configuration(source_path.str()));
        source = owned.get();
        source->add(TestPerson{"target", 1, std::nullopt});
        const auto rows = source->db().query(
            "SELECT globalId FROM TestPerson WHERE name='target'");
        if (rows.size() != 1) throw std::runtime_error("missing fixture target");
        target_gid = std::get<std::string>(rows[0].at("globalId"));

        config.sync_id = "filtered-window-regression";
        config.all_active_sync_ids = {config.sync_id};
        config.sync_filter = std::vector<lattice::sync_filter_entry>{
            {"TestPerson", "age = 1"}};
        config.chunk_size = 1; // send window = 2 * chunk_size
        config.upload_coalesce_ms = 0;
        config.checkpoint_passive_interval_ms = 0;
        config.checkpoint_truncate_interval_ms = 0;
        config.ack_timeout_base_ms = 60'000; // never part of this fixture's schedule
        auto transport = std::make_unique<lattice::mock_sync_transport>();
        wire = transport.get();
        sync = std::make_unique<lattice::synchronizer>(
            std::move(owned), config, std::move(transport));
        if (connect) {
            sync->connect();
            pump();
        }
    }

    ~filtered_window_fixture() {
        if (sync) sync->disconnect();
    }

    // Apply only frames that actually crossed the transport, and ACK only the
    // IDs the destination durably applied. Never ACK the source's unsent log.
    bool deliver_and_ack() {
        const auto messages = wire->get_sent_messages();
        if (delivered_messages == messages.size()) return false;
        std::vector<std::string> acknowledged;
        for (; delivered_messages < messages.size(); ++delivered_messages) {
            const auto event = lattice::server_sent_event::from_json(
                messages[delivered_messages].as_string());
            if (!event || event->event_type != lattice::server_sent_event::type::audit_log)
                throw std::runtime_error("unexpected fixture frame");
            const auto applied = lattice::apply_remote_changes_for(
                peer, event->audit_logs, "fixture-source");
            if (applied.size() != event->audit_logs.size())
                throw std::runtime_error("fixture peer could not apply delivered frame");
            acknowledged.insert(acknowledged.end(), applied.begin(), applied.end());
        }
        if (!acknowledged.empty()) {
            wire->simulate_message(lattice::transport_message::from_string(
                lattice::server_sent_event::make_ack(std::move(acknowledged)).to_json()));
        }
        return true;
    }

    void pump() {
        for (int pass = 0; pass < 32; ++pass) {
            sync->sync_now();
            if (!deliver_and_ack()) return;
        }
        throw std::runtime_error("fixture did not become idle within bounded passes");
    }

    void fill_window() {
        source->add(TestPerson{"blocker-one", 1, std::nullopt});
        source->add(TestPerson{"blocker-two", 1, std::nullopt});
        sync->sync_now();
    }

    void set_target_age(int64_t age) {
        source->db().execute("UPDATE TestPerson SET age=? WHERE globalId=?",
                             {age, target_gid});
        sync->sync_now();
    }

    bool target_pending() const {
        const auto entries = lattice::query_audit_log_for_sync(
            source->db(), config.sync_id, config.sync_filter);
        return std::any_of(entries.begin(), entries.end(), [&](const auto& entry) {
            return entry.global_row_id == target_gid;
        });
    }

    bool peer_has_target() {
        return peer.find_by_global_id<TestPerson>(target_gid).has_value();
    }

    bool undelivered_target_operation(const std::string& operation) const {
        const auto messages = wire->get_sent_messages();
        for (size_t index = delivered_messages; index < messages.size(); ++index) {
            const auto event = lattice::server_sent_event::from_json(messages[index].as_string());
            if (!event || event->event_type != lattice::server_sent_event::type::audit_log)
                continue;
            for (const auto& entry : event->audit_logs) {
                if (entry.global_row_id == target_gid && entry.operation == operation)
                    return true;
            }
        }
        return false;
    }

    void drop_undelivered_frames() {
        // A transport send is not proof that the peer received/persisted it.
        // Discard the captured frames without applying or acknowledging them.
        delivered_messages = wire->get_sent_messages().size();
    }

    std::string pending_target_wire() const {
        const auto messages = wire->get_sent_messages();
        for (size_t index = delivered_messages; index < messages.size(); ++index) {
            const auto event = lattice::server_sent_event::from_json(messages[index].as_string());
            if (!event || event->event_type != lattice::server_sent_event::type::audit_log) continue;
            for (const auto& entry : event->audit_logs) {
                if (entry.global_row_id == target_gid) return entry.to_json();
            }
        }
        return {};
    }

    int64_t outbox_count(const std::string& channel = "filtered-window-regression") const {
        const auto rows = source->db().query(
            "SELECT COUNT(*) AS n FROM _lattice_sync_outbox WHERE sync_id=?", {channel});
        return std::get<int64_t>(rows[0].at("n"));
    }

    bool target_is_member() const {
        return !source->db().query(
            "SELECT 1 FROM _lattice_sync_set WHERE sync_id=? AND table_name='TestPerson' "
            "AND global_row_id=?", {config.sync_id, target_gid}).empty();
    }

    void restart() {
        sync->disconnect();
        sync.reset(); // Destroy the owning connection and all in-memory retry state.
        auto owned = std::make_unique<lattice::lattice_db>(
            lattice::configuration(source_path.str()));
        source = owned.get();
        auto transport = std::make_unique<lattice::mock_sync_transport>();
        wire = transport.get();
        delivered_messages = 0;
        sync = std::make_unique<lattice::synchronizer>(
            std::move(owned), config, std::move(transport));
        sync->connect();
    }

    void reconnect_and_pump() {
        sync->disconnect();
        sync->connect();
        pump();
    }
};

} // namespace

TEST(FilteredSyncWindowDurability, OrdinaryUpdateSurvivesFullSendWindow) {
    filtered_window_fixture fixture;
    ASSERT_TRUE(fixture.peer_has_target());
    fixture.fill_window();
    ASSERT_EQ(fixture.sync->get_progress().pending_upload, 2);

    fixture.source->db().execute(
        "UPDATE TestPerson SET name='target-updated' WHERE globalId=?",
        {fixture.target_gid});
    fixture.sync->sync_now();
    EXPECT_TRUE(fixture.target_pending())
        << "A full send window must leave the unsent UPDATE durable";
    fixture.pump();

    const auto received = fixture.peer.find_by_global_id<TestPerson>(fixture.target_gid);
    ASSERT_TRUE(received.has_value());
    EXPECT_EQ(std::string(received->name), "target-updated");
    EXPECT_EQ(fixture.sync->get_progress().pending_upload, 0);
}

TEST(FilteredSyncWindowDurability, LeavingFilterSurvivesFullSendWindowAndReconnect) {
    filtered_window_fixture fixture;
    ASSERT_TRUE(fixture.peer_has_target());
    fixture.fill_window();
    ASSERT_EQ(fixture.sync->get_progress().pending_upload, 2);

    // The peer has the target already. Leaving the filter must eventually
    // deliver its removal even though the transport cannot send it yet.
    fixture.set_target_age(0);
    EXPECT_TRUE(fixture.target_pending())
        << "The unsent synthetic DELETE needs a durable retry source";
    fixture.pump();
    EXPECT_FALSE(fixture.peer_has_target())
        << "ACKing the blockers must allow the target removal to reach the peer";

    fixture.reconnect_and_pump();
    EXPECT_FALSE(fixture.peer_has_target())
        << "Reconnect must not leave a permanently stale filtered peer";
    EXPECT_EQ(fixture.sync->get_progress().pending_upload, 0);
}

TEST(FilteredSyncWindowDurability, ReenteringFilterSurvivesFullSendWindowAndReconnect) {
    filtered_window_fixture fixture;
    ASSERT_TRUE(fixture.peer_has_target());
    fixture.set_target_age(0);
    fixture.pump();
    ASSERT_FALSE(fixture.peer_has_target());

    fixture.fill_window();
    ASSERT_EQ(fixture.sync->get_progress().pending_upload, 2);
    // Only an UPDATE remains in the source history. Reentering the filter
    // converts it to a full-row INSERT because the peer no longer has it.
    fixture.set_target_age(1);
    EXPECT_TRUE(fixture.target_pending())
        << "The unsent synthetic INSERT needs a durable retry source";
    fixture.pump();
    EXPECT_TRUE(fixture.peer_has_target())
        << "ACKing the blockers must allow the target INSERT to reach the peer";

    fixture.reconnect_and_pump();
    EXPECT_TRUE(fixture.peer_has_target())
        << "Reconnect must recover the target rather than trust unsent membership";
    EXPECT_EQ(fixture.sync->get_progress().pending_upload, 0);
}

TEST(FilteredSyncWindowDurability, LeavingFilterRetriesAfterDisconnectBeforeAcknowledgment) {
    filtered_window_fixture fixture;
    ASSERT_TRUE(fixture.peer_has_target());
    fixture.set_target_age(0);
    ASSERT_TRUE(fixture.undelivered_target_operation("DELETE"));
    ASSERT_EQ(fixture.sync->get_progress().pending_upload, 1);
    fixture.drop_undelivered_frames();

    fixture.reconnect_and_pump();
    EXPECT_FALSE(fixture.peer_has_target())
        << "A synthetic removal sent before suspension must be retryable until ACK";
    EXPECT_EQ(fixture.sync->get_progress().pending_upload, 0);
}

TEST(FilteredSyncWindowDurability, ReenteringFilterRetriesAfterDisconnectBeforeAcknowledgment) {
    filtered_window_fixture fixture;
    fixture.set_target_age(0);
    fixture.pump();
    ASSERT_FALSE(fixture.peer_has_target());

    fixture.set_target_age(1);
    ASSERT_TRUE(fixture.undelivered_target_operation("INSERT"));
    ASSERT_EQ(fixture.sync->get_progress().pending_upload, 1);
    fixture.drop_undelivered_frames();

    fixture.reconnect_and_pump();
    EXPECT_TRUE(fixture.peer_has_target())
        << "A synthetic full-row INSERT sent before suspension must be retryable until ACK";
    EXPECT_EQ(fixture.sync->get_progress().pending_upload, 0);
}

TEST(FilteredSyncWindowDurability, RemovalRetainsPayloadAcrossSourceRestart) {
    filtered_window_fixture fixture;
    fixture.set_target_age(0);
    const auto original = fixture.pending_target_wire();
    ASSERT_FALSE(original.empty());
    ASSERT_EQ(fixture.outbox_count(), 1);
    fixture.restart();
    EXPECT_EQ(fixture.pending_target_wire(), original)
        << "Restart must replay the same admitted UUID and values";
    fixture.pump();
    EXPECT_FALSE(fixture.peer_has_target());
    EXPECT_EQ(fixture.outbox_count(), 0);
}

TEST(FilteredSyncWindowDurability, FullRowInsertRetainsPayloadAcrossSourceRestart) {
    filtered_window_fixture fixture;
    fixture.set_target_age(0);
    fixture.pump();
    ASSERT_FALSE(fixture.peer_has_target());
    fixture.set_target_age(1);
    const auto original = fixture.pending_target_wire();
    ASSERT_FALSE(original.empty());
    ASSERT_EQ(fixture.outbox_count(), 1);
    fixture.restart();
    EXPECT_EQ(fixture.pending_target_wire(), original);
    fixture.pump();
    EXPECT_TRUE(fixture.peer_has_target());
    EXPECT_EQ(fixture.outbox_count(), 0);
}

TEST(FilteredSyncWindowDurability, OrdinaryDeleteRetriesAfterSourceRestart) {
    filtered_window_fixture fixture;
    fixture.source->db().execute("DELETE FROM TestPerson WHERE globalId=?", {fixture.target_gid});
    fixture.sync->sync_now();
    ASSERT_TRUE(fixture.undelivered_target_operation("DELETE"));
    ASSERT_EQ(fixture.outbox_count(), 1);
    fixture.restart();
    fixture.pump();
    EXPECT_FALSE(fixture.peer_has_target());
    EXPECT_EQ(fixture.outbox_count(), 0);
}

TEST(FilteredSyncWindowDurability, ReconciliationDeleteRetriesAfterSourceRestart) {
    filtered_window_fixture fixture;
    fixture.config.sync_filter = std::vector<lattice::sync_filter_entry>{{"TestPerson", "age = 0"}};
    fixture.sync->update_sync_filter(*fixture.config.sync_filter);
    ASSERT_TRUE(fixture.undelivered_target_operation("DELETE"));
    ASSERT_EQ(fixture.outbox_count(), 1);
    fixture.restart();
    fixture.pump();
    EXPECT_FALSE(fixture.peer_has_target());
    EXPECT_EQ(fixture.outbox_count(), 0);
}

TEST(FilteredSyncWindowDurability, LatestFilterReconcilesAfterAdmittedDelivery) {
    filtered_window_fixture fixture;
    fixture.set_target_age(0); // An admitted removal is awaiting the peer's ACK.
    const auto removal = fixture.pending_target_wire();
    fixture.sync->update_sync_filter({{"TestPerson", "age = 0"}});
    EXPECT_EQ(fixture.outbox_count(), 1);
    EXPECT_EQ(fixture.pending_target_wire(), removal);
    fixture.pump();
    const auto target = fixture.peer.find_by_global_id<TestPerson>(fixture.target_gid);
    ASSERT_TRUE(target.has_value())
        << "Finish the admitted removal, then re-add from the latest filter";
    EXPECT_EQ(int(target->age), 0);
    EXPECT_EQ(fixture.outbox_count(), 0);
}

TEST(FilteredSyncWindowDurability, ClearingFilterDoesNotDiscardAdmittedDelivery) {
    filtered_window_fixture fixture;
    fixture.set_target_age(0);
    const auto removal = fixture.pending_target_wire();
    fixture.sync->clear_sync_filter();
    EXPECT_EQ(fixture.outbox_count(), 1);
    EXPECT_EQ(fixture.pending_target_wire(), removal);
    fixture.pump();
    EXPECT_FALSE(fixture.peer_has_target());
    EXPECT_EQ(fixture.outbox_count(), 0);
}

TEST(FilteredSyncWindowDurability, AcknowledgmentOnlyRemovesMatchingChannelDelivery) {
    filtered_window_fixture fixture;
    fixture.set_target_age(0);
    ASSERT_EQ(fixture.outbox_count(), 1);
    // The same wire UUID is deliberately present under another channel. A
    // matching ACK on this connection must never delete the other channel row.
    fixture.source->db().execute(
        "INSERT INTO _lattice_sync_outbox "
        "(sync_id, source_audit_id, source_global_id, event_global_id, payload) "
        "SELECT 'other-channel', source_audit_id, source_global_id, event_global_id, payload "
        "FROM _lattice_sync_outbox WHERE sync_id=?", {fixture.config.sync_id});
    fixture.wire->simulate_message(lattice::transport_message::from_string(
        lattice::server_sent_event::make_ack({"unrelated-uuid"}).to_json()));
    EXPECT_EQ(fixture.outbox_count(), 1);
    EXPECT_EQ(fixture.outbox_count("other-channel"), 1);
    fixture.pump();
    EXPECT_EQ(fixture.outbox_count(), 0);
    EXPECT_EQ(fixture.outbox_count("other-channel"), 1);
}

TEST(FilteredSyncWindowDurability, PrivateRemovalIsNotPublishedIntoGlobalAuditHistory) {
    filtered_window_fixture fixture;
    fixture.set_target_age(0);
    ASSERT_EQ(fixture.outbox_count(), 1);
    const auto delivery = lattice::audit_log_entry::from_json(fixture.pending_target_wire());
    ASSERT_TRUE(delivery.has_value());
    EXPECT_TRUE(fixture.source->db().query(
        "SELECT 1 FROM AuditLog WHERE globalId=?", {delivery->global_id}).empty());
    EXPECT_TRUE(fixture.source->db().query(
        "SELECT 1 FROM AuditLog WHERE globalRowId=? AND operation='DELETE'",
        {fixture.target_gid}).empty());

    auto other_owned = std::make_unique<lattice::lattice_db>(
        lattice::configuration(fixture.source_path.str()));
    auto other_transport = std::make_unique<lattice::mock_sync_transport>();
    auto* other_wire = other_transport.get();
    auto other_config = fixture.config;
    other_config.sync_id = "other-channel";
    other_config.all_active_sync_ids = {fixture.config.sync_id, other_config.sync_id};
    other_config.sync_filter = std::nullopt;
    lattice::synchronizer other(std::move(other_owned), other_config, std::move(other_transport));
    other.connect();
    for (const auto& frame : other_wire->get_sent_messages()) {
        const auto event = lattice::server_sent_event::from_json(frame.as_string());
        ASSERT_TRUE(event.has_value());
        for (const auto& entry : event->audit_logs) {
            EXPECT_NE(entry.global_id, delivery->global_id);
            EXPECT_NE(entry.operation, "DELETE");
        }
    }
    EXPECT_EQ(fixture.outbox_count(), 1);
    other.disconnect();
}

TEST(FilteredSyncWindowDurability, StagingRollbackKeepsSourcePendingAndMembership) {
    filtered_window_fixture fixture;
    int attempts = 0;
    struct ResetHook {
        ~ResetHook() { lattice::test_hooks::sync_outbox_after_insert = nullptr; }
    } reset;
    lattice::test_hooks::sync_outbox_after_insert = [&] {
        ++attempts;
        throw std::runtime_error("injected outbox staging failure");
    };
    // Commit notification may run inline; an exception must not be mistaken
    // for a rollback of the already committed application edit.
    try { fixture.set_target_age(0); } catch (const std::runtime_error&) {}
    EXPECT_THROW(fixture.sync->sync_now(), std::runtime_error);
    EXPECT_GT(attempts, 0);
    EXPECT_EQ(fixture.outbox_count(), 0);
    EXPECT_TRUE(fixture.target_is_member());
    EXPECT_TRUE(fixture.target_pending());
    EXPECT_TRUE(fixture.peer_has_target());
    lattice::test_hooks::sync_outbox_after_insert = nullptr;
    fixture.pump();
    EXPECT_FALSE(fixture.peer_has_target());
    EXPECT_EQ(fixture.outbox_count(), 0);
}

TEST(FilteredSyncWindowDurability, OutstandingStagingRemainsWithinSendWindow) {
    filtered_window_fixture fixture;
    for (int index = 0; index < 6; ++index)
        fixture.source->add(TestPerson{"extra-" + std::to_string(index), 1, std::nullopt});
    fixture.pump();
    fixture.source->db().execute("UPDATE TestPerson SET age=0");
    fixture.sync->sync_now();
    ASSERT_EQ(fixture.outbox_count(), 2);
    for (int pass = 0; pass < 12; ++pass) fixture.sync->sync_now();
    EXPECT_EQ(fixture.outbox_count(), 2)
        << "A stalled peer must not stage another window or mint new retry UUIDs";
    fixture.pump();
    EXPECT_TRUE(fixture.peer.db().query("SELECT 1 FROM TestPerson LIMIT 1").empty());
    EXPECT_EQ(fixture.outbox_count(), 0);
}

TEST(FilteredSyncWindowDurability, FirstConnectedCallbackCanRequestSync) {
    filtered_window_fixture fixture(false);
    bool connected_callback = false;
    fixture.sync->set_on_state_change([&](bool connected) {
        if (connected) {
            connected_callback = true;
            fixture.sync->sync_now();
        }
    });
    EXPECT_NO_THROW(fixture.sync->connect());
    EXPECT_TRUE(connected_callback);
    fixture.pump();
    EXPECT_TRUE(fixture.peer_has_target());
    fixture.sync->set_on_state_change({});
}

TEST(FilteredSyncWindowDurability, ClearingFilterWhileDisconnectedSurvivesConnect) {
    filtered_window_fixture fixture;
    ASSERT_TRUE(fixture.target_is_member());
    fixture.sync->disconnect();
    fixture.sync->clear_sync_filter();
    fixture.sync->connect();
    fixture.pump();
    EXPECT_FALSE(fixture.target_is_member());
}

TEST(FilteredSyncWindowDurability, PendingPayloadSurvivesSourceHistoryCompaction) {
    filtered_window_fixture fixture;
    fixture.set_target_age(0);
    fixture.pump();
    fixture.set_target_age(1);
    const auto original = fixture.pending_target_wire();
    ASSERT_FALSE(original.empty());
    ASSERT_EQ(fixture.outbox_count(), 1);
    const auto source_rows = fixture.source->db().query(
        "SELECT source_audit_id FROM _lattice_sync_outbox WHERE sync_id=?",
        {fixture.config.sync_id});
    ASSERT_EQ(source_rows.size(), 1u);
    const auto source_id = std::get<int64_t>(source_rows[0].at("source_audit_id"));
    EXPECT_GT(fixture.source->safe_compact_audit_log(), 0);
    ASSERT_TRUE(fixture.source->db().query("SELECT 1 FROM AuditLog WHERE id=?", {source_id}).empty());
    fixture.restart();
    EXPECT_EQ(fixture.pending_target_wire(), original);
    fixture.pump();
    EXPECT_TRUE(fixture.peer_has_target());
    EXPECT_EQ(fixture.outbox_count(), 0);
}

TEST(FilteredSyncWindowDurability, InvalidStoredPayloadDoesNotDiscardDelivery) {
    filtered_window_fixture fixture;
    fixture.set_target_age(0);
    ASSERT_EQ(fixture.outbox_count(), 1);
    fixture.source->db().execute(
        "UPDATE _lattice_sync_outbox SET payload='{' WHERE sync_id=?", {fixture.config.sync_id});
    EXPECT_THROW(fixture.restart(), std::runtime_error);
    EXPECT_EQ(fixture.outbox_count(), 1);
    EXPECT_TRUE(fixture.peer_has_target());
    EXPECT_TRUE(fixture.wire->get_sent_messages().empty());
}

TEST(FilteredSyncWindowDurability, UppercaseSyntheticAcknowledgmentResolvesSourceAndOnlyItsChannelOutbox) {
    filtered_window_fixture fixture;
    fixture.set_target_age(0);
    ASSERT_EQ(fixture.outbox_count(), 1);
    const auto rows = fixture.source->db().query(
        "SELECT source_audit_id,event_global_id,payload FROM _lattice_sync_outbox WHERE sync_id=?",
        {fixture.config.sync_id});
    ASSERT_EQ(rows.size(), 1u);
    const auto source_id = std::get<int64_t>(rows[0].at("source_audit_id"));
    const auto wire_id = std::get<std::string>(rows[0].at("event_global_id"));
    std::string upper = wire_id;
    for (auto& c : upper) if (c >= 'a' && c <= 'f') c -= 'a' - 'A';
    const auto delivery = lattice::audit_log_entry::from_json(std::get<std::string>(rows[0].at("payload")));
    ASSERT_TRUE(delivery);
    ASSERT_EQ(lattice::apply_remote_changes_for(fixture.peer, {*delivery}, "fixture-source").size(), 1u);
    fixture.source->db().execute(
        "INSERT INTO _lattice_sync_outbox(sync_id,source_audit_id,source_global_id,event_global_id,payload) "
        "SELECT 'other-channel',source_audit_id,source_global_id,event_global_id,payload "
        "FROM _lattice_sync_outbox WHERE sync_id=?", {fixture.config.sync_id});
    fixture.wire->simulate_message(lattice::transport_message::from_string(
        lattice::server_sent_event::make_ack({upper}).to_json()));
    EXPECT_EQ(fixture.outbox_count(), 0);
    EXPECT_EQ(fixture.outbox_count("other-channel"), 1);
    EXPECT_EQ(fixture.sync->get_progress().pending_upload, 0);
    EXPECT_FALSE(fixture.peer_has_target());
    // The synthetic wire UUID differs from the source UUID. Its ACK must
    // still resolve source bookkeeping through the outbox provenance lookup.
    const auto source = fixture.source->db().query("SELECT isSynchronized FROM AuditLog WHERE id=?", {source_id});
    ASSERT_EQ(source.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(source[0].at("isSynchronized")), 1);
}
