#include "TestHelpers.hpp"
#include <deque>

namespace {
using phase = lattice::synchronizer_base::finite_cycle_phase;
using failure = lattice::synchronizer_base::finite_cycle_failure;

class cycle_scheduler : public lattice::scheduler {
public:
    std::deque<std::function<void()>> work;
    bool active = true;
    bool executing = false;
    void invoke(std::function<void()>&& fn) override {
        if (active) work.push_back(std::move(fn));
    }
    bool is_on_thread() const noexcept override { return executing; }
    bool is_same_as(const scheduler* other) const noexcept override { return this == other; }
    bool can_invoke() const noexcept override { return active; }
    void shutdown() override { active = false; work.clear(); }
    void step() {
        if (work.empty()) return;
        auto fn = std::move(work.front()); work.pop_front();
        executing = true;
        try { fn(); } catch (...) { executing = false; throw; }
        executing = false;
    }
    void run() {
        size_t steps = 0;
        while (!work.empty()) {
            if (++steps > 1000) throw std::runtime_error("finite fixture did not settle");
            step();
        }
    }
};

class cycle_transport : public lattice::mock_sync_transport {
public:
    std::string url;
    size_t connections = 0;
    void connect(const std::string& target,
                 const std::map<std::string, std::string>& headers = {}) override {
        url = target; ++connections;
        mock_sync_transport::connect(target, headers);
    }
};

struct finite_fixture {
    TempDB path{"finite_sync"};
    std::shared_ptr<cycle_scheduler> scheduler = std::make_shared<cycle_scheduler>();
    lattice::lattice_db* db = nullptr;
    cycle_transport* wire = nullptr;
    lattice::sync_config config;
    std::unique_ptr<lattice::synchronizer> sync;
    size_t consumed = 0;

    explicit finite_fixture(size_t initial_rows = 0, bool finite = true, bool use_floor = true) {
        auto owned = std::make_unique<lattice::lattice_db>(
            lattice::configuration(path.str(), scheduler));
        db = owned.get();
        for (size_t i = 0; i < initial_rows; ++i)
            db->add(TestPerson{"initial-" + std::to_string(i), 1, std::nullopt});
        scheduler->run();
        config.sync_id = "wss:finite-test";
        config.websocket_url = "wss://example.invalid/sync?scope=test";
        config.all_active_sync_ids = {config.sync_id};
        config.finite_cycle = finite;
        config.use_upload_floor = use_floor;
        config.chunk_size = 1;
        config.ack_timeout_base_ms = 60'000;
        config.upload_coalesce_ms = 0;
        auto transport = std::make_unique<cycle_transport>();
        wire = transport.get();
        sync = std::make_unique<lattice::synchronizer>(
            std::move(owned), config, std::move(transport));
    }
    ~finite_fixture() { sync->disconnect(); }
    void connect() { sync->connect(); scheduler->run(); }
    void receive(const lattice::server_sent_event& event) {
        wire->simulate_message(lattice::transport_message::from_string(event.to_json()));
    }
    void marker() { receive(lattice::server_sent_event::make_catch_up_complete()); }
    auto status() { return sync->get_finite_cycle_status(); }
    std::vector<lattice::audit_log_entry> uploads() {
        std::vector<lattice::audit_log_entry> result;
        for (const auto& message : wire->get_sent_messages()) {
            const auto event = lattice::server_sent_event::from_json(message.as_string());
            if (event && event->event_type == lattice::server_sent_event::type::audit_log)
                result.insert(result.end(), event->audit_logs.begin(), event->audit_logs.end());
        }
        return result;
    }
    bool ack_window() {
        const auto messages = wire->get_sent_messages();
        std::vector<std::string> ids;
        for (; consumed < messages.size(); ++consumed) {
            const auto event = lattice::server_sent_event::from_json(messages[consumed].as_string());
            if (event && event->event_type == lattice::server_sent_event::type::audit_log)
                for (const auto& entry : event->audit_logs) ids.push_back(entry.global_id);
        }
        if (ids.empty()) return false;
        receive(lattice::server_sent_event::make_ack(std::move(ids)));
        scheduler->run();
        return true;
    }
    void ack_all() {
        for (size_t i = 0; i < 32; ++i) if (!ack_window()) return;
        throw std::runtime_error("finite fixture ACK loop did not settle");
    }
    lattice::audit_log_entry remote(std::string id = "remote-event") {
        lattice::audit_log_entry entry;
        entry.global_id = std::move(id);
        entry.global_row_id = "remote-row-" + entry.global_id;
        entry.table_name = "TestPerson";
        entry.operation = "INSERT";
        entry.timestamp = "2026-11-01 00:00:00";
        entry.changed_fields = {{"name", lattice::any_property("remote")},
                                {"age", lattice::any_property(int64_t(2))}};
        entry.changed_fields_names = {"name", "age"};
        return entry;
    }
};
}

TEST(FiniteSyncCycle, MarkerEncodingRejectsAmbiguityAndFalseValues) {
    auto marker = lattice::server_sent_event::make_catch_up_complete();
    auto parsed = lattice::server_sent_event::from_json(marker.to_json());
    ASSERT_TRUE(parsed);
    EXPECT_EQ(parsed->event_type, lattice::server_sent_event::type::catch_up_complete);
    for (const auto* invalid : {
        R"({"kind":"catchUpComplete","catchUpComplete":false})",
        R"({"kind":"catchUpComplete","catchUpComplete":1})",
        R"({"kind":"ack","catchUpComplete":true})",
        R"({"catchUpComplete":true,"auditLog":[]})"})
        EXPECT_FALSE(lattice::server_sent_event::from_json(invalid));
}

TEST(FiniteSyncCycle, EmptyStoreRequiresNegotiatedMarker) {
    finite_fixture fixture;
    fixture.connect();
    EXPECT_NE(fixture.wire->url.find("scope=test&lattice-catch-up-complete=1"), std::string::npos);
    EXPECT_EQ(fixture.status().phase, phase::running);
    EXPECT_TRUE(fixture.status().horizon_captured);
    EXPECT_EQ(fixture.status().upload_horizon, 0);
    fixture.marker();
    EXPECT_EQ(fixture.status().phase, phase::running); // ordered worker receipt, not wire receipt
    fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
}

TEST(FiniteSyncCycle, OlderServerWithoutMarkerNeverClaimsCompletion) {
    finite_fixture fixture(3);
    fixture.connect();
    fixture.ack_all();
    for (int i = 0; i < 10; ++i) fixture.sync->sync_now();
    fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::running);
    EXPECT_FALSE(fixture.status().catch_up_received);
    fixture.sync->cancel_finite_cycle();
    EXPECT_EQ(fixture.status().phase, phase::cancelled);
}

TEST(FiniteSyncCycle, MarkerAndUnrelatedAcknowledgmentCannotSkipUploadWindows) {
    finite_fixture fixture(5);
    fixture.connect();
    ASSERT_EQ(fixture.uploads().size(), 2u);
    fixture.marker();
    fixture.receive(lattice::server_sent_event::make_ack({"unrelated"}));
    fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::running);
    ASSERT_TRUE(fixture.ack_window());
    EXPECT_EQ(fixture.status().phase, phase::running);
    ASSERT_TRUE(fixture.ack_window());
    EXPECT_EQ(fixture.status().phase, phase::running);
    ASSERT_TRUE(fixture.ack_window());
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
    EXPECT_EQ(fixture.uploads().size(), 5u);
}

TEST(FiniteSyncCycle, LaterWritesRemainPendingForNextFreshCycle) {
    finite_fixture fixture(3);
    fixture.connect();
    const auto horizon = fixture.status().upload_horizon;
    fixture.db->add(TestPerson{"later", 1, std::nullopt});
    fixture.marker();
    fixture.scheduler->run();
    fixture.ack_all();
    ASSERT_EQ(fixture.status().phase, phase::work_complete);
    EXPECT_EQ(fixture.uploads().size(), 3u);
    EXPECT_LE(lattice::read_upload_floor(fixture.db->db(), fixture.config.sync_id), horizon);
    auto pending = lattice::query_audit_log_for_sync(fixture.db->db(), fixture.config.sync_id);
    ASSERT_EQ(pending.size(), 1u);
    EXPECT_GT(pending[0].id, horizon);
    fixture.sync->disconnect();
    fixture.connect();
    fixture.marker(); fixture.scheduler->run(); fixture.ack_all();
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
    EXPECT_EQ(fixture.uploads().size(), 4u);
}

TEST(FiniteSyncCycle, UpperBoundAppliesToExplicitPendingAndMissingStateBranches) {
    finite_fixture fixture(2);
    const auto entries = lattice::query_audit_log_for_sync(fixture.db->db(), fixture.config.sync_id);
    ASSERT_EQ(entries.size(), 2u);
    fixture.db->db().execute("INSERT INTO _lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,0)",
                            {entries[1].id, fixture.config.sync_id});
    EXPECT_TRUE(lattice::query_audit_log_for_sync(fixture.db->db(), fixture.config.sync_id,
                std::nullopt, 0, 0, 0).empty());
    const auto prefix = lattice::query_audit_log_for_sync(fixture.db->db(), fixture.config.sync_id,
                std::nullopt, 0, 0, entries[0].id);
    ASSERT_EQ(prefix.size(), 1u);
    EXPECT_EQ(prefix[0].id, entries[0].id);
}

TEST(FiniteSyncCycle, DownloadPagesApplyBeforeMarkerAndLaterLiveFramesStayUnacknowledged) {
    finite_fixture fixture;
    fixture.connect();
    fixture.receive(lattice::server_sent_event::make_audit_log({fixture.remote("page-one")}));
    fixture.receive(lattice::server_sent_event::make_audit_log({fixture.remote("page-two")}));
    fixture.marker();
    fixture.receive(lattice::server_sent_event::make_audit_log({fixture.remote("live")}));
    fixture.scheduler->step();
    EXPECT_FALSE(fixture.status().catch_up_received);
    EXPECT_EQ(fixture.db->db().query("SELECT * FROM TestPerson").size(), 1u);
    fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
    EXPECT_EQ(fixture.db->db().query("SELECT * FROM TestPerson").size(), 2u);
    std::vector<std::string> acked;
    for (const auto& message : fixture.wire->get_sent_messages()) {
        auto event = lattice::server_sent_event::from_json(message.as_string());
        if (event && event->event_type == lattice::server_sent_event::type::ack)
            acked.insert(acked.end(), event->acked_ids.begin(), event->acked_ids.end());
    }
    EXPECT_EQ(acked, (std::vector<std::string>{"page-one", "page-two"}));
}

TEST(FiniteSyncCycle, InvalidFrameBeforeMarkerCannotComplete) {
    finite_fixture fixture;
    int errors = 0;
    fixture.sync->set_on_error([&](const std::string&) { ++errors; });
    fixture.connect();
    fixture.wire->simulate_message(lattice::transport_message::from_string("{invalid"));
    fixture.marker(); fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::failed);
    EXPECT_EQ(fixture.status().failure, failure::invalid_frame);
    EXPECT_EQ(errors, 1);
}

TEST(FiniteSyncCycle, PartialApplyBeforeMarkerCannotComplete) {
    finite_fixture fixture;
    fixture.connect();
    fixture.db->db().execute(
        "CREATE TRIGGER reject_finite_remote BEFORE INSERT ON TestPerson "
        "WHEN NEW.name='remote' BEGIN SELECT RAISE(FAIL,'fixture apply rejection'); END");
    fixture.receive(lattice::server_sent_event::make_audit_log({fixture.remote()}));
    fixture.marker(); fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::failed);
    EXPECT_EQ(fixture.status().failure, failure::partial_apply);
}

TEST(FiniteSyncCycle, CancellationWinsOverQueuedMarkerWithoutSchedulerOrDatabaseWork) {
    finite_fixture fixture;
    fixture.connect();
    fixture.marker();
    const auto queued = fixture.scheduler->work.size();
    fixture.sync->cancel_finite_cycle();
    EXPECT_EQ(fixture.scheduler->work.size(), queued);
    EXPECT_EQ(fixture.status().phase, phase::cancelled);
    fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::cancelled);
}

TEST(FiniteSyncCycle, OldQueuedDownloadAckAndMarkerCannotCertifySuccessor) {
    finite_fixture fixture(1);
    fixture.connect();
    const auto generation = fixture.status().connection_generation;
    const auto first = fixture.uploads();
    fixture.receive(lattice::server_sent_event::make_audit_log({fixture.remote()}));
    fixture.receive(lattice::server_sent_event::make_ack({first[0].global_id}));
    fixture.marker();
    fixture.sync->disconnect();
    fixture.connect(); // drains both old jobs and new initial setup
    EXPECT_GT(fixture.status().connection_generation, generation);
    EXPECT_EQ(fixture.status().phase, phase::running);
    EXPECT_FALSE(fixture.status().catch_up_received);
    EXPECT_TRUE(fixture.db->db().query("SELECT 1 FROM TestPerson WHERE name='remote'").empty());
    EXPECT_EQ(fixture.uploads().size(), 2u); // old ACK did not suppress retransmission
    fixture.marker(); fixture.scheduler->run(); fixture.ack_all();
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
}

TEST(FiniteSyncCycle, TransportFailureIsTerminalAndDoesNotScheduleReconnect) {
    finite_fixture fixture;
    fixture.connect();
    fixture.wire->simulate_error("lost transport");
    fixture.marker(); fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::failed);
    EXPECT_EQ(fixture.status().failure, failure::transport);
    EXPECT_EQ(fixture.wire->connections, 1u);
    EXPECT_TRUE(fixture.scheduler->work.empty());
}

TEST(FiniteSyncCycle, RuntimeFilterChangeFailsReceiptAndIsAppliedOnNextCycle) {
    finite_fixture fixture;
    fixture.connect();
    fixture.sync->update_sync_filter({{"TestPerson", "age=1"}});
    fixture.marker(); fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::failed);
    EXPECT_EQ(fixture.status().failure, failure::configuration_changed);
    fixture.sync->disconnect(); fixture.connect();
    fixture.marker(); fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
}

TEST(FiniteSyncCycle, AppHandlersArePreservedAndEarlySyncWaitsForInitialSetup) {
    finite_fixture fixture(1);
    int opens = 0, completions = 0, progress = 0;
    fixture.sync->set_on_state_change([&](bool connected) {
        if (connected) { ++opens; fixture.sync->sync_now(); }
    });
    fixture.sync->set_on_sync_complete([&](const std::vector<std::string>&) { ++completions; });
    fixture.sync->set_on_progress([&](const auto&) { ++progress; });
    fixture.connect(); fixture.marker(); fixture.scheduler->run(); fixture.ack_all();
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
    EXPECT_EQ(opens, 1);
    EXPECT_EQ(completions, 1);
    EXPECT_GT(progress, 0);
}

TEST(FiniteSyncCycle, OrdinaryConnectionsDoNotNegotiateOrStopAtMarker) {
    finite_fixture fixture(0, false);
    fixture.connect();
    EXPECT_EQ(fixture.wire->url.find("lattice-catch-up-complete"), std::string::npos);
    fixture.marker(); fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::inactive);
    fixture.receive(lattice::server_sent_event::make_audit_log({fixture.remote()}));
    fixture.scheduler->run();
    EXPECT_EQ(fixture.db->db().query("SELECT * FROM TestPerson").size(), 1u);
}

TEST(FiniteSyncCycle, DurableOutboxWithoutSourceHistoryMustBeAcknowledged) {
    finite_fixture fixture;
    auto delivery = fixture.remote("durable-outbox-event");
    delivery.id = 0;
    // Provenance may already be compacted; payload and stable wire UUID are
    // the durable obligation, independent of the new cycle's audit horizon.
    fixture.db->db().execute(
        "INSERT INTO _lattice_sync_outbox(sync_id,source_audit_id,source_global_id,event_global_id,payload) "
        "VALUES(?,999,'compacted-source',?,?)",
        {fixture.config.sync_id, delivery.global_id, delivery.to_json()});
    fixture.connect(); fixture.marker(); fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::running);
    EXPECT_FALSE(fixture.status().horizon_captured);
    ASSERT_EQ(fixture.uploads().size(), 1u);
    EXPECT_EQ(fixture.uploads()[0].global_id, delivery.global_id);
    fixture.sync->disconnect(); fixture.connect();
    fixture.marker(); fixture.scheduler->run();
    ASSERT_EQ(fixture.uploads().size(), 2u);
    EXPECT_EQ(fixture.uploads()[1].global_id, delivery.global_id);
    fixture.ack_all();
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
    EXPECT_TRUE(fixture.db->db().query("SELECT 1 FROM _lattice_sync_outbox").empty());
}

TEST(FiniteSyncCycle, CleanupDisconnectPreservesCompletedWorkReceipt) {
    finite_fixture fixture;
    fixture.connect(); fixture.marker(); fixture.scheduler->run();
    ASSERT_EQ(fixture.status().phase, phase::work_complete);
    fixture.sync->cancel_finite_cycle(); fixture.sync->disconnect();
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
}

TEST(FiniteSyncCycle, LiveFramesAfterMarkerAreSkippedWhileUploadsStillAwaitAcknowledgment) {
    finite_fixture fixture(1);
    fixture.connect(); fixture.marker();
    fixture.receive(lattice::server_sent_event::make_audit_log({fixture.remote("later-live")}));
    fixture.scheduler->run();
    ASSERT_TRUE(fixture.status().catch_up_received);
    ASSERT_EQ(fixture.status().phase, phase::running);
    EXPECT_TRUE(fixture.db->db().query("SELECT 1 FROM TestPerson WHERE name='remote'").empty());
    for (const auto& message : fixture.wire->get_sent_messages()) {
        auto event = lattice::server_sent_event::from_json(message.as_string());
        ASSERT_TRUE(event);
        EXPECT_NE(event->event_type, lattice::server_sent_event::type::ack);
    }
    // A malformed post-marker frame cannot resolve the admitted upload.
    fixture.wire->simulate_message(lattice::transport_message::from_string("{invalid"));
    fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::running);
    fixture.ack_all();
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
}

TEST(FiniteSyncCycle, DisablingUploadFloorStillBoundsFiniteWindowsAndExcludesLaterWrites) {
    finite_fixture fixture(5, true, false);
    fixture.connect();
    ASSERT_EQ(fixture.uploads().size(), 2u);
    fixture.db->add(TestPerson{"after-horizon", 1, std::nullopt});
    fixture.marker(); fixture.scheduler->run();
    fixture.ack_window();
    EXPECT_EQ(fixture.uploads().size(), 4u);
    EXPECT_EQ(fixture.status().phase, phase::running);
    fixture.ack_all();
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
    EXPECT_EQ(fixture.uploads().size(), 5u);
    EXPECT_EQ(lattice::query_audit_log_for_sync(fixture.db->db(), fixture.config.sync_id).size(), 1u);
}

TEST(FiniteSyncCycle, FoundationUppercaseAcknowledgmentDrainsLowercaseUUIDWithoutTimeout) {
    finite_fixture fixture(1);
    const std::string lower = "01234567-89ab-cdef-0123-456789abcdef";
    const std::string upper = "01234567-89AB-CDEF-0123-456789ABCDEF";
    fixture.db->db().execute("UPDATE AuditLog SET globalId=?", {lower});
    fixture.connect(); fixture.marker(); fixture.scheduler->run();
    ASSERT_EQ(fixture.uploads().size(), 1u);
    ASSERT_EQ(fixture.uploads()[0].global_id, lower);
    fixture.receive(lattice::server_sent_event::make_ack({upper}));
    fixture.scheduler->run();
    EXPECT_EQ(fixture.sync->get_progress().pending_upload, 0);
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
    EXPECT_EQ(fixture.uploads().size(), 1u);
}

TEST(FiniteSyncCycle, LowercaseAcknowledgmentDrainsUppercaseStoredUUIDWithoutTimeout) {
    finite_fixture fixture(1);
    const std::string lower = "01234567-89ab-cdef-0123-456789abcdef";
    const std::string upper = "01234567-89AB-CDEF-0123-456789ABCDEF";
    fixture.db->db().execute("UPDATE AuditLog SET globalId=?", {upper});
    fixture.connect(); fixture.marker(); fixture.scheduler->run();
    ASSERT_EQ(fixture.uploads().size(), 1u);
    ASSERT_EQ(fixture.uploads()[0].global_id, upper);
    // Repeated upload requests must also use canonical in-flight keys.
    fixture.sync->sync_now(); fixture.scheduler->run();
    EXPECT_EQ(fixture.uploads().size(), 1u);
    fixture.receive(lattice::server_sent_event::make_ack({lower}));
    fixture.scheduler->run();
    EXPECT_EQ(fixture.sync->get_progress().pending_upload, 0);
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
}

TEST(FiniteSyncCycle, OpaqueOutboxIdentifiersKeepExactAcknowledgmentMatching) {
    finite_fixture fixture;
    auto delivery = fixture.remote("Opaque-Delivery-ID");
    fixture.db->db().execute(
        "INSERT INTO _lattice_sync_outbox(sync_id,event_global_id,payload) VALUES(?,?,?)",
        {fixture.config.sync_id, delivery.global_id, delivery.to_json()});
    fixture.connect(); fixture.marker(); fixture.scheduler->run();
    fixture.receive(lattice::server_sent_event::make_ack({"opaque-delivery-id"}));
    fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::running);
    EXPECT_EQ(fixture.sync->get_progress().pending_upload, 1);
    EXPECT_EQ(fixture.db->db().query("SELECT 1 FROM _lattice_sync_outbox").size(), 1u);
    fixture.receive(lattice::server_sent_event::make_ack({delivery.global_id}));
    fixture.scheduler->run();
    EXPECT_EQ(fixture.status().phase, phase::work_complete);
    EXPECT_EQ(fixture.sync->get_progress().pending_upload, 0);
    EXPECT_TRUE(fixture.db->db().query("SELECT 1 FROM _lattice_sync_outbox").empty());
}
