#include "TestHelpers.hpp"
#include <deque>
#include <mutex>

namespace {
// A caller-driven queue makes the control/replay boundary deterministic without
// sleeping. The pacer may enqueue work, so admission is protected by the mutex.
class floor_scheduler : public lattice::scheduler {
    std::mutex mutex_;
    std::deque<std::function<void()>> work_;
    std::atomic<bool> active_{true};
    const std::thread::id owner_ = std::this_thread::get_id();
    bool executing_ = false;
public:
    void invoke(std::function<void()>&& fn) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (active_) work_.push_back(std::move(fn));
    }
    bool is_on_thread() const noexcept override {
        return std::this_thread::get_id() == owner_ && executing_;
    }
    bool is_same_as(const scheduler* other) const noexcept override { return this == other; }
    bool can_invoke() const noexcept override { return active_; }
    void shutdown() override {
        std::lock_guard<std::mutex> lock(mutex_);
        active_ = false;
        work_.clear();
    }
    void run() {
        size_t steps = 0;
        for (;;) {
            std::function<void()> fn;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (work_.empty()) return;
                fn = std::move(work_.front());
                work_.pop_front();
            }
            if (++steps > 1000) throw std::runtime_error("floor reset fixture did not settle");
            executing_ = true;
            try { fn(); } catch (...) { executing_ = false; throw; }
            executing_ = false;
        }
    }
};

class floor_transport : public lattice::mock_sync_transport {
public:
    size_t connections = 0;
    void connect(const std::string& target,
                 const std::map<std::string, std::string>& headers = {}) override {
        ++connections;
        mock_sync_transport::connect(target, headers);
    }
};

struct floor_fixture {
    TempDB path{"floor_reset"};
    std::shared_ptr<floor_scheduler> scheduler = std::make_shared<floor_scheduler>();
    lattice::lattice_db* db = nullptr;
    floor_transport* wire = nullptr;
    std::unique_ptr<lattice::synchronizer> sync;
    std::vector<std::string> errors;

    explicit floor_fixture(bool finite = false) {
        auto owned = std::make_unique<lattice::lattice_db>(
            lattice::configuration(path.str(), scheduler));
        db = owned.get();
        auto seed = db->add(TestPerson{"seed", 0, std::nullopt});
        db->remove(seed);
        db->db().execute("DELETE FROM AuditLog");
        scheduler->run();
        lattice::sync_config config;
        config.sync_id = "wss:floor-reset";
        config.websocket_url = "wss://example.invalid/floor-reset";
        config.all_active_sync_ids = {config.sync_id};
        config.uploads_enabled = false;
        config.finite_cycle = finite;
        auto transport = std::make_unique<floor_transport>();
        wire = transport.get();
        sync = std::make_unique<lattice::synchronizer>(
            std::move(owned), config, std::move(transport));
        sync->set_on_error([this](const auto& message) { errors.push_back(message); });
        sync->connect();
        scheduler->run();
    }
    ~floor_fixture() { sync.reset(); }
    void receive_json(const std::string& text) {
        wire->simulate_message(lattice::transport_message::from_string(text));
    }
    void receive(const std::vector<lattice::audit_log_entry>& entries) {
        receive_json(lattice::server_sent_event::make_audit_log(entries).to_json());
        scheduler->run();
    }
    std::vector<std::string> acks() const {
        std::vector<std::string> ids;
        for (const auto& message : wire->get_sent_messages()) {
            const auto event = lattice::server_sent_event::from_json(message.as_string());
            if (event && event->event_type == lattice::server_sent_event::type::ack)
                ids.insert(ids.end(), event->acked_ids.begin(), event->acked_ids.end());
        }
        return ids;
    }
    size_t uploads() const {
        size_t count = 0;
        for (const auto& message : wire->get_sent_messages()) {
            const auto event = lattice::server_sent_event::from_json(message.as_string());
            if (event && event->event_type == lattice::server_sent_event::type::audit_log) ++count;
        }
        return count;
    }
    static std::vector<lattice::audit_log_entry> entries(size_t count) {
        std::vector<lattice::audit_log_entry> result;
        for (size_t i = 0; i < count; ++i) {
            lattice::audit_log_entry entry;
            entry.global_id = "floor-audit-" + std::to_string(i);
            entry.global_row_id = "floor-person-" + std::to_string(i);
            entry.table_name = "TestPerson";
            entry.operation = "INSERT";
            entry.timestamp = "2026-11-01T00:00:00Z";
            entry.changed_fields = {{"name", lattice::any_property("P" + std::to_string(i))},
                                    {"age", lattice::any_property(int64_t(i))}};
            entry.changed_fields_names = {"name", "age"};
            result.push_back(std::move(entry));
        }
        return result;
    }
    void expect_durable(size_t count, const std::string& cursor) {
        sync.reset();
        lattice::lattice_db reopened{lattice::configuration(path.str())};
        const auto rows = reopened.db().query("SELECT COUNT(*) AS count FROM TestPerson");
        EXPECT_EQ(std::get<int64_t>(rows[0].at("count")), int64_t(count));
        const auto slots = reopened.db().query("SELECT last_received_event_id AS cursor FROM _lattice_replication_slots WHERE sync_id='wss:floor-reset'");
        ASSERT_EQ(slots.size(), 1u);
        ASSERT_TRUE(std::holds_alternative<std::string>(slots[0].at("cursor")));
        EXPECT_EQ(std::get<std::string>(slots[0].at("cursor")), cursor);
    }
};
}

// The test uses the pre-existing parsing API so the unmodified parser fails by
// rejecting a valid control, rather than by requiring new symbols to compile.
TEST(SyncFloorReset, ParsesOnlyExplicitValidatedControlWithoutInventingAnAck) {
    for (const std::string head : {std::string{}, std::string("null"),
            std::string("\"ACDC6DC3-E249-4B8F-B9C1-5710AB11EC3E\"")}) {
        SCOPED_TRACE(head);
        const auto wire = std::string("{\"kind\":\"floorReset\",\"floorReason\":\"unknown client floor\"") +
            (head.empty() ? "" : ",\"floorReset\":" + head) + "}";
        std::string diagnostic = "previous error";
        auto event = lattice::server_sent_event::from_json(wire, &diagnostic);
        ASSERT_TRUE(event.has_value());
        EXPECT_TRUE(diagnostic.empty());
        EXPECT_NE(event->event_type, lattice::server_sent_event::type::ack);
        EXPECT_NE(event->event_type, lattice::server_sent_event::type::audit_log);
        EXPECT_TRUE(event->audit_logs.empty());
        EXPECT_TRUE(event->acked_ids.empty());
        const auto encoded = event->to_json();
        EXPECT_NE(encoded.find("\"kind\":\"floorReset\""), std::string::npos);
        EXPECT_NE(encoded.find("\"floorReason\":\"unknown client floor\""), std::string::npos);
        if (head.empty() || head == "null") EXPECT_EQ(encoded.find("\"floorReset\":"), std::string::npos);
        else EXPECT_NE(encoded.find("\"floorReset\":" + head), std::string::npos);
        auto roundtrip = lattice::server_sent_event::from_json(encoded);
        ASSERT_TRUE(roundtrip.has_value());
        EXPECT_EQ(roundtrip->event_type, event->event_type);
        EXPECT_EQ(roundtrip->to_json(), encoded);
        auto immutable = lattice::server_sent_event::from_json_immutable(wire);
        ASSERT_TRUE(immutable.has_value());
        EXPECT_EQ(immutable->event_type, event->event_type);
    }
}

// Green guard: malformed controls remain rejected; NACK and catch-up markers
// are separate controls already supported by the current protocol.
TEST(SyncFloorReset, MalformedMixedAndUnknownControlsRemainParseFailures) {
    for (const auto* wire : {
            R"({"kind":"floorReset"})",
            R"({"kind":"floorReset","floorReason":null})",
            R"({"kind":"floorReset","floorReason":42})",
            R"({"kind":"floorReset","floorReason":"","floorReset":42})",
            R"({"kind":"floorReset","floorReason":"","floorReset":"not-a-uuid"})",
            R"({"kind":"floorReset","floorReason":"","floorReset":"acdc6dc3-e249-4b8f-b9c1-5710ab11ec3g"})",
            R"({"kind":"floorReset","floorReason":"","floorReset":"acdc6dc3_e249-4b8f-b9c1-5710ab11ec3e"})",
            R"({"kind":"floorReset","floorReason":"","auditLog":[]})",
            R"({"kind":"floorReset","floorReason":"","ack":[]})",
            R"({"kind":"floorReset","floorReason":"","nack":[]})",
            R"({"kind":"floorReset","floorReason":"","catchUpComplete":true})",
            R"({"kind":"floorReset","floorReason":"","replayRequest":true})",
            R"({"kind":"floorReset","floorReason":"","unexpected":true})",
            R"({"floorReason":"","floorReset":null})",
            R"({"kind":"other","floorReason":"","floorReset":null})",
            R"({"kind":7,"floorReason":"","floorReset":null})",
            R"({"kind":"futureControl"})",
            R"({"kind":"rejected","rejected":"not implemented"})"}) {
        SCOPED_TRACE(wire);
        std::string diagnostic;
        EXPECT_FALSE(lattice::server_sent_event::from_json(wire, &diagnostic).has_value());
        EXPECT_FALSE(diagnostic.empty());
        EXPECT_FALSE(lattice::server_sent_event::from_json_immutable(wire).has_value());
    }
    EXPECT_TRUE(lattice::server_sent_event::from_json(R"({"kind":"nack","nack":[],"nackReason":"retry"})"));
    EXPECT_TRUE(lattice::server_sent_event::from_json(R"({"kind":"catchUpComplete","catchUpComplete":true})"));
}

TEST(SyncFloorReset, SameConnectionReplayEarnsCheckpointWhenRowsAlreadyMatch) {
    for (bool synthesized : {false, true}) {
        SCOPED_TRACE(synthesized);
        floor_fixture fixture;
        auto history = floor_fixture::entries(4);
        fixture.receive({history.begin(), history.begin() + 3});
        ASSERT_EQ(fixture.acks().size(), 3u);
        const auto rows_before = fixture.db->db().query("SELECT * FROM TestPerson ORDER BY id");
        const auto slots_before = fixture.db->db().query("SELECT * FROM _lattice_replication_slots");
        const auto receipts_before = fixture.db->db().query("SELECT * FROM _lattice_applied_receipts ORDER BY globalId");
        const auto audit_before = fixture.db->db().query("SELECT * FROM AuditLog ORDER BY id");
        const auto outbox_before = fixture.db->db().query("SELECT * FROM _lattice_sync_outbox ORDER BY receipt_id");
        const std::string head = "acdc6dc3-e249-4b8f-b9c1-5710ab11ec3e";
        fixture.receive_json("{\"kind\":\"floorReset\",\"floorReset\":\"" + head +
            "\",\"floorReason\":\"unknown client floor\"}");
        fixture.scheduler->run();
        EXPECT_EQ(fixture.db->db().query("SELECT * FROM _lattice_replication_slots"), slots_before);
        EXPECT_EQ(fixture.db->db().query("SELECT * FROM _lattice_applied_receipts ORDER BY globalId"), receipts_before);
        EXPECT_EQ(fixture.db->db().query("SELECT * FROM AuditLog ORDER BY id"), audit_before);
        EXPECT_EQ(fixture.db->db().query("SELECT * FROM _lattice_sync_outbox ORDER BY receipt_id"), outbox_before);
        EXPECT_EQ(fixture.acks().size(), 3u);
        EXPECT_EQ(fixture.sync->get_progress().received, 3);
        fixture.db->begin_transaction();
        EXPECT_FALSE(fixture.db->has_applied_sync_event(head));
        fixture.db->commit();
        auto replay = std::vector<lattice::audit_log_entry>(history.begin(), history.begin() + 3);
        for (size_t i = 0; i < replay.size(); ++i) {
            replay[i].global_id = i == 2 ? head : "compacted-audit-" + std::to_string(i);
            replay[i].synthesized = synthesized;
        }
        fixture.receive(replay);
        ASSERT_EQ(fixture.acks().size(), 6u);
        EXPECT_EQ(fixture.wire->connections, 1u);
        EXPECT_TRUE(fixture.errors.empty());
        EXPECT_EQ(fixture.db->db().query("SELECT * FROM TestPerson ORDER BY id"), rows_before);
        fixture.db->begin_transaction();
        EXPECT_TRUE(fixture.db->has_applied_sync_event(head));
        fixture.db->commit();
        if (!synthesized) {
            EXPECT_EQ(fixture.db->db().query("SELECT * FROM AuditLog ORDER BY id"), audit_before);
            EXPECT_EQ(fixture.db->db().query("SELECT 1 FROM _lattice_applied_receipts WHERE globalId=?", {head}).size(), 1u);
        }
        const auto slots = fixture.db->db().query("SELECT last_received_event_id AS cursor FROM _lattice_replication_slots WHERE sync_id='wss:floor-reset'");
        ASSERT_EQ(slots.size(), 1u);
        EXPECT_EQ(std::get<std::string>(slots[0].at("cursor")), head);
        fixture.receive({history.back()});
        EXPECT_EQ(fixture.acks().size(), 7u);
        EXPECT_EQ(fixture.wire->connections, 1u);
        EXPECT_EQ(fixture.uploads(), 0u);
        EXPECT_TRUE(fixture.errors.empty());
        fixture.expect_durable(4, history.back().global_id);
    }
}

TEST(SyncFloorReset, EmptyHistoryControlDoesNotCreateCheckpointAndLaterPushStillApplies) {
    floor_fixture fixture;
    const auto slots_before = fixture.db->db().query("SELECT * FROM _lattice_replication_slots");
    fixture.receive_json(R"({"kind":"floorReset","floorReason":"server lost history"})");
    fixture.receive_json(R"({"kind":"floorReset","floorReset":null,"floorReason":"empty durable log"})");
    fixture.scheduler->run();
    EXPECT_EQ(fixture.db->db().query("SELECT * FROM _lattice_replication_slots"), slots_before);
    EXPECT_TRUE(fixture.acks().empty());
    EXPECT_EQ(fixture.sync->get_progress().received, 0);
    auto history = floor_fixture::entries(1);
    fixture.receive(history);
    ASSERT_EQ(fixture.acks().size(), 1u);
    EXPECT_EQ(fixture.wire->connections, 1u);
    EXPECT_EQ(fixture.uploads(), 0u);
    EXPECT_TRUE(fixture.errors.empty());
    fixture.expect_durable(1, history.back().global_id);
}

// Finite cycles already fence invalid frames. A valid replay-boundary control
// must leave that cycle running; malformed controls still fail it before later
// queued rows can advance the checkpoint.
TEST(SyncFloorReset, InvalidControlFailsFiniteCycleWithoutAdvancingCheckpoint) {
    using phase = lattice::synchronizer_base::finite_cycle_phase;
    using failure = lattice::synchronizer_base::finite_cycle_failure;
    floor_fixture fixture(true);
    const auto history = floor_fixture::entries(3);
    fixture.receive({history.front()});
    ASSERT_EQ(fixture.acks().size(), 1u);
    fixture.receive_json(R"({"kind":"floorReset","floorReason":"unknown client floor"})");
    fixture.scheduler->run();
    EXPECT_EQ(fixture.sync->get_finite_cycle_status().phase, phase::running);
    EXPECT_FALSE(fixture.sync->get_finite_cycle_status().catch_up_received);
    EXPECT_TRUE(fixture.errors.empty());
    fixture.receive_json(R"({"kind":"floorReset","floorReason":false})");
    fixture.receive_json(lattice::server_sent_event::make_audit_log({history.back()}).to_json());
    fixture.scheduler->run();
    EXPECT_EQ(fixture.sync->get_finite_cycle_status().phase, phase::failed);
    EXPECT_EQ(fixture.sync->get_finite_cycle_status().failure, failure::invalid_frame);
    EXPECT_EQ(fixture.acks(), std::vector<std::string>{history.front().global_id});
    EXPECT_EQ(fixture.errors.size(), 1u);
    EXPECT_EQ(fixture.wire->connections, 1u);
    EXPECT_EQ(fixture.uploads(), 0u);
    fixture.expect_durable(1, history.front().global_id);
}
