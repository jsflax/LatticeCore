#include "TestHelpers.hpp"
#include <regex>
#include <set>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <csignal>
#include <cstdio>
#include <stdexcept>
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <unistd.h>
#include <sys/stat.h>
#endif

// ============================================================================
// Sync Unit Tests — AuditLog, sync protocol, synchronizer
// ============================================================================

// The native equivalent of a Swift FloatVector property: packed float BLOB plus is_vector schema.
struct FilterVectorRow {
    std::string name;
    int age;
    std::vector<uint8_t> embedding;
};
LATTICE_SCHEMA(FilterVectorRow, name, age, embedding);


#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
void bounded_mock_case(const std::function<void()>& body, unsigned watchdog_seconds = 10) {
    struct Style {
        std::string old = GTEST_FLAG_GET(death_test_style);
        ~Style() { GTEST_FLAG_SET(death_test_style, old); }
    } style;
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ASSERT_EXIT({
        sigset_t unblocked;
        sigemptyset(&unblocked);
        sigaddset(&unblocked, SIGALRM);
        if (std::signal(SIGALRM, SIG_DFL) == SIG_ERR ||
            sigprocmask(SIG_UNBLOCK, &unblocked, nullptr) != 0) _exit(2);
        alarm(watchdog_seconds);
        try {
            body();
            if (::testing::Test::HasFailure()) {
                // _exit bypasses buffered GTest stdout. Preserve bounded assertion diagnostics
                // on the captured stderr so a failing child reports the actual invariant.
                const auto* info=::testing::UnitTest::GetInstance()->current_test_info();
                if(info) {
                    const auto* result=info->result();int emitted=0;
                    for(int i=0;i<result->total_part_count() && emitted<16;++i) {
                        const auto& part=result->GetTestPartResult(i);
                        if(part.failed()) {++emitted;std::fprintf(stderr,"native_assertion %.512s:%d: %.4096s\n",
                            part.file_name()?part.file_name():"unknown",part.line_number(),part.summary());}
                    }
                }
                std::fflush(stderr);_exit(1);
            }
            std::fputs("mock_transport_complete\n", stderr);
            _exit(0);
        } catch (const std::exception& error) {
            std::fprintf(stderr, "mock_transport_failure: %s\n", error.what());
            _exit(1);
        }
    }, ::testing::ExitedWithCode(0), "mock_transport_complete");
}

// The default immediate scheduler can send on both the caller and pacer
// threads. The mock owns the synchronization of its own collected messages.
void mock_concurrent_send_snapshot() {
    lattice::mock_sync_transport transport;
    constexpr int messages_per_sender = 512;
    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    int ready = 0;
    bool start = false;
    std::atomic<bool> gate_timed_out{false};
    auto sender = [&](const char* prefix) {
        {
            std::unique_lock<std::mutex> lock(gate_mutex);
            ++ready;
            gate_cv.notify_all();
            if (!gate_cv.wait_for(lock, std::chrono::seconds(5), [&] { return start; })) {
                gate_timed_out.store(true);
                return;
            }
        }
        for (int i = 0; i < messages_per_sender; ++i) {
            transport.send(lattice::transport_message::from_string(
                std::string(prefix) + std::to_string(i) + std::string(128, 'x')));
            if (i % 8 == 0) std::this_thread::yield();
        }
    };
    std::thread left(sender, "left-");
    std::thread right(sender, "right-");
    bool both_ready;
    {
        std::unique_lock<std::mutex> lock(gate_mutex);
        both_ready = gate_cv.wait_for(lock, std::chrono::seconds(5), [&] { return ready == 2; });
        start = true; // release and join even if admission failed
    }
    gate_cv.notify_all();
    bool snapshots_valid = true;
    size_t prior_size = 0;
    for (int sample = 0; sample < 256; ++sample) {
        const auto snapshot = transport.get_sent_messages();
        if (snapshot.size() < prior_size || snapshot.size() > 2 * messages_per_sender) {
            snapshots_valid = false;
        }
        prior_size = snapshot.size();
        for (const auto& message : snapshot) {
            const auto text = message.as_string();
            if ((text.rfind("left-", 0) != 0 && text.rfind("right-", 0) != 0) ||
                text.size() < 133 || text.substr(text.size() - 128) != std::string(128, 'x')) {
                snapshots_valid = false;
            }
        }
        std::this_thread::yield();
    }
    left.join();
    right.join();
    ASSERT_TRUE(both_ready);
    ASSERT_FALSE(gate_timed_out.load());
    EXPECT_TRUE(snapshots_valid);
    const auto snapshot = transport.get_sent_messages();
    ASSERT_EQ(snapshot.size(), 2u * messages_per_sender);
    std::set<std::string> observed;
    for (const auto& message : snapshot) observed.insert(message.as_string());
    ASSERT_EQ(observed.size(), 2u * messages_per_sender);
    for (int i = 0; i < messages_per_sender; ++i) {
        EXPECT_EQ(observed.count("left-" + std::to_string(i) + std::string(128, 'x')), 1u);
        EXPECT_EQ(observed.count("right-" + std::to_string(i) + std::string(128, 'x')), 1u);
    }
    const auto retained_first = snapshot.front().as_string();
    transport.clear_sent_messages();
    EXPECT_TRUE(transport.get_sent_messages().empty());
    EXPECT_EQ(snapshot.size(), 2u * messages_per_sender);
    EXPECT_EQ(snapshot.front().as_string(), retained_first);
}

void mock_callback_reentry() {
    lattice::mock_sync_transport transport;
    int opens = 0;
    int messages = 0;
    int errors = 0;
    int closes = 0;
    transport.set_on_open([&] {
        ++opens;
        EXPECT_EQ(transport.state(), lattice::transport_state::open);
        transport.send(lattice::transport_message::from_string("open"));
        transport.set_on_open({});
    });
    transport.set_on_message([&](const lattice::transport_message& message) {
        ++messages;
        transport.send(message);
        transport.set_on_message([&](const lattice::transport_message&) { ++messages; });
    });
    transport.set_on_error([&](const std::string&) {
        ++errors;
        EXPECT_EQ(transport.get_sent_messages().size(), 2u);
        transport.set_on_error({});
    });
    transport.set_on_close([&](int code, const std::string&) {
        ++closes;
        EXPECT_EQ(code, 1000);
        EXPECT_EQ(transport.state(), lattice::transport_state::closed);
        transport.clear_sent_messages();
        transport.set_on_close({});
    });
    transport.connect("ws://mock");
    transport.simulate_message(lattice::transport_message::from_string("message"));
    transport.simulate_message(lattice::transport_message::from_string("replacement"));
    transport.simulate_error("error");
    transport.simulate_error("removed");
    transport.disconnect();
    transport.connect("ws://mock");
    transport.disconnect();
    EXPECT_EQ(opens, 1);
    EXPECT_EQ(messages, 2);
    EXPECT_EQ(errors, 1);
    EXPECT_EQ(closes, 1);
    EXPECT_TRUE(transport.get_sent_messages().empty());
}

} // namespace
TEST(MockTransport, ConcurrentSendAndSnapshotRetainEveryMessage) {
    bounded_mock_case(mock_concurrent_send_snapshot);
}
TEST(MockTransport, CallbacksReenterAndReplaceHandlersWithoutHoldingStateLock) {
    bounded_mock_case(mock_callback_reentry);
}
#endif

// ----------------------------------------------------------------------------
// AuditLog Triggers
// ----------------------------------------------------------------------------

TEST(Sync, InsertTrigger) {
    lattice::lattice_db db;
    db.db().execute("DELETE FROM AuditLog");

    auto person = db.add(TestPerson{"AuditTest", 25, std::nullopt});

    auto logs = db.db().query(
        "SELECT * FROM AuditLog WHERE tableName = 'TestPerson' AND operation = 'INSERT'");
    ASSERT_EQ(logs.size(), 1u);
    EXPECT_EQ(std::get<std::string>(logs[0].at("globalRowId")), person.global_id());

    auto fields = std::get<std::string>(logs[0].at("changedFields"));
    EXPECT_NE(fields.find("name"), std::string::npos);
    EXPECT_NE(fields.find("age"), std::string::npos);
}

TEST(Sync, UpdateTrigger) {
    lattice::lattice_db db;
    auto person = db.add(TestPerson{"Orig", 25, std::nullopt});
    db.db().execute("DELETE FROM AuditLog WHERE operation = 'UPDATE'");

    person.name = "Updated";

    auto logs = db.db().query(
        "SELECT * FROM AuditLog WHERE tableName = 'TestPerson' AND operation = 'UPDATE'");
    ASSERT_EQ(logs.size(), 1u);
    EXPECT_EQ(std::get<std::string>(logs[0].at("globalRowId")), person.global_id());
}

TEST(Sync, DeleteTrigger) {
    lattice::lattice_db db;
    auto person = db.add(TestPerson{"ToDelete", 25, std::nullopt});
    std::string gid = person.global_id();
    db.remove(person);

    auto logs = db.db().query(
        "SELECT * FROM AuditLog WHERE tableName = 'TestPerson' AND operation = 'DELETE' AND globalRowId = ?",
        {gid});
    EXPECT_EQ(logs.size(), 1u);
}

TEST(Sync, LinkTableTrigger) {
    lattice::lattice_db db;
    db.db().execute("DELETE FROM AuditLog");

    auto owner = db.add(TestOwner{"LinkOwner", nullptr});
    owner.pet = TestPet{"LinkPet", 15.0};

    auto logs = db.db().query(
        "SELECT * FROM AuditLog WHERE tableName LIKE '%TestOwner_TestPet_pet%' AND operation = 'INSERT'");
    EXPECT_EQ(logs.size(), 1u);
}

TEST(Sync, SyncControlDisables) {
    lattice::lattice_db db;
    db.db().execute("DELETE FROM AuditLog");

    // Disable sync
    db.db().execute("UPDATE _SyncControl SET disabled = 1 WHERE id = 1");
    auto silent = db.add(TestPerson{"SilentPerson", 30, std::nullopt});

    auto logs = db.db().query(
        "SELECT * FROM AuditLog WHERE globalRowId = ?", {silent.global_id()});
    EXPECT_EQ(logs.size(), 0u);

    // Re-enable
    db.db().execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");
    silent.name = "Updated";

    logs = db.db().query(
        "SELECT * FROM AuditLog WHERE globalRowId = ?", {silent.global_id()});
    EXPECT_EQ(logs.size(), 1u);
}

// ----------------------------------------------------------------------------
// AuditLog Compaction
// ----------------------------------------------------------------------------

TEST(Sync, CompactAuditLog) {
    lattice::lattice_db db;
    db.db().execute("DELETE FROM AuditLog");

    auto p1 = db.add(TestPerson{"Alice", 25, std::nullopt});
    auto p2 = db.add(TestPerson{"Bob", 30, std::string("bob@example.com")});
    auto d1 = db.add(TestDog{"Max", 25.5, true});

    p1.name = "Alice Updated";
    p1.age = 26;
    p2.email = std::optional<std::string>("bob.new@example.com");
    d1.weight = 26.0;

    // Pre-compact: at least 7 entries (3 INSERT + 4 UPDATE)
    auto pre = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog");
    EXPECT_GE(std::get<int64_t>(pre[0].at("cnt")), 7);

    int64_t created = db.compact_audit_log();
    EXPECT_EQ(created, 3);

    // Post-compact: 3 INSERT entries with current state
    auto post = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog");
    EXPECT_EQ(std::get<int64_t>(post[0].at("cnt")), 3);

    auto inserts = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog WHERE operation = 'INSERT'");
    EXPECT_EQ(std::get<int64_t>(inserts[0].at("cnt")), 3);

    // Verify compacted state has updated values
    auto alice_log = db.db().query(
        "SELECT changedFields FROM AuditLog WHERE tableName = 'TestPerson' AND globalRowId = ?",
        {p1.global_id()});
    ASSERT_EQ(alice_log.size(), 1u);
    auto fields = std::get<std::string>(alice_log[0].at("changedFields"));
    EXPECT_NE(fields.find("Alice Updated"), std::string::npos);
}

TEST(Sync, CompactAuditLogSchemaless) {
    TempDB tmp{"compact"};

    // Create with schema
    {
        lattice::configuration config(tmp.str());
        lattice::lattice_db db(config);
        auto p1 = db.add(TestPerson{"Alice", 30, std::string("alice@test.com")});
        p1.age = 31;
    }

    // Reopen without schema, compact
    {
        lattice::configuration config(tmp.str());
        lattice::lattice_db db(config);
        int64_t compacted = db.compact_audit_log();
        EXPECT_EQ(compacted, 1);
    }

    // Reopen with schema, verify data
    {
        lattice::configuration config(tmp.str());
        lattice::lattice_db db(config);
        auto persons = db.objects<TestPerson>();
        ASSERT_EQ(persons.size(), 1u);
        EXPECT_EQ(int(persons[0].age), 31);
    }
}

TEST(Sync, GenerateHistory) {
    lattice::lattice_db db;
    db.db().execute("DELETE FROM AuditLog");

    // Add objects with sync disabled
    db.db().execute("UPDATE _SyncControl SET disabled = 1 WHERE id = 1");
    db.add(TestPerson{"NoAudit1", 20, std::nullopt});
    db.add(TestPerson{"NoAudit2", 25, std::nullopt});
    db.db().execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");

    // Add one with audit
    db.add(TestPerson{"WithAudit", 30, std::nullopt});

    auto pre = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog");
    EXPECT_EQ(std::get<int64_t>(pre[0].at("cnt")), 1);

    int64_t generated = db.generate_history();
    EXPECT_EQ(generated, 2);

    auto post = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog");
    EXPECT_EQ(std::get<int64_t>(post[0].at("cnt")), 3);

    // Second run → nothing
    EXPECT_EQ(db.generate_history(), 0);
}

// ----------------------------------------------------------------------------
// Sync Protocol — JSON encoding/decoding
// ----------------------------------------------------------------------------

TEST(Sync, AuditLogEntryJson) {
    lattice::audit_log_entry entry;
    entry.id = 1;
    entry.global_id = "abc-123";
    entry.table_name = "TestPerson";
    entry.operation = "INSERT";
    entry.row_id = 42;
    entry.global_row_id = "row-uuid-456";
    entry.changed_fields = {{"name", lattice::any_property("Alice")},
                            {"age", lattice::any_property(30)}};
    entry.changed_fields_names = {"name", "age"};
    entry.timestamp = "2024-01-01T00:00:00Z";
    entry.is_from_remote = false;
    entry.is_synchronized = false;

    std::string json = entry.to_json();
    EXPECT_NE(json.find("\"tableName\":\"TestPerson\""), std::string::npos);
    EXPECT_NE(json.find("\"operation\":\"INSERT\""), std::string::npos);

    auto parsed = lattice::audit_log_entry::from_json(json);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->table_name, "TestPerson");
    EXPECT_EQ(parsed->operation, "INSERT");
    EXPECT_EQ(parsed->global_row_id, "row-uuid-456");
}

TEST(Sync, ServerSentEventJson) {
    lattice::audit_log_entry entry;
    entry.global_id = "abc-123";
    entry.table_name = "TestPerson";
    entry.operation = "INSERT";
    entry.global_row_id = "row-uuid";
    entry.changed_fields = {{"name", lattice::any_property("Alice")}};
    entry.changed_fields_names = {"name"};
    entry.timestamp = "2024-01-01T00:00:00Z";

    // Audit log event
    auto audit_event = lattice::server_sent_event::make_audit_log({entry});
    auto json = audit_event.to_json();
    EXPECT_NE(json.find("\"auditLog\":["), std::string::npos);

    auto parsed = lattice::server_sent_event::from_json(json);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->event_type, lattice::server_sent_event::type::audit_log);
    EXPECT_EQ(parsed->audit_logs.size(), 1u);

    // Ack event
    auto ack = lattice::server_sent_event::make_ack({"id-1", "id-2", "id-3"});
    auto ack_json = ack.to_json();
    auto parsed_ack = lattice::server_sent_event::from_json(ack_json);
    ASSERT_TRUE(parsed_ack.has_value());
    EXPECT_EQ(parsed_ack->event_type, lattice::server_sent_event::type::ack);
    EXPECT_EQ(parsed_ack->acked_ids.size(), 3u);
}

TEST(Sync, ServerSentNackRoundTripsWithoutAcknowledging) {
    // Exact keys emitted by Swift ServerSentEvent.nack(ids:reason:).
    const std::string wire = R"({"kind":"nack","nack":["1F7219D7-9FE9-4ABA-883C-835720F80EC2"],"nackReason":"relay apply failed [no-throw-shortfall]"})";
    std::string diagnostic = "stale error";
    auto event = lattice::server_sent_event::from_json(wire, &diagnostic);
    ASSERT_TRUE(event.has_value());
    EXPECT_TRUE(diagnostic.empty());
    EXPECT_EQ(event->event_type, lattice::server_sent_event::type::nack);
    EXPECT_EQ(event->nacked_ids, std::vector<std::string>({"1F7219D7-9FE9-4ABA-883C-835720F80EC2"}));
    EXPECT_EQ(event->nack_reason, "relay apply failed [no-throw-shortfall]");
    EXPECT_TRUE(event->acked_ids.empty());
    EXPECT_TRUE(event->audit_logs.empty());

    const auto encoded = lattice::server_sent_event::make_nack(
        event->nacked_ids, event->nack_reason).to_json();
    EXPECT_EQ(encoded.find("\"ack\":"), std::string::npos);
    EXPECT_EQ(encoded.find("\"auditLog\":"), std::string::npos);
    EXPECT_EQ(encoded.find("\"replayRequest\":"), std::string::npos);
    auto roundtrip = lattice::server_sent_event::from_json(encoded);
    ASSERT_TRUE(roundtrip.has_value());
    EXPECT_EQ(roundtrip->event_type, lattice::server_sent_event::type::nack);
    EXPECT_EQ(roundtrip->nacked_ids, event->nacked_ids);
    EXPECT_EQ(roundtrip->nack_reason, event->nack_reason);
}

TEST(Sync, ServerSentNackOptionalReasonAndLegacyKeyFrames) {
    for (const auto& wire : {R"({"nack":["id-1"]})",
                             R"({"kind":"nack","nack":["id-1"],"nackReason":null})"}) {
        std::string diagnostic = "stale error";
        auto event = lattice::server_sent_event::from_json(wire, &diagnostic);
        ASSERT_TRUE(event.has_value());
        EXPECT_TRUE(diagnostic.empty());
        EXPECT_EQ(event->event_type, lattice::server_sent_event::type::nack);
        EXPECT_TRUE(event->nack_reason.empty());
    }
    // Old peers omitted kind; diagnostics must not make that an error.
    EXPECT_TRUE(lattice::server_sent_event::from_json(R"({"ack":["id-1"]})"));
    EXPECT_TRUE(lattice::server_sent_event::from_json(R"({"auditLog":[]})"));
    EXPECT_TRUE(lattice::server_sent_event::from_json(R"({"replayRequest":true})"));
}

TEST(Sync, ServerSentNackRejectsMalformedAndAmbiguousFrames) {
    const std::vector<std::pair<std::string, std::string>> cases = {
        {R"({"nack":"SECRET"})", "nack is not an array"},
        {R"({"nack":["id-1",42]})", "invalid nack entry"},
        {R"({"nack":[""]})", "invalid nack entry"},
        {R"({"nack":["id-1"],"nackReason":{"SECRET":true}})", "nackReason is not a string"},
        {R"({"kind":"nack","ack":["id-1"]})", "sync frame kind does not match payload"},
        {R"({"kind":"ack","nack":["id-1"]})", "sync frame kind does not match payload"},
        {R"({"nack":["id-1"],"ack":["id-1"]})", "ambiguous sync frame payload"},
        {R"({"nack":["id-1"],"auditLog":[]})", "ambiguous sync frame payload"},
        {R"({"nack":["id-1"],"replayRequest":true})", "ambiguous sync frame payload"},
        {R"({"ack":["id-1"],"rejected":"SECRET"})", "ambiguous sync frame payload"},
        {R"({"nack":["id-1"],"nackReason":"SECRET})", "malformed sync JSON"},
        {R"(["SECRET"])", "sync frame is not an object"},
    };
    for (const auto& [wire, expected] : cases) {
        std::string diagnostic;
        EXPECT_FALSE(lattice::server_sent_event::from_json(wire, &diagnostic));
        EXPECT_EQ(diagnostic, expected);
        EXPECT_EQ(diagnostic.find("SECRET"), std::string::npos);
    }
}

TEST(Sync, ServerSentAuditFailureReportsPositionWithoutWireValues) {
    lattice::audit_log_entry valid;
    valid.global_id = "valid-entry";
    valid.table_name = "TestPerson";
    valid.operation = "INSERT";
    valid.global_row_id = "valid-row";
    valid.changed_fields = {{"name", lattice::any_property("Alice")}};
    valid.changed_fields_names = {"name"};
    valid.timestamp = "2024-01-01T00:00:00Z";
    const auto wire = std::string("{\"auditLog\":[") + valid.to_json() +
        R"(,{"globalId":"SECRET","tableName":42}]})";
    std::string diagnostic;
    EXPECT_FALSE(lattice::server_sent_event::from_json(wire, &diagnostic));
    EXPECT_EQ(diagnostic, "invalid auditLog entry at index 1");
    EXPECT_EQ(diagnostic.find("SECRET"), std::string::npos);
    EXPECT_EQ(diagnostic.find("Alice"), std::string::npos);
    // Failure is whole-frame; there is no partially parsed event to apply.
    EXPECT_FALSE(lattice::server_sent_event::from_json(wire));
    EXPECT_TRUE(lattice::server_sent_event::from_json(R"({"ack":[]})", &diagnostic));
    EXPECT_TRUE(diagnostic.empty());
}

TEST(Sync, GenerateInstructionInsert) {
    lattice::audit_log_entry entry;
    entry.table_name = "TestPerson";
    entry.operation = "INSERT";
    entry.global_row_id = "uuid-123";
    entry.changed_fields = {{"name", lattice::any_property("Bob")},
                            {"age", lattice::any_property(25)}};
    entry.changed_fields_names = {"name", "age"};

    auto [sql, params] = entry.generate_instruction();
    EXPECT_NE(sql.find("INSERT INTO TestPerson"), std::string::npos);
    EXPECT_NE(sql.find("ON CONFLICT(globalId) DO UPDATE"), std::string::npos);
    EXPECT_EQ(params.size(), 3u);  // globalId, name, age
}

TEST(Sync, GenerateInstructionUpdate) {
    lattice::audit_log_entry entry;
    entry.table_name = "TestPerson";
    entry.operation = "UPDATE";
    entry.global_row_id = "uuid-123";
    entry.changed_fields = {{"name", lattice::any_property("Bobby")}};
    entry.changed_fields_names = {"name"};

    auto [sql, params] = entry.generate_instruction();
    EXPECT_NE(sql.find("UPDATE TestPerson SET"), std::string::npos);
    EXPECT_NE(sql.find("WHERE globalId = ?"), std::string::npos);
    // Value-guarded update (B2): the WHERE re-binds each changed value via
    // "col IS NOT ?" so changes()==0 identifies a genuine no-op.
    EXPECT_NE(sql.find("name IS NOT ?"), std::string::npos);
    EXPECT_EQ(params.size(), 3u);  // name, globalId, guard re-bind of name
}

TEST(Sync, GenerateInstructionDelete) {
    lattice::audit_log_entry entry;
    entry.table_name = "TestPerson";
    entry.operation = "DELETE";
    entry.global_row_id = "uuid-123";

    auto [sql, params] = entry.generate_instruction();
    EXPECT_NE(sql.find("DELETE FROM TestPerson WHERE globalId = ?"), std::string::npos);
    EXPECT_EQ(params.size(), 1u);
}

// ----------------------------------------------------------------------------
// Synchronizer with Mock WebSocket
// ----------------------------------------------------------------------------

TEST(Sync, LegacyNetworkFactoryAcceptsSchedulerContext) {
    // mock_network_factory implements only the original no-argument virtual.
    // The scheduler-aware entry point must preserve that implementation.
    lattice::mock_network_factory legacy_factory;
    lattice::network_factory& factory = legacy_factory;
    auto scheduler = std::make_shared<lattice::immediate_scheduler>();
    auto transport = factory.create_sync_transport(scheduler);
    ASSERT_NE(transport, nullptr);
    EXPECT_EQ(transport.get(), legacy_factory.last_websocket());
    EXPECT_EQ(transport->state(), lattice::transport_state::closed);

    transport->connect("ws://test.invalid/scheduler-fallback");
    EXPECT_EQ(transport->state(), lattice::transport_state::open);
    transport->disconnect();
}

TEST(Sync, SynchronizerConnectAndUpload) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"sync"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));

    lattice::sync_config config;
    config.websocket_url = "ws://localhost:8080/sync";
    config.authorization_token = "test-token";
    config.sync_id = "test-sync";
    config.all_active_sync_ids = {"test-sync"};

    db->add(TestPerson{"SyncTest", 40, std::nullopt});

    lattice::synchronizer sync(std::move(db), config);
    auto* mock_ws = mock_factory->last_websocket();
    ASSERT_NE(mock_ws, nullptr);

    bool connected = false;
    sync.set_on_state_change([&](bool state) { connected = state; });
    sync.connect();
    EXPECT_TRUE(connected);
    EXPECT_TRUE(sync.is_connected());

    // Trigger upload
    sync.sync_now();
    auto sent = mock_ws->get_sent_messages();
    ASSERT_FALSE(sent.empty());
    EXPECT_NE(sent[0].as_string().find("\"auditLog\""), std::string::npos);

    sync.disconnect();
    EXPECT_FALSE(sync.is_connected());
}

TEST(Sync, SynchronizerReceiveAck) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"syncack"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    lattice::lattice_db reader_db(lattice::configuration(tmp.str()));

    lattice::sync_config config;
    config.websocket_url = "ws://localhost:8080/sync";
    config.authorization_token = "test-token";
    config.sync_id = "test-sync";
    config.all_active_sync_ids = {"test-sync"};

    db->add(TestPerson{"AckTest", 40, std::nullopt});
    lattice::synchronizer sync(std::move(db), config);
    auto* mock_ws = mock_factory->last_websocket();

    sync.connect();
    sync.sync_now();

    // Send ack for all unsynced entries
    auto unsynced = lattice::query_audit_log(reader_db.db(), true);
    ASSERT_FALSE(unsynced.empty());

    std::vector<std::string> to_ack;
    for (const auto& e : unsynced) to_ack.push_back(e.global_id);
    mock_ws->simulate_message(lattice::transport_message::from_string(
        lattice::server_sent_event::make_ack(to_ack).to_json()));

    auto still_unsynced = lattice::query_audit_log(reader_db.db(), true);
    EXPECT_TRUE(still_unsynced.empty());

    sync.disconnect();
}

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(Sync, SynchronizerNackReportsReasonWithoutAcknowledgingOrImmediateResend) {
    bounded_mock_case([] {
        auto factory = std::make_shared<lattice::mock_network_factory>();
        lattice::set_network_factory(factory);
        TempDB tmp{"syncnack"};
        auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
        lattice::lattice_db reader(lattice::configuration(tmp.str()));
        db->add(TestPerson{"NackTest", 40, std::nullopt});
        lattice::sync_config config;
        config.websocket_url = "ws://localhost:8080/sync";
        config.sync_id = "nack-test";
        config.all_active_sync_ids = {config.sync_id};
        config.upload_coalesce_ms = 0;
        config.checkpoint_passive_interval_ms = 0;
        std::vector<std::string> errors;
        size_t completions = 0;
        lattice::synchronizer sync(std::move(db), config);
        sync.set_on_error([&](const std::string& error) { errors.push_back(error); });
        sync.set_on_sync_complete([&](const std::vector<std::string>&) { ++completions; });
        auto* transport = factory->last_websocket();
        ASSERT_NE(transport, nullptr);
        sync.connect();
        const auto before = sync.get_progress();
        ASSERT_GT(before.pending_upload, 0);
        const auto sent = transport->get_sent_messages().size();
        auto pending = lattice::query_audit_log(reader.db(), true);
        ASSERT_FALSE(pending.empty());
        std::vector<std::string> ids;
        for (const auto& entry : pending) ids.push_back(entry.global_id);
        const auto floor_before = reader.db().query(
            "SELECT upload_floor FROM _lattice_replication_slots WHERE sync_id = ?",
            {config.sync_id});
        const auto nack = lattice::server_sent_event::make_nack(ids, "relay apply failed").to_json();
        for (int i = 0; i < 8; ++i) {
            transport->simulate_message(lattice::transport_message::from_string(nack));
            sync.sync_now(); // must not free the NACKed window for a new send
        }
        ASSERT_EQ(errors.size(), 8u);
        for (const auto& error : errors) EXPECT_EQ(error, "Sync upload rejected: relay apply failed");
        EXPECT_EQ(completions, 0u);
        EXPECT_EQ(transport->get_sent_messages().size(), sent);
        EXPECT_EQ(sync.get_progress().pending_upload, before.pending_upload);
        EXPECT_EQ(sync.get_progress().acked, before.acked);
        EXPECT_EQ(lattice::query_audit_log(reader.db(), true).size(), pending.size());
        EXPECT_EQ(reader.db().query(
            "SELECT upload_floor FROM _lattice_replication_slots WHERE sync_id = ?",
            {config.sync_id}), floor_before);

        transport->simulate_message(lattice::transport_message::from_string(
            lattice::server_sent_event::make_nack({"unrelated-id"}, "").to_json()));
        EXPECT_EQ(errors.back(), "Sync upload rejected: no reason supplied");
        EXPECT_EQ(transport->get_sent_messages().size(), sent);
        transport->simulate_message(lattice::transport_message::from_string(
            lattice::server_sent_event::make_ack(ids).to_json()));
        EXPECT_TRUE(lattice::query_audit_log(reader.db(), true).empty());
        EXPECT_EQ(sync.get_progress().pending_upload, 0);
        EXPECT_EQ(completions, 1u);
        sync.disconnect();
    });
}

TEST(Sync, SynchronizerNackRetainsAckTimeoutRecovery) {
    bounded_mock_case([] {
        auto factory = std::make_shared<lattice::mock_network_factory>();
        lattice::set_network_factory(factory);
        TempDB tmp{"nacktimeout"};
        auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
        lattice::lattice_db reader(lattice::configuration(tmp.str()));
        db->add(TestPerson{"TimeoutNack", 40, std::nullopt});
        lattice::sync_config config;
        config.websocket_url = "ws://localhost:8080/sync";
        config.sync_id = "nack-timeout";
        config.all_active_sync_ids = {config.sync_id};
        config.upload_coalesce_ms = 0;
        config.checkpoint_passive_interval_ms = 0;
        config.ack_timeout_base_ms = 300;
        lattice::synchronizer sync(std::move(db), config);
        auto* transport = factory->last_websocket();
        ASSERT_NE(transport, nullptr);
        sync.connect();
        const auto initial_sends = transport->get_sent_messages().size();
        ASSERT_GT(initial_sends, 0u);
        const auto pending = lattice::query_audit_log(reader.db(), true);
        std::vector<std::string> ids;
        for (const auto& entry : pending) ids.push_back(entry.global_id);
        ASSERT_FALSE(ids.empty());
        transport->simulate_message(lattice::transport_message::from_string(
            lattice::server_sent_event::make_nack(ids, "temporary rejection").to_json()));
        // No-immediate-resend is pinned by the preceding NACK test. Do not
        // assert an early send count here: a stalled runner may legitimately
        // cross this test's short watchdog deadline before this point.
        // The pre-existing watchdog eventually retries without a new write.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (transport->get_sent_messages().size() == initial_sends &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_GT(transport->get_sent_messages().size(), initial_sends);
        EXPECT_EQ(lattice::query_audit_log(reader.db(), true).size(), pending.size());
        EXPECT_EQ(sync.get_progress().acked, 0);
        sync.disconnect();
    });
}
#endif

TEST(Sync, SynchronizerReceiveRemote) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"syncremote"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    lattice::lattice_db reader_db(lattice::configuration(tmp.str()));

    lattice::sync_config config;
    config.websocket_url = "ws://localhost:8080/sync";
    config.authorization_token = "test-token";
    config.sync_id = "test-sync";
    config.all_active_sync_ids = {"test-sync"};

    lattice::synchronizer sync(std::move(db), config);
    auto* mock_ws = mock_factory->last_websocket();
    sync.connect();

    // Simulate remote INSERT
    lattice::audit_log_entry remote;
    remote.global_id = "remote-audit-1";
    remote.table_name = "TestPerson";
    remote.operation = "INSERT";
    remote.row_id = 999;
    remote.global_row_id = "remote-person-uuid";
    remote.changed_fields = {{"name", lattice::any_property("RemotePerson")},
                             {"age", lattice::any_property(50)}};
    remote.changed_fields_names = {"name", "age"};
    remote.timestamp = "2024-01-01T00:00:00Z";

    mock_ws->simulate_message(lattice::transport_message::from_string(
        lattice::server_sent_event::make_audit_log({remote}).to_json()));

    // Verify ack sent back
    auto ack_sent = mock_ws->get_sent_messages();
    ASSERT_FALSE(ack_sent.empty());

    // Verify remote person created
    auto found = reader_db.find_by_global_id<TestPerson>("remote-person-uuid");
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(std::string(found->name), "RemotePerson");
    EXPECT_EQ(int(found->age), 50);

    sync.disconnect();
}

// ----------------------------------------------------------------------------
// Synchronizer destroy race
// ----------------------------------------------------------------------------

class slow_mock_transport : public lattice::sync_transport {
public:
    void connect(const std::string&,
                 const std::map<std::string, std::string>&) override {
        state_ = lattice::transport_state::open;
        if (on_open_) on_open_();
    }
    void disconnect() override { state_ = lattice::transport_state::closed; }
    lattice::transport_state state() const override { return state_; }
    void send(const lattice::transport_message&) override {}
    void set_on_open(on_open_handler h) override { on_open_ = h; }
    void set_on_message(on_message_handler) override {}
    void set_on_error(on_error_handler) override {}
    void set_on_close(on_close_handler) override {}
private:
    lattice::transport_state state_ = lattice::transport_state::closed;
    on_open_handler on_open_;
};

TEST(Sync, DestroyRaceNocrash) {
    TempDB tmp{"destroyrace"};

    lattice::configuration db_cfg(tmp.str(),
                                  std::make_shared<lattice::std_thread_scheduler>());
    auto db = std::make_unique<lattice::lattice_db>(db_cfg);

    for (int i = 0; i < 500; i++) {
        db->add(TestPerson{"race-" + std::to_string(i), i, std::nullopt});
    }

    lattice::sync_config sync_cfg;
    sync_cfg.sync_id = "destroy-race";
    sync_cfg.all_active_sync_ids = {"destroy-race"};
    sync_cfg.sync_filter = std::vector<lattice::sync_filter_entry>{
        {"TestPerson", std::string("name = 'IMPOSSIBLE'")}
    };

    auto transport = std::make_unique<slow_mock_transport>();
    auto sync = std::make_unique<lattice::synchronizer>(
        std::move(db), sync_cfg, std::move(transport));
    sync->connect();

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    sync.reset();  // Destroy while scheduler thread may be running
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    // If we get here without crashing, the test passes
}

// ----------------------------------------------------------------------------
// Apply remote changes with vec0 (E2E bug scenario)
// ----------------------------------------------------------------------------

TEST(Sync, ApplyRemoteVec0OnTarget) {
    TempDB tmp{"syncvec0"};

    // Set up sync_db that receives remote changes
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));

    // Create the vector table setup
    db->db().execute(
        "CREATE TABLE IF NOT EXISTS VectorDoc ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "globalId TEXT UNIQUE NOT NULL, "
        "title TEXT NOT NULL, "
        "embedding BLOB)");
    db->ensure_vec0_table("VectorDoc", "embedding", 3);

    // Simulate applying a remote INSERT with an embedding
    auto emb = pack_floats({1.0f, 0.0f, 0.0f});
    // Build the hex string for the BLOB
    std::string hex;
    for (auto byte : emb) {
        char buf[3];
        snprintf(buf, sizeof(buf), "%02X", byte);
        hex += buf;
    }

    db->db().execute("UPDATE _SyncControl SET disabled = 1 WHERE id = 1");
    db->db().execute(
        "INSERT INTO VectorDoc (globalId, title, embedding) "
        "VALUES ('remote-vec-1', 'RemoteDoc', unhex(?))",
        {hex});
    db->db().execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");

    // The vec0 trigger should have populated the vec0 table
    // OR we need to reconcile
    db->reconcile_vec0("VectorDoc", "embedding");

    // Now knn_query should find the remote document
    auto qvec = pack_floats({1.0f, 0.0f, 0.0f});
    auto results = db->knn_query("VectorDoc", "embedding", qvec, 1,
                                  lattice::lattice_db::distance_metric::l2);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].global_id, "remote-vec-1");
}

// ============================================================================
// Exploratory tests targeting known sync bugs
// ============================================================================

// ----------------------------------------------------------------------------
// BLOB round-trip through audit_log_entry JSON (BlobColumnRoundTrip bug)
// The BLOB goes: trigger hex() → AuditLog JSON → to_json() → from_json()
//              → generate_instruction(schema) → unhex(?) → INSERT
// Each step can corrupt the data.
// ----------------------------------------------------------------------------

TEST(Sync, BlobJsonRoundTrip) {
    // Simulate what happens when a BLOB column goes through the full
    // sync JSON serialization/deserialization cycle.

    // Original float vector
    std::vector<float> original = {1.0f, 2.0f, 3.0f, 4.5f, -0.5f};
    auto blob = pack_floats(original);

    // Step 1: The audit trigger stores hex(blob) as a string in changedFields JSON.
    // Simulate: json_object('embedding', hex(blob)) produces a plain hex string.
    std::string hex_str;
    for (auto byte : blob) {
        char buf[3];
        snprintf(buf, sizeof(buf), "%02X", byte);
        hex_str += buf;
    }

    // Step 2: Build an audit_log_entry as if read from AuditLog table
    // The changedFields from the trigger is: {"label":"test","embedding":"0000803F..."}
    // parse_changed_fields reads this and creates any_property values
    std::string trigger_json = "{\"label\":\"blob test\",\"embedding\":\"" + hex_str + "\"}";
    auto fields = lattice::audit_log_entry::parse_changed_fields(trigger_json);
    ASSERT_EQ(fields.size(), 2u);
    ASSERT_NE(fields.find("embedding"), fields.end());

    // The embedding should be string_kind (raw hex from trigger, no {kind,value} wrapper)
    auto& emb_prop = fields.at("embedding");
    EXPECT_EQ(emb_prop.kind, lattice::any_property_kind::string_kind);

    // Step 3: Serialize to sync JSON (to_json → from_json round-trip)
    lattice::audit_log_entry entry;
    entry.global_id = "blob-test-1";
    entry.table_name = "VectorDoc";
    entry.operation = "INSERT";
    entry.global_row_id = "blob-uuid-1";
    entry.changed_fields = fields;
    entry.changed_fields_names = {"label", "embedding"};
    entry.timestamp = "2024-01-01T00:00:00Z";

    std::string json = entry.to_json();
    auto parsed = lattice::audit_log_entry::from_json(json);
    ASSERT_TRUE(parsed.has_value());

    // Step 4: Check that the parsed entry's embedding field survived
    auto emb_it = parsed->changed_fields.find("embedding");
    ASSERT_NE(emb_it, parsed->changed_fields.end());

    // After round-trip through {kind, value} JSON, it should still be usable
    auto col_val = emb_it->second.to_column_value();

    // Step 5: generate_instruction with schema marking embedding as BLOB
    std::unordered_map<std::string, lattice::column_type> schema;
    schema["label"] = lattice::column_type::text;
    schema["embedding"] = lattice::column_type::blob;
    auto [sql, params] = parsed->generate_instruction(schema);

    // SQL should contain unhex(?) for the embedding column
    EXPECT_NE(sql.find("unhex(?)"), std::string::npos)
        << "SQL should use unhex(?) for BLOB columns: " << sql;

    // Step 6: Actually apply it to a real database and verify
    lattice::lattice_db db;
    db.db().execute(
        "CREATE TABLE IF NOT EXISTS VectorDoc ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "globalId TEXT UNIQUE NOT NULL, "
        "label TEXT NOT NULL, "
        "embedding BLOB)");

    db.db().execute(sql, params);

    // Verify the BLOB was stored correctly
    auto rows = db.db().query(
        "SELECT label, embedding FROM VectorDoc WHERE globalId = 'blob-uuid-1'");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("label")), "blob test");

    // Verify embedding round-trips correctly
    auto stored_blob = std::get<std::vector<uint8_t>>(rows[0].at("embedding"));
    ASSERT_EQ(stored_blob.size(), blob.size());
    auto stored_floats = unpack_floats(stored_blob);
    ASSERT_EQ(stored_floats.size(), original.size());
    for (size_t i = 0; i < original.size(); i++) {
        EXPECT_NEAR(stored_floats[i], original[i], 0.001)
            << "Float mismatch at index " << i;
    }
}

TEST(Sync, BlobSurvivesServerSentEventRoundTrip) {
    // Full SSE round-trip: entry → SSE JSON → parse → apply
    std::vector<float> original = {1.0f, 2.0f, 3.0f, 4.5f, -0.5f};
    auto blob = pack_floats(original);
    std::string hex_str;
    for (auto byte : blob) {
        char buf[3];
        snprintf(buf, sizeof(buf), "%02X", byte);
        hex_str += buf;
    }

    lattice::audit_log_entry entry;
    entry.global_id = "sse-blob-1";
    entry.table_name = "VectorDoc";
    entry.operation = "INSERT";
    entry.global_row_id = "sse-blob-uuid";
    entry.changed_fields = {
        {"label", lattice::any_property("sse test")},
        {"embedding", lattice::any_property(hex_str)}  // stored as string_kind
    };
    entry.changed_fields_names = {"label", "embedding"};
    entry.timestamp = "2024-01-01T00:00:00Z";

    // Full SSE round-trip
    auto sse = lattice::server_sent_event::make_audit_log({entry});
    auto sse_json = sse.to_json();
    auto parsed_sse = lattice::server_sent_event::from_json(sse_json);
    ASSERT_TRUE(parsed_sse.has_value());
    ASSERT_EQ(parsed_sse->audit_logs.size(), 1u);

    auto& parsed_entry = parsed_sse->audit_logs[0];

    // Apply to database
    lattice::lattice_db db;
    db.db().execute(
        "CREATE TABLE IF NOT EXISTS VectorDoc ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "globalId TEXT UNIQUE NOT NULL, "
        "label TEXT NOT NULL, "
        "embedding BLOB)");

    auto schema = db.get_table_schema("VectorDoc");
    auto [sql, params] = parsed_entry.generate_instruction(schema);
    db.db().execute(sql, params);

    // Verify
    auto rows = db.db().query(
        "SELECT label, embedding FROM VectorDoc WHERE globalId = 'sse-blob-uuid'");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("label")), "sse test");

    auto stored = std::get<std::vector<uint8_t>>(rows[0].at("embedding"));
    auto floats = unpack_floats(stored);
    ASSERT_EQ(floats.size(), original.size());
    for (size_t i = 0; i < original.size(); i++) {
        EXPECT_NEAR(floats[i], original[i], 0.001);
    }
}

TEST(Sync, BlobVec0QueryableAfterRemoteApply) {
    // E2E: sync applies INSERT with BLOB → vec0 trigger fires →
    // knn_query finds the data on the SAME connection
    TempDB tmp{"blobvec0"};

    lattice::lattice_db db(lattice::configuration(tmp.str()));
    db.db().execute(
        "CREATE TABLE IF NOT EXISTS VectorDoc ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "globalId TEXT UNIQUE NOT NULL, "
        "label TEXT NOT NULL, "
        "embedding BLOB)");
    db.ensure_vec0_table("VectorDoc", "embedding", 5);

    // Simulate apply_remote_changes: disable sync, insert, re-enable
    std::vector<float> vec = {1.0f, 2.0f, 3.0f, 4.5f, -0.5f};
    auto blob = pack_floats(vec);
    std::string hex_str;
    for (auto byte : blob) {
        char buf[3];
        snprintf(buf, sizeof(buf), "%02X", byte);
        hex_str += buf;
    }

    db.db().execute("UPDATE _SyncControl SET disabled = 1 WHERE id = 1");
    db.db().execute(
        "INSERT INTO VectorDoc (globalId, label, embedding) "
        "VALUES ('sync-vec-1', 'synced doc', unhex(?))",
        {hex_str});
    db.db().execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");

    // vec0 trigger fires even with sync_disabled (it doesn't check sync_disabled)
    auto qvec = pack_floats(vec);
    auto results = db.knn_query("VectorDoc", "embedding", qvec, 1,
                                 lattice::lattice_db::distance_metric::l2);
    ASSERT_EQ(results.size(), 1u) << "vec0 data should be queryable after sync insert";
    EXPECT_EQ(results[0].global_id, "sync-vec-1");
    EXPECT_NEAR(results[0].distance, 0.0, 0.001);
}

TEST(Sync, BlobVec0CrossConnectionAfterRemoteApply) {
    // THE ACTUAL BUG: sync_db applies INSERT → main_db can't query vec0
    TempDB tmp{"blobvec0cross"};

    // sync_db applies the change
    lattice::lattice_db sync_db(lattice::configuration(tmp.str()));
    sync_db.db().execute(
        "CREATE TABLE IF NOT EXISTS VectorDoc ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "globalId TEXT UNIQUE NOT NULL, "
        "label TEXT NOT NULL, "
        "embedding BLOB)");
    sync_db.ensure_vec0_table("VectorDoc", "embedding", 5);

    std::vector<float> vec = {1.0f, 2.0f, 3.0f, 4.5f, -0.5f};
    auto blob = pack_floats(vec);
    std::string hex;
    for (auto b : blob) { char buf[3]; snprintf(buf, 3, "%02X", b); hex += buf; }

    sync_db.db().execute("UPDATE _SyncControl SET disabled = 1 WHERE id = 1");
    sync_db.db().execute(
        "INSERT INTO VectorDoc (globalId, label, embedding) "
        "VALUES ('cross-vec-1', 'cross doc', unhex(?))",
        {hex});
    sync_db.db().execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");

    // main_db opens a separate connection — can it query vec0?
    lattice::lattice_db main_db(lattice::configuration(tmp.str()));

    // The base table row should be visible
    auto rows = main_db.db().query("SELECT label FROM VectorDoc WHERE globalId = 'cross-vec-1'");
    ASSERT_EQ(rows.size(), 1u) << "Base table row should be visible cross-connection";
    EXPECT_EQ(std::get<std::string>(rows[0].at("label")), "cross doc");

    // knn_query with self-healing reconciliation
    auto qvec = pack_floats(vec);
    auto results = main_db.knn_query("VectorDoc", "embedding", qvec, 1,
                                      lattice::lattice_db::distance_metric::l2);
    EXPECT_EQ(results.size(), 1u)
        << "BUG: vec0 data from sync_db not queryable on main_db even after reconciliation";
    if (!results.empty()) {
        EXPECT_EQ(results[0].global_id, "cross-vec-1");
    }
}

// ----------------------------------------------------------------------------
// Deleted row skipped on catchup (DeletedRowSkippedOnCatchup bug)
// When INSERT + DELETE for the same globalId are in the AuditLog,
// applying both should result in no row.
// ----------------------------------------------------------------------------

TEST(Sync, DeletedRowCancelsOnApply) {
    TempDB tmp{"deletecatchup"};
    lattice::lattice_db db(lattice::configuration(tmp.str()));

    // Simulate catchup: source had INSERT then DELETE for the same object
    lattice::audit_log_entry insert_entry;
    insert_entry.global_id = "catchup-1";
    insert_entry.table_name = "TestPerson";
    insert_entry.operation = "INSERT";
    insert_entry.row_id = 1;
    insert_entry.global_row_id = "deleted-person-uuid";
    insert_entry.changed_fields = {
        {"name", lattice::any_property("will be deleted")},
        {"age", lattice::any_property(25)}
    };
    insert_entry.changed_fields_names = {"name", "age"};
    insert_entry.timestamp = "2024-01-01T00:00:01Z";

    lattice::audit_log_entry delete_entry;
    delete_entry.global_id = "catchup-2";
    delete_entry.table_name = "TestPerson";
    delete_entry.operation = "DELETE";
    delete_entry.row_id = 1;
    delete_entry.global_row_id = "deleted-person-uuid";
    delete_entry.timestamp = "2024-01-01T00:00:02Z";

    // Apply both in order — should result in no row
    lattice::apply_remote_changes(db, {insert_entry, delete_entry});

    // The deleted person should NOT exist
    auto persons = db.objects<TestPerson>();
    EXPECT_EQ(persons.size(), 0u)
        << "INSERT + DELETE should cancel out on catchup";

    // Now apply a second INSERT (the "alive" note)
    lattice::audit_log_entry alive_entry;
    alive_entry.global_id = "catchup-3";
    alive_entry.table_name = "TestPerson";
    alive_entry.operation = "INSERT";
    alive_entry.row_id = 2;
    alive_entry.global_row_id = "alive-person-uuid";
    alive_entry.changed_fields = {
        {"name", lattice::any_property("alive note")},
        {"age", lattice::any_property(30)}
    };
    alive_entry.changed_fields_names = {"name", "age"};
    alive_entry.timestamp = "2024-01-01T00:00:03Z";

    lattice::apply_remote_changes(db, {alive_entry});

    // Only the alive person should exist
    persons = db.objects<TestPerson>();
    EXPECT_EQ(persons.size(), 1u);
    if (!persons.empty()) {
        EXPECT_EQ(std::string(persons[0].name), "alive note");
    }
}

TEST(Sync, ChunkedProcessing) {
    // >50 entries should process in multiple chunks
    lattice::lattice_db db;
    db.db().execute("DELETE FROM AuditLog");

    // Add 60 objects to generate 60 audit entries
    db.write([&]() {
        for (int i = 0; i < 60; i++) {
            db.add(TestPerson{"Chunk" + std::to_string(i), i, std::nullopt});
        }
    });

    auto logs = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog");
    EXPECT_GE(std::get<int64_t>(logs[0].at("cnt")), 60);
}

TEST(Sync, DuplicateEntrySkipped) {
    TempDB tmp{"dupskip"};
    lattice::lattice_db db(lattice::configuration(tmp.str()));

    lattice::audit_log_entry entry;
    entry.global_id = "dup-1";
    entry.table_name = "TestPerson";
    entry.operation = "INSERT";
    entry.row_id = 1;
    entry.global_row_id = "dup-person-uuid";
    entry.changed_fields = {
        {"name", lattice::any_property("Duplicate")},
        {"age", lattice::any_property(25)}
    };
    entry.changed_fields_names = {"name", "age"};
    entry.timestamp = "2024-01-01T00:00:00Z";

    // Apply once
    lattice::apply_remote_changes(db, {entry});
    EXPECT_EQ(db.objects<TestPerson>().size(), 1u);

    // Apply again — should be skipped (same globalId)
    lattice::apply_remote_changes(db, {entry});
    EXPECT_EQ(db.objects<TestPerson>().size(), 1u);
}

TEST(Sync, ReplayAcknowledgesFirstUnionAndLinkEntries) {
    lattice::lattice_db source;
    lattice::lattice_db target;
    lattice::union_descriptor choice;
    choice.union_table_name = "_ReplayChoice";
    choice.cases = {{"text", {{"", lattice::column_type::text}}}};
    lattice::union_descriptor collision;
    collision.union_table_name = "_ReplayLinkNames";
    collision.cases = {{"lhs", {{"", lattice::column_type::text}}},
                       {"rhs", {{"", lattice::column_type::text}}},
                       {"rhs_type", {{"", lattice::column_type::text}}}};
    for (auto* db : {&source, &target}) {
        db->ensure_union_table(choice.union_table_name, choice, "TestPerson", "choice");
        db->ensure_union_table(collision.union_table_name, collision, "TestPerson", "collision");
    }
    source.ensure_link_table("_ReplayLinks");
    source.ensure_virtual_link_table("_ReplayVirtualLinks");
    source.db().execute("DELETE FROM AuditLog");
    source.db().execute("INSERT INTO _ReplayChoice(globalId,\"case\",text) VALUES "
                        "('choice-1','text','first'),('choice-2','text','second')");
    source.db().execute("INSERT INTO _ReplayLinkNames(globalId,\"case\",lhs) VALUES "
                        "('collision-1','lhs','first'),('collision-2','lhs','second')");
    source.db().execute("INSERT INTO _ReplayLinks(globalId,lhs,rhs) VALUES "
                        "('link-1','owner','child')");
    source.db().execute("INSERT INTO _ReplayVirtualLinks(globalId,lhs,rhs,rhs_type) VALUES "
                        "('virtual-1','owner','child','TestPerson')");
    ASSERT_FALSE(target.db().table_exists("_ReplayLinks"));
    ASSERT_FALSE(target.db().table_exists("_ReplayVirtualLinks"));

    // Replay real trigger-produced audit records through the wire decoder.
    const auto entries = lattice::query_audit_log(source.db());
    ASSERT_EQ(entries.size(), 6u);
    const auto wire = lattice::server_sent_event::make_audit_log(entries).to_json();
    const auto decoded = lattice::server_sent_event::from_json(wire);
    ASSERT_TRUE(decoded.has_value());
    std::vector<std::string> expected;
    for (const auto& entry : entries) expected.push_back(entry.global_id);
    EXPECT_EQ(lattice::apply_remote_changes(target, decoded->audit_logs), expected);
    const auto copied = lattice::query_audit_log(target.db());
    ASSERT_EQ(copied.size(), entries.size());
    for (size_t i = 0; i < entries.size(); ++i) EXPECT_EQ(copied[i].global_id, expected[i]);
    for (const auto* table : {"_ReplayChoice", "_ReplayLinkNames", "_ReplayLinks", "_ReplayVirtualLinks"}) {
        EXPECT_EQ(target.db().query(std::string("SELECT * FROM ") + table + " ORDER BY globalId"),
                  source.db().query(std::string("SELECT * FROM ") + table + " ORDER BY globalId"));
    }
    EXPECT_EQ(target.db().query("SELECT name FROM sqlite_master WHERE type='index' AND name=?",
                               {std::string("idx__ReplayLinks_rhs_lhs")}).size(), 1u);
    EXPECT_TRUE(target.db().query("SELECT name FROM sqlite_master WHERE type='index' AND name IN "
                                 "('idx__ReplayChoice_rhs_lhs','idx__ReplayLinkNames_rhs_lhs')").empty());

    // An existing virtual table must retain its kind even when an entry omits
    // rhs_type (DELETE does not need a full link payload).
    auto deletion = decoded->audit_logs.back();
    ASSERT_EQ(deletion.table_name, "_ReplayVirtualLinks");
    deletion.global_id = "virtual-delete-audit";
    deletion.operation = "DELETE";
    deletion.changed_fields.clear();
    deletion.changed_fields_names.clear();
    EXPECT_EQ(lattice::apply_remote_changes(target, {deletion}),
              std::vector<std::string>{deletion.global_id});
    EXPECT_TRUE(target.db().query("SELECT * FROM _ReplayVirtualLinks").empty());
    EXPECT_TRUE(target.db().query("SELECT name FROM sqlite_master WHERE type='index' AND name=?",
                                 {std::string("idx__ReplayVirtualLinks_rhs_lhs")}).empty());
}

TEST(Sync, FailedLinkEnsureIsNotMemoizedAsSuccess) {
    lattice::lattice_db db;
    db.db().execute("CREATE TABLE _ReplayEnsureRetry(lhs TEXT NOT NULL,rhs TEXT NOT NULL,"
                    "globalId TEXT UNIQUE,PRIMARY KEY(lhs,rhs))");
    // A conflicting schema object makes the required reverse-index DDL fail.
    db.db().execute("CREATE TABLE idx__ReplayEnsureRetry_rhs_lhs(value TEXT)");
    ASSERT_THROW(db.db().execute("CREATE INDEX IF NOT EXISTS idx__ReplayEnsureRetry_rhs_lhs "
                                 "ON _ReplayEnsureRetry(rhs,lhs)"), lattice::db_error);
    std::vector<lattice::audit_log_entry> entries;
    std::vector<std::string> expected;
    for (int i = 0; i < 2; ++i) {
        lattice::audit_log_entry entry;
        entry.global_id = "retry-audit-" + std::to_string(i);
        entry.global_row_id = "retry-row-" + std::to_string(i);
        entry.table_name = "_ReplayEnsureRetry";
        entry.operation = "INSERT";
        entry.timestamp = "2026-10-28T00:00:00Z";
        entry.changed_fields = {{"lhs", lattice::any_property("owner")},
                                {"rhs", lattice::any_property("child-" + std::to_string(i))}};
        entry.changed_fields_names = {"lhs", "rhs"};
        expected.push_back(entry.global_id);
        entries.push_back(std::move(entry));
    }
    EXPECT_TRUE(lattice::apply_remote_changes(db, entries).empty());
    EXPECT_TRUE(db.db().query("SELECT * FROM _ReplayEnsureRetry").empty());
    EXPECT_TRUE(lattice::query_audit_log(db.db()).empty());
    db.db().execute("DROP TABLE idx__ReplayEnsureRetry_rhs_lhs");
    EXPECT_EQ(lattice::apply_remote_changes(db, entries), expected);
    EXPECT_EQ(db.db().query("SELECT * FROM _ReplayEnsureRetry").size(), 2u);
    EXPECT_EQ(lattice::query_audit_log(db.db()).size(), 2u);
}

// ----------------------------------------------------------------------------
// Backoff: flapping endpoint must walk the exponential ladder
// ----------------------------------------------------------------------------

namespace {
/// Transport that opens successfully and then immediately errors — the
/// "flapping endpoint" shape that used to pin backoff at base_delay forever
/// (reconnect_attempts_ was reset on every open).
class flappy_mock_transport : public lattice::sync_transport {
public:
    void connect(const std::string&,
                 const std::map<std::string, std::string>&) override {
        {
            std::lock_guard<std::mutex> lock(m_);
            connect_times_.push_back(std::chrono::steady_clock::now());
        }
        state_ = lattice::transport_state::open;
        if (on_open_) on_open_();
        state_ = lattice::transport_state::closed;
        if (on_error_) on_error_("simulated flap");
    }
    void disconnect() override { state_ = lattice::transport_state::closed; }
    lattice::transport_state state() const override { return state_; }
    void send(const lattice::transport_message&) override {}
    void set_on_open(on_open_handler h) override { on_open_ = h; }
    void set_on_message(on_message_handler) override {}
    void set_on_error(on_error_handler h) override { on_error_ = h; }
    void set_on_close(on_close_handler) override {}

    size_t connect_count() {
        std::lock_guard<std::mutex> lock(m_);
        return connect_times_.size();
    }
    std::vector<std::chrono::steady_clock::time_point> connect_times() {
        std::lock_guard<std::mutex> lock(m_);
        return connect_times_;
    }
private:
    std::mutex m_;
    std::vector<std::chrono::steady_clock::time_point> connect_times_;
    lattice::transport_state state_ = lattice::transport_state::closed;
    on_open_handler on_open_;
    on_error_handler on_error_;
};
}  // namespace

TEST(Sync, BackoffNotResetOnFlappyOpen) {
    TempDB tmp{"flappybackoff"};
    lattice::configuration db_cfg(tmp.str(),
                                  std::make_shared<lattice::std_thread_scheduler>());
    auto db = std::make_unique<lattice::lattice_db>(db_cfg);

    lattice::sync_config cfg;
    cfg.sync_id = "flappy";
    cfg.all_active_sync_ids = {"flappy"};
    cfg.base_delay_seconds = 0.2;
    cfg.max_delay_seconds = 10.0;
    cfg.stable_connection_ms = 60'000;  // flaps are instant — never "stable"

    auto transport = std::make_unique<flappy_mock_transport>();
    auto* flappy = transport.get();
    auto sync = std::make_unique<lattice::synchronizer>(
        std::move(db), cfg, std::move(transport));

    sync->connect();

    // Wait for at least 4 connects (initial + 3 backoff reconnects:
    // 0.2s + 0.4s + 0.8s ≈ 1.4s plus scheduling slack).
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (flappy->connect_count() < 4 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    auto times = flappy->connect_times();
    ASSERT_GE(times.size(), 4u) << "expected ≥4 connect attempts, got " << times.size();

    // Inter-connect gaps must grow: with attempts never reset on open, gap i
    // is ~base * 2^i. Assert the 3rd gap comfortably exceeds the 1st — a
    // reset-on-open regression pins every gap at ~base and fails this.
    auto gap = [&](size_t i) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   times[i + 1] - times[i]).count();
    };
    EXPECT_GE(gap(2), gap(0) * 2)
        << "backoff did not escalate: gaps " << gap(0) << "ms, "
        << gap(1) << "ms, " << gap(2) << "ms";

    sync.reset();
}

// ----------------------------------------------------------------------------
// Progress counters: windowed catch-up keeps pending == in-flight, exactly
// ----------------------------------------------------------------------------

TEST(Sync, WindowedCounterSanity) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"windowcounters"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));

    lattice::sync_config cfg;
    cfg.websocket_url = "ws://localhost:8080/sync";
    cfg.authorization_token = "test-token";
    cfg.sync_id = "window-sync";
    cfg.all_active_sync_ids = {"window-sync"};
    cfg.chunk_size = 2;  // window = 2 * chunk_size = 4

    const int kEntries = 10;
    for (int i = 0; i < kEntries; ++i) {
        db->add(TestPerson{"win-" + std::to_string(i), i, std::nullopt});
    }

    lattice::synchronizer sync(std::move(db), cfg);
    auto* mock_ws = mock_factory->last_websocket();
    ASSERT_NE(mock_ws, nullptr);

    sync.connect();
    ASSERT_TRUE(sync.is_connected());

    // The catch-up ships in windows of ≤4. Repeatedly ACK whatever was sent;
    // at every step pending must equal the (windowed) in-flight count — the
    // pre-fix accounting counted the whole classified backlog per pass, so
    // pending ratcheted upward and never returned to 0.
    std::regex gid_re("\"globalId\"\\s*:\\s*\"([^\"]+)\"");
    std::set<std::string> acked_ids;
    int64_t max_pending_seen = 0;

    for (int round = 0; round < 20 && (int)acked_ids.size() < kEntries; ++round) {
        auto progress = sync.get_progress();
        max_pending_seen = std::max(max_pending_seen, progress.pending_upload);
        ASSERT_LE(progress.pending_upload, 4)
            << "pending exceeded the send window during catch-up";

        // Collect globalIds from every frame sent so far and ACK the new ones.
        std::vector<std::string> to_ack;
        for (const auto& msg : mock_ws->get_sent_messages()) {
            auto s = msg.as_string();
            for (std::sregex_iterator it(s.begin(), s.end(), gid_re), end;
                 it != end; ++it) {
                auto id = (*it)[1].str();
                if (acked_ids.insert(id).second) to_ack.push_back(id);
            }
        }
        if (to_ack.empty()) break;
        mock_ws->simulate_message(lattice::transport_message::from_string(
            lattice::server_sent_event::make_ack(to_ack).to_json()));
    }

    auto final_progress = sync.get_progress();
    EXPECT_EQ((int)acked_ids.size(), kEntries) << "not every entry was sent+ACKed";
    EXPECT_EQ(final_progress.pending_upload, 0) << "pending must return to 0";
    EXPECT_EQ(final_progress.acked, kEntries);
    EXPECT_EQ(final_progress.total_upload, kEntries)
        << "total must count actually-sent entries exactly (no backlog re-count)";
    EXPECT_LE(max_pending_seen, 4);

    sync.disconnect();
    // A second pass finds nothing new to send.
    auto sent_before = mock_ws->get_sent_messages().size();
    sync.connect();
    sync.sync_now();
    // (Frames may include non-audit control messages; assert no audit re-send
    // by checking no NEW globalIds appear.)
    std::set<std::string> post_ids;
    for (const auto& msg : mock_ws->get_sent_messages()) {
        auto s = msg.as_string();
        for (std::sregex_iterator it(s.begin(), s.end(), gid_re), end;
             it != end; ++it) {
            post_ids.insert((*it)[1].str());
        }
    }
    EXPECT_EQ(post_ids.size(), acked_ids.size())
        << "reconnect re-sent already-ACKed entries";
    (void)sent_before;
    sync.disconnect();
}

// ----------------------------------------------------------------------------
// Throttle: a write burst coalesces into leading-edge + one trailing tick
// ----------------------------------------------------------------------------

TEST(Sync, ThrottleCoalescing) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"throttle"};
    auto owned_db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    // The synchronizer takes ownership; the raw pointer stays valid for the
    // test's lifetime and lets writes hit the AuditLog observer synchronously
    // (no cross-process notification fuzz).
    auto* db_ptr = owned_db.get();

    lattice::sync_config cfg;
    cfg.websocket_url = "ws://localhost:8080/sync";
    cfg.authorization_token = "test-token";
    cfg.sync_id = "throttle-sync";
    cfg.all_active_sync_ids = {"throttle-sync"};
    cfg.upload_coalesce_ms = 200;

    lattice::synchronizer sync(std::move(owned_db), cfg);
    auto* mock_ws = mock_factory->last_websocket();
    ASSERT_NE(mock_ws, nullptr);
    sync.connect();
    ASSERT_TRUE(sync.is_connected());

    auto audit_frames = [&] {
        size_t n = 0;
        for (const auto& msg : mock_ws->get_sent_messages()) {
            if (msg.as_string().find("\"auditLog\"") != std::string::npos) n++;
        }
        return n;
    };
    ASSERT_EQ(audit_frames(), 0u) << "empty DB must send nothing on connect";

    // First write after idle: leading edge — must ship without waiting for
    // the window.
    db_ptr->add(TestPerson{"burst-0", 0, std::nullopt});
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
        while (audit_frames() < 1 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    EXPECT_EQ(audit_frames(), 1u) << "leading edge did not fire immediately";

    // Burst inside the window: all coalesce into ONE trailing-edge tick.
    for (int i = 1; i < 10; ++i) {
        db_ptr->add(TestPerson{"burst-" + std::to_string(i), i, std::nullopt});
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));  // window 200 + slack

    const auto frames = audit_frames();
    EXPECT_GE(frames, 2u) << "trailing-edge tick never fired — burst entries stranded";
    EXPECT_LE(frames, 3u) << "burst was not coalesced (" << frames << " audit frames for 10 writes; "
                             "pre-fix behavior sent one pass per write)";

    // Every entry must have shipped despite the coalescing.
    std::regex gid_re("\"globalId\"\\s*:\\s*\"([^\"]+)\"");
    std::set<std::string> ids;
    for (const auto& msg : mock_ws->get_sent_messages()) {
        auto s = msg.as_string();
        for (std::sregex_iterator it(s.begin(), s.end(), gid_re), end; it != end; ++it) {
            ids.insert((*it)[1].str());
        }
    }
    EXPECT_EQ(ids.size(), 10u) << "coalescing dropped entries";

    sync.disconnect();
}

// ----------------------------------------------------------------------------
// WAL checkpoint: primitive fail-fast semantics + pacer-driven truncation
// ----------------------------------------------------------------------------

TEST(Sync, CheckpointPrimitiveUnderLoad) {
    TempDB tmp{"ckptprim"};
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    const auto wal_path = tmp.str() + "-wal";

    for (int i = 0; i < 200; ++i) {
        db.add(TestPerson{"wal-" + std::to_string(i), i, std::nullopt});
    }
    ASSERT_TRUE(std::filesystem::exists(wal_path));
    ASSERT_GT(std::filesystem::file_size(wal_path), 0u);

    // No readers: TRUNCATE must fully reset the WAL.
    auto res = db.db().wal_checkpoint(true, 250);
    EXPECT_EQ(res.busy, 0);
    EXPECT_EQ(std::filesystem::file_size(wal_path), 0u);

    // Grow again, then pin the WAL with a held read transaction on a second
    // connection — TRUNCATE must FAIL FAST (bounded by the busy budget),
    // not stall the writer indefinitely.
    for (int i = 0; i < 50; ++i) {
        db.add(TestPerson{"wal2-" + std::to_string(i), i, std::nullopt});
    }
    lattice::lattice_db reader{lattice::configuration(tmp.str())};
    reader.db().execute("BEGIN");
    (void)reader.db().query("SELECT COUNT(*) FROM TestPerson");

    const auto t0 = std::chrono::steady_clock::now();
    auto busy_res = db.db().wal_checkpoint(true, 250);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();
    EXPECT_NE(busy_res.busy, 0) << "checkpoint should report busy with a pinned reader";
    EXPECT_LT(elapsed, 1500) << "TRUNCATE must fail fast, not stall (took " << elapsed << "ms)";

    // PASSIVE never blocks and still makes backfill progress.
    auto passive = db.db().wal_checkpoint(false);
    EXPECT_EQ(passive.rc, 0);

    reader.db().execute("COMMIT");
    auto after = db.db().wal_checkpoint(true, 250);
    EXPECT_EQ(after.busy, 0);
    EXPECT_EQ(std::filesystem::file_size(wal_path), 0u);
}

TEST(Sync, PacerDrivenTruncateWhenIdle) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"ckptpacer"};
    auto owned_db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    auto* db_ptr = owned_db.get();
    const auto sync_wal = tmp.str() + "-wal";

    lattice::sync_config cfg;
    cfg.websocket_url = "ws://localhost:8080/sync";
    cfg.authorization_token = "test-token";
    cfg.sync_id = "ckpt-sync";
    cfg.all_active_sync_ids = {"ckpt-sync"};
    cfg.upload_coalesce_ms = 50;
    cfg.checkpoint_passive_interval_ms = 100;
    cfg.checkpoint_truncate_interval_ms = 150;

    lattice::synchronizer sync(std::move(owned_db), cfg);
    auto* mock_ws = mock_factory->last_websocket();
    ASSERT_NE(mock_ws, nullptr);
    sync.connect();

    for (int i = 0; i < 100; ++i) {
        db_ptr->add(TestPerson{"pcw-" + std::to_string(i), i, std::nullopt});
    }
    // ACK until ALL 100 entries are confirmed — the burst ships in a leading
    // tick + a trailing tick ~coalesce_ms later, so an early pending==0 (after
    // ACKing just the leading entry) does NOT mean the backlog is done.
    //
    // Model a REAL server: ACK every delivery, including at-least-once
    // DUPLICATES. A trailing coalesced tick can enumerate entries just before
    // the floor advance from a prior ACK commits and re-send them; if the
    // duplicate is never re-ACKed it stays in_flight forever and pending
    // never drains (observed on the slower Linux container, where the tick
    // interleaves the ACK round-trip that macOS timing never splits).
    std::regex gid_re("\"globalId\"\\s*:\\s*\"([^\"]+)\"");
    std::set<std::string> acked;
    size_t seen_msgs = 0;
    auto ack_new_messages = [&]() {
        auto msgs = mock_ws->get_sent_messages();
        std::vector<std::string> to_ack;
        for (size_t m = seen_msgs; m < msgs.size(); ++m) {
            auto s = msgs[m].as_string();
            for (std::sregex_iterator it(s.begin(), s.end(), gid_re), end; it != end; ++it) {
                auto id = (*it)[1].str();
                acked.insert(id);
                to_ack.push_back(id);  // duplicates included, like a real server
            }
        }
        seen_msgs = msgs.size();
        if (!to_ack.empty()) {
            mock_ws->simulate_message(lattice::transport_message::from_string(
                lattice::server_sent_event::make_ack(to_ack).to_json()));
        }
        return !to_ack.empty();
    };
    for (int round = 0; round < 60 && (int)acked.size() < 100; ++round) {
        if (!ack_new_messages()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    ASSERT_EQ((int)acked.size(), 100) << "not all burst entries were sent";
    // Pending drains to 0 once the last ACK lands — keep ACKing late
    // redeliveries while we wait, as a live server would.
    {
        const auto ack_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (sync.get_progress().pending_upload != 0 &&
               std::chrono::steady_clock::now() < ack_deadline) {
            ack_new_messages();
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }
    ASSERT_EQ(sync.get_progress().pending_upload, 0);

    // Within a few heartbeat cycles the idle-gated TRUNCATE must land and
    // reset the -wal (there are no other connections pinning it).
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    uintmax_t wal_size = std::filesystem::exists(sync_wal)
        ? std::filesystem::file_size(sync_wal) : 0;
    while (wal_size > 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        wal_size = std::filesystem::exists(sync_wal)
            ? std::filesystem::file_size(sync_wal) : 0;
    }
    if (wal_size > 0) {
        // Diagnostic split: if a direct TRUNCATE succeeds, the pacer path is
        // broken; if it reports busy, a connection is pinning the WAL.
        auto direct = db_ptr->db().wal_checkpoint(true, 250);
        std::cerr << "[diag] direct TRUNCATE: rc=" << direct.rc
                  << " busy=" << direct.busy
                  << " log=" << direct.log_frames
                  << " ckpt=" << direct.checkpointed
                  << " wal_now=" << (std::filesystem::exists(sync_wal)
                                         ? std::filesystem::file_size(sync_wal) : 0)
                  << std::endl;
    }
    EXPECT_EQ(wal_size, 0u) << "pacer never truncated the WAL while idle";

    sync.disconnect();
}

// Results spec §3.2 second enforcement caller (item-A adversarial finding 3):
// the sync pacer runs read-pool maintenance at its maybe_checkpoint cadence,
// aggregated per PATH — the keepers live on the APP handles, not on the
// synchronizer's own lattice_db instance, so a local-only call would police
// nothing. Pinned minimally: an idle keeper on a same-path app handle (tiny
// TTL) must be TTL-retired by the pacer alone — no manual
// run_read_pool_maintenance call anywhere.
TEST(Sync, PacerRunsReadPoolMaintenancePerPath) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"pacermaint"};
    // App-side handle: owns the keeper the pacer must police.
    lattice::lattice_db app{lattice::configuration(tmp.str())};
    app.add(TestPerson{"x", 1, std::nullopt});
    app.set_read_generation_ttl_ms(50);

    auto gen = app.acquire_read_generation();
    ASSERT_NE(gen, 0u);
    (void)app.query_at_generation(gen, "SELECT 1 AS one");
    ASSERT_EQ(app.local_read_generations_outstanding(), 1u);

    // Synchronizer topology: its OWN lattice_db on the same path; the pacer
    // heartbeat doubles as the maintenance cadence. No connect() needed —
    // the pacer starts with the synchronizer.
    lattice::sync_config cfg;
    cfg.websocket_url = "ws://localhost:8080/sync";
    cfg.authorization_token = "test-token";
    cfg.sync_id = "maint-sync";
    cfg.all_active_sync_ids = {"maint-sync"};
    cfg.upload_coalesce_ms = 50;
    cfg.checkpoint_passive_interval_ms = 100;
    lattice::synchronizer sync(
        std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())), cfg);

    // Within a few heartbeats the pacer's per-path maintenance must
    // TTL-retire the APP instance's idle generation.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (app.local_read_generations_outstanding() != 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_EQ(app.local_read_generations_outstanding(), 0u)
        << "the pacer never ran per-path read-pool maintenance";
    EXPECT_FALSE(app.query_at_generation(gen, "SELECT 1 AS one").has_value())
        << "the TTL-retired generation must refuse reads";
    app.release_read_generation(gen);  // late release: no-op

    sync.disconnect();
}

// ----------------------------------------------------------------------------
// Upload floor: incremental cursor — bounded scans, at-least-once preserved
// ----------------------------------------------------------------------------

namespace {
int64_t read_floor(lattice::database& db, const std::string& sync_id) {
    auto rows = db.query(
        "SELECT upload_floor FROM _lattice_replication_slots WHERE sync_id = ?",
        {sync_id});
    if (rows.empty()) return -1;
    return std::get<int64_t>(rows[0].at("upload_floor"));
}

std::set<std::string> collect_and_ack_all(lattice::mock_sync_transport* ws,
                                          lattice::synchronizer& sync,
                                          int expected,
                                          int max_rounds = 60) {
    std::regex gid_re("\"globalId\"\\s*:\\s*\"([^\"]+)\"");
    std::set<std::string> acked;
    for (int round = 0; round < max_rounds && (int)acked.size() < expected; ++round) {
        std::vector<std::string> to_ack;
        for (const auto& msg : ws->get_sent_messages()) {
            auto s = msg.as_string();
            for (std::sregex_iterator it(s.begin(), s.end(), gid_re), end; it != end; ++it) {
                auto id = (*it)[1].str();
                if (acked.insert(id).second) to_ack.push_back(id);
            }
        }
        if (!to_ack.empty()) {
            ws->simulate_message(lattice::transport_message::from_string(
                lattice::server_sent_event::make_ack(to_ack).to_json()));
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
    }
    return acked;
}
}  // namespace

TEST(Sync, CursorAdvancesOnAck) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"cursorack"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    auto* db_ptr = db.get();

    lattice::sync_config cfg;
    cfg.websocket_url = "ws://localhost:8080/sync";
    cfg.authorization_token = "test-token";
    cfg.sync_id = "cursor-sync";
    cfg.all_active_sync_ids = {"cursor-sync"};

    const int kEntries = 25;
    for (int i = 0; i < kEntries; ++i) {
        db_ptr->add(TestPerson{"cur-" + std::to_string(i), i, std::nullopt});
    }
    auto max_id_rows = db_ptr->db().query("SELECT MAX(id) AS m FROM AuditLog");
    const int64_t max_audit_id = std::get<int64_t>(max_id_rows[0].at("m"));

    lattice::synchronizer sync(std::move(db), cfg);
    auto* mock_ws = mock_factory->last_websocket();
    ASSERT_NE(mock_ws, nullptr);
    sync.connect();

    auto acked = collect_and_ack_all(mock_ws, sync, kEntries);
    ASSERT_EQ((int)acked.size(), kEntries);

    // Floor reaches the highest enumerated audit id once everything resolves.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    int64_t floor = -1;
    while (std::chrono::steady_clock::now() < deadline) {
        floor = read_floor(db_ptr->db(), "cursor-sync");
        if (floor >= max_audit_id) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    EXPECT_GE(floor, max_audit_id) << "floor did not advance after full ACK";

    // A fresh pass finds nothing (bounded query returns empty; no re-sends).
    auto frames_before = mock_ws->get_sent_messages().size();
    sync.sync_now();
    EXPECT_EQ(mock_ws->get_sent_messages().size(), frames_before)
        << "fully-ACKed backlog was re-sent";
    sync.disconnect();
}

TEST(Sync, CursorHoldsBelowUnackedAndResendsAfterReconnect) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"cursorhold"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    auto* db_ptr = db.get();

    lattice::sync_config cfg;
    cfg.websocket_url = "ws://localhost:8080/sync";
    cfg.authorization_token = "test-token";
    cfg.sync_id = "hold-sync";
    cfg.all_active_sync_ids = {"hold-sync"};
    cfg.chunk_size = 3;  // window = 6 < backlog

    const int kEntries = 20;
    for (int i = 0; i < kEntries; ++i) {
        db_ptr->add(TestPerson{"hold-" + std::to_string(i), i, std::nullopt});
    }

    lattice::synchronizer sync(std::move(db), cfg);
    auto* mock_ws = mock_factory->last_websocket();
    ASSERT_NE(mock_ws, nullptr);
    sync.connect();

    // ACK exactly the FIRST window, nothing more: floor must stay below the
    // first unACKed id.
    std::regex gid_re("\"globalId\"\\s*:\\s*\"([^\"]+)\"");
    std::vector<std::string> first_window;
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (first_window.empty() && std::chrono::steady_clock::now() < deadline) {
            for (const auto& msg : mock_ws->get_sent_messages()) {
                auto s = msg.as_string();
                for (std::sregex_iterator it(s.begin(), s.end(), gid_re), end; it != end; ++it) {
                    first_window.push_back((*it)[1].str());
                }
            }
            if (first_window.empty()) std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }
    ASSERT_FALSE(first_window.empty());
    ASSERT_LE(first_window.size(), 6u) << "send window exceeded 2*chunk_size";
    mock_ws->simulate_message(lattice::transport_message::from_string(
        lattice::server_sent_event::make_ack(first_window).to_json()));

    // Everything eventually ships window by window (post-ACK kicks) — but at
    // this instant, unACKed entries exist and the floor must sit below them.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto pending_rows = db_ptr->db().query(
        "SELECT MIN(a.id) AS m FROM AuditLog a WHERE a.isSynchronized = 0"
        " AND NOT EXISTS (SELECT 1 FROM _lattice_sync_state ss"
        "  WHERE ss.audit_entry_id = a.id AND ss.sync_id = 'hold-sync' AND ss.is_synchronized = 1)");
    if (!pending_rows.empty() &&
        std::holds_alternative<int64_t>(pending_rows[0].at("m"))) {
        const int64_t min_pending = std::get<int64_t>(pending_rows[0].at("m"));
        const int64_t floor = read_floor(db_ptr->db(), "hold-sync");
        EXPECT_LT(floor, min_pending)
            << "floor advanced past a pending (unACKed) entry — data loss";
    }

    // Sever the transport mid-backlog, reconnect: the remaining entries are
    // re-queried (bounded) and re-sent — none skipped.
    mock_ws->simulate_error("simulated drop");
    sync.connect();
    auto acked = collect_and_ack_all(mock_ws, sync, kEntries);
    EXPECT_EQ((int)acked.size(), kEntries)
        << "entries were lost across the reconnect (floor skipped them)";
    sync.disconnect();
}

TEST(Sync, CursorCrashRecoveryStaleLowFloor) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"cursorcrash"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    auto* db_ptr = db.get();

    lattice::sync_config cfg;
    cfg.websocket_url = "ws://localhost:8080/sync";
    cfg.authorization_token = "test-token";
    cfg.sync_id = "crash-sync";
    cfg.all_active_sync_ids = {"crash-sync"};

    for (int i = 0; i < 10; ++i) {
        db_ptr->add(TestPerson{"cr-" + std::to_string(i), i, std::nullopt});
    }
    lattice::synchronizer sync(std::move(db), cfg);
    auto* mock_ws = mock_factory->last_websocket();
    sync.connect();
    auto acked = collect_and_ack_all(mock_ws, sync, 10);
    ASSERT_EQ((int)acked.size(), 10);

    // Simulate a crash BEFORE the floor persisted: force it stale-low.
    db_ptr->db().execute(
        "UPDATE _lattice_replication_slots SET upload_floor = 0 WHERE sync_id = 'crash-sync'");

    // Reconnect: worst case is a re-scan; the sync_state conditions filter
    // everything back out — nothing is re-sent, and the floor re-advances.
    mock_ws->simulate_error("crash");
    auto frames_before = mock_ws->get_sent_messages().size();
    sync.connect();
    sync.sync_now();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    std::regex gid_re("\"globalId\"\\s*:\\s*\"([^\"]+)\"");
    std::set<std::string> post;
    for (size_t i = frames_before; i < mock_ws->get_sent_messages().size(); ++i) {
        auto s = mock_ws->get_sent_messages()[i].as_string();
        for (std::sregex_iterator it(s.begin(), s.end(), gid_re), end; it != end; ++it) {
            post.insert((*it)[1].str());
        }
    }
    EXPECT_TRUE(post.empty()) << "stale-low floor caused re-sends of synced entries";
    sync.disconnect();
}

TEST(Sync, CursorBoundedQueryUnderStall) {
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    TempDB tmp{"cursorstall"};
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    auto* db_ptr = db.get();

    lattice::sync_config cfg;
    cfg.websocket_url = "ws://localhost:8080/sync";
    cfg.authorization_token = "test-token";
    cfg.sync_id = "stall-sync";
    cfg.all_active_sync_ids = {"stall-sync"};
    cfg.chunk_size = 2;  // window = 4

    const int kBacklog = 50;
    for (int i = 0; i < kBacklog; ++i) {
        db_ptr->add(TestPerson{"st-" + std::to_string(i), i, std::nullopt});
    }
    lattice::synchronizer sync(std::move(db), cfg);
    auto* mock_ws = mock_factory->last_websocket();
    sync.connect();  // server never ACKs — stalled

    // Multiple passes against the stalled server: every audit frame must be
    // window-bounded (the pre-floor behavior re-sent/re-parsed the whole
    // backlog per pass).
    std::regex entries_re("\"globalId\"");
    for (int i = 0; i < 3; ++i) {
        sync.sync_now();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    for (const auto& msg : mock_ws->get_sent_messages()) {
        auto s = msg.as_string();
        if (s.find("\"auditLog\"") == std::string::npos) continue;
        size_t n = 0;
        for (std::sregex_iterator it(s.begin(), s.end(), entries_re), end; it != end; ++it) n++;
        // Each entry serializes exactly one top-level globalId; chunks are
        // ≤ chunk_size entries. Generous bound: ≤ 3x chunk to absorb nested
        // globalId-like fields in changedFields payloads.
        EXPECT_LE(n, cfg.chunk_size * 3)
            << "a frame carried far more than one chunk — unbounded re-send";
    }
    // pending stays window-bounded, never inflates toward the backlog size.
    EXPECT_LE(sync.get_progress().pending_upload, (int64_t)(cfg.chunk_size * 2));
    sync.disconnect();
}

TEST(Sync, ExistingDbGainsUploadFloor) {
    TempDB tmp{"floormigrate"};
    // Create a DB and fingerprint it, then verify a reopen exposes the new
    // column (the epoch bump forces the slow path through the guarded ALTER).
    {
        lattice::lattice_db db{lattice::configuration(tmp.str())};
        db.add(TestPerson{"mig", 1, std::nullopt});
    }
    {
        lattice::lattice_db db{lattice::configuration(tmp.str())};
        // Must not throw, and the column must exist with default 0.
        db.db().execute(R"(
            INSERT INTO _lattice_replication_slots (sync_id, last_active_at)
            VALUES ('mig-sync', datetime('now'))
            ON CONFLICT(sync_id) DO UPDATE SET last_active_at = datetime('now')
        )");
        auto rows = db.db().query(
            "SELECT upload_floor FROM _lattice_replication_slots WHERE sync_id = 'mig-sync'");
        ASSERT_EQ(rows.size(), 1u);
        EXPECT_EQ(std::get<int64_t>(rows[0].at("upload_floor")), 0);
    }
}

// ----------------------------------------------------------------------------
// flush_changes change→audit lookup must be indexed (epoch 3)
// ----------------------------------------------------------------------------
// Without idx_audit_log_change_lookup, every pass-2 lookup in flush_changes
// reverse-SCANs the whole AuditLog. Observed live: per-sync-mode applies
// write one _lattice_sync_state row per entry, and each write's futile
// lookup scanned a 187k-row AuditLog — minutes of CPU per 1000-entry batch,
// starving the IPC ACK path into a permanent resend storm. (The bookkeeping
// tables are additionally skipped in pass 2 now; this pins the index for
// the model-table lookups that remain.)
TEST(Sync, AuditChangeLookupIndexed) {
    TempDB tmp{"idxcheck"};
    lattice::lattice_db db(lattice::configuration(tmp.str()));

    auto count = db.db().query(
        "SELECT COUNT(*) AS c FROM sqlite_master "
        "WHERE type='index' AND name='idx_audit_log_change_lookup'");
    ASSERT_EQ(count.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(count[0].at("c")), 1);

    auto plan = db.db().query(
        "EXPLAIN QUERY PLAN SELECT id, globalId FROM AuditLog "
        "WHERE tableName = 'X' AND rowId = 1 AND operation = 'INSERT' "
        "ORDER BY id DESC LIMIT 1");
    ASSERT_FALSE(plan.empty());
    bool uses_index = false;
    for (const auto& row : plan) {
        auto it = row.find("detail");
        if (it == row.end() || !std::holds_alternative<std::string>(it->second)) continue;
        const auto& d = std::get<std::string>(it->second);
        EXPECT_EQ(d.find("SCAN AuditLog"), std::string::npos)
            << "pass-2 lookup shape still scans: " << d;
        if (d.find("idx_audit_log_change_lookup") != std::string::npos) uses_index = true;
    }
    EXPECT_TRUE(uses_index) << "lookup does not use idx_audit_log_change_lookup";
}

// ----------------------------------------------------------------------------
// Destroy-from-callback: no self-spin, no self-join
// ----------------------------------------------------------------------------
// An observer callback that releases the LAST reference to its lattice runs
// ~lattice_db ON the notify thread while for_each_alive still holds the
// guard's notify_refcount — the drain wait must exclude the current thread's
// own holds or it waits for itself forever (observed live: 97%-CPU yield
// spin, 184 CPU-minutes, intermittent full-suite hangs). The scheduler
// shutdown reached from its own worker must detach, not self-join.
TEST(Sync, DestroyFromObserverCallbackDoesNotHang) {
    TempDB tmp{"cbdestroy"};
    auto a = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
    lattice::lattice_db b{lattice::configuration(tmp.str())};  // same path

    std::atomic<bool> fired{false};
    a->add_table_observer("AuditLog",
        [&a, &fired](const std::vector<lattice::lattice_db::change_event>&) {
            if (a) a.reset();  // destroy A from inside its own notification
            fired.store(true, std::memory_order_release);
        });

    b.add(TestPerson{"boom", 1, std::nullopt});  // cross-instance notify → A

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!fired.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(fired.load()) << "observer never fired";
    EXPECT_EQ(a, nullptr) << "callback did not destroy the instance";
}

// 1.0 item I1: configuration::sync_tuning overlays only its SET fields onto a
// sync_config; unset fields keep sync.hpp's defaults (forward-only — the
// tuning struct never re-states a default).
TEST(SyncTuningTest, ApplyOverlaysOnlySetFields) {
    lattice::sync_config defaults;  // capture library defaults for comparison
    lattice::sync_config cfg;
    lattice::configuration::sync_tuning tuning;

    tuning.chunk_size = 1;
    tuning.upload_coalesce_ms = 750;
    tuning.use_upload_floor = false;
    tuning.apply(cfg);

    EXPECT_EQ(cfg.chunk_size, 1u);
    EXPECT_EQ(cfg.upload_coalesce_ms, 750);
    EXPECT_FALSE(cfg.use_upload_floor);
    // Unset knobs must remain exactly the library defaults.
    EXPECT_EQ(cfg.max_reconnect_attempts, defaults.max_reconnect_attempts);
    EXPECT_EQ(cfg.base_delay_seconds, defaults.base_delay_seconds);
    EXPECT_EQ(cfg.max_delay_seconds, defaults.max_delay_seconds);
    EXPECT_EQ(cfg.stable_connection_ms, defaults.stable_connection_ms);
    EXPECT_EQ(cfg.checkpoint_passive_interval_ms, defaults.checkpoint_passive_interval_ms);
    EXPECT_EQ(cfg.checkpoint_truncate_interval_ms, defaults.checkpoint_truncate_interval_ms);

    // Empty tuning is a strict no-op.
    lattice::sync_config untouched;
    lattice::configuration::sync_tuning{}.apply(untouched);
    EXPECT_EQ(untouched.chunk_size, defaults.chunk_size);
    EXPECT_EQ(untouched.upload_coalesce_ms, defaults.upload_coalesce_ms);
    EXPECT_EQ(untouched.use_upload_floor, defaults.use_upload_floor);
}

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <exception>
#include <stdexcept>
namespace {
struct sync_operation_gate {
    std::mutex mutex;
    std::condition_variable cv;
    bool signalled = false;
    void signal() {
        { std::lock_guard<std::mutex> lock(mutex); signalled = true; }
        cv.notify_all();
    }
    bool wait() {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::seconds(5), [&] { return signalled; });
    }
};

// Real synchronizer transport boundary. Only the first send is held; later
// sends ACK synchronously to exercise same-thread callback reentry as well.
struct held_sync_transport final : lattice::mock_sync_transport {
    sync_operation_gate entered, release;
    std::atomic<bool> first{true};
    std::atomic<int> active_sends{0};
    std::atomic<bool> overlapped{false};
    void send(const lattice::transport_message& message) override {
        struct Active {
            held_sync_transport& owner;
            explicit Active(held_sync_transport& value) : owner(value) {
                if (owner.active_sends.fetch_add(1) != 0) owner.overlapped.store(true);
            }
            ~Active() { owner.active_sends.fetch_sub(1); }
        } active{*this};
        lattice::mock_sync_transport::send(message);
        auto event = lattice::server_sent_event::from_json(message.as_string());
        if (!event || event->event_type != lattice::server_sent_event::type::audit_log ||
            event->audit_logs.empty()) throw std::runtime_error("expected nonempty audit frame");
        if (first.exchange(false)) {
            entered.signal();
            if (!release.wait()) throw std::runtime_error("initial send gate timed out");
            // The second caller submitted this frame's exact ACK while held.
            return;
        }
        std::vector<std::string> ids;
        for (const auto& entry : event->audit_logs) ids.push_back(entry.global_id);
        simulate_message(lattice::transport_message::from_string(
            lattice::server_sent_event::make_ack(ids).to_json()));
    }
};
}

TEST(Sync, ImmediateQueueSerializesFilterAndACKDuringUpload) {
    bounded_mock_case([] {
        auto require = [](bool condition, const char* message) {
            if (!condition) throw std::runtime_error(message);
        };
        TempDB tmp{"sync_operation_queue"};
        lattice::configuration db_config(tmp.str());
        db_config.audit_retention_seconds = 0;
        auto owned = std::make_unique<lattice::lattice_db>(db_config);
        auto* db = owned.get();
        for (int age = 1; age <= 3; ++age)
            db->add(TestPerson{"queue-" + std::to_string(age), age, std::nullopt});
        auto seeds = db->db().query(
            "SELECT a.globalId AS audit_gid, a.globalRowId AS row_gid, p.age AS age "
            "FROM AuditLog a JOIN TestPerson p ON p.globalId=a.globalRowId "
            "WHERE a.tableName='TestPerson' AND a.operation='INSERT' ORDER BY p.age");
        require(seeds.size() == 3, "exact three original INSERT audits required");
        std::vector<std::string> audit_ids, row_ids;
        for (size_t i = 0; i < seeds.size(); ++i) {
            require(std::get<int64_t>(seeds[i].at("age")) == static_cast<int64_t>(i + 1),
                    "seed age identity mismatch");
            audit_ids.push_back(std::get<std::string>(seeds[i].at("audit_gid")));
            row_ids.push_back(std::get<std::string>(seeds[i].at("row_gid")));
        }
        lattice::sync_config config;
        config.sync_id = "controlled-operation-queue";
        config.all_active_sync_ids = {config.sync_id};
        config.upload_coalesce_ms = 0;
        config.checkpoint_passive_interval_ms = 0;
        config.checkpoint_truncate_interval_ms = 0;
        config.narrowing_emits_removals = false;
        config.sync_filter = std::vector<lattice::sync_filter_entry>{{"TestPerson", "age = 1"}};
        auto transport = std::make_unique<held_sync_transport>();
        auto* wire = transport.get();
        std::mutex ack_mutex;
        std::vector<std::string> acked;
        std::atomic<bool> ack_inside_send{false};
        lattice::synchronizer sync(std::move(owned), config, std::move(transport));
        sync.set_on_sync_complete([&](const std::vector<std::string>& ids) {
            if (wire->active_sends.load() != 0) ack_inside_send.store(true);
            std::lock_guard<std::mutex> lock(ack_mutex);
            acked.insert(acked.end(), ids.begin(), ids.end());
        });
        std::exception_ptr opening_error, caller_error;
        std::thread opening([&] {
            try { sync.connect(); } catch (...) { opening_error = std::current_exception(); }
        });
        const bool entered = wire->entered.wait();
        sync_operation_gate submitted;
        std::thread caller;
        bool caller_returned = false;
        if (entered) {
            caller = std::thread([&] {
                try {
                    // Both arrive while the same real upload is held. Only the
                    // latest filter may reconcile; neither may run concurrently.
                    sync.update_sync_filter({{"TestPerson", "age = 2"}});
                    sync.update_sync_filter({{"TestPerson", "age = 3"}});
                    wire->simulate_message(lattice::transport_message::from_string(
                        lattice::server_sent_event::make_ack({audit_ids[0]}).to_json()));
                    sync.sync_now();
                } catch (...) { caller_error = std::current_exception(); }
                submitted.signal();
            });
            caller_returned = submitted.wait();
        }
        const auto held_messages = wire->get_sent_messages();
        bool no_ack_while_held;
        { std::lock_guard<std::mutex> lock(ack_mutex); no_ack_while_held = acked.empty(); }
        wire->release.signal(); // Always release before joining or throwing.
        if (caller.joinable()) caller.join();
        opening.join(); // Idle inline drain includes all queued filter/ACK work.
        if (opening_error) std::rethrow_exception(opening_error);
        if (caller_error) std::rethrow_exception(caller_error);
        require(entered && caller_returned, "second caller must return while first send remains held");
        require(held_messages.size() == 1 && no_ack_while_held,
                "filter/ACK work executed before the held upload returned");
        require(!wire->overlapped.load() && !ack_inside_send.load(),
                "real sends or ACK completion overlapped/reentered a send");

        auto sent_entries = [&] {
            std::vector<lattice::audit_log_entry> result;
            for (const auto& message : wire->get_sent_messages()) {
                auto event = lattice::server_sent_event::from_json(message.as_string());
                require(event && event->event_type == lattice::server_sent_event::type::audit_log,
                        "unexpected transport frame");
                result.insert(result.end(), event->audit_logs.begin(), event->audit_logs.end());
            }
            return result;
        };
        auto sent = sent_entries();
        require(sent.size() == 2 && sent[0].global_id == audit_ids[0] &&
                sent[1].global_id == audit_ids[2] && sent[0].operation == "INSERT" &&
                sent[1].operation == "INSERT", "expected exact first/latest INSERT audits, no middle filter");
        { std::lock_guard<std::mutex> lock(ack_mutex);
          require(acked == std::vector<std::string>({audit_ids[0], audit_ids[2]}),
                  "queued and reentrant ACKs must complete in exact order"); }
        require(sync.get_progress().pending_upload == 0, "both exact frames must be ACKed");
        auto members = db->db().query(
            "SELECT global_row_id FROM _lattice_sync_set WHERE sync_id = ? AND table_name = 'TestPerson'",
            {config.sync_id});
        require(members.size() == 1 && std::get<std::string>(members[0].at("global_row_id")) == row_ids[2],
                "latest filter membership must select only age 3");

        // A subsequent write tests persistent latest-filter state, not just
        // which of two queued lambdas happened to emit during the first drain.
        db->db().execute("UPDATE TestPerson SET name = ? WHERE age = ?", {std::string("middle-updated"), int64_t{2}});
        db->db().execute("UPDATE TestPerson SET name = ? WHERE age = ?", {std::string("latest-updated"), int64_t{3}});
        auto updated = db->db().query(
            "SELECT globalId FROM AuditLog WHERE tableName='TestPerson' AND operation='UPDATE' "
            "AND globalRowId = ? ORDER BY id DESC LIMIT 1", {row_ids[2]});
        require(updated.size() == 1, "latest row UPDATE audit missing");
        const auto update_id = std::get<std::string>(updated[0].at("globalId"));
        sync.sync_now();
        sent = sent_entries();
        require(sent.size() == 3 && sent[2].global_id == update_id && sent[2].operation == "UPDATE" &&
                sent[2].global_row_id == row_ids[2], "only the exact latest-filter UPDATE may be sent");
        require(sent[2].changed_fields_to_json().find("latest-updated") != std::string::npos,
                "latest UPDATE value must cross the actual transport");
        { std::lock_guard<std::mutex> lock(ack_mutex);
          require(acked == std::vector<std::string>({audit_ids[0], audit_ids[2], update_id}),
                  "final exact audit ACK set/order mismatch"); }
        require(!wire->overlapped.load() && !ack_inside_send.load() &&
                sync.get_progress().pending_upload == 0, "final send/ACK serialization or pending state invalid");
        sync.disconnect(); // Lifecycle changes occur only after both callers joined.
    });
}
#endif

// Dedicated sync connections must inherit the actual parent timeout, for both
// statement admission and database::begin_transaction's finite IMMEDIATE budget.
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace sync_child_timeout_regressions {
constexpr int parent_timeout_ms = 2000;

void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

struct network_factory_restore {
    std::shared_ptr<lattice::network_factory> prior = lattice::get_network_factory();
    network_factory_restore() {
        lattice::set_network_factory(std::make_shared<lattice::mock_network_factory>());
    }
    ~network_factory_restore() { lattice::set_network_factory(std::move(prior)); }
};

struct owned_path_cleanup {
    std::filesystem::path path;
    ~owned_path_cleanup() {
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
    }
};

// All child access stays inside the registry callback's lifetime guard. Use the
// actual child's existing worker, so the probe cannot join a scheduled sync
// transaction from another thread. A missing ending exits the fresh test child
// without unwinding the still-captured stack or destroying a borrowed child.
struct child_observation {
    int configured_timeout = -1;
    int64_t sqlite_timeout_before = -1;
    int64_t sqlite_timeout_after = -1;
    bool idle_before = false;
    bool idle_after = false;
    bool began = false;
    std::string begin_error;
};

child_observation observe_child(lattice::lattice_db& parent, bool attempt_begin) {
    child_observation result;
    size_t children = 0;
    lattice::instance_registry::instance().for_each_alive(parent.config().path,
        [&](lattice::lattice_db* actual) {
            if (actual == &parent) return;
            ++children;
            require(children == 1, "fixture must have exactly one dedicated child per path");
            require(actual->config().path == parent.config().path,
                    "dedicated child must use the actual parent store");
            require(!actual->config().is_sync_enabled() && !actual->config().is_ipc_enabled(),
                    "dedicated child must not recursively configure transports");
            const auto worker = actual->get_scheduler();
            require(dynamic_cast<lattice::std_thread_scheduler*>(worker.get()) != nullptr &&
                    worker->can_invoke(), "dedicated child must retain its real worker");
            std::mutex mutex;
            std::condition_variable completed;
            bool done = false;
            std::exception_ptr failure;
            worker->invoke([&, actual] {
                try {
                    result.configured_timeout = actual->config().busy_timeout_ms;
                    auto timeout = [&] {
                        const auto rows = actual->db().query("PRAGMA busy_timeout");
                        require(rows.size() == 1 && rows[0].size() == 1,
                                "actual busy_timeout PRAGMA must return one scalar");
                        const auto* value = std::get_if<int64_t>(&rows[0].begin()->second);
                        require(value != nullptr, "actual busy_timeout must be an integer");
                        return *value;
                    };
                    result.sqlite_timeout_before = timeout();
                    result.idle_before = !actual->db().is_in_transaction();
                    require(result.idle_before, "child must be idle before BEGIN probe");
                    if (attempt_begin) {
                        try {
                            actual->db().begin_transaction();
                            result.began = true;
                        } catch (const lattice::db_error& error) {
                            result.begin_error = error.what(); // before diagnostics/other SQL
                        }
                        if (result.began) actual->db().rollback();
                    }
                    result.idle_after = !actual->db().is_in_transaction();
                    result.sqlite_timeout_after = timeout();
                } catch (...) {
                    failure = std::current_exception();
                }
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    done = true;
                    completed.notify_one();
                }
            });
            std::unique_lock<std::mutex> lock(mutex);
            if (!completed.wait_for(lock, std::chrono::seconds(5), [&] { return done; })) {
                std::fputs("sync child worker ending unknown\n", stderr);
                std::fflush(stderr);
                _exit(3);
            }
            if (failure) std::rethrow_exception(failure);
        });
    require(children == 1, "automatic setup must construct an actual dedicated child");
    return result;
}

void require_timeout(const child_observation& actual) {
    require(actual.configured_timeout == parent_timeout_ms,
            "dedicated child configuration lost the parent's 2000ms timeout");
    require(actual.sqlite_timeout_before == parent_timeout_ms &&
            actual.sqlite_timeout_after == parent_timeout_ms,
            "actual child connection lost or failed to restore the 2000ms busy handler");
    require(actual.idle_before && actual.idle_after,
            "probe must leave the actual child outside any transaction");
}

void wss_child_timeout_and_blocked_begin() {
    network_factory_restore network;
    TempDB file("sync-timeout-wss");
    const auto lock_path = file.str() + ".sync.lock";
    require(!std::filesystem::exists(lock_path), "fixture lock must be fresh");
    owned_path_cleanup lock_cleanup{lock_path};
    lattice::configuration config(file.str(), "ws://mock.invalid/timeout", "test-token");
    config.busy_timeout_ms = parent_timeout_ms;
    config.tuning.checkpoint_passive_interval_ms = 60000;
    lattice::lattice_db parent(config); // automatic WSS setup creates the child
    require(parent.is_sync_connected(), "mock WSS must actually connect");
    parent.disconnect_sync(); // no mock transport work may join the BEGIN probe
    require(!parent.is_sync_connected(), "mock WSS must disconnect before probe");
    require_timeout(observe_child(parent, false));

    lattice::database holder(file.str(), lattice::database::open_mode::read_write,
                             parent_timeout_ms);
    lattice::transaction held(holder); // genuine independent BEGIN IMMEDIATE
    const auto blocked = observe_child(parent, true);
    require_timeout(blocked);
    require(!blocked.began &&
            blocked.begin_error.find("Failed to begin transaction: database is locked") !=
                std::string::npos,
            "actual child BEGIN must fail while the other connection owns the writer");
    require(holder.is_in_transaction(), "failed child BEGIN must not end the holder");
    held.rollback();
    const auto retried = observe_child(parent, true);
    require_timeout(retried);
    require(retried.began && retried.begin_error.empty(),
            "same actual child must acquire and roll back after holder release");
    parent.close();
}

void ipc_children_inherit_timeout() {
    network_factory_restore network;
    TempDB source_file("sync-timeout-source");
    TempDB target_file("sync-timeout-target");
    const std::string channel = "child-timeout-" + std::to_string(::getpid());
    const auto socket = std::filesystem::temp_directory_path() /
        ("ct-" + std::to_string(::getpid()) + ".sock");
    const auto socket_lock = socket.string() + ".lock";
    require(socket.is_absolute() && socket.string().size() < 104,
            "set TMPDIR to a short isolated directory for portable Unix socket paths");
    require(!std::filesystem::exists(socket) && !std::filesystem::exists(socket_lock),
            "IPC socket and lock must be fresh fixture-owned paths");
    owned_path_cleanup socket_cleanup{socket};
    owned_path_cleanup lock_cleanup{socket_lock};
    lattice::configuration source_config(source_file.str());
    source_config.busy_timeout_ms = parent_timeout_ms;
    source_config.tuning.checkpoint_passive_interval_ms = 60000;
    source_config.ipc_targets.push_back({channel, std::nullopt, socket.string()});
    lattice::configuration target_config(target_file.str());
    target_config.busy_timeout_ms = parent_timeout_ms;
    target_config.tuning.checkpoint_passive_interval_ms = 60000;
    target_config.ipc_targets.push_back({channel, std::nullopt, socket.string()});
    lattice::lattice_db source(source_config); // actual server endpoint
    lattice::lattice_db target(target_config); // actual client/probe connection
    const auto sent = source.add(TestPerson{"child-timeout-delivered", 37, std::nullopt});
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    bool delivered = false;
    do {
        const auto rows = target.objects<TestPerson>();
        delivered = rows.size() == 1 && rows[0].name.detach() == "child-timeout-delivered" &&
                    rows[0].age.detach() == 37 && rows[0].global_id() == sent.global_id();
        if (delivered) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (std::chrono::steady_clock::now() < deadline);
    require(delivered, "real IPC must deliver the exact source row before child inspection");
    require_timeout(observe_child(source, false));
    require_timeout(observe_child(target, false));
    target.close();
    source.close();
}
} // namespace sync_child_timeout_regressions

TEST(SyncChildTimeout, WSSActualChildInheritsBusyHandlerAndFiniteBeginBudget) {
    bounded_mock_case(sync_child_timeout_regressions::wss_child_timeout_and_blocked_begin);
}

TEST(SyncChildTimeout, RealIPCBothActualChildrenInheritBusyHandler) {
    bounded_mock_case(sync_child_timeout_regressions::ipc_children_inherit_timeout);
}
#endif

namespace {
// A queued scheduler makes the disconnect-vs-backoff ordering deterministic.
class lifecycle_queue : public lattice::scheduler {
    std::mutex mutex_;
    std::vector<std::function<void()>> jobs_;
    bool accepting_ = true;
public:
    void invoke(std::function<void()>&& fn) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (accepting_) jobs_.push_back(std::move(fn));
    }
    bool is_on_thread() const noexcept override { return false; }
    bool is_same_as(const lattice::scheduler* other) const noexcept override { return other == this; }
    bool can_invoke() const noexcept override { return true; }
    void shutdown() override { std::lock_guard<std::mutex> lock(mutex_); accepting_ = false; jobs_.clear(); }
    void drain() {
        for (int i = 0; i < 100; ++i) {
            std::vector<std::function<void()>> batch;
            { std::lock_guard<std::mutex> lock(mutex_); batch.swap(jobs_); }
            if (batch.empty()) return;
            for (auto& job : batch) job();
        }
        FAIL() << "lifecycle queue failed to drain";
    }
};
class lifecycle_transport : public lattice::mock_sync_transport {
public:
    int connects = 0;
    bool complete_open_during_disconnect = false;
    on_open_handler pending_open;
    lattice::transport_state current = lattice::transport_state::closed;
    void set_on_open(on_open_handler callback) override { pending_open = std::move(callback); }
    void connect(const std::string&, const std::map<std::string, std::string>&) override {
        ++connects; current = lattice::transport_state::connecting;
    }
    void disconnect() override {
        if (complete_open_during_disconnect) {
            complete_open_during_disconnect = false;
            if (pending_open) pending_open();
        }
        current = lattice::transport_state::closed;
    }
    lattice::transport_state state() const override { return current; }
    void open() { current = lattice::transport_state::open; if (pending_open) pending_open(); }
    void fail() { current = lattice::transport_state::closed; simulate_error("fixture failure"); }
};
}

TEST(Sync, ExplicitDisconnectCancelsQueuedRetryAcrossLaterConnect) {
    TempDB tmp{"local_first_retry"};
    auto queue = std::make_shared<lifecycle_queue>();
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str(), queue));
    lattice::sync_config cfg;
    cfg.websocket_url = "ws://fixture.invalid/retry";
    cfg.sync_id = "fixture-retry";
    cfg.base_delay_seconds = 0.01;
    auto transport = std::make_unique<lifecycle_transport>();
    auto* observed = transport.get();
    lattice::synchronizer sync(std::move(db), cfg, std::move(transport));
    sync.connect(); observed->fail();
    sync.disconnect();
    queue->drain();
    EXPECT_EQ(observed->connects, 1);
    sync.connect(); observed->fail(); // A second delayed retry belongs to this generation.
    sync.disconnect(); sync.connect();
    queue->drain();
    EXPECT_EQ(observed->connects, 3); // No stale retry may replace the newest attempt.
    sync.connect();
    EXPECT_EQ(observed->connects, 3); // Starting an already-connecting transport is idempotent.
    sync.disconnect();
}

TEST(Sync, GenericTransportRetiresContextOnlyAtDestruction) {
    struct Context { int connects = 0, disconnects = 0, destroyed = 0; } context;
    {
        lattice::generic_sync_transport transport(&context,
            [](void* p, const void*, const void*) { ++static_cast<Context*>(p)->connects; },
            [](void* p) { ++static_cast<Context*>(p)->disconnects; },
            [](void*) { return lattice::transport_state::closed; }, nullptr);
        transport.set_destroy_callback([](void* p) { ++static_cast<Context*>(p)->destroyed; });
        transport.connect("ws://fixture"); transport.disconnect(); transport.connect("ws://fixture");
        EXPECT_EQ(context.connects, 2);
        EXPECT_EQ(context.disconnects, 1);
        EXPECT_EQ(context.destroyed, 0);
    }
    EXPECT_EQ(context.destroyed, 1);
}

TEST(Sync, DeferredSiblingDoesNotConsumeAutomaticOwnerHandoff) {
    struct Restore { std::shared_ptr<lattice::network_factory> value;
        ~Restore() { lattice::set_network_factory(value); } } restore{lattice::get_network_factory()};
    lattice::set_network_factory(std::make_shared<lattice::mock_network_factory>());
    TempDB tmp{"deferred_handoff"};
    lattice::configuration automatic(tmp.str(), "ws://fixture.invalid/handoff", "fixture");
    lattice::lattice_db owner(automatic);
    auto deferred = automatic; deferred.sync_autostart = false;
    lattice::lattice_db dormant(deferred); // Registered before the eligible sibling.
    lattice::lattice_db waiting(automatic);
    EXPECT_TRUE(owner.is_sync_agent());
    EXPECT_FALSE(dormant.is_sync_agent());
    EXPECT_FALSE(waiting.is_sync_agent());
    owner.close();
    EXPECT_FALSE(dormant.is_sync_agent());
    EXPECT_TRUE(waiting.is_sync_agent());
    EXPECT_TRUE(waiting.is_sync_connected());
    waiting.disconnect_sync(); waiting.close(); dormant.close();
}

TEST(Sync, DisconnectFencesAlreadyAdmittedOpenBeforePublishingFinalState) {
    TempDB tmp{"local_first_open_drain"};
    auto queue = std::make_shared<lifecycle_queue>();
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str(), queue));
    lattice::sync_config cfg;
    cfg.websocket_url = "ws://fixture.invalid/open-drain";
    cfg.sync_id = "fixture-open-drain";
    auto transport = std::make_unique<lifecycle_transport>();
    auto* observed = transport.get();
    lattice::synchronizer sync(std::move(db), cfg, std::move(transport));
    int connected_callbacks = 0;
    sync.set_on_state_change([&](bool connected) { if (connected) ++connected_callbacks; });
    sync.connect();
    // Deterministic interleaving: transport's drain completes an open callback
    // that was already admitted before disconnect reached the transport lock.
    observed->complete_open_during_disconnect = true;
    sync.disconnect();
    EXPECT_FALSE(sync.is_connected());
    queue->drain();
    EXPECT_EQ(connected_callbacks, 0);
    sync.connect();
    EXPECT_EQ(observed->connects, 2); // A stale true must not block resumption.
    sync.disconnect();
}

TEST(Sync, TerminalErrorPublishesDisconnectedWithoutRequiringCloseCallback) {
    TempDB tmp{"local_first_terminal_error"};
    auto queue = std::make_shared<lifecycle_queue>();
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str(), queue));
    lattice::sync_config cfg;
    cfg.websocket_url = "ws://fixture.invalid/terminal-error";
    cfg.sync_id = "fixture-terminal-error";
    cfg.max_reconnect_attempts = -1; // Focus on terminal state, not a delayed retry.
    auto transport = std::make_unique<lifecycle_transport>();
    auto* observed = transport.get();
    lattice::synchronizer sync(std::move(db), cfg, std::move(transport));
    std::vector<bool> states;
    sync.set_on_state_change([&](bool connected) { states.push_back(connected); });
    sync.connect(); observed->open(); queue->drain();
    ASSERT_EQ(states, std::vector<bool>({true}));
    observed->fail();
    EXPECT_FALSE(sync.is_connected());
    queue->drain();
    EXPECT_EQ(states, std::vector<bool>({true, false}));
    // Some transports also deliver close after error; do not emit down twice.
    observed->lattice::mock_sync_transport::disconnect();
    queue->drain();
    EXPECT_EQ(states, std::vector<bool>({true, false}));
    sync.disconnect();
}

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(Sync, ConnectRemainsIdempotentAcrossOpenStateHandoff) {
    bounded_mock_case([] {
        class inspected_synchronizer : public lattice::synchronizer {
        public:
            using lattice::synchronizer::synchronizer;
            bool connecting() const { return is_connecting_.load(); }
            uint64_t generation() const { return connection_generation_.load(); }
        };
        TempDB tmp{"local_first_open_handoff"};
        auto queue = std::make_shared<lifecycle_queue>();
        auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str(), queue));
        lattice::sync_config cfg;
        cfg.websocket_url = "ws://fixture.invalid/open-handoff";
        cfg.sync_id = "fixture-open-handoff";
        auto transport = std::make_unique<lifecycle_transport>();
        auto* observed = transport.get();
        inspected_synchronizer sync(std::move(db), cfg, std::move(transport));
        std::vector<bool> states;
        sync.set_on_state_change([&](bool connected) { states.push_back(connected); });
        sync.connect();
        const auto generation = sync.generation();

        struct restore_level {
            lattice::log_level previous = lattice::get_log_level();
            ~restore_level() { lattice::set_log_level(previous); }
        } restore;
        std::unique_lock<std::mutex> log_gate(lattice::g_log_mutex());
        lattice::set_log_level(lattice::log_level::info);
        // The real callback reaches LOG_INFO after clearing connecting. Holding
        // the existing logger mutex exposes the former both-false interval.
        std::thread opener([&] { observed->open(); });
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (sync.connecting() && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        const bool reached_handoff = !sync.connecting();
        std::atomic<bool> returned{false};
        std::thread connector([&] { sync.connect(); returned = true; });
        const auto return_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!returned && std::chrono::steady_clock::now() < return_deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        const bool returned_before_open_completed = returned.load();
        // Release and join both workers before assertions, even on failure.
        log_gate.unlock();
        opener.join();
        connector.join();
        EXPECT_TRUE(reached_handoff);
        EXPECT_TRUE(returned_before_open_completed);
        EXPECT_EQ(observed->connects, 1);
        EXPECT_EQ(sync.generation(), generation);
        EXPECT_TRUE(sync.is_connected());
        queue->drain();
        EXPECT_EQ(states, std::vector<bool>({true}));
        sync.disconnect();
    });
}
#endif

TEST(Sync, StateRegistrationCatchupRejectsRetiredConnection) {
    TempDB tmp{"local_first_state_catchup"};
    auto queue = std::make_shared<lifecycle_queue>();
    auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str(), queue));
    lattice::sync_config cfg;
    cfg.websocket_url = "ws://fixture.invalid/state-catchup";
    cfg.sync_id = "fixture-state-catchup";
    auto transport = std::make_unique<lifecycle_transport>();
    auto* observed = transport.get();
    lattice::synchronizer sync(std::move(db), cfg, std::move(transport));
    sync.connect(); observed->open(); queue->drain();
    std::vector<bool> states;
    auto record = [&](bool connected) { states.push_back(connected); };

    sync.set_on_state_change(record); // Queues catch-up for the current connection.
    sync.disconnect(); sync.connect();
    queue->drain();
    EXPECT_TRUE(states.empty()); // The new attempt has not opened yet.
    observed->open(); queue->drain();
    EXPECT_EQ(states, std::vector<bool>({true}));

    states.clear();
    sync.set_on_state_change(record);
    sync.disconnect(); sync.connect(); observed->open();
    queue->drain();
    EXPECT_EQ(states, std::vector<bool>({true})); // Only the new open may report true.

    states.clear();
    sync.set_on_state_change(record);
    sync.set_on_state_change({}); // Removing a handler also invalidates queued catch-up.
    queue->drain();
    EXPECT_TRUE(states.empty());
    sync.disconnect();
}


namespace {
lattice::audit_log_entry byte_budget_entry(int ordinal, size_t bytes) {
    lattice::audit_log_entry entry;
    entry.id = ordinal + 1;
    entry.global_id = "byte-audit-" + std::to_string(ordinal);
    entry.global_row_id = "byte-row-" + std::to_string(ordinal);
    entry.table_name = "AssetChunk";
    entry.operation = "INSERT";
    entry.changed_fields["payload"] = lattice::any_property(std::vector<uint8_t>(bytes, 0xab));
    entry.changed_fields_names = {"payload"};
    return entry;
}
}

TEST(Sync, EncodedByteBoundaryIncludesEnvelopeAndBinaryExpansion) {
    std::vector<lattice::audit_log_entry> rows{byte_budget_entry(0, 64 * 1024)};
    auto full = lattice::encode_audit_log_prefix(rows, 0, 1);
    ASSERT_GT(full.json.size(), 2 * 64 * 1024u);
    auto exact = lattice::encode_audit_log_prefix(rows, 0, 1, full.json.size());
    EXPECT_EQ(exact.count, 1u);
    EXPECT_EQ(exact.json.size(), full.json.size());
    const auto oversized = lattice::encode_audit_log_prefix(rows, 0, 1, full.json.size() - 1);
    EXPECT_EQ(oversized.count, 1u);
    EXPECT_EQ(oversized.json, full.json);
    auto decoded = lattice::server_sent_event::from_json(exact.json);
    ASSERT_TRUE(decoded);
    ASSERT_EQ(decoded->audit_logs.size(), 1u);
    EXPECT_EQ(decoded->audit_logs.front().changed_fields.at("payload").to_column_value(),
              rows.front().changed_fields.at("payload").to_column_value());
}

TEST(Sync, SingleEntryFrameRetainsExactEscapedAndBinaryWire) {
    auto entry = byte_budget_entry(0, 256);
    entry.changed_fields["text"] = lattice::any_property(std::string("quoted \"value\"\\path\n]}/"));
    entry.changed_fields_names.push_back("text");
    const auto encoded = lattice::encode_audit_log_prefix({entry}, 0, 1);
    EXPECT_EQ(lattice::detail::single_encoded_audit_log_json(encoded), entry.to_json());
    ASSERT_EQ(encoded.entry_charges.size(), 1u);
    EXPECT_EQ(encoded.entry_charges[0], encoded.json.size());
    const auto empty = lattice::encode_audit_log_prefix({entry}, 0, 1, 4096, 0);
    EXPECT_THROW(lattice::detail::single_encoded_audit_log_json(empty), std::invalid_argument);
    const auto multiple = lattice::encode_audit_log_prefix({entry, byte_budget_entry(1, 16)}, 0, 2);
    ASSERT_EQ(multiple.count, 2u);
    EXPECT_THROW(lattice::detail::single_encoded_audit_log_json(multiple), std::invalid_argument);
    auto inconsistent = encoded;
    --inconsistent.entry_charges[0];
    EXPECT_THROW(lattice::detail::single_encoded_audit_log_json(inconsistent), std::invalid_argument);
}

TEST(Sync, ByteLimitedPrefixesPreserveEveryOrdinalExactlyOnce) {
    std::vector<lattice::audit_log_entry> rows;
    for (int i = 0; i < 19; ++i) rows.push_back(byte_budget_entry(i, 256 * 1024));
    size_t next = 0, frames = 0;
    while (next < rows.size()) {
        auto frame = lattice::encode_audit_log_prefix(rows, next, 1000);
        ASSERT_GT(frame.count, 0u);
        ASSERT_LE(frame.json.size(), lattice::sync_max_encoded_message_bytes);
        auto decoded = lattice::server_sent_event::from_json(frame.json);
        ASSERT_TRUE(decoded);
        ASSERT_EQ(decoded->audit_logs.size(), frame.count);
        for (size_t i = 0; i < frame.count; ++i)
            EXPECT_EQ(decoded->audit_logs[i].global_id, rows[next + i].global_id);
        next += frame.count;
        ++frames;
    }
    EXPECT_GE(frames, 3u);
    EXPECT_EQ(next, rows.size());
}

TEST(Sync, OversizedRowNeverHidesBehindOrSkipsPastAPartialPrefix) {
    std::vector<lattice::audit_log_entry> rows{
        byte_budget_entry(0, 16), byte_budget_entry(1, 2048), byte_budget_entry(2, 16)};
    auto prefix = lattice::encode_audit_log_prefix(rows, 0, rows.size(), 1024);
    ASSERT_EQ(prefix.count, 1u);
    const auto oversized = lattice::encode_audit_log_prefix(rows, prefix.count, rows.size(), 1024);
    ASSERT_EQ(oversized.count, 1u);
    const auto decoded = lattice::server_sent_event::from_json(oversized.json);
    ASSERT_TRUE(decoded);
    ASSERT_EQ(decoded->audit_logs.size(), 1u);
    EXPECT_EQ(decoded->audit_logs.front().global_id, "byte-audit-1");
    const auto suffix = lattice::encode_audit_log_prefix(rows, 2, rows.size(), 1024);
    EXPECT_EQ(suffix.count, 1u);
}

TEST(Sync, ExhaustedByteCreditDoesNotBecomeAnOversizedRowFailure) {
    std::vector<lattice::audit_log_entry> rows{byte_budget_entry(0, 128)};
    auto full = lattice::encode_audit_log_prefix(rows, 0, 1, 1024, 2048);
    ASSERT_EQ(full.count, 1u);
    auto held = lattice::encode_audit_log_prefix(rows, 0, 1, 1024, full.entry_charges[0] - 1);
    EXPECT_EQ(held.count, 0u);
    EXPECT_TRUE(held.json.empty());
    auto released = lattice::encode_audit_log_prefix(rows, 0, 1, 1024, full.entry_charges[0]);
    EXPECT_EQ(released.count, 1u);
    EXPECT_EQ(released.json, full.json);
}

TEST(Sync, MaterializedAuditBudgetReturnsPrefixAndContinuation) {
    TempDB tmp{"byteprefix"};
    lattice::lattice_db db(lattice::configuration(tmp.str()));
    for (int i = 0; i < 8; ++i)
        db.add(TestPerson{std::string(1024, 'x') + std::to_string(i), i, std::nullopt});
    bool cut = false;
    auto rows = lattice::query_audit_log_for_sync(db.db(), "bytes", std::nullopt, 0, 100, std::nullopt, 4096, &cut);
    ASSERT_FALSE(rows.empty());
    EXPECT_LT(rows.size(), 8u);
    EXPECT_TRUE(cut); // classifier skips must still schedule continuation
    int64_t previous = 0;
    for (const auto& row : rows) { EXPECT_GT(row.id, previous); previous = row.id; }
    EXPECT_THROW(lattice::query_audit_log_for_sync(db.db(), "bytes", std::nullopt, 0, 100, std::nullopt, 128), std::length_error);
    // Budget refusal is read-only: no audit row or sync disposition changed.
    auto all = lattice::query_audit_log_for_sync(db.db(), "bytes");
    EXPECT_EQ(all.size(), 8u);
}

TEST(Sync, PendingBudgetQueryErrorsNeverLookLikeEmptySuccess) {
    TempDB tmp{"bytequeryerror"};
    lattice::lattice_db db(lattice::configuration(tmp.str()));
    db.add(TestPerson{"pending", 1, std::nullopt});
    db.db().execute("ALTER TABLE AuditLog RENAME TO AuditLogHeld");
    EXPECT_THROW(lattice::query_audit_log_for_sync(db.db(), "errors", std::nullopt, 0, 100, std::nullopt, 4096), lattice::db_error);
    db.db().execute("ALTER TABLE AuditLogHeld RENAME TO AuditLog");
    EXPECT_EQ(lattice::query_audit_log_for_sync(db.db(), "errors").size(), 1u);
    db.db().close();
    EXPECT_THROW(lattice::query_audit_log_for_sync(db.db(), "errors", std::nullopt, 0, 100, std::nullopt, 4096), lattice::db_error);
}

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(Sync, StalledTransportPlateausAtEncodedByteWindowThenDrains) {
    bounded_mock_case([] {
        auto factory = std::make_shared<lattice::mock_network_factory>();
        lattice::set_network_factory(factory);
        TempDB tmp{"bytewindow"};
        auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
        for (int i = 0; i < 12; ++i)
            db->add(TestPerson{std::string(1024 * 1024, 'x') + std::to_string(i), i, std::nullopt});
        lattice::sync_config cfg;
        cfg.websocket_url = "ws://localhost/sync"; cfg.sync_id = "byte-window";
        cfg.all_active_sync_ids = {cfg.sync_id};
        lattice::synchronizer sync(std::move(db), cfg);
        auto* transport = factory->last_websocket();
        ASSERT_NE(transport, nullptr);
        sync.connect();
        auto messages = transport->get_sent_messages();
        size_t outstanding = 0;
        for (const auto& message : messages) {
            auto data = message.as_string();
            auto parsed = lattice::server_sent_event::from_json(data);
            if (parsed && parsed->event_type == lattice::server_sent_event::type::audit_log) {
                EXPECT_LE(data.size(), lattice::sync_max_encoded_message_bytes);
                outstanding += data.size();
            }
        }
        EXPECT_LE(outstanding, 2 * lattice::sync_max_encoded_message_bytes);
        const size_t count = messages.size();
        for (int i = 0; i < 3; ++i) sync.sync_now();
        EXPECT_EQ(transport->get_sent_messages().size(), count);
        std::set<std::string> acked;
        for (int pass = 0; pass < 20 && acked.size() < 12; ++pass) {
            std::vector<std::string> ids;
            for (const auto& message : transport->get_sent_messages()) {
                const auto bytes = message.as_string();
                auto event = lattice::server_sent_event::from_json(bytes);
                if (!event || event->event_type != lattice::server_sent_event::type::audit_log) continue;
                EXPECT_LE(bytes.size(), lattice::sync_max_encoded_message_bytes);
                for (const auto& row : event->audit_logs) if (acked.insert(row.global_id).second) ids.push_back(row.global_id);
            }
            if (ids.empty()) break;
            transport->simulate_message(lattice::transport_message::from_string(
                lattice::server_sent_event::make_ack(ids).to_json()));
        }
        EXPECT_EQ(acked.size(), 12u);
        EXPECT_EQ(sync.get_progress().pending_upload, 0);
        EXPECT_EQ(sync.get_progress().total_upload, 12);
    });
}

TEST(Sync, OversizedSingleUploadIsSentAloneAndAckReleasesFollowingRows) {
    bounded_mock_case([] {
        auto factory = std::make_shared<lattice::mock_network_factory>();
        lattice::set_network_factory(factory);
        TempDB tmp{"byteoversize"};
        auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
        db->add(TestPerson{"before", 0, std::nullopt});
        db->add(TestPerson{std::string(lattice::sync_max_encoded_message_bytes, 'x'), 1, std::nullopt});
        db->add(TestPerson{"after", 2, std::nullopt});
        auto* owned = db.get();
        const auto pending = lattice::query_audit_log_for_sync(owned->db(), "byte-oversize");
        ASSERT_EQ(pending.size(), 3u);
        lattice::sync_config cfg;
        cfg.websocket_url = "ws://localhost/sync"; cfg.sync_id = "byte-oversize";
        cfg.all_active_sync_ids = {cfg.sync_id};
        std::string failure;
        lattice::synchronizer sync(std::move(db), cfg);
        auto* transport = factory->last_websocket();
        ASSERT_NE(transport, nullptr);
        sync.set_on_error([&](const std::string& error) { failure = error; });
        sync.connect();
        std::set<std::string> acknowledged;
        bool saw_oversized = false;
        for (int pass = 0; pass < 4 && acknowledged.size() < pending.size(); ++pass) {
            std::vector<std::string> ids;
            for (const auto& message : transport->get_sent_messages()) {
                const auto bytes = message.as_string();
                const auto event = lattice::server_sent_event::from_json(bytes);
                if (!event || event->event_type != lattice::server_sent_event::type::audit_log) continue;
                if (bytes.size() > lattice::sync_max_encoded_message_bytes) {
                    saw_oversized = true;
                    ASSERT_EQ(event->audit_logs.size(), 1u);
                    EXPECT_EQ(event->audit_logs.front().global_id, pending[1].global_id);
                    EXPECT_EQ(event->audit_logs.front().changed_fields.at("name").to_column_value(),
                              pending[1].changed_fields.at("name").to_column_value());
                }
                for (const auto& row : event->audit_logs)
                    if (acknowledged.insert(row.global_id).second) ids.push_back(row.global_id);
            }
            if (ids.empty()) break;
            transport->simulate_message(lattice::transport_message::from_string(
                lattice::server_sent_event::make_ack(ids).to_json()));
        }
        EXPECT_TRUE(saw_oversized);
        EXPECT_TRUE(failure.empty()) << failure;
        EXPECT_EQ(acknowledged.size(), pending.size());
        for (const auto& row : pending) EXPECT_EQ(acknowledged.count(row.global_id), 1u);
        EXPECT_TRUE(lattice::query_audit_log_for_sync(owned->db(), cfg.sync_id).empty());
        EXPECT_EQ(sync.get_progress().pending_upload, 0);
    },30);
}
#endif

// Distinct filtered-delivery outbox candidate; source-only until a bounded qualification run.
#include "lattice/sync_delivery.hpp"
namespace {
int64_t outbox_number(lattice::lattice_db& db, const std::string& sql,
                      const std::vector<lattice::column_value_t>& args = {}) {
    const auto rows=db.db().query(sql,args);
    if(rows.size()!=1 || !std::holds_alternative<int64_t>(rows[0].at("n")))
        throw std::runtime_error("outbox fixture expected one exact scalar");
    return std::get<int64_t>(rows[0].at("n"));
}
lattice::detail::sync_delivery outbox_delivery(const lattice::audit_log_entry& source) {
    lattice::detail::sync_delivery receipt;
    receipt.source_id=source.id; receipt.source_global_id=source.global_id;
    receipt.wire=source; receipt.membership=1;
    receipt.routing_key="[{\"table\":\"TestPerson\",\"where\":null}]";
    return receipt;
}
std::vector<lattice::audit_log_entry> outbox_sent(lattice::mock_sync_transport& transport) {
    std::vector<lattice::audit_log_entry> rows;
    for(const auto& message:transport.get_sent_messages()) {
        auto event=lattice::server_sent_event::from_json(message.as_string());
        if(event && event->event_type==lattice::server_sent_event::type::audit_log)
            rows.insert(rows.end(),event->audit_logs.begin(),event->audit_logs.end());
    }
    return rows;
}
lattice::sync_config outbox_config(const std::string& name) {
    lattice::sync_config cfg;
    cfg.websocket_url="ws://localhost/sync"; cfg.sync_id=name;
    cfg.all_active_sync_ids={name}; cfg.chunk_size=2;
    cfg.sync_filter=std::vector<lattice::sync_filter_entry>{{"TestPerson",std::nullopt}};
    return cfg;
}
void outbox_ack(lattice::mock_sync_transport& transport,const std::vector<std::string>& ids) {
    transport.simulate_message(lattice::transport_message::from_string(
        lattice::server_sent_event::make_ack(ids).to_json()));
}
}

TEST(SyncDelivery, RefusalLeavesOriginalAndMembershipUntouched) {
    TempDB tmp{"outboxrefusal"}; lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"receipt",1,std::nullopt});
    const auto source=lattice::query_audit_log(db.db()).back();
    auto delivery=outbox_delivery(source);
    delivery.wire.changed_fields["name"]=lattice::any_property(std::string(2*lattice::sync_max_encoded_message_bytes,'x'));
    EXPECT_THROW(lattice::detail::admit_sync_delivery(db,"outbox",delivery),std::length_error);
    EXPECT_EQ(outbox_number(db,"SELECT COUNT(*) AS n FROM _lattice_sync_outbox"),0);
    EXPECT_EQ(outbox_number(db,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
    EXPECT_EQ(outbox_number(db,"SELECT COUNT(*) AS n FROM _lattice_sync_state"),0);
    EXPECT_EQ(lattice::query_audit_log_for_sync(db.db(),"outbox").size(),1u);
    delivery=outbox_delivery(source); delivery.source_global_id="wrong-source";
    EXPECT_THROW(lattice::detail::admit_sync_delivery(db,"outbox",delivery),lattice::db_error);
    EXPECT_EQ(outbox_number(db,"SELECT COUNT(*) AS n FROM _lattice_sync_outbox"),0);
}

TEST(SyncDelivery, EntityBarrierAndRetainedByteCapacityDoNotReplaceReceipts) {
    TempDB tmp{"outboxcapacity"}; lattice::lattice_db db{lattice::configuration(tmp.str())};
    for(int i=0;i<3;++i) db.add(TestPerson{"row"+std::to_string(i),i,std::nullopt});
    const auto sources=lattice::query_audit_log(db.db()); ASSERT_EQ(sources.size(),3u);
    for(size_t i=0;i<sources.size();++i) {
        auto receipt=outbox_delivery(sources[i]);
        receipt.wire.changed_fields["name"]=lattice::any_property(std::string(3*1024*1024,'x'));
        EXPECT_EQ(lattice::detail::admit_sync_delivery(db,"outbox",receipt),i<2);
    }
    auto duplicate=outbox_delivery(sources[0]); duplicate.wire.global_id="different-wire";
    EXPECT_FALSE(lattice::detail::admit_sync_delivery(db,"outbox",duplicate));
    const auto stored=lattice::detail::read_sync_outbox(db.db(),"outbox");
    ASSERT_EQ(stored.size(),2u); EXPECT_EQ(stored[0].wire.global_id,sources[0].global_id);
    EXPECT_EQ(outbox_number(db,"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE is_synchronized=1"),0);
    EXPECT_EQ(outbox_number(db,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
}

TEST(SyncDelivery, ExactWireBytesSurviveReopenAndMalformedReceiptFailsClosed) {
    TempDB tmp{"outboxreopen"}; std::string exact,wire_id;
    {
        lattice::lattice_db db{lattice::configuration(tmp.str())}; db.add(TestPerson{"offline",1,std::nullopt});
        auto delivery=outbox_delivery(lattice::query_audit_log(db.db()).back());
        delivery.wire.timestamp="123.456789"; delivery.wire.global_id="derived-stable-identity";
        exact=delivery.wire.to_json(); wire_id=delivery.wire.global_id;
        ASSERT_TRUE(lattice::detail::admit_sync_delivery(db,"outbox",delivery));
    }
    lattice::lattice_db reopened{lattice::configuration(tmp.str())};
    auto stored=lattice::detail::read_sync_outbox(reopened.db(),"outbox");
    ASSERT_EQ(stored.size(),1u); EXPECT_EQ(stored[0].wire_json,exact); EXPECT_EQ(stored[0].wire.global_id,wire_id);
    reopened.db().execute("UPDATE _lattice_sync_outbox SET encoded_charge=encoded_charge+1");
    EXPECT_THROW(lattice::detail::read_sync_outbox(reopened.db(),"outbox"),lattice::db_error);
    EXPECT_EQ(outbox_number(reopened,"SELECT COUNT(*) AS n FROM _lattice_sync_outbox"),1);
}

TEST(SyncDelivery, CachedWireReadStillObservesCurrentBytesBudgetAndSourceIdentity) {
    TempDB tmp{"outboxcachedread"}; lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"original",1,std::nullopt});
    auto delivery=outbox_delivery(lattice::query_audit_log(db.db()).back());
    ASSERT_TRUE(lattice::detail::admit_sync_delivery(db,"cache",delivery));
    const auto cached=lattice::detail::read_sync_outbox(db.db(),"cache");
    ASSERT_EQ(cached.size(),1u);
    const auto same=lattice::detail::read_sync_outbox(db.db(),"cache",cached);
    ASSERT_EQ(same.size(),1u); EXPECT_EQ(same[0].wire.to_json(),cached[0].wire.to_json());
    // A sibling edit with the same receipt/source identity must decode its new bytes.
    auto changed=delivery.wire; changed.changed_fields["name"]=lattice::any_property(std::string("changed"));
    const auto wire=changed.to_json();
    constexpr size_t envelope=sizeof("{\"kind\":\"auditLog\",\"auditLog\":[")-1+2;
    db.db().execute("UPDATE _lattice_sync_outbox SET wire_json=?,encoded_charge=? WHERE sync_id='cache'",
        {wire,static_cast<int64_t>(wire.size()+envelope)});
    const auto fresh=lattice::detail::read_sync_outbox(db.db(),"cache",cached);
    ASSERT_EQ(fresh.size(),1u); EXPECT_EQ(fresh[0].wire.to_json(),wire);
    db.db().execute("UPDATE _lattice_sync_outbox SET encoded_charge=encoded_charge+1 WHERE sync_id='cache'");
    EXPECT_THROW(lattice::detail::read_sync_outbox(db.db(),"cache",fresh),lattice::db_error);
    // Production CHECKs correctly prevent this write. Explicitly simulate a damaged stored
    // receipt only in this isolated fixture, then exercise the independent reader validation.
    EXPECT_THROW(db.db().execute("UPDATE _lattice_sync_outbox SET membership=7 WHERE sync_id='cache'"),lattice::db_error);
    db.db().execute("PRAGMA ignore_check_constraints=ON");
    const auto ignored=db.db().query("PRAGMA ignore_check_constraints");
    ASSERT_EQ(ignored.size(),1u); ASSERT_EQ(std::get<int64_t>(ignored[0].at("ignore_check_constraints")),1);
    db.db().execute("UPDATE _lattice_sync_outbox SET encoded_charge=encoded_charge-1,membership=7 WHERE sync_id='cache'");
    EXPECT_THROW(lattice::detail::read_sync_outbox(db.db(),"cache",fresh),lattice::db_error);
    db.db().execute("UPDATE _lattice_sync_outbox SET membership=4294967296 WHERE sync_id='cache'");
    EXPECT_THROW(lattice::detail::read_sync_outbox(db.db(),"cache",fresh),lattice::db_error);
    db.db().execute("UPDATE _lattice_sync_outbox SET membership=1,wire_json='{' WHERE sync_id='cache'");
    EXPECT_THROW(lattice::detail::read_sync_outbox(db.db(),"cache",fresh),lattice::db_error);
    db.db().execute("UPDATE _lattice_sync_outbox SET wire_json=? WHERE sync_id='cache'",{wire});
    db.db().execute("UPDATE AuditLog SET globalId='changed-source' WHERE id=?",{delivery.source_id});
    EXPECT_THROW(lattice::detail::read_sync_outbox(db.db(),"cache",fresh),lattice::db_error);
    db.db().execute("UPDATE AuditLog SET globalId=? WHERE id=?",{delivery.source_global_id,delivery.source_id});
    db.db().execute("UPDATE _lattice_sync_outbox SET routing_key=? WHERE sync_id='cache'",
        {std::string(lattice::detail::sync_outbox_bytes,'x')});
    EXPECT_THROW(lattice::detail::read_sync_outbox(db.db(),"cache",fresh),lattice::db_error);
    db.db().execute("DELETE FROM _lattice_sync_outbox WHERE sync_id='cache'");
    EXPECT_TRUE(lattice::detail::read_sync_outbox(db.db(),"cache",fresh).empty());
    db.db().execute("PRAGMA ignore_check_constraints=OFF");
}

TEST(SyncDelivery, PendingReceiptPinsPruningAndHistoryUntilExplicitChannelRetirement) {
    TempDB tmp{"outboxretention"}; lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"retained",1,std::nullopt});
    auto source=lattice::query_audit_log(db.db()).back();
    ASSERT_TRUE(lattice::detail::admit_sync_delivery(db,"retired-slot",outbox_delivery(source)));
    lattice::register_replication_slot(db.db(),"other",false);
    db.db().execute("UPDATE _lattice_replication_slots SET upload_floor=999999");
    EXPECT_EQ(db.safe_compact_audit_log(),0);
    EXPECT_EQ(outbox_number(db,"SELECT COUNT(*) AS n FROM AuditLog WHERE id=?",{source.id}),1);
    EXPECT_THROW(db.force_compact_audit_log(),lattice::db_error);
    db.reset_sync_state("other");
    EXPECT_EQ(lattice::detail::read_sync_outbox(db.db(),"retired-slot").size(),1u);
    db.remove_sync_channel_state("retired-slot");
    EXPECT_TRUE(lattice::detail::read_sync_outbox(db.db(),"retired-slot").empty());
    EXPECT_NO_THROW(db.force_compact_audit_log());
}

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(SyncDelivery, FilteredSyntheticOversizeDoesNotResolveItsOriginalUpdate) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"outboxsyntheticlarge"};
        auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())); auto* owned=db.get();
        db->add(TestPerson{std::string(2*lattice::sync_max_encoded_message_bytes,'x'),0,std::nullopt});
        db->db().execute("UPDATE AuditLog SET isSynchronized=1");
        db->db().execute("UPDATE TestPerson SET age=1");
        const auto source=lattice::query_audit_log(db->db()).back();
        auto cfg=outbox_config("synthetic-large"); cfg.sync_filter->at(0).where_clause="age=1";
        std::string error; lattice::synchronizer sync(std::move(db),cfg);
        sync.set_on_error([&](const std::string& value){error=value;}); sync.connect();
        EXPECT_NE(error.find(source.global_id),std::string::npos) << error;
        EXPECT_NE(error.find(source.table_name),std::string::npos) << error;
        EXPECT_TRUE(outbox_sent(*factory->last_websocket()).empty());
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_outbox"),0);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
        EXPECT_EQ(outbox_number(*owned,"SELECT isSynchronized AS n FROM AuditLog WHERE id=?",{source.id}),0);
        EXPECT_LT(lattice::read_upload_floor(owned->db(),cfg.sync_id),source.id);
    });
}

TEST(SyncDelivery, PartialAckOrdersEachEntityAndCommitsMembershipAtomically) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"outboxpartial"}; auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())); auto* owned=db.get();
        db->add(TestPerson{"first",1,std::nullopt}); db->add(TestPerson{"second",2,std::nullopt});
        db->db().execute("UPDATE TestPerson SET age=3 WHERE name='first'");
        auto cfg=outbox_config("partial"); lattice::synchronizer sync(std::move(db),cfg); sync.connect();
        auto* transport=factory->last_websocket(); auto initial=outbox_sent(*transport);
        ASSERT_EQ(initial.size(),2u);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
        outbox_ack(*transport,{initial[1].global_id});
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),1);
        EXPECT_EQ(outbox_sent(*transport).size(),2u); // first entity UPDATE cannot overtake INSERT
        outbox_ack(*transport,{initial[0].global_id});
        auto next=outbox_sent(*transport); ASSERT_EQ(next.size(),3u);
        EXPECT_EQ(next.back().operation,"UPDATE");
        EXPECT_EQ(next.back().global_row_id,initial[0].global_row_id);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),2);
        outbox_ack(*transport,{next.back().global_id});
        EXPECT_TRUE(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).empty());
        EXPECT_EQ(sync.get_progress().pending_upload,0);
    });
}

TEST(SyncDelivery, AckFailureRollsBackMembershipSourceAndReceiptTogether) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"outboxrollback"}; auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())); auto* owned=db.get();
        db->add(TestPerson{"rollback",1,std::nullopt}); auto cfg=outbox_config("rollback");
        lattice::synchronizer sync(std::move(db),cfg); sync.connect(); auto* transport=factory->last_websocket();
        auto sent=outbox_sent(*transport); ASSERT_EQ(sent.size(),1u);
        owned->db().execute("CREATE TRIGGER reject_outbox_ack BEFORE DELETE ON _lattice_sync_outbox "
                           "BEGIN SELECT RAISE(ABORT,'fixture ACK rollback'); END");
        outbox_ack(*transport,{sent[0].global_id});
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_outbox"),1);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE is_synchronized=0"),1);
        owned->db().execute("DROP TRIGGER reject_outbox_ack"); outbox_ack(*transport,{sent[0].global_id});
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),1);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_outbox"),0);
    });
}

TEST(SyncDelivery, RestartAfterRemoteApplyBeforeAckReusesExactDerivedIdentity) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB local{"outboxrestart"}, remote{"outboxremote"};
        lattice::lattice_db receiver{lattice::configuration(remote.str())}; receiver.add(TestPerson{"fixture seed",0,std::nullopt});
        receiver.db().execute("DELETE FROM TestPerson"); receiver.db().execute("DELETE FROM AuditLog");
        const auto cfg=outbox_config("restart"); std::string wire_id;
        {
            auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(local.str()));
            db->add(TestPerson{"before",0,std::nullopt}); db->db().execute("UPDATE AuditLog SET isSynchronized=1");
            db->db().execute("UPDATE TestPerson SET age=1");
            lattice::synchronizer sync(std::move(db),cfg); sync.connect();
            auto sent=outbox_sent(*factory->last_websocket()); ASSERT_FALSE(sent.empty());
            ASSERT_EQ(sent[0].operation,"INSERT"); ASSERT_TRUE(sent[0].synthesized);
            wire_id=sent[0].global_id;
            ASSERT_EQ(lattice::apply_remote_changes_for(receiver,{sent[0]},"remote").size(),1u);
            sync.disconnect(); // intentionally lose ACK, then destroy process-owned source handle
        }
        auto reopened=std::make_unique<lattice::lattice_db>(lattice::configuration(local.str())); auto* owned=reopened.get();
        const auto receipt=lattice::detail::read_sync_outbox(reopened->db(),cfg.sync_id); ASSERT_FALSE(receipt.empty());
        const auto durable=receipt[0].wire_json;
        lattice::synchronizer sync(std::move(reopened),cfg); sync.connect(); auto* transport=factory->last_websocket();
        auto replay=outbox_sent(*transport); ASSERT_FALSE(replay.empty()); EXPECT_EQ(replay[0].global_id,wire_id);
        bool exact_frame=false;
        for(const auto& message:transport->get_sent_messages())
            exact_frame |= message.as_string().find(durable)!=std::string::npos;
        EXPECT_TRUE(exact_frame);
        ASSERT_EQ(lattice::apply_remote_changes_for(receiver,{replay[0]},"remote").size(),1u);
        EXPECT_EQ(outbox_number(receiver,"SELECT COUNT(*) AS n FROM TestPerson"),1);
        outbox_ack(*transport,{wire_id});
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),1);
    });
}

TEST(SyncDelivery, NarrowingOrdersRemovalAfterContentAndStaleAckCannotRestoreMembership) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"outboxnarrow"}; auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())); auto* owned=db.get();
        db->add(TestPerson{"private",1,std::nullopt}); auto cfg=outbox_config("ipc:narrow");
        lattice::synchronizer sync(std::move(db),cfg); sync.connect(); auto* transport=factory->last_websocket();
        auto initial=outbox_sent(*transport); ASSERT_EQ(initial.size(),1u);
        sync.update_sync_filter({});
        auto after=outbox_sent(*transport); ASSERT_EQ(after.size(),2u);
        EXPECT_EQ(after[0].operation,"INSERT"); EXPECT_EQ(after[1].operation,"DELETE");
        EXPECT_EQ(after[1].changed_fields_names,std::vector<std::string>{"__lattice_filter_removal"});
        outbox_ack(*transport,{initial[0].global_id});
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
        EXPECT_EQ(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).size(),1u);
        outbox_ack(*transport,{after[1].global_id});
        EXPECT_TRUE(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).empty());
    });
}

TEST(SyncDelivery, ReconnectUnderNarrowedGroupPolicyCancelsWithoutResendingOrDeleting) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"outboxgroup"}; auto cfg=outbox_config("wss:group"); cfg.narrowing_emits_removals=false;
        std::string old;
        {
            auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())); db->add(TestPerson{"group",1,std::nullopt});
            lattice::synchronizer sync(std::move(db),cfg); sync.connect();
            const auto sent=outbox_sent(*factory->last_websocket()); ASSERT_EQ(sent.size(),1u); old=sent[0].global_id;
            sync.disconnect();
        }
        cfg.sync_filter=std::vector<lattice::sync_filter_entry>{};
        auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())); auto* owned=db.get();
        lattice::synchronizer sync(std::move(db),cfg); sync.connect(); auto* transport=factory->last_websocket();
        EXPECT_TRUE(outbox_sent(*transport).empty());
        EXPECT_TRUE(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).empty());
        outbox_ack(*transport,{old});
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
    });
}

TEST(SyncDelivery, ClearingFilterRepairsPossiblyAppliedDeleteBeforeLaterUpdates) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"outboxwiden"}; auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())); auto* owned=db.get();
        db->add(TestPerson{"widen",1,std::nullopt}); auto cfg=outbox_config("ipc:widen");
        lattice::synchronizer sync(std::move(db),cfg); sync.connect(); auto* transport=factory->last_websocket();
        auto initial=outbox_sent(*transport); ASSERT_EQ(initial.size(),1u); outbox_ack(*transport,{initial[0].global_id});
        sync.update_sync_filter({}); auto narrowed=outbox_sent(*transport); ASSERT_EQ(narrowed.back().operation,"DELETE");
        const auto delete_id=narrowed.back().global_id;
        sync.clear_sync_filter(); auto widened=outbox_sent(*transport);
        ASSERT_EQ(widened.back().operation,"INSERT"); EXPECT_TRUE(widened.back().synthesized);
        EXPECT_NE(widened.back().global_id,initial[0].global_id);
        outbox_ack(*transport,{delete_id});
        EXPECT_EQ(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).size(),1u);
        outbox_ack(*transport,{widened.back().global_id});
        EXPECT_TRUE(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).empty());
    });
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(SyncDelivery, TimeoutAndReconnectResendExactReceiptWithoutEarlyMembership) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"outboxtimeout"}; auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())); auto* owned=db.get();
        db->add(TestPerson{"retry",1,std::nullopt}); auto cfg=outbox_config("retry"); cfg.ack_timeout_base_ms=20;
        lattice::synchronizer sync(std::move(db),cfg); sync.connect(); auto* transport=factory->last_websocket();
        auto initial=outbox_sent(*transport); ASSERT_EQ(initial.size(),1u);
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
        while(outbox_sent(*transport).size()<2 && std::chrono::steady_clock::now()<deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        auto retry=outbox_sent(*transport); ASSERT_GE(retry.size(),2u);
        for(const auto& row:retry) EXPECT_EQ(row.global_id,initial[0].global_id);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
        const auto retained=lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id); ASSERT_EQ(retained.size(),1u);
        sync.disconnect(); transport->clear_sent_messages(); sync.connect();
        auto reopened=outbox_sent(*transport); ASSERT_FALSE(reopened.empty()); EXPECT_EQ(reopened[0].global_id,initial[0].global_id);
        EXPECT_EQ(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id)[0].wire_json,retained[0].wire_json);
        outbox_ack(*transport,{initial[0].global_id});
        EXPECT_TRUE(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).empty());
    });
}

TEST(SyncDelivery, FilteredByteDeferralPreservesEverySourceUntilItsOwnAck) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"outboxbytecredit"}; auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())); auto* owned=db.get();
        const auto started=std::chrono::steady_clock::now();
        auto trace=[&](const char* stage,int pass=-1) {
            const auto ms=std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-started).count();
            std::fprintf(stderr,"filtered_bytes stage=%s pass=%d elapsed_ms=%lld\n",stage,pass,static_cast<long long>(ms));
        };
        trace("insert-start");
        for(int i=0;i<10;++i) db->add(TestPerson{std::string(1024*1024,'x')+std::to_string(i),i,std::nullopt});
        trace("insert-complete");
        auto cfg=outbox_config("filtered-bytes"); cfg.chunk_size=100;
        lattice::synchronizer sync(std::move(db),cfg); sync.connect(); auto* transport=factory->last_websocket();
        trace("connect-complete");
        // Observe each collected message once. Re-decoding the entire growing mock history
        // after each one-row ACK measures fixture JSON work instead of the delivery pipeline.
        size_t observed_messages=0;
        std::vector<lattice::audit_log_entry> observed_entries;
        const auto observe=[&]() -> const std::vector<lattice::audit_log_entry>& {
            trace("observe-snapshot-start");
            const auto messages=transport->get_sent_messages();
            trace("observe-snapshot-complete");
            std::fprintf(stderr,"filtered_bytes observed=%zu snapshot_messages=%zu\n",observed_messages,messages.size());
            if(messages.size()<observed_messages) throw std::runtime_error("mock message history shrank");
            for(;observed_messages<messages.size();++observed_messages) {
                if(observed_messages<32) {
                    std::fprintf(stderr,"filtered_bytes message=%zu bytes=%zu\n",observed_messages,messages[observed_messages].data.size());
                    trace("observe-parse-start",static_cast<int>(observed_messages));
                }
                const auto event=lattice::server_sent_event::from_json(messages[observed_messages].as_string());
                if(observed_messages<32) trace("observe-parse-complete",static_cast<int>(observed_messages));
                if(!event) throw std::runtime_error("invalid mock message");
                if(event->event_type==lattice::server_sent_event::type::audit_log)
                    observed_entries.insert(observed_entries.end(),event->audit_logs.begin(),event->audit_logs.end());
                if(observed_messages<32) trace("observe-retain-complete",static_cast<int>(observed_messages));
            }
            return observed_entries;
        };
        const auto& initial=observe(); ASSERT_GT(initial.size(),0u); ASSERT_LT(initial.size(),10u);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
        EXPECT_EQ(lattice::query_audit_log_for_sync(owned->db(),cfg.sync_id).size(),10u);
        trace("initial-checks-complete");
        std::set<std::string> acked;
        // Reuse the same validated-wire cache as the production owner. Every read
        // still checks current SQL budgets/identities and exact wire bytes; unchanged
        // retained payloads need not be JSON-decoded again for this byte sum.
        std::vector<lattice::detail::sync_delivery> retained;
        for(int pass=0;pass<20 && acked.size()<10;++pass) {
            trace("read-sent",pass);
            const auto& sent=observe(); std::vector<std::string> ids;
            // Partial ACK only one row, deliberately newest first. Other retained receipts and
            // unsent originals remain pending independently of the released byte credit.
            for(auto it=sent.rbegin();it!=sent.rend();++it) if(!acked.count(it->global_id)) {ids={it->global_id};break;}
            trace("ack-start",pass);
            ASSERT_FALSE(ids.empty()); acked.insert(ids[0]); outbox_ack(*transport,ids); sync.sync_now();
            trace("ack-sync-complete",pass);
            EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),static_cast<int64_t>(acked.size()));
            retained=lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id,retained);
            size_t bytes=0;for(const auto& receipt:retained) bytes+=lattice::detail::sync_delivery_retained_bytes(receipt,receipt.wire_json);
            EXPECT_LE(bytes,lattice::detail::sync_outbox_bytes);
        }
        trace("loop-complete");
        EXPECT_EQ(acked.size(),10u); EXPECT_TRUE(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).empty());
        EXPECT_TRUE(lattice::query_audit_log_for_sync(owned->db(),cfg.sync_id).empty());
    }, 30); // Hang guard for the 10 MiB Debug correctness fixture, not a latency SLA.
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
class outbox_probe final : public lattice::synchronizer {
public:
    using lattice::synchronizer::synchronizer;
    using lattice::synchronizer::build_insert_entry_from_current_row;
    using lattice::synchronizer::upload_pending_changes;
    auto filter() const {return config_.sync_filter;}
};
class outbox_queue final : public lattice::scheduler {
    std::mutex mutex_;
    std::vector<std::function<void()>> jobs_;
    bool accepting_=true;
public:
    void invoke(std::function<void()>&& work) override {std::lock_guard<std::mutex> lock(mutex_);if(accepting_) jobs_.push_back(std::move(work));}
    bool is_on_thread() const noexcept override {return false;}
    bool is_same_as(const lattice::scheduler* other) const noexcept override {return this==other;}
    bool can_invoke() const noexcept override {return true;}
    void shutdown() override {std::lock_guard<std::mutex> lock(mutex_);accepting_=false;jobs_.clear();}
    void drain(bool newest_first=false) {
        for(int pass=0;pass<100;++pass) {
            std::vector<std::function<void()>> batch;
            {std::lock_guard<std::mutex> lock(mutex_);batch.swap(jobs_);}
            if(batch.empty()) return;
            if(newest_first) std::reverse(batch.begin(),batch.end());
            newest_first=false;
            for(auto& work:batch) work();
        }
        FAIL()<<"outbox scheduler did not drain";
    }
};
}

TEST(SyncDelivery, ReversedFilterCallbacksCannotRestoreStaleAuthority) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>();lattice::set_network_factory(factory);
        auto scheduler=std::make_shared<outbox_queue>(); TempDB tmp{"outboxfilterorder"};
        lattice::configuration config(tmp.str());config.sched=scheduler;
        auto db=std::make_unique<lattice::lattice_db>(config);
        db->add(TestPerson{"one",1,std::nullopt});db->add(TestPerson{"two",2,std::nullopt});
        auto cfg=outbox_config("filter-order");cfg.sync_filter=std::vector<lattice::sync_filter_entry>{};
        outbox_probe sync(std::move(db),cfg);sync.connect();scheduler->drain();
        auto* transport=factory->last_websocket();transport->clear_sent_messages();
        sync.update_sync_filter({{"TestPerson",std::string("age=1")}});
        sync.update_sync_filter({{"TestPerson",std::string("age=2")}});
        sync.upload_pending_changes(); // before queued latest policy applies, requests revoke sends
        EXPECT_TRUE(outbox_sent(*transport).empty());
        scheduler->drain(true); // newest callback first; the stale one must not overwrite it
        ASSERT_TRUE(sync.filter());ASSERT_EQ(sync.filter()->size(),1u);
        EXPECT_EQ(sync.filter()->at(0).where_clause,std::optional<std::string>{"age=2"});
        const auto sent=outbox_sent(*transport);ASSERT_EQ(sent.size(),1u);
        EXPECT_EQ(std::get<int64_t>(sent[0].changed_fields.at("age").value),2);
    });
}

TEST(SyncDelivery, CurrentSnapshotProjectionRefusesBeforeHydrationAndSupportsLinkRows) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>();lattice::set_network_factory(factory);
        TempDB tmp{"outboxsnapshotbound"};auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));auto* owned=db.get();
        db->add(TestPerson{std::string(9*1024*1024,'x'),1,std::nullopt});
        const auto gid=std::get<std::string>(db->db().query("SELECT globalId FROM TestPerson")[0].at("globalId"));
        outbox_probe sync(std::move(db),outbox_config("snapshot-bound")); // no connection/reconciliation
        EXPECT_THROW(sync.build_insert_entry_from_current_row("TestPerson",gid),std::length_error);
        owned->db().execute("CREATE TABLE _TestPerson_links(globalId TEXT PRIMARY KEY,lhs TEXT,rhs TEXT)");
        owned->db().execute("INSERT INTO _TestPerson_links VALUES('link-id','left','right')");
        auto link=sync.build_insert_entry_from_current_row("_TestPerson_links","link-id");
        ASSERT_TRUE(link);EXPECT_EQ(link->row_id,0);EXPECT_EQ(link->global_row_id,"link-id");
        EXPECT_EQ(std::get<std::string>(link->changed_fields.at("lhs").value),"left");
    });
}

TEST(SyncDelivery, VectorFilterEnableAndReenableDrainPastSnapshotWindowWithRegisteredLinks) {
    bounded_mock_case([] {
        // Test-local stderr breadcrumbs survive the unchanged SIGALRM watchdog.
        // No per-row logging: the existing finite drain loops bound this output.
        struct phase_trace {
            std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
            void emit(const char* phase, int pass = -1, size_t count = 0) const {
                const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - started).count();
                std::fprintf(stderr, "vector_filter_phase phase=%s pass=%d count=%zu elapsed_ms=%lld\n",
                             phase, pass, count, static_cast<long long>(elapsed));
                std::fflush(stderr);
            }
        } trace;
        struct scope_marker {
            const phase_trace& trace;
            const char* phase;
            ~scope_marker() { trace.emit(phase); }
        };
        const scope_marker fixture_exit{trace, "fixture_teardown_done"};
        trace.emit("setup_begin");
        auto factory=std::make_shared<lattice::mock_network_factory>();lattice::set_network_factory(factory);
        auto scheduler=std::make_shared<outbox_queue>();TempDB tmp{"vectorfilterreconcile"};
        lattice::configuration config(tmp.str());config.sched=scheduler;
        auto db=std::make_unique<lattice::lattice_db>(config);auto* owned=db.get();
        trace.emit("database_ready");
        auto schema=lattice::managed<FilterVectorRow>::schema();
        schema.properties.back().is_vector=true;
        const auto embedding=pack_floats({1.0f,2.0f});
        std::vector<FilterVectorRow> rows;
        for(int i=0;i<257;++i) rows.push_back({"selected-"+std::to_string(i),1,embedding});
        rows.push_back({"excluded",2,embedding});
        trace.emit("initial_bulk_begin");
        db->add_bulk_with_schema(std::move(rows),schema);
        trace.emit("initial_bulk_done");
        ASSERT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _FilterVectorRow_embedding_vec"),258);
        ASSERT_GT(outbox_number(*owned,"SELECT COUNT(*) AS n FROM pragma_table_list "
            "WHERE schema='main' AND type='shadow' AND name LIKE '_FilterVectorRow_embedding_vec%'"),0);
        const auto parent=std::get<std::string>(owned->db().query(
            "SELECT globalId FROM FilterVectorRow WHERE name='selected-0'")[0].at("globalId"));
        // Use the real registration APIs, including an empty-parent legacy-compatible registration.
        trace.emit("registered_links_begin");
        owned->ensure_link_table("_FilterVectorRow_links");
        owned->ensure_virtual_link_table("_FilterVectorRow_polymorphic","FilterVectorRow");
        owned->ensure_geo_bounds_list_table("FilterVectorRow","zones");
        owned->db().execute("INSERT INTO _FilterVectorRow_links(lhs,rhs) VALUES(?,?)",{parent,parent});
        owned->db().execute("INSERT INTO _FilterVectorRow_polymorphic(lhs,rhs,rhs_type) VALUES(?,?,'FilterVectorRow')",{parent,parent});
        owned->db().execute("INSERT INTO _FilterVectorRow_zones(parent_id,minLat,maxLat,minLon,maxLon) VALUES(?,0,1,0,1)",{parent});
        trace.emit("registered_links_done");
        auto cfg=outbox_config("ipc:vector-filter");cfg.chunk_size=64;
        cfg.sync_filter=std::vector<lattice::sync_filter_entry>{};
        std::vector<std::string> errors;
        const scope_marker sync_teardown_done{trace, "sync_teardown_done"};
        trace.emit("sync_construct_begin");
        outbox_probe sync(std::move(db),cfg);
        const scope_marker sync_teardown_begin{trace, "sync_teardown_begin"};
        trace.emit("sync_construct_done");
        sync.set_on_error([&](const std::string& error){errors.push_back(error);});
        trace.emit("connect_begin");
        sync.connect();
        trace.emit("initial_drain_begin");
        scheduler->drain();
        trace.emit("initial_drain_done");
        auto* transport=factory->last_websocket();
        ASSERT_TRUE(outbox_sent(*transport).empty());
        transport->clear_sent_messages();
        std::vector<lattice::audit_log_entry> received;
        auto drain=[&](const std::string& operation) {
            trace.emit("drain_enter");
            received.clear();std::set<std::string> seen;
            for(int pass=0;pass<16;++pass) {
                trace.emit("drain_pass_begin", pass);
                scheduler->drain();
                trace.emit("scheduler_drained", pass);
                const auto batch=outbox_sent(*transport);transport->clear_sent_messages();
                trace.emit("batch_read", pass, batch.size());
                if(batch.empty()) break;
                std::vector<std::string> acks;
                for(const auto& entry:batch) {
                    ASSERT_TRUE(seen.insert(entry.global_id).second);
                    EXPECT_EQ(entry.operation,operation);
                    EXPECT_TRUE(entry.table_name=="FilterVectorRow" || entry.table_name=="_FilterVectorRow_links" ||
                        entry.table_name=="_FilterVectorRow_polymorphic" || entry.table_name=="_FilterVectorRow_zones");
                    if(operation=="INSERT" && entry.table_name=="FilterVectorRow") {
                        EXPECT_EQ(std::get<int64_t>(entry.changed_fields.at("age").value),1);
                        EXPECT_EQ(entry.changed_fields.count("embedding"),1u);
                    }
                    received.push_back(entry);acks.push_back(entry.global_id);
                }
                outbox_ack(*transport,acks);
                trace.emit("ack_returned", pass, acks.size());
            }
            EXPECT_TRUE(errors.empty())<<(errors.empty()?"":errors.front());
            EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_outbox"),0);
            EXPECT_EQ(sync.get_progress().pending_upload,0);
            trace.emit("drain_return", -1, received.size());
        };
        const std::vector<lattice::sync_filter_entry> enabled={{"FilterVectorRow",std::string("age=1")}};
        trace.emit("enable_begin");
        sync.update_sync_filter(enabled);drain("INSERT");
        trace.emit("enable_done", -1, received.size());
        ASSERT_EQ(received.size(),260u); // 257 vector rows plus all three registered internal rows.
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),260);
        // Exercise the same OFF->ON state transition, with new rows admitted while OFF.
        trace.emit("disable_begin");
        sync.update_sync_filter({});drain("DELETE");
        trace.emit("disable_done", -1, received.size());
        ASSERT_EQ(received.size(),260u);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
        std::vector<FilterVectorRow> later;
        for(int i=0;i<20;++i) later.push_back({"while-off-"+std::to_string(i),1,embedding});
        trace.emit("while_off_bulk_begin");
        owned->add_bulk_with_schema(std::move(later),schema);sync.sync_now();
        trace.emit("while_off_drain_begin");
        scheduler->drain();
        trace.emit("while_off_drain_done");
        ASSERT_TRUE(outbox_sent(*transport).empty());
        trace.emit("reenable_begin");
        sync.update_sync_filter(enabled);drain("INSERT");
        trace.emit("reenable_done", -1, received.size());
        ASSERT_EQ(received.size(),280u);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),280);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _FilterVectorRow_embedding_vec"),278);
        trace.emit("body_return");
    });
}

TEST(SyncDelivery, RepeatedFreshResetUsesNewCohortsDespiteOldAcksAndIncludesUnsentCurrentRows) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>();lattice::set_network_factory(factory);
        TempDB tmp{"outboxresetcohorts"};auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));auto* owned=db.get();
        for(const auto& name:{"acknowledged","pending","unsent","deleted"}) db->add(TestPerson{name,1,std::nullopt});
        db->db().execute("DELETE FROM TestPerson WHERE name='deleted'");
        auto cfg=outbox_config("ipc:reset-cohorts");cfg.chunk_size=1;cfg.use_upload_floor=false;
        lattice::synchronizer sync(std::move(db),cfg);sync.connect();auto* transport=factory->last_websocket();
        const auto initial=outbox_sent(*transport);ASSERT_EQ(initial.size(),2u);
        outbox_ack(*transport,{initial[0].global_id}); // one ACKed, one uncertain, one never sent, one deleted
        const auto request=lattice::transport_message::from_string(lattice::server_sent_event::make_replay_request().to_json());
        transport->clear_sent_messages();transport->simulate_message(request);
        const auto first=outbox_sent(*transport);ASSERT_EQ(first.size(),2u);
        const auto cutoff1=lattice::detail::read_sync_reset_cutoff(owned->db(),cfg.sync_id);
        for(const auto& row:first) {EXPECT_TRUE(row.synthesized);EXPECT_GT(row.id,cutoff1);EXPECT_NE(row.global_id,initial[0].global_id);EXPECT_NE(row.global_id,initial[1].global_id);}
        transport->clear_sent_messages();transport->simulate_message(request);
        const auto second=outbox_sent(*transport);ASSERT_EQ(second.size(),2u);
        EXPECT_GT(lattice::detail::read_sync_reset_cutoff(owned->db(),cfg.sync_id),cutoff1);
        const auto before=lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id);ASSERT_EQ(before.size(),3u);
        outbox_ack(*transport,{initial[1].global_id,first[0].global_id,first[1].global_id});
        const auto after=lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id);ASSERT_EQ(after.size(),before.size());
        for(size_t i=0;i<after.size();++i) EXPECT_EQ(after[i].wire.global_id,before[i].wire.global_id);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
        std::set<std::string> names,acked;
        for(int pass=0;pass<8;++pass) {
            std::vector<std::string> ids;
            for(const auto& row:outbox_sent(*transport)) if(acked.insert(row.global_id).second) {
                ids.push_back(row.global_id);names.insert(std::get<std::string>(row.changed_fields.at("name").value));
            }
            if(ids.empty()) break;outbox_ack(*transport,ids);
        }
        EXPECT_EQ(names,(std::set<std::string>{"acknowledged","pending","unsent"}));
        EXPECT_TRUE(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).empty());
        owned->db().execute("UPDATE TestPerson SET age=2 WHERE name='acknowledged'");sync.sync_now();
        const auto writes=outbox_sent(*transport);EXPECT_EQ(writes.back().operation,"UPDATE");
        EXPECT_GT(writes.back().id,lattice::detail::read_sync_reset_cutoff(owned->db(),cfg.sync_id));
    });
}
#endif

TEST(SyncDelivery, FreshResetRollbackRestartAndSlotExpiryPreserveIdentityBoundary) {
    TempDB tmp{"outboxresetdurable"};int64_t cutoff=0;
    {
        lattice::lattice_db db{lattice::configuration(tmp.str())};db.add(TestPerson{"durable",1,std::nullopt});
        const auto source=lattice::query_audit_log(db.db()).back();ASSERT_TRUE(lattice::detail::admit_sync_delivery(db,"reset",outbox_delivery(source)));
        db.db().execute("CREATE TRIGGER reject_reset BEFORE DELETE ON _lattice_sync_outbox BEGIN SELECT RAISE(ABORT,'reset rollback'); END");
        EXPECT_THROW(db.reset_filtered_peer_snapshot("reset"),lattice::db_error);
        EXPECT_EQ(lattice::detail::read_sync_reset_cutoff(db.db(),"reset"),0);
        EXPECT_EQ(lattice::detail::read_sync_outbox(db.db(),"reset").size(),1u);
        EXPECT_EQ(outbox_number(db,"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE is_synchronized=0"),1);
        db.db().execute("DROP TRIGGER reject_reset");db.reset_filtered_peer_snapshot("reset");
        cutoff=lattice::detail::read_sync_reset_cutoff(db.db(),"reset");EXPECT_EQ(cutoff,source.id);
        EXPECT_TRUE(lattice::query_audit_log_for_sync(db.db(),"reset",std::nullopt,0).empty());
        lattice::register_replication_slot(db.db(),"reset",false);
        db.db().execute("DELETE FROM _lattice_replication_slots WHERE sync_id='reset'");
        lattice::register_replication_slot(db.db(),"reset",false);
        EXPECT_EQ(lattice::detail::read_sync_reset_cutoff(db.db(),"reset"),cutoff);
    }
    lattice::lattice_db db{lattice::configuration(tmp.str())};
    EXPECT_EQ(lattice::detail::read_sync_reset_cutoff(db.db(),"reset"),cutoff);
    EXPECT_TRUE(lattice::query_audit_log_for_sync(db.db(),"reset",std::nullopt,0).empty());
    db.add(TestPerson{"after",2,std::nullopt});const auto pending=lattice::query_audit_log_for_sync(db.db(),"reset",std::nullopt,0);
    ASSERT_EQ(pending.size(),1u);EXPECT_GT(pending[0].id,cutoff);
}

TEST(SyncDelivery, GenericRewindPreservesItsContractAndRefusesActiveFilteredState) {
    TempDB tmp{"outboxgenericreset"};lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"rewind",1,std::nullopt});const auto source=lattice::query_audit_log(db.db()).back();
    lattice::register_replication_slot(db.db(),"plain",false);lattice::advance_upload_floor(db.db(),"plain",source.id);
    db.reset_sync_state("plain");const auto pending=lattice::query_audit_log_for_sync(db.db(),"plain");
    ASSERT_EQ(pending.size(),1u);EXPECT_EQ(pending[0].global_id,source.global_id);
    ASSERT_TRUE(lattice::detail::admit_sync_delivery(db,"filtered",outbox_delivery(source)));
    EXPECT_THROW(db.reset_sync_state("filtered"),lattice::db_error);
    EXPECT_EQ(lattice::detail::read_sync_outbox(db.db(),"filtered").size(),1u);
    db.reset_filtered_peer_snapshot("filtered");
    EXPECT_THROW(db.reset_sync_state("filtered"),lattice::db_error);
    EXPECT_EQ(lattice::detail::read_sync_reset_cutoff(db.db(),"filtered"),source.id);
}

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(SyncDelivery, FreshResetRebuildsActualRelationshipJunctionsWithNewIdentities) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>();lattice::set_network_factory(factory);
        TempDB local{"outboxrelation"},remote{"outboxrelationpeer"};
        auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(local.str()));
        auto owner=db->add(TestOwner{"owner",nullptr});owner.pet=TestPet{"pet",15.0};
        const auto original=lattice::query_audit_log(db->db());
        const auto link_table=std::get<std::string>(db->db().query(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE '_TestOwner_TestPet_pet'")[0].at("name"));
        auto cfg=outbox_config("ipc:relation-reset");cfg.chunk_size=100;
        cfg.sync_filter=std::vector<lattice::sync_filter_entry>{{"TestOwner",std::nullopt},{"TestPet",std::nullopt}};
        db->reset_filtered_peer_snapshot(cfg.sync_id);
        lattice::lattice_db receiver{lattice::configuration(remote.str())};
        auto seed=receiver.add(TestOwner{"seed",nullptr});seed.pet=TestPet{"seed pet",1.0};
        receiver.db().execute("DELETE FROM "+link_table);receiver.db().execute("DELETE FROM TestOwner");receiver.db().execute("DELETE FROM TestPet");receiver.db().execute("DELETE FROM AuditLog");
        lattice::synchronizer sync(std::move(db),cfg);sync.connect();auto* transport=factory->last_websocket();
        const auto sent=outbox_sent(*transport);ASSERT_EQ(sent.size(),3u);
        bool found_link=false;
        for(const auto& entry:sent) {
            EXPECT_TRUE(entry.synthesized);
            for(const auto& old:original) EXPECT_NE(entry.global_id,old.global_id);
            if(entry.table_name==link_table) found_link=true;
        }
        EXPECT_TRUE(found_link);
        EXPECT_EQ(lattice::apply_remote_changes_for(receiver,sent,"peer").size(),sent.size());
        EXPECT_EQ(outbox_number(receiver,"SELECT COUNT(*) AS n FROM TestOwner"),1);
        EXPECT_EQ(outbox_number(receiver,"SELECT COUNT(*) AS n FROM TestPet"),1);
        EXPECT_EQ(outbox_number(receiver,"SELECT COUNT(*) AS n FROM "+link_table),1);
    });
}

TEST(SyncDelivery, FreshSnapshotPagesResumeUntilEveryCurrentRowIsAcknowledged) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>();lattice::set_network_factory(factory);
        TempDB tmp{"outboxsnapshotpages"};auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));auto* owned=db.get();
        db->begin_transaction();for(int i=0;i<257;++i) db->add(TestPerson{"row"+std::to_string(i),i,std::nullopt});db->commit();
        auto cfg=outbox_config("snapshot-pages");cfg.chunk_size=128;db->reset_filtered_peer_snapshot(cfg.sync_id);
        lattice::synchronizer sync(std::move(db),cfg);sync.connect();auto* transport=factory->last_websocket();
        EXPECT_LE(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).size(),128u);
        for(int pass=0;pass<3;++pass) {
            sync.sync_now(); // repeated upload passes cannot accumulate unACKed pages
            EXPECT_LE(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).size(),128u);
        }
        std::set<std::string> acked,rows;
        for(int pass=0;pass<300 && rows.size()<257;++pass) {
            std::vector<std::string> ids;
            for(const auto& entry:outbox_sent(*transport)) if(acked.insert(entry.global_id).second) {
                EXPECT_TRUE(entry.synthesized);ids.push_back(entry.global_id);rows.insert(entry.global_row_id);
            }
            ASSERT_FALSE(ids.empty());outbox_ack(*transport,ids);
        }
        EXPECT_EQ(rows.size(),257u);
        EXPECT_TRUE(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).empty());
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set WHERE sync_id=?",{cfg.sync_id}),257);
    });
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
bool outbox_wait_quiescent(lattice::lattice_db& db,const std::string& channel) {
    const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
    do {
        try {lattice::detail::with_quiescent_sync_channel(db,channel,[] {});return true;}
        catch(const lattice::db_error&) {std::this_thread::sleep_for(std::chrono::milliseconds(10));}
    } while(std::chrono::steady_clock::now()<deadline);
    return false;
}
}
TEST(SyncDelivery, RetirementRefusesDisconnectedLifetimeAndPermitsReuseAfterDestruction) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>();lattice::set_network_factory(factory);
        TempDB tmp{"outboxretirelifetime"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"retire",1,std::nullopt});const auto cfg=outbox_config("ipc:reusable");
        observer.reset_filtered_peer_snapshot(cfg.sync_id);
        auto sync=std::make_unique<lattice::synchronizer>(
            std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())),cfg);
        sync->connect();ASSERT_FALSE(lattice::detail::read_sync_outbox(observer.db(),cfg.sync_id).empty());
        sync->disconnect(); // stopping transport is insufficient: its old callbacks still exist
        EXPECT_THROW(observer.remove_sync_channel_state(cfg.sync_id),lattice::db_error);
        EXPECT_THROW(observer.reset_sync_state(cfg.sync_id),lattice::db_error);
        EXPECT_NO_THROW(observer.remove_sync_channel_state("another-channel"));
        EXPECT_GT(lattice::detail::read_sync_reset_cutoff(observer.db(),cfg.sync_id),0);
        sync.reset(); // destructor revokes callbacks; detached watchdog releases on its next check
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
        EXPECT_NO_THROW(observer.remove_sync_channel_state(cfg.sync_id));
        EXPECT_EQ(lattice::detail::read_sync_reset_cutoff(observer.db(),cfg.sync_id),0);
        EXPECT_TRUE(lattice::detail::read_sync_outbox(observer.db(),cfg.sync_id).empty());
        auto replacement=std::make_unique<lattice::synchronizer>(
            std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())),cfg);
        replacement->connect();EXPECT_FALSE(outbox_sent(*factory->last_websocket()).empty());
        EXPECT_THROW(observer.remove_sync_channel_state(cfg.sync_id),lattice::db_error);
        replacement.reset();ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
        EXPECT_NO_THROW(observer.remove_sync_channel_state(cfg.sync_id));
    });
}

TEST(SyncDelivery, RetirementUsesFileIdentityAcrossHardLinkAliases) {
    TempDB path{"outboxidentity"},alias{"outboxidentityalias"};
    {lattice::lattice_db initial{lattice::configuration(path.str())};initial.add(TestPerson{"alias",1,std::nullopt});}
    // Finish all WAL work through the original name before introducing an alias. SQLite WAL
    // sidecar paths are name-based; this fixture tests inode identity, not WAL alias support.
    {
        lattice::database setup{path.str(),lattice::database::open_mode::read_write_existing};
        const auto mode=setup.query("PRAGMA journal_mode=DELETE");
        ASSERT_EQ(mode.size(),1u);ASSERT_EQ(mode[0].size(),1u);
        const auto* actual=std::get_if<std::string>(&mode[0].begin()->second);
        ASSERT_NE(actual,nullptr);ASSERT_EQ(*actual,"delete");
    }
    std::filesystem::create_hard_link(path.path,alias.path);
    struct stat originalStat{},aliasStat{};
    ASSERT_EQ(::stat(path.str().c_str(),&originalStat),0);
    ASSERT_EQ(::stat(alias.str().c_str(),&aliasStat),0);
    ASSERT_EQ(originalStat.st_dev,aliasStat.st_dev);ASSERT_EQ(originalStat.st_ino,aliasStat.st_ino);
    // Neither alias writes. The guard must refuse before retirement SQL, independently of
    // write capability; first establish that both aliases really opened and see saved data.
    lattice::configuration first(path.str()),second(alias.str());first.read_only=true;second.read_only=true;
    lattice::lattice_db owner{first},other{second};
    ASSERT_EQ(sqlite3_db_readonly(owner.db().handle(),"main"),1);
    ASSERT_EQ(sqlite3_db_readonly(other.db().handle(),"main"),1);
    for(auto* store:{&owner,&other}) {
        const auto rows=store->db().query("SELECT name FROM TestPerson");
        ASSERT_EQ(rows.size(),1u);ASSERT_EQ(rows[0].size(),1u);
        const auto* name=std::get_if<std::string>(&rows[0].begin()->second);
        ASSERT_NE(name,nullptr);ASSERT_EQ(*name,"alias");
    }
    auto lifetime=lattice::detail::acquire_sync_channel_lifetime(owner,"alias");bool ran=false;
    EXPECT_THROW(lattice::detail::with_quiescent_sync_channel(other,"alias",[&] {ran=true;}),lattice::db_error);
    EXPECT_FALSE(ran);lifetime.reset();
    EXPECT_NO_THROW(lattice::detail::with_quiescent_sync_channel(other,"alias",[&] {ran=true;}));EXPECT_TRUE(ran);
}

TEST(SyncDelivery, RetirementDistinguishesPrivateMemoryAndSharesNamedMemoryPager) {
    lattice::lattice_db privateA{lattice::configuration(":memory:")},privateB{lattice::configuration(":memory:")};
    auto privateLifetime=lattice::detail::acquire_sync_channel_lifetime(privateA,"memory");
    EXPECT_THROW(privateA.remove_sync_channel_state("memory"),lattice::db_error);
    EXPECT_NO_THROW(privateB.remove_sync_channel_state("memory"));
    TempDB name{"outboxsharedmemory"};const std::string uri="file:"+name.str()+"?mode=memory&cache=shared";
    lattice::lattice_db sharedA{lattice::configuration(uri)},sharedB{lattice::configuration(uri)};
    auto sharedLifetime=lattice::detail::acquire_sync_channel_lifetime(sharedA,"memory");
    EXPECT_THROW(sharedB.remove_sync_channel_state("memory"),lattice::db_error);
    sharedLifetime.reset();EXPECT_NO_THROW(sharedB.remove_sync_channel_state("memory"));
}

TEST(SyncDelivery, RetirementReservationRejectsNewLifetimesAndUnwindsOnFailure) {
    TempDB tmp{"outboxretireexception"};lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"kept",1,std::nullopt});db.reset_filtered_peer_snapshot("retire");
    const auto cutoff=lattice::detail::read_sync_reset_cutoff(db.db(),"retire");
    EXPECT_THROW(lattice::detail::with_quiescent_sync_channel(db,"retire",[&] {
        EXPECT_THROW(lattice::detail::acquire_sync_channel_lifetime(db,"retire"),lattice::db_error);
        throw std::runtime_error("reservation unwind");
    }),std::runtime_error);
    {auto token=lattice::detail::acquire_sync_channel_lifetime(db,"retire");EXPECT_NE(token,nullptr);}
    db.db().execute("CREATE TRIGGER reject_retirement BEFORE DELETE ON _lattice_meta BEGIN SELECT RAISE(ABORT,'retirement rollback'); END");
    EXPECT_THROW(db.remove_sync_channel_state("retire"),lattice::db_error);
    EXPECT_EQ(lattice::detail::read_sync_reset_cutoff(db.db(),"retire"),cutoff);
    db.db().execute("DROP TRIGGER reject_retirement");
    db.begin_transaction();EXPECT_THROW(db.remove_sync_channel_state("retire"),lattice::db_error);db.rollback();
    EXPECT_EQ(lattice::detail::read_sync_reset_cutoff(db.db(),"retire"),cutoff);
    EXPECT_NO_THROW(db.remove_sync_channel_state("retire"));
    EXPECT_EQ(lattice::detail::read_sync_reset_cutoff(db.db(),"retire"),0);
}

TEST(SyncDelivery, FreshReplayAfterUnfilteredChannelUsesNewFilteredIdentities) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>();lattice::set_network_factory(factory);
        TempDB tmp{"outboxunfilteredreset"};auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));auto* owned=db.get();
        db->add(TestPerson{"before",1,std::nullopt});auto cfg=outbox_config("ipc:formerly-unfiltered");cfg.sync_filter=std::nullopt;
        lattice::synchronizer sync(std::move(db),cfg);sync.connect();auto* transport=factory->last_websocket();
        const auto old=outbox_sent(*transport);ASSERT_EQ(old.size(),1u);
        sync.update_sync_filter({{"TestPerson",std::nullopt}});transport->clear_sent_messages();
        transport->simulate_message(lattice::transport_message::from_string(lattice::server_sent_event::make_replay_request().to_json()));
        const auto fresh=outbox_sent(*transport);ASSERT_EQ(fresh.size(),1u);
        EXPECT_TRUE(fresh[0].synthesized);EXPECT_NE(fresh[0].global_id,old[0].global_id);
        outbox_ack(*transport,{old[0].global_id});
        EXPECT_EQ(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).size(),1u);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
        outbox_ack(*transport,{fresh[0].global_id});EXPECT_TRUE(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).empty());
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),1);
    });
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(SyncDelivery, QueuedGenericSchedulerAckIsRevokedBeforeDestructionAndHoldsRetirement) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>();lattice::set_network_factory(factory);
        outbox_queue queue;
        auto scheduler=std::make_shared<lattice::generic_scheduler>(&queue,
            std::function<void(std::function<void()>&&,void*)>([](std::function<void()>&& work,void* context) {
                static_cast<outbox_queue*>(context)->invoke(std::move(work));
            }),
            +[](void*) {return false;},
            +[](const lattice::scheduler*,void*) {return false;},
            +[](void*) {return true;}); // real generic_scheduler inherits no-op shutdown
        TempDB tmp{"outboxqueuedretire"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"queued",1,std::nullopt});auto cfg=outbox_config("ipc:queued-reuse");
        lattice::configuration configuration(tmp.str());configuration.sched=scheduler;
        auto sync=std::make_unique<lattice::synchronizer>(std::make_unique<lattice::lattice_db>(configuration),cfg);
        sync->connect();queue.drain();auto* transport=factory->last_websocket();
        const auto sent=outbox_sent(*transport);ASSERT_EQ(sent.size(),1u);
        outbox_ack(*transport,{sent[0].global_id}); // remains queued: no durable ACK yet
        ASSERT_EQ(lattice::detail::read_sync_outbox(observer.db(),cfg.sync_id).size(),1u);
        sync.reset();
        EXPECT_THROW(observer.remove_sync_channel_state(cfg.sync_id),lattice::db_error);
        queue.drain(); // revoked ACK must not touch the freed synchronizer or alter the receipt
        EXPECT_EQ(lattice::detail::read_sync_outbox(observer.db(),cfg.sync_id).size(),1u);
        EXPECT_EQ(outbox_number(observer,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
        // The detached ACK watchdog also owns a revoked gate until its next <=100ms check.
        bool quiescent=false;const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
        while(!quiescent && std::chrono::steady_clock::now()<deadline) {
            try {lattice::detail::with_quiescent_sync_channel(observer,cfg.sync_id,[&] {quiescent=true;});}
            catch(const lattice::db_error&) {std::this_thread::sleep_for(std::chrono::milliseconds(10));}
        }
        ASSERT_TRUE(quiescent);observer.remove_sync_channel_state(cfg.sync_id);
        auto replacement=std::make_unique<lattice::synchronizer>(
            std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())),cfg);
        replacement->connect();EXPECT_FALSE(outbox_sent(*factory->last_websocket()).empty());
    });
}

TEST(SyncDelivery, CallbackGateDrainsAdmittedWorkAndRevokesQueuedWork) {
    bounded_mock_case([] {
        TempDB tmp{"outboxgatedrain"};lattice::lattice_db db{lattice::configuration(tmp.str())};
        auto gate=std::make_shared<lattice::detail::sync_callback_gate>(
            lattice::detail::acquire_sync_channel_lifetime(db,"gate"));
        std::mutex mutex;std::condition_variable changed;bool entered=false,release=false;
        std::atomic<bool> revoked{false},stale_ran{false},timed_out{false};
        auto queued=lattice::detail::guard_sync_callback(gate,[&] {stale_ran=true;});
        std::thread active(lattice::detail::guard_sync_callback(gate,[&] {
            std::unique_lock<std::mutex> lock(mutex);entered=true;changed.notify_all();
            if(!changed.wait_for(lock,std::chrono::seconds(2),[&] {return release;})) timed_out=true;
        }));
        bool admitted;
        {std::unique_lock<std::mutex> lock(mutex);admitted=changed.wait_for(lock,std::chrono::seconds(2),[&] {return entered;});}
        std::thread teardown([&] {revoked=gate->revoke_and_wait();});
        EXPECT_TRUE(admitted);EXPECT_FALSE(revoked.load());
        EXPECT_THROW(db.remove_sync_channel_state("gate"),lattice::db_error);
        {std::lock_guard<std::mutex> lock(mutex);release=true;}changed.notify_all();
        active.join();teardown.join();EXPECT_FALSE(timed_out.load());EXPECT_TRUE(revoked.load());
        queued();EXPECT_FALSE(stale_ran.load());
        // The queued wrapper still owns its reservation even after being invoked. Dropping it
        // (as scheduler work-item cleanup does) and the caller's gate permits retirement.
        gate.reset();EXPECT_THROW(db.remove_sync_channel_state("gate"),lattice::db_error);
    });
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(SyncDelivery, NativeManagedAckCallbackRetainsSynchronizerAndDatabaseThroughOwnerRelease) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>();lattice::set_network_factory(factory);
        TempDB tmp{"outboxmanagedrelease"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"owned",1,std::nullopt});
        lattice::configuration config(tmp.str(),std::make_shared<lattice::std_thread_scheduler>());
        const std::weak_ptr<lattice::scheduler> scheduler=config.sched;
        auto db=std::make_unique<lattice::lattice_db>(config);auto* owned=db.get();config.sched.reset();
        const auto cfg=outbox_config("ipc:managed-release");
        auto sync=lattice::synchronizer::create_owned(std::move(db),cfg);
        const std::weak_ptr<lattice::synchronizer> weak=sync;
        std::atomic<bool> completed{false},retained{false},read_succeeded{false};
        sync->set_on_sync_complete([&](const std::vector<std::string>&) {
            sync.reset();retained=!weak.expired();
            read_succeeded=outbox_number(*owned,"SELECT COUNT(*) AS n FROM TestPerson")==1;
            completed=true;
        });
        sync->connect();auto* transport=factory->last_websocket();
        auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
        while(outbox_sent(*transport).empty() && std::chrono::steady_clock::now()<deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        const auto sent=outbox_sent(*transport);ASSERT_EQ(sent.size(),1u);
        outbox_ack(*transport,{sent[0].global_id});
        deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
        while(!completed && std::chrono::steady_clock::now()<deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        ASSERT_TRUE(completed.load());EXPECT_TRUE(retained.load());EXPECT_TRUE(read_succeeded.load());
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
        EXPECT_TRUE(weak.expired());EXPECT_TRUE(scheduler.expired());
        EXPECT_NO_THROW(observer.remove_sync_channel_state(cfg.sync_id));
    });
}

TEST(SyncDelivery, ThreadSchedulerSelfReleaseKeepsQueuedLoopStateAlive) {
    bounded_mock_case([] {
        auto scheduler=std::make_shared<lattice::std_thread_scheduler>();
        const std::weak_ptr<lattice::std_thread_scheduler> weak=scheduler;
        std::mutex mutex;std::condition_variable changed;bool release=false;
        std::atomic<bool> queued_ran{false},timed_out{false};
        scheduler->invoke([&] {
            {std::unique_lock<std::mutex> lock(mutex);
             if(!changed.wait_for(lock,std::chrono::seconds(2),[&] {return release;})) timed_out=true;}
            scheduler.reset(); // last scheduler release occurs inside its own worker
        });
        scheduler->invoke([&] {queued_ran=true;});
        {std::lock_guard<std::mutex> lock(mutex);release=true;}changed.notify_all();
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
        while(!queued_ran && std::chrono::steady_clock::now()<deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        EXPECT_FALSE(timed_out.load());EXPECT_TRUE(queued_ran.load());EXPECT_TRUE(weak.expired());
    });
}

TEST(SyncDelivery, ThreadSchedulerExternalShutdownJoinsThroughConcurrentSelfShutdown) {
    bounded_mock_case([] {
        auto scheduler=std::make_shared<lattice::std_thread_scheduler>();auto* raw=scheduler.get();
        std::mutex mutex;std::condition_variable changed;bool entered=false,release=false;
        std::atomic<bool> joined{false},queued_ran{false},after_shutdown{false},timed_out{false};
        scheduler->invoke([&] {
            {std::unique_lock<std::mutex> lock(mutex);entered=true;changed.notify_all();
             if(!changed.wait_for(lock,std::chrono::seconds(2),[&] {return release;})) timed_out=true;}
            raw->shutdown(); // external caller may already own the thread handle for joining
        });
        scheduler->invoke([&] {queued_ran=true;});
        bool admitted;
        {std::unique_lock<std::mutex> lock(mutex);admitted=changed.wait_for(lock,std::chrono::seconds(2),[&] {return entered;});}
        std::thread closer([&] {scheduler->shutdown();joined=true;});
        EXPECT_TRUE(admitted);EXPECT_FALSE(joined.load());
        {std::lock_guard<std::mutex> lock(mutex);release=true;}changed.notify_all();closer.join();
        EXPECT_FALSE(timed_out.load());EXPECT_TRUE(joined.load());EXPECT_TRUE(queued_ran.load());EXPECT_FALSE(scheduler->can_invoke());
        scheduler->invoke([&] {after_shutdown=true;});EXPECT_FALSE(after_shutdown.load());
    });
}
#endif


#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
struct publication_signal {
    std::mutex mutex;std::condition_variable changed;bool signalled=false;
    void set() {{std::lock_guard<std::mutex> lock(mutex);signalled=true;}changed.notify_all();}
    bool wait(std::chrono::milliseconds timeout=std::chrono::seconds(3)) {std::unique_lock<std::mutex> lock(mutex);return changed.wait_for(lock,timeout,[&] {return signalled;});}
};
struct publication_transport_state {
    std::atomic<int> connects{0},disconnects{0},audit_attempts{0};
    std::function<void()> before_disconnect,before_audit;
    std::mutex messages_mutex;std::vector<std::string> audit_messages;
    std::vector<std::string> messages() {std::lock_guard<std::mutex> lock(messages_mutex);return audit_messages;}
};
class publication_transport final : public lattice::mock_sync_transport {
    const std::shared_ptr<publication_transport_state> probe_;
public:
    explicit publication_transport(std::shared_ptr<publication_transport_state> probe):probe_(std::move(probe)) {}
    void connect(const std::string& url,const std::map<std::string,std::string>& headers={}) override {
        ++probe_->connects;lattice::mock_sync_transport::connect(url,headers);
    }
    void disconnect() override {
        ++probe_->disconnects;if(probe_->before_disconnect) probe_->before_disconnect();
        lattice::mock_sync_transport::disconnect();
    }
    void send(const lattice::transport_message& message) override {
        const auto json=message.as_string();
        if(json.find("\"kind\":\"auditLog\"")!=std::string::npos) {
            ++probe_->audit_attempts;if(probe_->before_audit) probe_->before_audit();
            {std::lock_guard<std::mutex> lock(probe_->messages_mutex);probe_->audit_messages.push_back(json);}
        }
        lattice::mock_sync_transport::send(message);
    }
};
std::shared_ptr<lattice::synchronizer> publication_owner(const std::string& path,
        const lattice::sync_config& cfg,const std::shared_ptr<publication_transport_state>& probe) {
    lattice::configuration config(path,std::make_shared<lattice::std_thread_scheduler>());
    std::unique_ptr<lattice::sync_transport> transport=std::make_unique<publication_transport>(probe);
    return lattice::synchronizer::create_owned(std::make_unique<lattice::lattice_db>(config),cfg,std::move(transport));
}
bool publication_lock_available(const std::string& path) {
    const int fd=::open(path.c_str(),O_RDWR);
    if(fd<0) throw std::runtime_error("Cannot inspect fixture ownership lock");
    const bool acquired=::flock(fd,LOCK_EX|LOCK_NB)==0;
    if(acquired) ::flock(fd,LOCK_UN);::close(fd);return acquired;
}
struct publication_paths {
    std::string socket,lock;
    explicit publication_paths(const TempDB& tmp,bool ipc) {
        if(ipc) {
            const char* configured=std::getenv("LATTICE_IPC_TEST_DIRECTORY");
            const auto directory=configured ? std::filesystem::path(configured) : tmp.path.parent_path();
            socket=(directory/(tmp.path.stem().string()+".sock")).string();lock=socket+".lock";
            // Darwin's sockaddr_un.sun_path is 104 bytes. The qualification harness supplies
            // a short explicit localdev directory; do not silently fall back to /tmp.
            if(socket.size()>=104) throw std::runtime_error("Set LATTICE_IPC_TEST_DIRECTORY to a shorter test directory");
        } else lock=tmp.str()+".sync.lock";
    }
    ~publication_paths() {std::error_code ignored;if(!socket.empty()) std::filesystem::remove(socket,ignored);std::filesystem::remove(lock,ignored);}
};
class publication_network_factory final : public lattice::network_factory {
public:
    std::atomic<int> creations{0};std::atomic<bool> fail_next{false};
    std::function<void()> before_create;
    const std::shared_ptr<publication_transport_state> probe=std::make_shared<publication_transport_state>();
    std::unique_ptr<lattice::http_client> create_http_client() override {return std::make_unique<lattice::null_http_client>();}
    std::unique_ptr<lattice::sync_transport> create_sync_transport() override {
        if(fail_next.exchange(false)) throw std::runtime_error("fixture transport creation failure");
        if(before_create) before_create();++creations;return std::make_unique<publication_transport>(probe);
    }
};
}

TEST(SyncPublication, RevokedAcceptanceCannotPublishOrConnectAfterFactoryReturns) {
    bounded_mock_case([] {
        TempDB tmp{"pubrevoked"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        auto cfg=outbox_config("ipc:revoked-publication");publication_signal entered,release;
        auto probe=std::make_shared<publication_transport_state>();std::atomic<bool> timed_out{false};
        auto state=std::make_shared<lattice::detail::ipc_publication_state>(cfg,
            [&](auto,const auto& settings) {entered.set();if(!release.wait()) timed_out=true;return publication_owner(tmp.str(),settings,probe);});
        std::thread accepting([&] {state->accept(nullptr);});
        const bool reached=entered.wait();auto retired=state->revoke();release.set();accepting.join();
        EXPECT_TRUE(reached);EXPECT_FALSE(timed_out);EXPECT_EQ(retired,nullptr);EXPECT_EQ(state->current(),nullptr);
        EXPECT_EQ(probe->connects,0);EXPECT_FALSE(state->accepting());
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
    });
}

TEST(SyncPublication, OlderFactoryCannotReplaceNewerPublishedOwner) {
    bounded_mock_case([] {
        TempDB tmp{"pubreplace"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        auto cfg=outbox_config("ipc:replace-publication");publication_signal first_entered,release;
        auto first=std::make_shared<publication_transport_state>(),second=std::make_shared<publication_transport_state>();
        std::atomic<int> count{0};std::atomic<bool> timed_out{false};
        auto state=std::make_shared<lattice::detail::ipc_publication_state>(cfg,[&](auto,const auto& settings) {
            const int ordinal=++count;
            if(ordinal==1) {first_entered.set();if(!release.wait()) timed_out=true;}
            return publication_owner(tmp.str(),settings,ordinal==1 ? first : second);
        });
        std::thread accepting([&] {state->accept(nullptr);});const bool reached=first_entered.wait();
        state->accept(nullptr);auto published=state->current();release.set();accepting.join();
        EXPECT_TRUE(reached);EXPECT_FALSE(timed_out);ASSERT_NE(published,nullptr);
        EXPECT_EQ(state->current(),published);EXPECT_EQ(first->connects,0);EXPECT_EQ(second->connects,1);
        auto retired=state->revoke();retired->disconnect();retired.reset();published.reset();state.reset();
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
    });
}

TEST(SyncPublication, FailedNewerFactoryDoesNotCancelPublishedPredecessorActivation) {
    bounded_mock_case([] {
        TempDB tmp{"pubfailed"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        auto cfg=outbox_config("ipc:failed-successor");publication_signal disconnect_entered,release;
        auto first=std::make_shared<publication_transport_state>(),second=std::make_shared<publication_transport_state>();
        std::atomic<int> count{0};std::atomic<bool> blocked{false},timed_out{false};
        first->before_disconnect=[&] {if(!blocked.exchange(true)) {disconnect_entered.set();if(!release.wait()) timed_out=true;}};
        auto state=std::make_shared<lattice::detail::ipc_publication_state>(cfg,[&](auto,const auto& settings) {
            const int ordinal=++count;if(ordinal==3) throw std::runtime_error("newer factory failed");
            return publication_owner(tmp.str(),settings,ordinal==1 ? first : second);
        });
        state->accept(nullptr);
        std::thread replacement([&] {state->accept(nullptr);});const bool reached=disconnect_entered.wait();
        auto published=state->current();state->accept(nullptr);EXPECT_TRUE(state->failure().has_value());
        release.set();replacement.join();EXPECT_TRUE(reached);EXPECT_FALSE(timed_out);ASSERT_NE(published,nullptr);
        EXPECT_EQ(state->current(),published);EXPECT_EQ(second->connects,1);EXPECT_FALSE(state->failure().has_value());
        auto retired=state->revoke();retired->disconnect();retired.reset();published.reset();state.reset();
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
    });
}

TEST(SyncPublication, SendProgressDoesNotHoldConnectionLockAgainstFilterPublication) {
    bounded_mock_case([] {
        TempDB tmp{"pubfilter"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"pending",1,std::nullopt});auto cfg=outbox_config("ipc:progress-filter");
        auto probe=std::make_shared<publication_transport_state>();publication_signal progress_entered,filter_returned,completed;
        std::atomic<bool> once{false},timed_out{false};
        auto state=std::make_shared<lattice::detail::ipc_publication_state>(cfg,
            [&](auto,const auto& settings) {return publication_owner(tmp.str(),settings,probe);});
        state->set_progress([&](const auto& progress) {
            if(progress.pending_upload>0 && !once.exchange(true)) {
                progress_entered.set();if(!filter_returned.wait()) timed_out=true;completed.set();
            }
        });
        state->accept(nullptr);const bool reached=progress_entered.wait();
        // Before the correction this request waits for the connection lock held by the callback
        // above; the callback times out waiting for this exact request, proving the reverse edge.
        state->update_filter(std::vector<lattice::sync_filter_entry>{});filter_returned.set();
        EXPECT_TRUE(reached);EXPECT_TRUE(completed.wait());EXPECT_FALSE(timed_out);
        auto retired=state->revoke();retired->disconnect();retired.reset();state.reset();
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
    });
}

TEST(SyncPublication, ReentrantDrainReturnsWithoutClaimingOrDiscardingPendingDelivery) {
    bounded_mock_case([] {
        TempDB tmp{"pubdrain"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"pending",1,std::nullopt});auto cfg=outbox_config("ipc:reentrant-drain");
        auto probe=std::make_shared<publication_transport_state>();publication_signal completed;std::atomic<bool> once{false};
        auto state=std::make_shared<lattice::detail::ipc_publication_state>(cfg,
            [&](auto,const auto& settings) {return publication_owner(tmp.str(),settings,probe);});
        state->set_progress([&](const auto& progress) {
            if(progress.pending_upload>0 && !once.exchange(true)) {
                auto current=state->current();
                // A self-scheduler wait cannot complete. The bounded subprocess alarm catches
                // regression without relying on a fragile elapsed-time threshold.
                current->drain(std::chrono::steady_clock::time_point::max());
                EXPECT_GT(current->get_progress().pending_upload,0);completed.set();
            }
        });
        state->accept(nullptr);ASSERT_TRUE(completed.wait());
        EXPECT_EQ(outbox_number(observer,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
        EXPECT_EQ(lattice::detail::read_sync_outbox(observer.db(),cfg.sync_id).size(),1u);
        auto retired=state->revoke();retired->disconnect();retired.reset();state.reset();
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
    });
}

TEST(SyncPublication, ThrowingEnqueueRetainsReceiptAndArmsExactRetryOutsideRoutingLock) {
    bounded_mock_case([] {
        TempDB tmp{"pubsendthrow"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"retry",1,std::nullopt});auto cfg=outbox_config("ipc:throwing-enqueue");cfg.ack_timeout_base_ms=20;
        auto probe=std::make_shared<publication_transport_state>();std::atomic<bool> first{true};
        probe->before_audit=[&] {if(first.exchange(false)) throw std::runtime_error("uncertain send");};
        publication_signal failed,retried;
        auto state=std::make_shared<lattice::detail::ipc_publication_state>(cfg,
            [&](auto,const auto& settings) {return publication_owner(tmp.str(),settings,probe);});
        state->set_error([&](const auto&) {failed.set();});state->set_progress([&](const auto&) {if(probe->audit_attempts>=2) retried.set();});
        state->accept(nullptr);ASSERT_TRUE(failed.wait());
        const auto before=lattice::detail::read_sync_outbox(observer.db(),cfg.sync_id);ASSERT_EQ(before.size(),1u);
        ASSERT_TRUE(retried.wait());const auto after=lattice::detail::read_sync_outbox(observer.db(),cfg.sync_id);
        ASSERT_EQ(after.size(),1u);EXPECT_EQ(before[0].wire_json,after[0].wire_json);EXPECT_EQ(before[0].wire.global_id,after[0].wire.global_id);
        EXPECT_EQ(outbox_number(observer,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
        auto retired=state->revoke();retired->disconnect();retired.reset();state.reset();
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
    });
}

TEST(SyncPublication, RealIpcCloseFromProgressDrainsEndpointAndRetainsFlockUntilCallbackReturns) {
    bounded_mock_case([] {
        TempDB tmp{"pubipc"};publication_paths paths(tmp,true);lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"native IPC",1,std::nullopt});
        lattice::configuration config(tmp.str());config.ipc_targets.push_back({"fixture",std::nullopt,paths.socket});
        auto owner=std::make_unique<lattice::lattice_db>(config);
        publication_signal closed,release,completed;std::atomic<bool> once{false},lock_held{false},timed_out{false};
        owner->set_on_sync_progress([&](const auto& progress) {
            if(progress.sync_id=="ipc:fixture" && progress.pending_upload>0 && !once.exchange(true)) {
                owner->close();lock_held=!publication_lock_available(paths.lock);closed.set();
                if(!release.wait()) timed_out=true;completed.set();
            }
        });
        lattice::ipc_socket_client peer(paths.socket);peer.connect("");const bool reached=closed.wait();
        // Endpoint stop joins the accept loop and unlinks its socket before close returns.
        EXPECT_FALSE(std::filesystem::exists(paths.socket));EXPECT_TRUE(owner->is_closed());EXPECT_TRUE(lock_held);
        release.set();EXPECT_TRUE(completed.wait());peer.disconnect();owner.reset();
        EXPECT_TRUE(reached);EXPECT_FALSE(timed_out);ASSERT_TRUE(outbox_wait_quiescent(observer,"ipc:fixture"));
        EXPECT_TRUE(publication_lock_available(paths.lock));
        // A fresh same-ID owner is allowed only after actual teardown. The pending source still exists.
        lattice::lattice_db replacement(config);EXPECT_TRUE(replacement.is_sync_agent());replacement.close();
    });
}

TEST(SyncPublication, WssCloseKeepsFlockAndDefersSiblingHandoffUntilCallbackRelease) {
    bounded_mock_case([] {
        auto factory=std::make_shared<publication_network_factory>();lattice::set_network_factory(factory);
        TempDB tmp{"pubwss"};publication_paths paths(tmp,false);lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"native WSS",1,std::nullopt});
        lattice::configuration config(tmp.str(),"ws://fixture","token");config.sync_autostart=false;
        auto owner=std::make_unique<lattice::lattice_db>(config);auto sibling=std::make_unique<lattice::lattice_db>(config);
        publication_signal closed,release,completed;std::atomic<bool> once{false},lock_held{false},timed_out{false};
        owner->set_on_sync_progress([&](const auto& progress) {
            if(progress.pending_upload>0 && !once.exchange(true)) {
                owner->close();lock_held=!publication_lock_available(paths.lock);closed.set();
                if(!release.wait()) timed_out=true;completed.set();
            }
        });
        owner->connect_sync();const bool reached=closed.wait();
        sibling->connect_sync(); // records desired ownership, but cannot steal the callback's flock/key
        EXPECT_FALSE(sibling->is_sync_connected());EXPECT_EQ(factory->creations,1);EXPECT_TRUE(lock_held);
        release.set();EXPECT_TRUE(completed.wait());EXPECT_TRUE(reached);EXPECT_FALSE(timed_out);
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(3);
        while(!sibling->is_sync_connected() && std::chrono::steady_clock::now()<deadline) std::this_thread::sleep_for(std::chrono::milliseconds(10));
        EXPECT_TRUE(sibling->is_sync_connected());EXPECT_EQ(factory->creations,2);
        // Clear the shared user callback before fixture captures leave scope.
        sibling->set_on_sync_progress({});sibling->close();owner.reset();sibling.reset();
        ASSERT_TRUE(outbox_wait_quiescent(observer,"wss:ws://fixture"));EXPECT_TRUE(publication_lock_available(paths.lock));
    });
}

TEST(SyncPublication, FailedWssSetupReleasesKeyAndFlockForSameOwnerRetry) {
    bounded_mock_case([] {
        auto factory=std::make_shared<publication_network_factory>();lattice::set_network_factory(factory);
        TempDB tmp{"pubretry"};publication_paths paths(tmp,false);
        lattice::configuration config(tmp.str(),"ws://fixture-retry","token");config.sync_autostart=false;
        lattice::lattice_db owner(config);factory->fail_next=true;
        EXPECT_THROW(owner.connect_sync(),std::runtime_error);EXPECT_FALSE(owner.is_sync_agent());
        EXPECT_TRUE(publication_lock_available(paths.lock));EXPECT_NO_THROW(owner.connect_sync());
        EXPECT_TRUE(owner.is_sync_agent());owner.close();
    });
}
#endif


#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(SyncPublication, StateHandlerReplacementAllowsCapturedCallbackToFinishAndReenterSetter) {
    bounded_mock_case([] {
        TempDB tmp{"pubstatehandler"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        auto cfg=outbox_config("state-handler");auto probe=std::make_shared<publication_transport_state>();
        auto sync=publication_owner(tmp.str(),cfg,probe);publication_signal entered,release,new_called;
        std::atomic<int> old_calls{0},new_calls{0};std::atomic<bool> timed_out{false},old_finished{false};
        sync->set_on_state_change([&](bool connected) {
            if(!connected) return;++old_calls;
            sync->set_on_state_change({}); // reentrant setter must not wait on the callback's own lock
            entered.set();if(!release.wait()) timed_out=true;old_finished=true;
        });
        sync->connect();const bool reached=entered.wait();
        sync->set_on_state_change([&](bool connected) {if(connected) {++new_calls;new_called.set();}});
        release.set();EXPECT_TRUE(new_called.wait());EXPECT_TRUE(reached);EXPECT_FALSE(timed_out);
        EXPECT_TRUE(old_finished);EXPECT_EQ(old_calls,1);EXPECT_EQ(new_calls,1);
        sync->set_on_state_change({});sync->disconnect();sync.reset();
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
    });
}

TEST(SyncPublication, ErrorHandlerReplacementAndCallbackOwnerReleaseKeepAdmittedStateAlive) {
    bounded_mock_case([] {
        TempDB tmp{"puberrorhandler"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        auto cfg=outbox_config("error-handler");
        lattice::configuration config(tmp.str(),std::make_shared<lattice::std_thread_scheduler>());
        auto transport=std::make_unique<lattice::mock_sync_transport>();auto* raw=transport.get();
        auto sync=lattice::synchronizer::create_owned(std::make_unique<lattice::lattice_db>(config),cfg,std::move(transport));
        const std::weak_ptr<lattice::synchronizer> weak=sync;publication_signal entered,release,old_finished,new_called;
        std::atomic<int> old_calls{0},new_calls{0};std::atomic<bool> timed_out{false},retained{false};
        sync->set_on_error([&](const auto&) {
            ++old_calls;sync->set_on_error({});entered.set();if(!release.wait()) timed_out=true;old_finished.set();
        });
        // A valid rejection reports an error without retiring the transport.
        const auto rejection = lattice::server_sent_event::make_nack({"probe"}, "probe failure").to_json();
        sync->connect();raw->simulate_message(lattice::transport_message::from_string(rejection));
        const bool reached=entered.wait();
        sync->set_on_error([&](const auto&) {
            ++new_calls;sync->disconnect();sync.reset();retained=!weak.expired();new_called.set();
        });
        release.set();EXPECT_TRUE(old_finished.wait());
        raw->simulate_message(lattice::transport_message::from_string(rejection));
        EXPECT_TRUE(new_called.wait());EXPECT_TRUE(reached);EXPECT_FALSE(timed_out);EXPECT_TRUE(retained);
        EXPECT_EQ(old_calls,1);EXPECT_EQ(new_calls,1);
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));EXPECT_TRUE(weak.expired());
    });
}

TEST(SyncPublication, LiveOwnerNoopSetupKeepsFilterAdmissionOpen) {
    bounded_mock_case([] {
        struct retryable_owner : lattice::lattice_db {
            using lattice::lattice_db::lattice_db;
            using lattice::lattice_db::setup_sync_if_configured;
        };
        auto factory=std::make_shared<publication_network_factory>();lattice::set_network_factory(factory);
        TempDB tmp{"pubwssnoop"};publication_paths paths(tmp,false);
        lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"excluded",1,std::nullopt});observer.add(TestPerson{"included",2,std::nullopt});
        lattice::configuration config(tmp.str(),"ws://fixture-noop","token");config.sync_autostart=false;
        config.sync_filter=std::vector<lattice::sync_filter_entry>{};
        retryable_owner owner(config);owner.connect_sync();
        const auto connected_deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
        while(!owner.is_sync_connected() && std::chrono::steady_clock::now()<connected_deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        ASSERT_TRUE(owner.is_sync_connected());ASSERT_TRUE(factory->probe->messages().empty());
        publication_signal disconnect_entered,release,setup_returned,uploaded;
        std::atomic<bool> timed_out{false},failed{false};
        factory->probe->before_disconnect=[&] {
            disconnect_entered.set();if(!release.wait(std::chrono::seconds(5))) timed_out=true;
        };
        factory->probe->before_audit=[&] {uploaded.set();};
        // Keep an actual live-owner borrow outstanding. No-op setup must not
        // start a transition and wait for that borrow before accepting policy.
        std::thread disconnecting([&] {try {owner.disconnect_sync();} catch(...) {failed=true;}});
        const bool reached=disconnect_entered.wait();
        std::thread retrying([&] {
            try {owner.setup_sync_if_configured(true);} catch(...) {failed=true;}
            setup_returned.set();
        });
        const bool returned_before_release=setup_returned.wait(std::chrono::seconds(1));
        try {owner.update_sync_filter({{"TestPerson",std::string("age=2")}});} catch(...) {failed=true;}
        release.set();disconnecting.join();retrying.join();factory->probe->before_disconnect={};
        EXPECT_TRUE(reached);EXPECT_TRUE(returned_before_release);EXPECT_FALSE(timed_out);EXPECT_FALSE(failed);
        owner.connect_sync();EXPECT_TRUE(uploaded.wait());
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
        auto messages=factory->probe->messages();
        while(messages.empty() && std::chrono::steady_clock::now()<deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));messages=factory->probe->messages();
        }
        ASSERT_FALSE(messages.empty());size_t rows=0;
        for(const auto& json:messages) {
            const auto event=lattice::server_sent_event::from_json(json);ASSERT_TRUE(event);
            for(const auto& row:event->audit_logs) {++rows;EXPECT_EQ(std::get<int64_t>(row.changed_fields.at("age").value),2);}
        }
        EXPECT_EQ(rows,1u);owner.close();ASSERT_TRUE(outbox_wait_quiescent(observer,"wss:ws://fixture-noop"));
    });
}

TEST(SyncPublication, WssNarrowingDuringFactoryAdmissionAppliesBeforeFirstUpload) {
    bounded_mock_case([] {
        auto factory=std::make_shared<publication_network_factory>();lattice::set_network_factory(factory);
        TempDB tmp{"pubwssfilter"};publication_paths paths(tmp,false);lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"excluded",1,std::nullopt});observer.add(TestPerson{"included",2,std::nullopt});
        lattice::configuration config(tmp.str(),"ws://fixture-filter","token");config.sync_autostart=false;
        lattice::lattice_db owner(config);publication_signal entered,release,uploaded;
        std::atomic<bool> timed_out{false},failed{false};
        factory->before_create=[&] {entered.set();if(!release.wait()) timed_out=true;};
        factory->probe->before_audit=[&] {uploaded.set();};
        std::thread starting([&] {try {owner.connect_sync();} catch(...) {failed=true;}});
        const bool reached=entered.wait();owner.update_sync_filter({{"TestPerson",std::string("age=2")}});
        release.set();starting.join();EXPECT_TRUE(reached);EXPECT_FALSE(timed_out);EXPECT_FALSE(failed);
        EXPECT_TRUE(uploaded.wait());
        // Wait for the sender to publish the recorded frame after its hook signals.
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
        auto messages=factory->probe->messages();
        while(messages.empty() && std::chrono::steady_clock::now()<deadline) {std::this_thread::sleep_for(std::chrono::milliseconds(5));messages=factory->probe->messages();}
        ASSERT_FALSE(messages.empty());size_t rows=0;
        for(const auto& json:messages) {
            const auto event=lattice::server_sent_event::from_json(json);ASSERT_TRUE(event);
            for(const auto& row:event->audit_logs) {++rows;EXPECT_EQ(std::get<int64_t>(row.changed_fields.at("age").value),2);}
        }
        EXPECT_EQ(rows,1u);owner.close();ASSERT_TRUE(outbox_wait_quiescent(observer,"wss:ws://fixture-filter"));
    });
}
#endif


#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(SyncPublication, DifferentUrlReplacementRetriesAfterOldCallbackReleasesOwnership) {
    bounded_mock_case([] {
        auto factory=std::make_shared<publication_network_factory>();lattice::set_network_factory(factory);
        TempDB tmp{"puburlhandoff"};publication_paths paths(tmp,false);lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"handoff",1,std::nullopt});
        lattice::configuration old_config(tmp.str(),"ws://old-fixture","token");old_config.sync_autostart=false;
        lattice::configuration new_config(tmp.str(),"ws://new-fixture","token");new_config.sync_autostart=false;
        lattice::lattice_db old_owner(old_config),new_owner(new_config);
        publication_signal entered,release,completed;std::atomic<bool> once{false},timed_out{false};
        old_owner.set_on_sync_progress([&](const auto& progress) {
            if(progress.sync_id=="wss:ws://old-fixture" && progress.pending_upload>0 && !once.exchange(true)) {
                entered.set();if(!release.wait()) timed_out=true;completed.set();
            }
        });
        old_owner.connect_sync();const bool reached=entered.wait();
        // This retires the different-URL owner, but its callback still owns the flock. The
        // bounded native drain may time out; the requested successor must retry on final release.
        new_owner.connect_sync();EXPECT_FALSE(new_owner.is_sync_connected());EXPECT_EQ(factory->creations,1);
        EXPECT_FALSE(publication_lock_available(paths.lock));release.set();EXPECT_TRUE(completed.wait());
        EXPECT_TRUE(reached);EXPECT_FALSE(timed_out);
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(3);
        while(!new_owner.is_sync_connected() && std::chrono::steady_clock::now()<deadline) std::this_thread::sleep_for(std::chrono::milliseconds(10));
        EXPECT_TRUE(new_owner.is_sync_connected());EXPECT_EQ(factory->creations,2);EXPECT_FALSE(old_owner.is_sync_connected());
        new_owner.set_on_sync_progress({});new_owner.close();old_owner.close();
        ASSERT_TRUE(outbox_wait_quiescent(observer,"wss:ws://old-fixture"));
        ASSERT_TRUE(outbox_wait_quiescent(observer,"wss:ws://new-fixture"));EXPECT_TRUE(publication_lock_available(paths.lock));
    });
}
#endif


#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(SyncPublication, CompletionHandlerReplacementKeepsCapturedAckAndReentrantCloseSafe) {
    bounded_mock_case([] {
        TempDB tmp{"pubackhandler"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"first",1,std::nullopt});observer.add(TestPerson{"second",2,std::nullopt});
        auto cfg=outbox_config("completion-handler");cfg.chunk_size=1;
        lattice::configuration config(tmp.str(),std::make_shared<lattice::std_thread_scheduler>());
        auto transport=std::make_unique<lattice::mock_sync_transport>();auto* raw=transport.get();
        auto sync=lattice::synchronizer::create_owned(std::make_unique<lattice::lattice_db>(config),cfg,std::move(transport));
        const std::weak_ptr<lattice::synchronizer> weak=sync;publication_signal entered,release,old_finished,new_called;
        std::atomic<int> old_calls{0},new_calls{0};std::atomic<bool> timed_out{false},retained{false};
        sync->set_on_sync_complete([&](const auto&) {
            ++old_calls;sync->set_on_sync_complete({});entered.set();if(!release.wait()) timed_out=true;old_finished.set();
        });
        sync->connect();const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(2);
        while(outbox_sent(*raw).size()<2 && std::chrono::steady_clock::now()<deadline) std::this_thread::sleep_for(std::chrono::milliseconds(10));
        const auto sent=outbox_sent(*raw);ASSERT_EQ(sent.size(),2u);outbox_ack(*raw,{sent[0].global_id});
        const bool reached=entered.wait();
        sync->set_on_sync_complete([&](const auto&) {
            ++new_calls;sync->disconnect();sync.reset();retained=!weak.expired();new_called.set();
        });
        release.set();EXPECT_TRUE(old_finished.wait());outbox_ack(*raw,{sent[1].global_id});
        EXPECT_TRUE(new_called.wait());EXPECT_TRUE(reached);EXPECT_FALSE(timed_out);EXPECT_TRUE(retained);
        EXPECT_EQ(old_calls,1);EXPECT_EQ(new_calls,1);
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));EXPECT_TRUE(weak.expired());
        EXPECT_TRUE(lattice::detail::read_sync_outbox(observer.db(),cfg.sync_id).empty());
        EXPECT_EQ(outbox_number(observer,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),2);
    });
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
class policy_admission_probe final : public lattice::synchronizer {
public:
    using lattice::synchronizer::synchronizer;
    using lattice::synchronizer::send_entries;
    std::unique_lock<std::recursive_mutex> hold_transport_gate() {return std::unique_lock<std::recursive_mutex>(connection_mutex_);}
    size_t reserved_count() {std::lock_guard<std::mutex> lock(in_flight_mutex_);return in_flight_ids_.size();}
};
}

TEST(SyncPolicyAdmission, RequestBeforeFinalReservationRejectsPreparedFrameWithoutWaitingForTransportGate) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>();lattice::set_network_factory(factory);
        TempDB tmp{"policybefore"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"before",1,std::nullopt});auto cfg=outbox_config("policy-before");
        auto entries=lattice::query_audit_log_for_sync(observer.db(),cfg.sync_id);ASSERT_EQ(entries.size(),1u);
        auto queue=std::make_shared<outbox_queue>();lattice::configuration config(tmp.str());config.sched=queue;
        {
            policy_admission_probe sync(std::make_unique<lattice::lattice_db>(config),cfg);sync.connect();
            auto* wire=factory->last_websocket();auto held=sync.hold_transport_gate();
            publication_signal sender_started,request_returned;std::exception_ptr sending_error,request_error;
            std::thread sending([&] {sender_started.set();try {sync.send_entries(entries);} catch(...) {sending_error=std::current_exception();}});
            const bool started=sender_started.wait();
            std::thread requesting([&] {try {sync.update_sync_filter({});} catch(...) {request_error=std::current_exception();}request_returned.set();});
            const bool returned=request_returned.wait();
            // The request must publish while the transport gate is held elsewhere. Once the
            // prepared sender is admitted to that gate, its old policy epoch must fail validation.
            EXPECT_EQ(sync.reserved_count(),0u);held.unlock();requesting.join();sending.join();
            if(sending_error) std::rethrow_exception(sending_error);if(request_error) std::rethrow_exception(request_error);
            EXPECT_TRUE(started);EXPECT_TRUE(returned);EXPECT_TRUE(entries.empty());EXPECT_EQ(sync.reserved_count(),0u);
            EXPECT_TRUE(outbox_sent(*wire).empty());
            EXPECT_EQ(lattice::query_audit_log_for_sync(observer.db(),cfg.sync_id).size(),1u);
            sync.disconnect();
        }
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
    });
}

TEST(SyncPolicyAdmission, RequestAfterReservationReturnsDuringEnqueueAndRejectsEveryLaterFrame) {
    bounded_mock_case([] {
        TempDB tmp{"policyafter"};lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"first",1,std::nullopt});observer.add(TestPerson{"later",2,std::nullopt});
        auto cfg=outbox_config("policy-after");cfg.chunk_size=1;
        auto entries=lattice::query_audit_log_for_sync(observer.db(),cfg.sync_id);ASSERT_EQ(entries.size(),2u);
        const auto first_id=entries[0].global_id;
        auto queue=std::make_shared<outbox_queue>();lattice::configuration config(tmp.str());config.sched=queue;
        auto probe=std::make_shared<publication_transport_state>();publication_signal admitted,release,request_returned;
        std::atomic<bool> timed_out{false};probe->before_audit=[&] {admitted.set();if(!release.wait()) timed_out=true;};
        {
            policy_admission_probe sync(std::make_unique<lattice::lattice_db>(config),cfg,std::make_unique<publication_transport>(probe));
            sync.connect();std::exception_ptr sending_error,request_error;
            std::thread sending([&] {try {sync.send_entries(entries);} catch(...) {sending_error=std::current_exception();}});
            const bool reached=admitted.wait();EXPECT_EQ(sync.reserved_count(),1u);
            std::thread requesting([&] {try {sync.update_sync_filter({});} catch(...) {request_error=std::current_exception();}request_returned.set();});
            const bool returned=request_returned.wait();
            // Exactly one bounded frame was admitted before the new request. It may finish;
            // the second frame must not reserve/send under the stale policy afterward.
            release.set();requesting.join();sending.join();
            if(sending_error) std::rethrow_exception(sending_error);if(request_error) std::rethrow_exception(request_error);
            EXPECT_TRUE(reached);EXPECT_TRUE(returned);EXPECT_FALSE(timed_out);ASSERT_EQ(entries.size(),1u);
            EXPECT_EQ(entries[0].global_id,first_id);EXPECT_EQ(probe->audit_attempts,1);
            const auto messages=probe->messages();ASSERT_EQ(messages.size(),1u);
            const auto sent=lattice::server_sent_event::from_json(messages[0]);ASSERT_TRUE(sent);ASSERT_EQ(sent->audit_logs.size(),1u);
            EXPECT_EQ(sent->audit_logs[0].global_id,first_id);
            EXPECT_EQ(lattice::query_audit_log_for_sync(observer.db(),cfg.sync_id).size(),2u);
            EXPECT_EQ(outbox_number(observer,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
            sync.disconnect();
        }
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
    });
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
class upload_suppression_probe final : public lattice::synchronizer {
public:
    using lattice::synchronizer::synchronizer;
    using lattice::synchronizer::send_entries;
    using lattice::synchronizer::reconcile_sync_filter;
    using lattice::synchronizer::upload_pending_changes;
};
}

TEST(SyncUploadsDisabled, LocalWritesAndFilterChangesStayLocalWhileDownloadsAreAcknowledged) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"downloadonly"}; auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
        auto* owned=db.get(); db->add(TestPerson{"local",1,std::nullopt});
        auto cfg=outbox_config("wss:downloadonly"); cfg.uploads_enabled=false;
        upload_suppression_probe sync(std::move(db),cfg); sync.connect();
        auto* transport=factory->last_websocket(); ASSERT_NE(transport,nullptr);
        auto pending=lattice::query_audit_log_for_sync(owned->db(),cfg.sync_id); ASSERT_EQ(pending.size(),1u);
        sync.send_entries(pending); sync.reconcile_sync_filter(); sync.upload_pending_changes();
        sync.update_sync_filter({}); sync.clear_sync_filter();
        owned->add(TestPerson{"later-local",2,std::nullopt}); sync.upload_pending_changes();
        EXPECT_TRUE(outbox_sent(*transport).empty());
        EXPECT_TRUE(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).empty());
        EXPECT_EQ(lattice::read_upload_floor(owned->db(),cfg.sync_id),0);
        EXPECT_EQ(lattice::query_audit_log_for_sync(owned->db(),cfg.sync_id).size(),2u);

        TempDB remote_file{"downloadsource"}; lattice::lattice_db remote{lattice::configuration(remote_file.str())};
        remote.add(TestPerson{"downloaded",3,std::nullopt});
        const auto incoming=lattice::query_audit_log(remote.db()); ASSERT_EQ(incoming.size(),1u);
        transport->simulate_message(lattice::transport_message::from_string(
            lattice::server_sent_event::make_audit_log(incoming).to_json()));
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM TestPerson WHERE name='downloaded'"),1);
        bool acknowledged=false;
        for(const auto& message:transport->get_sent_messages()) {
            const auto event=lattice::server_sent_event::from_json(message.as_string());
            if(event && event->event_type==lattice::server_sent_event::type::ack)
                acknowledged |= std::find(event->acked_ids.begin(),event->acked_ids.end(),incoming[0].global_id)!=event->acked_ids.end();
        }
        EXPECT_TRUE(acknowledged); EXPECT_TRUE(outbox_sent(*transport).empty());
        sync.disconnect(); sync.connect();
        EXPECT_TRUE(outbox_sent(*factory->last_websocket()).empty());
    });
}

TEST(SyncUploadsDisabled, RetainedDeliverySurvivesDownloadOnlyReopenAndResumesExactIdentity) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"retaineddownload"}; auto cfg=outbox_config("wss:retaineddownload");
        lattice::lattice_db observer{lattice::configuration(tmp.str())};
        observer.add(TestPerson{"pending",1,std::nullopt});
        {
            auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
            lattice::synchronizer sync(std::move(db),cfg); sync.connect();
        }
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
        const auto original=lattice::detail::read_sync_outbox(observer.db(),cfg.sync_id);
        ASSERT_EQ(original.size(),1u);
        const auto floor=lattice::read_upload_floor(observer.db(),cfg.sync_id);
        const auto sources=observer.db().query("SELECT id,globalId,isSynchronized FROM AuditLog ORDER BY id");
        const auto states=observer.db().query("SELECT * FROM _lattice_sync_state ORDER BY sync_id,audit_entry_id");
        const auto expect_retained=[&] {
            const auto retained=lattice::detail::read_sync_outbox(observer.db(),cfg.sync_id);
            ASSERT_EQ(retained.size(),1u);
            EXPECT_EQ(retained[0].receipt_id,original[0].receipt_id);
            EXPECT_EQ(retained[0].source_id,original[0].source_id);
            EXPECT_EQ(retained[0].source_global_id,original[0].source_global_id);
            EXPECT_EQ(retained[0].wire.global_id,original[0].wire.global_id);
            EXPECT_EQ(retained[0].wire_json,original[0].wire_json);
            EXPECT_EQ(retained[0].routing_key,original[0].routing_key);
            EXPECT_EQ(retained[0].membership,original[0].membership);
            EXPECT_EQ(retained[0].charge,original[0].charge);
            EXPECT_EQ(retained[0].legacy_receipt,original[0].legacy_receipt);
            EXPECT_EQ(lattice::read_upload_floor(observer.db(),cfg.sync_id),floor);
            EXPECT_EQ(observer.db().query("SELECT id,globalId,isSynchronized FROM AuditLog ORDER BY id"),sources);
            EXPECT_EQ(observer.db().query("SELECT * FROM _lattice_sync_state ORDER BY sync_id,audit_entry_id"),states);
        };
        cfg.uploads_enabled=false;
        {
            auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
            upload_suppression_probe sync(std::move(db),cfg); sync.connect(); auto* transport=factory->last_websocket();
            int completed=0;
            sync.set_on_sync_complete([&](const auto&) {++completed;});
            sync.update_sync_filter({{"TestPerson",std::string("age > 100")}});
            // No ACK under the narrowed policy may cancel the old delivery or
            // resolve an unattempted source, including a late exact receipt.
            outbox_ack(*transport,{}); expect_retained();
            outbox_ack(*transport,{"ffffffff-ffff-4fff-8fff-ffffffffffff"}); expect_retained();
            outbox_ack(*transport,{original[0].source_global_id}); expect_retained();
            outbox_ack(*transport,{original[0].wire.global_id}); expect_retained();
            EXPECT_EQ(completed,0);
            sync.reconcile_sync_filter(); sync.upload_pending_changes();
            transport->simulate_message(lattice::transport_message::from_string(
                lattice::server_sent_event::make_replay_request().to_json()));
            expect_retained(); sync.clear_sync_filter(); expect_retained();
            EXPECT_TRUE(outbox_sent(*transport).empty());
            sync.set_on_sync_complete({});
        }
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
        cfg.finite_cycle=true;
        {
            auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
            lattice::synchronizer sync(std::move(db),cfg); sync.connect(); auto* transport=factory->last_websocket();
            using phase=lattice::synchronizer_base::finite_cycle_phase;
            EXPECT_EQ(sync.get_finite_cycle_status().phase,phase::running); expect_retained();
            transport->simulate_message(lattice::transport_message::from_string(
                lattice::server_sent_event::make_catch_up_complete().to_json()));
            EXPECT_EQ(sync.get_finite_cycle_status().phase,phase::work_complete); expect_retained();
            EXPECT_TRUE(outbox_sent(*transport).empty());
        }
        ASSERT_TRUE(outbox_wait_quiescent(observer,cfg.sync_id));
        cfg.uploads_enabled=true; cfg.finite_cycle=false;
        auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
        lattice::synchronizer resumed(std::move(db),cfg); resumed.connect();
        const auto sent=outbox_sent(*factory->last_websocket()); ASSERT_FALSE(sent.empty());
        EXPECT_EQ(sent[0].global_id,original[0].wire.global_id);
        EXPECT_EQ(sent[0].to_json(),original[0].wire_json);
    });
}

TEST(SyncUploadsDisabled, FiniteDownloadWaitsForCatchUpWithoutResolvingPendingUpload) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"finitedownload"}; auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str()));
        auto* owned=db.get(); db->add(TestPerson{"still-local",1,std::nullopt});
        auto cfg=outbox_config("wss:finitedownload"); cfg.uploads_enabled=false; cfg.finite_cycle=true;
        lattice::synchronizer sync(std::move(db),cfg); sync.connect(); auto* transport=factory->last_websocket();
        using phase=lattice::synchronizer_base::finite_cycle_phase;
        EXPECT_EQ(sync.get_finite_cycle_status().phase,phase::running);
        transport->simulate_message(lattice::transport_message::from_string(
            lattice::server_sent_event::make_catch_up_complete().to_json()));
        EXPECT_EQ(sync.get_finite_cycle_status().phase,phase::work_complete);
        EXPECT_TRUE(outbox_sent(*transport).empty());
        EXPECT_EQ(lattice::read_upload_floor(owned->db(),cfg.sync_id),0);
        EXPECT_EQ(lattice::query_audit_log_for_sync(owned->db(),cfg.sync_id).size(),1u);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE is_synchronized=1"),0);
    });
}
#endif

TEST(SyncPrecision, AuthoritativeSnapshotRepairsOldAuditWithoutChangingStoredRows) {
    lattice::lattice_db db;
    const double original = 1788921763.083822;
    auto dog = db.add(TestDog{"Existing", original, true});
    // Existing databases can contain historical 15-digit JSON; preserve it.
    db.db().execute("UPDATE AuditLog SET changedFields=json_object('weight',1788921763.08382) WHERE globalRowId=?", {dog.global_id()});
    EXPECT_THROW(db.append_current_snapshots({"TestDog"}, "snapshot-1"), std::runtime_error);
    db.begin_transaction();
    EXPECT_EQ(db.append_current_snapshots({"TestDog"}, "snapshot-1"), 1);
    EXPECT_EQ(db.append_current_snapshots({"TestDog"}, "snapshot-1"), 0);
    db.commit();
    auto rows = db.db().query("SELECT changedFields FROM AuditLog WHERE globalRowId=? ORDER BY id", {dog.global_id()});
    ASSERT_EQ(rows.size(), 2u);
    auto old = lattice::audit_log_entry::parse_changed_fields(std::get<std::string>(rows[0].at("changedFields")));
    auto exact = lattice::audit_log_entry::parse_changed_fields(std::get<std::string>(rows[1].at("changedFields")));
    EXPECT_NE(std::get<double>(old.at("weight").to_column_value()), original);
    EXPECT_EQ(std::get<double>(exact.at("weight").to_column_value()), original);
    EXPECT_EQ(std::get<double>(db.db().query("SELECT weight FROM TestDog")[0].at("weight")), original);
    db.begin_transaction();
    EXPECT_EQ(db.append_current_snapshots({"TestDog"}, "rolled-back"), 1);
    db.rollback();
    db.begin_transaction();
    EXPECT_EQ(db.append_current_snapshots({"TestDog"}, "rolled-back"), 1);
    db.commit();
}

// Immutable sync channels: these call the actual transactional apply, including
// the legacy INSERT ... ON CONFLICT DO UPDATE path they must constrain.
namespace {
lattice::audit_log_entry immutable_person(std::string row, std::string event, std::string name = "Astra") {
    lattice::audit_log_entry entry;
    entry.global_id = std::move(event);
    entry.global_row_id = std::move(row);
    entry.table_name = "TestPerson";
    entry.operation = "INSERT";
    entry.timestamp = "2026-09-08T12:00:00Z";
    entry.changed_fields_names = {"name", "age", "email"};
    entry.changed_fields = {{"name", name}, {"age", 30}, {"email", nullptr}};
    return entry;
}
void immutable_schema(lattice::lattice_db& db) { (void) db.objects<TestPerson>(); }
int64_t immutable_count(lattice::lattice_db& db) {
    return std::get<int64_t>(db.db().query("SELECT count(*) AS count FROM TestPerson")[0].at("count"));
}
}

TEST(ImmutableSync, IdenticalSemanticReplay) {
    lattice::lattice_db db;
    immutable_schema(db);
    auto entry = immutable_person("agent-1", "event-1");
    ASSERT_EQ(lattice::apply_remote_changes_immutable(db, {entry}, {"testperson"}).size(), 1u);
    entry.row_id = 999;
    entry.timestamp = "2027-01-01T00:00:00Z";
    std::reverse(entry.changed_fields_names.begin(), entry.changed_fields_names.end());
    entry.changed_fields["age"] = lattice::any_property(int64_t(30));
    EXPECT_EQ(lattice::apply_remote_changes_immutable(db, {entry}, {"TESTPERSON"}).size(), 1u);
    entry.global_id = "replay-new-event";
    entry.changed_fields["age"] = lattice::any_property(30.0);
    EXPECT_EQ(lattice::apply_remote_changes_immutable(db, {entry}, {"TestPerson"}).size(), 1u);
    EXPECT_EQ(immutable_count(db), 1);
}

TEST(ImmutableSync, ChangedInsertAndReusedAuditIdentityRejected) {
    lattice::lattice_db db;
    immutable_schema(db);
    auto entry = immutable_person("agent-1", "event-1");
    lattice::apply_remote_changes_immutable(db, {entry}, {"TestPerson"});
    entry.changed_fields["name"] = lattice::any_property("Fable");
    EXPECT_THROW(lattice::apply_remote_changes_immutable(db, {entry}, {"TestPerson"}), std::runtime_error);
    entry.global_id = "event-2";
    EXPECT_THROW(lattice::apply_remote_changes_immutable(db, {entry}, {"TestPerson"}), std::runtime_error);
    entry = immutable_person("different-row", "event-1");
    EXPECT_THROW(lattice::apply_remote_changes_immutable(db, {entry}, {"TestPerson"}), std::runtime_error);
    EXPECT_EQ(immutable_count(db), 1);
    EXPECT_EQ(std::get<std::string>(db.db().query("SELECT name FROM TestPerson")[0].at("name")), "Astra");
}

TEST(ImmutableSync, SameFrameConflictRollsBackWholeLargeFrame) {
    lattice::lattice_db db;
    immutable_schema(db);
    std::vector<lattice::audit_log_entry> entries;
    for (int i = 0; i < 75; ++i) entries.push_back(immutable_person("row-" + std::to_string(i), "event-" + std::to_string(i)));
    entries.push_back(immutable_person("row-0", "conflict", "Fable"));
    EXPECT_THROW(lattice::apply_remote_changes_immutable(db, entries, {"TestPerson"}), std::runtime_error);
    EXPECT_EQ(immutable_count(db), 0);
    EXPECT_TRUE(db.db().query("SELECT id FROM AuditLog WHERE tableName = 'TestPerson'").empty());
    // A retry must not encounter policy receipts from the rolled-back frame.
    entries.pop_back();
    EXPECT_EQ(lattice::apply_remote_changes_immutable(db, entries, {"TestPerson"}).size(), 75u);
    EXPECT_EQ(immutable_count(db), 75);
}

TEST(ImmutableSync, MixedMutableAndImmutableFrameRollsBack) {
    lattice::lattice_db db;
    immutable_schema(db);
    auto seed = immutable_person("seed", "seed-event");
    lattice::apply_remote_changes_immutable(db, {seed}, {"TestPerson"});
    (void) db.objects<TestTrip>();
    auto first = immutable_person("trip", "trip-event");
    first.table_name = "TestTrip";
    first.changed_fields_names = {"name", "days", "notes"};
    first.changed_fields = {{"name", "Trip"}, {"days", 1}, {"notes", nullptr}};
    auto conflict = immutable_person("seed", "new-event", "Changed");
    EXPECT_THROW(lattice::apply_remote_changes_immutable(db, {first, conflict}, {"TestPerson"}), std::runtime_error);
    EXPECT_TRUE(db.db().query("SELECT id FROM TestTrip").empty());
    EXPECT_EQ(immutable_count(db), 1);
}

TEST(ImmutableSync, UpdateDeleteAndIncompleteSnapshotsRejected) {
    lattice::lattice_db db;
    immutable_schema(db);
    for (const auto& operation : {"UPDATE", "DELETE"}) {
        auto entry = immutable_person("row", operation);
        entry.operation = operation;
        EXPECT_THROW(lattice::apply_remote_changes_immutable(db, {entry}, {"TestPerson"}), std::runtime_error);
    }
    auto incomplete = immutable_person("row", "incomplete");
    incomplete.changed_fields.erase("email");
    incomplete.changed_fields_names.pop_back();
    EXPECT_THROW(lattice::apply_remote_changes_immutable(db, {incomplete}, {"TestPerson"}), std::runtime_error);
    EXPECT_EQ(immutable_count(db), 0);
}

TEST(ImmutableSync, IdentitySurvivesAuditCompactionAndLocalDeletion) {
    lattice::lattice_db db;
    immutable_schema(db);
    auto entry = immutable_person("row", "event");
    lattice::apply_remote_changes_immutable(db, {entry}, {"TestPerson"});
    db.db().execute("DELETE FROM AuditLog");
    db.db().execute("DELETE FROM TestPerson");
    entry.global_id = "new-event";
    entry.changed_fields["name"] = lattice::any_property("Changed");
    EXPECT_THROW(lattice::apply_remote_changes_immutable(db, {entry}, {"TestPerson"}), std::runtime_error);
    EXPECT_EQ(immutable_count(db), 0);
}

TEST(ImmutableSync, ConcurrentConflictingInsertsHaveOneWinner) {
    TempDB file{"immutable-race"};
    lattice::lattice_db first{lattice::configuration(file.str())};
    lattice::lattice_db second{lattice::configuration(file.str())};
    immutable_schema(first); immutable_schema(second);
    std::atomic<int> ready{0}, successes{0}, conflicts{0};
    auto insert = [&](lattice::lattice_db& db, const std::string& name) {
        ready.fetch_add(1);
        while (ready.load() < 2) std::this_thread::yield();
        try {
            auto ids = lattice::apply_remote_changes_immutable(db, {immutable_person("row", name, name)}, {"TestPerson"});
            if (ids.size() == 1) successes.fetch_add(1);
        } catch (const std::runtime_error&) { conflicts.fetch_add(1); }
    };
    std::thread a(insert, std::ref(first), "Astra");
    std::thread b(insert, std::ref(second), "Fable");
    a.join(); b.join();
    EXPECT_EQ(successes.load(), 1);
    EXPECT_EQ(conflicts.load(), 1);
    EXPECT_EQ(immutable_count(first), 1);
}

TEST(ImmutableSync, DefaultMutableBehaviorUnchanged) {
    lattice::lattice_db db;
    immutable_schema(db);
    auto entry = immutable_person("row", "event-1");
    EXPECT_EQ(lattice::apply_remote_changes(db, {entry}).size(), 1u);
    entry.global_id = "event-2";
    entry.changed_fields["name"] = lattice::any_property("Changed");
    EXPECT_EQ(lattice::apply_remote_changes_immutable(db, {entry}, {}).size(), 1u);
    EXPECT_EQ(std::get<std::string>(db.db().query("SELECT name FROM TestPerson")[0].at("name")), "Changed");
}

TEST(ImmutableSync, StrictParserRejectsMalformedEntries) {
    lattice::server_sent_event event;
    event.event_type = lattice::server_sent_event::type::audit_log;
    event.audit_logs = {immutable_person("row", "event")};
    EXPECT_TRUE(lattice::server_sent_event::from_json_immutable(event.to_json()).has_value());
    auto malformed = event.to_json();
    auto array = malformed.find("\"auditLog\":[");
    ASSERT_NE(array, std::string::npos);
    malformed.insert(array + std::string("\"auditLog\":[").size(), "0,");
    EXPECT_FALSE(lattice::server_sent_event::from_json_immutable(malformed).has_value());
}

TEST(ImmutableSync, UnknownLaterTableRollsBackFrame) {
    lattice::lattice_db db;
    immutable_schema(db);
    auto unknown = immutable_person("unknown", "unknown-event");
    unknown.table_name = "DoesNotExist";
    EXPECT_THROW(lattice::apply_remote_changes_immutable(db,
        {immutable_person("row", "event"), unknown}, {"TestPerson"}), std::runtime_error);
    EXPECT_EQ(immutable_count(db), 0);
}

TEST(ImmutableSync, ConcurrentIdenticalInsertsBothAcknowledge) {
    TempDB file{"immutable-identical-race"};
    lattice::lattice_db first{lattice::configuration(file.str())};
    lattice::lattice_db second{lattice::configuration(file.str())};
    immutable_schema(first); immutable_schema(second);
    std::atomic<int> ready{0}, successes{0}, failures{0};
    auto insert = [&](lattice::lattice_db& db) {
        ready.fetch_add(1);
        while (ready.load() < 2) std::this_thread::yield();
        try {
            if (lattice::apply_remote_changes_immutable(db,
                {immutable_person("row", "event")}, {"TestPerson"}).size() == 1) successes.fetch_add(1);
        } catch (...) { failures.fetch_add(1); }
    };
    std::thread a(insert, std::ref(first));
    std::thread b(insert, std::ref(second));
    a.join(); b.join();
    EXPECT_EQ(successes.load(), 2);
    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(immutable_count(first), 1);
}

TEST(ImmutableSync, BlobRepresentationsNormalizeAndChangedBytesReject) {
    lattice::lattice_db db;
    db.db().execute("CREATE TABLE ImmutableBlob (id INTEGER PRIMARY KEY AUTOINCREMENT, globalId TEXT UNIQUE NOT NULL, payload BLOB)");
    auto entry = immutable_person("row", "event");
    entry.table_name = "ImmutableBlob";
    entry.changed_fields_names = {"payload"};
    entry.changed_fields = {{"payload", std::string("0A00FF")}};
    EXPECT_EQ(lattice::apply_remote_changes_immutable(db, {entry}, {"ImmutableBlob"}).size(), 1u);
    entry.global_id = "replay";
    entry.changed_fields["payload"] = lattice::any_property(std::vector<uint8_t>{10, 0, 255});
    EXPECT_EQ(lattice::apply_remote_changes_immutable(db, {entry}, {"ImmutableBlob"}).size(), 1u);
    entry.global_id = "changed";
    entry.changed_fields["payload"] = lattice::any_property(std::vector<uint8_t>{10, 0, 254});
    EXPECT_THROW(lattice::apply_remote_changes_immutable(db, {entry}, {"ImmutableBlob"}), std::runtime_error);
}



TEST(ImmutableSync, AuthorizationAtCommitRollsBackWholeFrameAndRecovers) {
    lattice::lattice_db db;
    immutable_schema(db);
    auto entry = immutable_person("owner-row", "owner-event");
    int checks = 0;
    EXPECT_THROW(lattice::apply_remote_changes_immutable(db, {entry}, {}, [&] {
        ++checks;
        if (checks == 2) EXPECT_EQ(immutable_count(db), 1);
        return checks == 1;
    }), std::runtime_error);
    EXPECT_EQ(checks, 2);
    EXPECT_EQ(immutable_count(db), 0);
    EXPECT_TRUE(db.db().query("SELECT id FROM AuditLog WHERE tableName='TestPerson'").empty());
    EXPECT_FALSE(db.db().is_in_transaction());
    EXPECT_EQ(lattice::apply_remote_changes_immutable(db, {entry}, {}, [] { return true; }).size(), 1u);
}

TEST(ImmutableSync, RejectionDoesNotJoinOrRollbackCallersTransaction) {
    lattice::lattice_db db;
    immutable_schema(db);
    db.begin_transaction();
    db.db().execute("INSERT INTO TestPerson(globalId,name,age) VALUES ('local','local',1)");
    EXPECT_THROW(lattice::apply_remote_changes_immutable(db,
        {immutable_person("remote", "event")}, {"TestPerson"}), std::runtime_error);
    EXPECT_TRUE(db.db().is_in_transaction());
    EXPECT_EQ(immutable_count(db), 1);
    db.rollback();
    EXPECT_EQ(immutable_count(db), 0);
}

TEST(ImmutableSync, CommitFailureHasNoReceiptsOrModelEffects) {
    lattice::lattice_db db;
    immutable_schema(db);
    auto entry = immutable_person("commit-row", "commit-event");
    sqlite3_set_authorizer(db.db().handle(), [](void*, int action, const char* verb,
        const char*, const char*, const char*) -> int {
        return action == SQLITE_TRANSACTION && verb && std::string(verb) == "COMMIT"
            ? SQLITE_DENY : SQLITE_OK;
    }, nullptr);
    EXPECT_THROW(lattice::apply_remote_changes_immutable(db, {entry}, {"TestPerson"}), std::runtime_error);
    sqlite3_set_authorizer(db.db().handle(), nullptr, nullptr);
    EXPECT_FALSE(db.db().is_in_transaction());
    EXPECT_EQ(immutable_count(db), 0);
    EXPECT_TRUE(db.db().query("SELECT id FROM AuditLog WHERE tableName='TestPerson'").empty());
    EXPECT_EQ(lattice::apply_remote_changes_immutable(db, {entry}, {"TestPerson"}).size(), 1u);
}

TEST(ImmutableSync, AuthorizedAckRollsBackWhenOwnerRevokes) {
    lattice::lattice_db db;
    immutable_schema(db);
    auto object = db.add(TestPerson{"Pending", 1, std::nullopt});
    auto rows = db.db().query("SELECT globalId FROM AuditLog WHERE globalRowId=?", {object.global_id()});
    ASSERT_EQ(rows.size(), 1u);
    const auto event = std::get<std::string>(rows[0].at("globalId"));
    int checks = 0;
    EXPECT_THROW(lattice::mark_audit_entries_synced_authorized(db, {event}, [&] {
        return ++checks == 1;
    }), std::runtime_error);
    EXPECT_EQ(checks, 2);
    EXPECT_EQ(std::get<int64_t>(db.db().query("SELECT isSynchronized FROM AuditLog WHERE globalId=?", {event})[0].at("isSynchronized")), 0);
    lattice::mark_audit_entries_synced_authorized(db, {event}, [] { return true; });
    EXPECT_EQ(std::get<int64_t>(db.db().query("SELECT isSynchronized FROM AuditLog WHERE globalId=?", {event})[0].at("isSynchronized")), 1);
}

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
// These regressions use the real sender and mock transport; the pre-change
// sender refuses the first row at its 4 MiB batch bound.
TEST(Sync, TwoLargeRowsWaitForAckWithinTheEightMiBWindow) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"two-large-rows"};
        auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())); auto* owned=db.get();
        db->add(TestPerson{std::string(5*1024*1024,'a'),1,std::nullopt});
        db->add(TestPerson{std::string(5*1024*1024,'b'),2,std::nullopt});
        const auto pending=lattice::query_audit_log_for_sync(db->db(),"two-large"); ASSERT_EQ(pending.size(),2u);
        auto cfg=outbox_config("two-large"); cfg.sync_filter=std::nullopt;
        std::string error;
        lattice::synchronizer sync(std::move(db),cfg);
        sync.set_on_error([&](const std::string& value){error=value;}); sync.connect();
        auto* transport=factory->last_websocket();
        auto sent=outbox_sent(*transport); ASSERT_EQ(sent.size(),1u);
        EXPECT_EQ(sent[0].global_id,pending[0].global_id);
        sync.sync_now();
        EXPECT_EQ(outbox_sent(*transport).size(),1u);
        EXPECT_EQ(sync.get_progress().pending_upload,1);
        outbox_ack(*transport,{pending[0].global_id});
        sent=outbox_sent(*transport); ASSERT_EQ(sent.size(),2u);
        EXPECT_EQ(sent[1].global_id,pending[1].global_id);
        for(const auto& message:transport->get_sent_messages()) {
            const auto bytes=message.as_string(); const auto event=lattice::server_sent_event::from_json(bytes);
            if(!event || event->event_type!=lattice::server_sent_event::type::audit_log) continue;
            EXPECT_GT(bytes.size(),lattice::sync_max_encoded_message_bytes);
            EXPECT_LE(bytes.size(),2*lattice::sync_max_encoded_message_bytes);
            EXPECT_EQ(event->audit_logs.size(),1u);
        }
        outbox_ack(*transport,{pending[1].global_id});
        EXPECT_TRUE(lattice::query_audit_log_for_sync(owned->db(),cfg.sync_id).empty());
        EXPECT_EQ(sync.get_progress().pending_upload,0);
        EXPECT_TRUE(error.empty()) << error;
    },30);
}

TEST(Sync, OverEightMiBUploadNamesTheRowWithoutAdvancingItsFloor) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"over-eight-mib"};
        auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())); auto* owned=db.get();
        db->add(TestPerson{std::string(8*1024*1024,'x'),1,std::nullopt});
        const auto source=lattice::query_audit_log(db->db()).back();
        auto cfg=outbox_config("over-eight"); cfg.sync_filter=std::nullopt;
        std::string error;
        lattice::synchronizer sync(std::move(db),cfg);
        sync.set_on_error([&](const std::string& value){error=value;}); sync.connect();
        EXPECT_NE(error.find(source.global_id),std::string::npos) << error;
        EXPECT_NE(error.find(source.table_name),std::string::npos) << error;
        EXPECT_TRUE(outbox_sent(*factory->last_websocket()).empty());
        EXPECT_EQ(lattice::read_upload_floor(owned->db(),cfg.sync_id),0);
        EXPECT_EQ(sync.get_progress().pending_upload,0);
        EXPECT_EQ(sync.get_progress().total_upload,0);
        EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE is_synchronized=1"),0);
        EXPECT_EQ(lattice::query_audit_log_for_sync(owned->db(),cfg.sync_id).size(),1u);
    },30);
}

TEST(SyncDelivery, LargeFilteredSnapshotReplaysExactBytesUntilAck) {
    bounded_mock_case([] {
        auto factory=std::make_shared<lattice::mock_network_factory>(); lattice::set_network_factory(factory);
        TempDB tmp{"large-filtered-replay"};
        auto cfg=outbox_config("large-filtered"); cfg.sync_filter->at(0).where_clause="age=1";
        std::string exact,wire_id,source_id,error;
        {
            auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())); auto* owned=db.get();
            db->add(TestPerson{std::string(5*1024*1024,'x'),0,std::nullopt});
            db->db().execute("UPDATE AuditLog SET isSynchronized=1");
            db->db().execute("UPDATE TestPerson SET age=1");
            source_id=lattice::query_audit_log(db->db()).back().global_id;
            lattice::synchronizer sync(std::move(db),cfg);
            sync.set_on_error([&](const std::string& value){error=value;}); sync.connect();
            const auto sent=outbox_sent(*factory->last_websocket()); ASSERT_EQ(sent.size(),1u);
            wire_id=sent[0].global_id; EXPECT_NE(wire_id,source_id);
            EXPECT_EQ(sent[0].operation,"INSERT");
            const auto retained=lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id);
            ASSERT_EQ(retained.size(),1u); exact=retained[0].wire_json;
            EXPECT_GT(retained[0].charge,lattice::sync_max_encoded_message_bytes);
            EXPECT_LE(retained[0].charge,2*lattice::sync_max_encoded_message_bytes);
            EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
            EXPECT_EQ(outbox_number(*owned,"SELECT isSynchronized AS n FROM AuditLog WHERE globalId=?",{source_id}),0);
            sync.disconnect();
        }
        {
            auto db=std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str())); auto* owned=db.get();
            lattice::synchronizer sync(std::move(db),cfg);
            sync.set_on_error([&](const std::string& value){error=value;}); sync.connect();
            auto* transport=factory->last_websocket();
            const auto sent=outbox_sent(*transport); ASSERT_EQ(sent.size(),1u);
            EXPECT_EQ(sent[0].global_id,wire_id);
            const auto retained=lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id);
            ASSERT_EQ(retained.size(),1u); EXPECT_EQ(retained[0].wire_json,exact);
            EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
            outbox_ack(*transport,{wire_id});
            EXPECT_TRUE(lattice::detail::read_sync_outbox(owned->db(),cfg.sync_id).empty());
            EXPECT_EQ(outbox_number(*owned,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),1);
            EXPECT_EQ(outbox_number(*owned,"SELECT isSynchronized AS n FROM AuditLog WHERE globalId=?",{source_id}),1);
            EXPECT_TRUE(error.empty()) << error;
        }
    },30);
}
#endif

TEST(SyncDelivery, AdmissionCountsMetadataAtTheEightMiBBoundary) {
    TempDB tmp{"admit-retained-boundary"}; lattice::lattice_db db{lattice::configuration(tmp.str())};
    db.add(TestPerson{"",1,std::nullopt});
    auto delivery=outbox_delivery(lattice::query_audit_log(db.db()).back());
    delivery.wire.changed_fields["name"]=lattice::any_property(std::string{});
    const auto empty_wire=delivery.wire.to_json();
    const auto overhead=lattice::detail::sync_delivery_retained_bytes(delivery,empty_wire);
    ASSERT_LT(overhead,lattice::detail::sync_outbox_bytes);
    for(size_t extra:{size_t{1},size_t{0}}) {
        delivery.wire.changed_fields["name"]=lattice::any_property(
            std::string(lattice::detail::sync_outbox_bytes-overhead+extra,'x'));
        ASSERT_EQ(lattice::detail::sync_delivery_retained_bytes(delivery,delivery.wire.to_json()),
                  lattice::detail::sync_outbox_bytes+extra);
        if(extra) {
            try { (void)lattice::detail::admit_sync_delivery(db,"boundary",delivery); FAIL()<<"over-budget receipt admitted"; }
            catch(const std::length_error& error) {
                const std::string message=error.what();
                EXPECT_NE(message.find(delivery.wire.global_id),std::string::npos)<<message;
                EXPECT_NE(message.find(delivery.wire.table_name),std::string::npos)<<message;
                EXPECT_NE(message.find("retained"),std::string::npos)<<message;
            }
            EXPECT_EQ(outbox_number(db,"SELECT COUNT(*) AS n FROM _lattice_sync_outbox"),0);
        } else {
            ASSERT_TRUE(lattice::detail::admit_sync_delivery(db,"boundary",delivery));
            const auto stored=lattice::detail::read_sync_outbox(db.db(),"boundary");
            ASSERT_EQ(stored.size(),1u);
            EXPECT_EQ(lattice::detail::sync_delivery_retained_bytes(stored[0],stored[0].wire_json),
                      lattice::detail::sync_outbox_bytes);
        }
        EXPECT_EQ(outbox_number(db,"SELECT COUNT(*) AS n FROM _lattice_sync_set"),0);
        EXPECT_EQ(outbox_number(db,"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE is_synchronized=1"),0);
    }
}

// Read-only hosts do not retry failed downloads via the upload loop. A receive
// gap must redial from the committed prefix, fencing every queued old frame.
namespace {
std::vector<lattice::audit_log_entry> recovery_entries(size_t count) {
    std::vector<lattice::audit_log_entry> entries;
    for (size_t i = 0; i < count; ++i) {
        lattice::audit_log_entry e;
        e.global_id = "recover-audit-" + std::to_string(i);
        e.table_name = "TestPerson";
        e.operation = "INSERT";
        e.global_row_id = "recover-person-" + std::to_string(i);
        e.changed_fields = {{"name", lattice::any_property("P" + std::to_string(i))},
                            {"age", lattice::any_property(int64_t(i))}};
        e.changed_fields_names = {"name", "age"};
        e.timestamp = "2026-09-09T12:00:00Z";
        entries.push_back(std::move(e));
    }
    return entries;
}

class readonly_replay_transport : public lattice::sync_transport {
public:
    std::vector<lattice::audit_log_entry> history;
    std::function<void(size_t)> before_connect;
    std::atomic<size_t> connections{0};
    std::atomic<size_t> uploads{0};
    void connect(const std::string& url,
                 const std::map<std::string, std::string>&) override {
        size_t attempt;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            urls_.push_back(url);
            attempt = urls_.size();
        }
        connections = attempt;
        if (before_connect) before_connect(attempt);
        state_ = lattice::transport_state::open;
        if (on_open_) on_open_();
        if (attempt > 1) {
            // A real readonly server enumerates strictly AFTER this checkpoint.
            // It sends no unsolicited replay and has no client uploads to retry.
            size_t first = 0;
            auto pos = url.find("last-event-id=");
            if (pos != std::string::npos) {
                auto checkpoint = url.substr(pos + 14);
                auto it = std::find_if(history.begin(), history.end(), [&](const auto& e) {
                    return e.global_id == checkpoint;
                });
                if (it != history.end()) first = size_t(it - history.begin()) + 1;
            }
            deliver({history.begin() + first, history.end()});
        }
    }
    void disconnect() override { state_ = lattice::transport_state::closed; }
    lattice::transport_state state() const override { return state_; }
    void send(const lattice::transport_message& message) override {
        auto event = lattice::server_sent_event::from_json(message.as_string());
        if (event && event->event_type == lattice::server_sent_event::type::audit_log) ++uploads;
        if (!event || event->event_type != lattice::server_sent_event::type::ack) return;
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& id : event->acked_ids) acks_.emplace_back(connections.load(), id);
    }
    void set_on_open(on_open_handler handler) override { on_open_ = std::move(handler); }
    void set_on_message(on_message_handler handler) override { on_message_ = std::move(handler); }
    void set_on_error(on_error_handler handler) override { on_error_ = std::move(handler); }
    void set_on_close(on_close_handler handler) override { on_close_ = std::move(handler); }
    void deliver(const std::vector<lattice::audit_log_entry>& entries) {
        deliver_json(lattice::server_sent_event::make_audit_log(entries).to_json());
    }
    void deliver_json(const std::string& json) {
        if (on_message_) on_message_(lattice::transport_message::from_string(json));
    }
    std::vector<std::string> urls() {
        std::lock_guard<std::mutex> lock(mutex_); return urls_;
    }
    std::vector<std::pair<size_t, std::string>> acks() {
        std::lock_guard<std::mutex> lock(mutex_); return acks_;
    }
private:
    std::mutex mutex_;
    std::vector<std::string> urls_;
    std::vector<std::pair<size_t, std::string>> acks_;
    std::atomic<lattice::transport_state> state_{lattice::transport_state::closed};
    on_open_handler on_open_;
    on_message_handler on_message_;
    on_error_handler on_error_;
    on_close_handler on_close_;
};

struct recovery_fixture {
    TempDB tmp{"readonly-recovery"};
    std::shared_ptr<lattice::std_thread_scheduler> scheduler =
        std::make_shared<lattice::std_thread_scheduler>();
    std::atomic<int> errors{0};
    std::mutex error_mutex;
    std::string last_error;
    readonly_replay_transport* wire;
    lattice::lattice_db* database;
    std::unique_ptr<lattice::synchronizer> sync;
    explicit recovery_fixture(size_t count, double delay = 0.05, int maximum = 3,
                              bool websocket = true, bool observer = false) {
        auto db = std::make_unique<lattice::lattice_db>(lattice::configuration(tmp.str(), scheduler));
        database = db.get();
        auto seed = db->add(TestPerson{"seed", 0, std::nullopt});
        db->remove(seed);
        db->db().execute("DELETE FROM AuditLog");
        auto transport = std::make_unique<readonly_replay_transport>();
        wire = transport.get();
        wire->history = recovery_entries(count);
        lattice::sync_config config;
        config.sync_id = "wss:readonly-recovery";
        if (websocket) config.websocket_url = "wss://readonly.invalid/room";
        config.uploads_enabled = false;
        config.is_observer = observer;
        config.base_delay_seconds = delay;
        config.max_delay_seconds = delay * 4;
        config.max_reconnect_attempts = maximum;
        config.stable_connection_ms = 60'000;
        sync = std::make_unique<lattice::synchronizer>(std::move(db), config, std::move(transport));
        sync->set_on_error([this](const std::string& message) {
            std::lock_guard<std::mutex> lock(error_mutex);
            last_error = message;
            ++errors;
        });
    }
    ~recovery_fixture() {
        sync.reset(); // Join apply/reconnect before clearing captured test hooks.
        lattice::test_hooks::apply_chunk_begin = nullptr;
    }
    template<class Predicate> bool wait(Predicate predicate, int milliseconds = 2500) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(milliseconds);
        while (!predicate() && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        return predicate();
    }
    void expect_complete(size_t count) {
        EXPECT_TRUE(wait([&] { return sync->get_progress().received >= int64_t(count); }));
        sync.reset();
        lattice::lattice_db reopened{lattice::configuration(tmp.str())};
        auto rows = reopened.db().query("SELECT COUNT(*) AS count FROM TestPerson");
        EXPECT_EQ(std::get<int64_t>(rows[0].at("count")), int64_t(count));
        auto slot = reopened.db().query("SELECT last_received_event_id AS cursor FROM _lattice_replication_slots WHERE sync_id='wss:readonly-recovery'");
        ASSERT_EQ(slot.size(), 1u);
        ASSERT_TRUE(std::holds_alternative<std::string>(slot[0].at("cursor")));
        EXPECT_EQ(std::get<std::string>(slot[0].at("cursor")), "recover-audit-" + std::to_string(count - 1));
    }
};
}

TEST(SyncRecovery, ReadonlyZeroApplyRedialsWithoutAnyFutureUploads) {
    recovery_fixture fixture(4);
    auto failures = std::make_shared<std::atomic<int>>(2);
    lattice::test_hooks::apply_chunk_begin = [failures](size_t) {
        if (failures->fetch_sub(1) > 0) throw std::runtime_error("injected write contention");
    };
    fixture.sync->connect();
    fixture.wire->deliver(fixture.wire->history);
    EXPECT_TRUE(fixture.wait([&] { return fixture.wire->connections >= 2; }));
    EXPECT_GE(fixture.errors.load(), 1);
    auto urls = fixture.wire->urls();
    if (urls.size() >= 2) EXPECT_EQ(urls[1].find("last-event-id="), std::string::npos);
    fixture.expect_complete(4);
    {
        std::lock_guard<std::mutex> lock(fixture.error_mutex);
        EXPECT_EQ(fixture.last_error,
            "Download incomplete: applied 0 of 4 entries; reconnecting from the committed checkpoint.");
    }
}

TEST(SyncRecovery, MissingMiddleFencesAlreadyQueuedFollowingFrameAndResumesPrefix) {
    recovery_fixture fixture(60);
    auto failures = std::make_shared<std::atomic<int>>(2);
    lattice::test_hooks::apply_chunk_begin = [failures](size_t start) {
        if (start == 50 && failures->fetch_sub(1) > 0)
            throw std::runtime_error("injected second-chunk failure");
    };
    fixture.sync->connect();
    fixture.wire->deliver({fixture.wire->history.begin(), fixture.wire->history.begin() + 55});
    fixture.wire->deliver({fixture.wire->history.begin() + 55, fixture.wire->history.end()});
    EXPECT_TRUE(fixture.wait([&] { return fixture.wire->connections >= 2; }));
    auto urls = fixture.wire->urls();
    ASSERT_GE(urls.size(), 2u);
    EXPECT_NE(urls[1].find("last-event-id=recover-audit-49"), std::string::npos);
    auto acks = fixture.wire->acks();
    EXPECT_NE(std::find(acks.begin(), acks.end(), std::make_pair(size_t(1), std::string("recover-audit-49"))), acks.end());
    EXPECT_EQ(std::find(acks.begin(), acks.end(), std::make_pair(size_t(1), std::string("recover-audit-59"))), acks.end());
    fixture.expect_complete(60);
}

TEST(SyncRecovery, FailedFirstRowDoesNotSeedNullCheckpointFromLaterSuccessfulRows) {
    recovery_fixture fixture(4);
    fixture.database->db().execute("CREATE TRIGGER reject_first BEFORE INSERT ON TestPerson WHEN NEW.age=0 BEGIN SELECT RAISE(ABORT,'injected first-row failure'); END");
    fixture.wire->before_connect = [&](size_t attempt) {
        if (attempt == 2) fixture.database->db().execute("DROP TRIGGER reject_first");
    };
    fixture.sync->connect();
    fixture.wire->deliver(fixture.wire->history);
    EXPECT_TRUE(fixture.wait([&] { return fixture.wire->connections >= 2; }));
    auto urls = fixture.wire->urls();
    ASSERT_GE(urls.size(), 2u);
    EXPECT_EQ(urls[1].find("last-event-id="), std::string::npos);
    fixture.expect_complete(4);
}

TEST(SyncRecovery, FilterRemovalAcksDoNotHideAnOrdinaryApplyFailure) {
    recovery_fixture fixture(2);
    auto failures = std::make_shared<std::atomic<int>>(2);
    lattice::test_hooks::apply_chunk_begin = [failures](size_t) {
        if (failures->fetch_sub(1) > 0) throw std::runtime_error("injected write contention");
    };
    auto removal = fixture.wire->history.back();
    removal.operation = "DELETE";
    removal.changed_fields_names.push_back("__lattice_filter_removal");
    fixture.wire->history.back() = removal;
    fixture.sync->connect();
    fixture.wire->deliver(fixture.wire->history);
    EXPECT_TRUE(fixture.wait([&] { return fixture.wire->connections >= 2; }));
    EXPECT_TRUE(fixture.wait([&] { return fixture.sync->get_progress().received >= 1; }));
    EXPECT_GE(fixture.errors.load(), 1);
}

TEST(SyncRecovery, MalformedMiddleEntryCannotBeSilentlySkipped) {
    recovery_fixture fixture(3);
    auto json = lattice::server_sent_event::make_audit_log(fixture.wire->history).to_json();
    // Missing required tableName makes one entry undecodable while siblings remain valid.
    auto first = json.find("\"tableName\"");
    auto middle = json.find("\"tableName\"", first + 1);
    ASSERT_NE(middle, std::string::npos);
    json.replace(middle, 11, "\"wrongName\"");
    fixture.sync->connect();
    fixture.wire->deliver_json(json);
    EXPECT_TRUE(fixture.wait([&] { return fixture.wire->connections >= 2; }));
    auto urls = fixture.wire->urls();
    ASSERT_GE(urls.size(), 2u);
    EXPECT_EQ(urls[1].find("last-event-id="), std::string::npos);
    fixture.expect_complete(3);
}

TEST(SyncRecovery, PersistentFailureHasBoundedAttemptsAndExplicitDisconnectCancelsRedial) {
    {
        recovery_fixture fixture(1, 0.05, 2);
        lattice::test_hooks::apply_chunk_begin = [](size_t) { throw std::runtime_error("persistent failure"); };
        fixture.sync->connect();
        fixture.wire->deliver(fixture.wire->history);
        EXPECT_TRUE(fixture.wait([&] { return fixture.errors >= 3; }));
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        EXPECT_EQ(fixture.wire->connections, 3u);
        EXPECT_EQ(fixture.sync->get_progress().received, 0);
    }
    {
        recovery_fixture fixture(1, 0.5);
        lattice::test_hooks::apply_chunk_begin = [](size_t) { throw std::runtime_error("persistent failure"); };
        fixture.sync->connect();
        fixture.wire->deliver(fixture.wire->history);
        ASSERT_TRUE(fixture.wait([&] { return fixture.errors >= 1; }));
        fixture.sync->disconnect();
        std::this_thread::sleep_for(std::chrono::milliseconds(650));
        EXPECT_EQ(fixture.wire->connections, 1u);
    }
}


// Green guard: intentionally acknowledged removals do not create a receive gap.
TEST(SyncRecovery, CompleteFrameWithFilterRemovalDoesNotTriggerFalseRecovery) {
    recovery_fixture fixture(2);
    auto removal = fixture.wire->history.back();
    removal.operation = "DELETE";
    removal.changed_fields_names.push_back("__lattice_filter_removal");
    fixture.wire->history.back() = removal;
    fixture.sync->connect();
    fixture.wire->deliver(fixture.wire->history);
    EXPECT_TRUE(fixture.wait([&] { return fixture.wire->acks().size() == 2; }));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_EQ(fixture.errors, 0);
    EXPECT_EQ(fixture.wire->connections, 1u);
    EXPECT_EQ(fixture.sync->get_progress().received, 1);
}

TEST(SyncRecovery, IPCPartialApplyKeepsExistingSenderRetryPath) {
    recovery_fixture fixture(3, 0.05, 3, false);
    auto failures = std::make_shared<std::atomic<int>>(2);
    lattice::test_hooks::apply_chunk_begin = [failures](size_t) {
        if (failures->fetch_sub(1) > 0) throw std::runtime_error("injected IPC write contention");
    };
    fixture.sync->connect();
    fixture.wire->deliver(fixture.wire->history);
    EXPECT_TRUE(fixture.wait([&] { return fixture.errors >= 1; }));
    {
        std::lock_guard<std::mutex> lock(fixture.error_mutex);
        EXPECT_EQ(fixture.last_error,
            "Download incomplete: applied 0 of 3 entries; waiting for sender retry.");
    }
    EXPECT_TRUE(fixture.sync->is_connected());
    // A peer's existing ACK timeout retries the unacknowledged frame.
    fixture.wire->deliver(fixture.wire->history);
    EXPECT_TRUE(fixture.wait([&] { return fixture.wire->acks().size() == 3; }));
    EXPECT_EQ(fixture.wire->connections, 1u);
    fixture.expect_complete(3);
}

// Green guard: the existing connection generation still fences queued deliveries.
TEST(SyncRecovery, QueuedOldFrameCannotApplyAfterAReplacementConnectionOpens) {
    recovery_fixture fixture(3);
    fixture.sync->connect();
    std::mutex mutex;
    std::condition_variable condition;
    bool blocked = false, release = false;
    fixture.scheduler->invoke([&] {
        std::unique_lock<std::mutex> lock(mutex);
        blocked = true;
        condition.notify_one();
        condition.wait(lock, [&] { return release; });
    });
    {
        std::unique_lock<std::mutex> lock(mutex);
        const bool started = condition.wait_for(lock, std::chrono::seconds(2), [&] { return blocked; });
        if (!started) {
            release = true;
            lock.unlock();
            condition.notify_one();
            fixture.sync.reset();
            FAIL() << "scheduler did not reach the bounded fixture gate";
        }
    }
    // This delivery captures the OLD generation, but cannot execute yet.
    fixture.wire->deliver({fixture.wire->history.back()});
    fixture.sync->disconnect();
    fixture.sync->connect(); // A new open must not admit the old queued delivery.
    {
        std::lock_guard<std::mutex> lock(mutex);
        release = true;
    }
    condition.notify_one();
    EXPECT_TRUE(fixture.wait([&] { return fixture.sync->get_progress().received >= 3; }));
    auto acks = fixture.wire->acks();
    EXPECT_EQ(acks.size(), 3u) << "old frame was applied/ACKed on the replacement connection";
    fixture.expect_complete(3);
}

TEST(SyncRecovery, MalformedNackRedialsWithoutAcknowledgingItsIds) {
    recovery_fixture fixture(3);
    fixture.sync->connect();
    fixture.wire->deliver_json(R"({"kind":"nack","nack":["SECRET",42]})");
    ASSERT_TRUE(fixture.wait([&] { return fixture.wire->connections >= 2; }));
    auto urls = fixture.wire->urls();
    ASSERT_GE(urls.size(), 2u);
    EXPECT_EQ(urls[1].find("last-event-id="), std::string::npos);
    auto acks = fixture.wire->acks();
    for (const auto& [attempt, id] : acks) {
        EXPECT_GT(attempt, 1u);
        EXPECT_NE(id, "SECRET");
    }
    {
        std::lock_guard<std::mutex> lock(fixture.error_mutex);
        EXPECT_NE(fixture.last_error.find("invalid nack entry"), std::string::npos);
        EXPECT_EQ(fixture.last_error.find("SECRET"), std::string::npos);
    }
    fixture.expect_complete(3);
}

TEST(SyncRecovery, InvalidControlStillFencesQueuedHistoryAndRecoversFromCommittedCursor) {
    for (const auto* wire : {R"({"kind":"floorReset","floorReason":false})", R"({"kind":"futureControl"})"}) {
        SCOPED_TRACE(wire);
        recovery_fixture fixture(3);
        fixture.sync->connect();
        fixture.wire->deliver({fixture.wire->history.front()});
        ASSERT_TRUE(fixture.wait([&] { return fixture.wire->acks().size() == 1; }));
        fixture.wire->deliver_json(wire);
        fixture.wire->deliver({fixture.wire->history.back()}); // must not skip the middle row
        ASSERT_TRUE(fixture.wait([&] { return fixture.wire->connections >= 2; }));
        const auto urls = fixture.wire->urls();
        ASSERT_GE(urls.size(), 2u);
        EXPECT_NE(urls[1].find("last-event-id=recover-audit-0"), std::string::npos);
        const auto acks = fixture.wire->acks();
        EXPECT_EQ(std::find(acks.begin(), acks.end(), std::make_pair(size_t(1), std::string("recover-audit-2"))), acks.end());
        EXPECT_GE(fixture.errors, 1);
        EXPECT_EQ(fixture.wire->uploads, 0u);
        fixture.expect_complete(3);
    }
}


TEST(SyncRecovery, RejectedFrameDoesNotFenceQueuedPagesOrRedial) {
    recovery_fixture fixture(4);
    fixture.sync->connect();
    fixture.wire->deliver({fixture.wire->history.begin(), fixture.wire->history.begin() + 2});
    fixture.wire->deliver_json(R"({"kind":"rejected","rejected":"write policy refused"})");
    fixture.wire->deliver({fixture.wire->history.begin() + 2, fixture.wire->history.end()});
    ASSERT_TRUE(fixture.wait([&] { return fixture.sync->get_progress().received >= 4; }));
    EXPECT_EQ(fixture.wire->connections, 1u);
    fixture.expect_complete(4);
    EXPECT_EQ(fixture.errors, 1);
    {
        std::lock_guard<std::mutex> lock(fixture.error_mutex);
        EXPECT_EQ(fixture.last_error, "Sync upload rejected: write policy refused");
    }
}

TEST(SyncRecovery, RejectedFramesRequireAStringAndASingleMatchingPayload) {
    for (const auto* wire : {R"({"kind":"rejected","rejected":"policy refused"})",
                             R"({"kind":"rejected","rejected":""})",
                             R"({"rejected":"legacy refusal"})"}) {
        SCOPED_TRACE(wire);
        const auto event = lattice::server_sent_event::from_json(wire);
        ASSERT_TRUE(event);
        EXPECT_EQ(event->event_type, lattice::server_sent_event::type::nack);
        EXPECT_TRUE(event->nacked_ids.empty());
        EXPECT_TRUE(event->acked_ids.empty());
        EXPECT_TRUE(event->audit_logs.empty());
    }
    for (const auto* wire : {R"({"kind":"rejected","rejected":null})",
                             R"({"kind":"rejected","rejected":12})",
                             R"({"kind":"rejected","rejected":[]})",
                             R"({"kind":"rejected","rejected":"no","ack":[]})",
                             R"({"kind":"nack","rejected":"no"})"}) {
        SCOPED_TRACE(wire);
        std::string diagnostic;
        EXPECT_FALSE(lattice::server_sent_event::from_json(wire, &diagnostic));
        EXPECT_FALSE(diagnostic.empty());
    }
}


TEST(SyncRecovery, ObserverDisconnectRetainsANullCursorUntilTheGapIsRepaired) {
    // Delay the automatic retry so the explicit disconnect/connect path owns
    // this attempt; disconnect cancels that delay without waiting for it.
    recovery_fixture fixture(4, 60.0, 1, true, true);
    fixture.database->db().execute("CREATE TRIGGER reject_first BEFORE INSERT ON TestPerson "
        "WHEN NEW.age=0 BEGIN SELECT RAISE(ABORT,'injected first-row failure'); END");
    fixture.sync->connect();
    fixture.wire->deliver(fixture.wire->history);
    ASSERT_TRUE(fixture.wait([&] { return fixture.errors >= 1; }));
    const auto count = [&] {
        return std::get<int64_t>(fixture.database->db().query("SELECT COUNT(*) AS n FROM TestPerson")[0].at("n"));
    };
    const auto slots = [&] {
        return fixture.database->db().query("SELECT last_received_event_id AS cursor,is_observer "
            "FROM _lattice_replication_slots WHERE sync_id='wss:readonly-recovery'");
    };
    ASSERT_EQ(count(),3);
    const auto before = slots();
    ASSERT_EQ(before.size(),1u);
    EXPECT_EQ(std::get<int64_t>(before[0].at("is_observer")),1);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(before[0].at("cursor")));

    fixture.sync->disconnect();
    const auto held = slots();
    ASSERT_EQ(held.size(),1u);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(held[0].at("cursor")));
    fixture.database->db().execute("DROP TRIGGER reject_first");
    fixture.sync->connect();
    const auto urls = fixture.wire->urls();
    ASSERT_EQ(urls.size(),2u);
    EXPECT_EQ(urls[1].find("last-event-id="),std::string::npos);
    ASSERT_TRUE(fixture.wait([&] {
        const auto acks = fixture.wire->acks();
        return std::count_if(acks.begin(),acks.end(),[](const auto& ack) { return ack.first == 2; }) == 4;
    }));
    EXPECT_EQ(count(),4);
    const auto repaired = slots();
    ASSERT_EQ(repaired.size(),1u);
    EXPECT_EQ(std::get<std::string>(repaired[0].at("cursor")),"recover-audit-3");
    fixture.sync->disconnect();
    EXPECT_TRUE(slots().empty());
}
