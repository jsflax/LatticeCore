#include "TestHelpers.hpp"
#include <regex>
#include <set>

// ============================================================================
// Sync Unit Tests — AuditLog, sync protocol, synchronizer
// ============================================================================

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
    EXPECT_EQ(params.size(), 2u);  // name, globalId
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
    auto& sent = mock_ws->get_sent_messages();
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
    auto& ack_sent = mock_ws->get_sent_messages();
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
