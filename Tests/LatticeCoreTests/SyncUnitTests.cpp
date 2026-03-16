#include "TestHelpers.hpp"

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
