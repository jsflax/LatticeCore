#include "TestHelpers.hpp"
#ifndef __linux__
#include <lattice.hpp>          // swift_lattice, swift_lattice_ref
#include <dynamic_object.hpp>   // dynamic_object, dynamic_object_ref

// ============================================================================
// Bridge Layer Tests — swift_lattice + dynamic_object (what Swift actually uses)
// ============================================================================

// Helper: create a swift_schema_entry for a simple table
static lattice::swift_schema_entry make_schema(
    const std::string& table_name,
    std::initializer_list<std::pair<std::string, lattice::property_descriptor>> props) {
    lattice::swift_schema_entry entry;
    entry.table_name = table_name;
    for (const auto& [name, desc] : props) {
        entry.properties[name] = desc;
    }
    return entry;
}

// Helper: create a swift_dynamic_object with properties properly set
static lattice::swift_dynamic_object make_sdo(
    const std::string& table_name,
    const lattice::SwiftSchema& schema) {
    lattice::swift_dynamic_object sdo;
    sdo.table_name = table_name;
    sdo.properties = schema;
    return sdo;
}

static lattice::property_descriptor text_prop(const std::string& name) {
    lattice::property_descriptor desc;
    desc.name = name;
    desc.type = lattice::column_type::text;
    desc.kind = lattice::property_kind::primitive;
    return desc;
}

static lattice::property_descriptor int_prop(const std::string& name) {
    lattice::property_descriptor desc;
    desc.name = name;
    desc.type = lattice::column_type::integer;
    desc.kind = lattice::property_kind::primitive;
    return desc;
}

static lattice::property_descriptor real_prop(const std::string& name) {
    lattice::property_descriptor desc;
    desc.name = name;
    desc.type = lattice::column_type::real;
    desc.kind = lattice::property_kind::primitive;
    return desc;
}

static lattice::property_descriptor blob_prop(const std::string& name) {
    lattice::property_descriptor desc;
    desc.name = name;
    desc.type = lattice::column_type::blob;
    desc.kind = lattice::property_kind::primitive;
    desc.is_vector = true;
    return desc;
}

// ============================================================================
// swift_lattice CRUD via dynamic_object
// ============================================================================

TEST(Bridge, CreateAndReadViaDynamicObject) {
    TempDB tmp{"bridge_crud"};
    lattice::SchemaVector schemas = {
        make_schema("BridgePerson", {
            {"name", text_prop("name")},
            {"age", int_prop("age")},
        })
    };

    lattice::swift_configuration config(tmp.str());
    auto* lattice_ref = lattice::swift_lattice_ref::create(config, schemas);
    auto& db = *lattice_ref->get();

    // Create via dynamic_object (use scope to destroy before query)
    {
        auto sdo = make_sdo("BridgePerson", schemas[0].properties);
        sdo.values["name"] = std::string("Alice");
        sdo.values["age"] = int64_t(30);
        lattice::dynamic_object obj(sdo);
        db.add(obj);
    }

    // Query — verify DB state
    auto results = db.objects("BridgePerson");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].get_string("name"), "Alice");
    EXPECT_EQ(int(results[0].get_int("age")), 30);
}

TEST(Bridge, UpdateViaDynamicObject) {
    TempDB tmp{"bridge_update"};
    lattice::SchemaVector schemas = {
        make_schema("BridgePerson", {
            {"name", text_prop("name")},
            {"age", int_prop("age")},
        })
    };

    lattice::swift_configuration config(tmp.str());
    auto* lattice_ref = lattice::swift_lattice_ref::create(config, schemas);
    auto& db = *lattice_ref->get();

    auto sdo = make_sdo("BridgePerson", schemas[0].properties);
    sdo.values["name"] = std::string("Bob");
    sdo.values["age"] = int64_t(25);

    { lattice::dynamic_object obj(sdo); db.add(obj); }

    // Update via managed object from query
    auto results = db.objects("BridgePerson");
    ASSERT_EQ(results.size(), 1u);
    results[0].set_value("name", std::string("Bobby"));
    results[0].set_value("age", int64_t(26));

    auto updated = db.objects("BridgePerson");
    ASSERT_EQ(updated.size(), 1u);
    EXPECT_EQ(updated[0].get_string("name"), "Bobby");
    EXPECT_EQ(int(updated[0].get_int("age")), 26);
}

TEST(Bridge, DeleteViaDynamicObject) {
    TempDB tmp{"bridge_delete"};
    lattice::SchemaVector schemas = {
        make_schema("BridgePerson", {
            {"name", text_prop("name")},
            {"age", int_prop("age")},
        })
    };

    lattice::swift_configuration config(tmp.str());
    auto* lattice_ref = lattice::swift_lattice_ref::create(config, schemas);
    auto& db = *lattice_ref->get();

    auto sdo = make_sdo("BridgePerson", schemas[0].properties);
    sdo.values["name"] = std::string("Charlie");
    sdo.values["age"] = int64_t(35);

    { lattice::dynamic_object obj(sdo); db.add(obj); }
    EXPECT_EQ(db.objects("BridgePerson").size(), 1u);

    // Remove via managed object from query
    auto to_delete = db.objects("BridgePerson");
    ASSERT_EQ(to_delete.size(), 1u);
    db.remove(to_delete[0]);
    EXPECT_EQ(db.objects("BridgePerson").size(), 0u);
}

// ============================================================================
// BLOB / Vector data through the bridge layer
// ============================================================================

TEST(Bridge, BlobSetGetRoundTrip) {
    TempDB tmp{"bridge_blob"};
    lattice::SchemaVector schemas = {
        make_schema("BridgeVec", {
            {"label", text_prop("label")},
            {"embedding", blob_prop("embedding")},
        })
    };

    lattice::swift_configuration config(tmp.str());
    auto* lattice_ref = lattice::swift_lattice_ref::create(config, schemas);
    auto& db = *lattice_ref->get();

    // Create object with BLOB data
    std::vector<float> original = {1.0f, 2.0f, 3.0f, 4.5f, -0.5f};
    auto blob = pack_floats(original);

    auto sdo = make_sdo("BridgeVec", schemas[0].properties);
    sdo.values["label"] = std::string("test vec");
    sdo.values["embedding"] = blob;

    { lattice::dynamic_object obj(sdo); db.add(obj); }

    // Read back via query
    auto results = db.objects("BridgeVec");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].get_string("label"), "test vec");

    // Get BLOB back and verify float data
    // managed<swift_dynamic_object> doesn't expose get_data directly —
    // read BLOB from raw SQL to verify storage
    auto blob_rows = db.db().query(
        "SELECT embedding FROM BridgeVec WHERE globalId = ?",
        {std::string(results[0].global_id())});
    ASSERT_EQ(blob_rows.size(), 1u);
    auto stored_blob = std::get<std::vector<uint8_t>>(blob_rows[0].at("embedding"));
    ASSERT_EQ(stored_blob.size(), blob.size());
    auto stored_floats = unpack_floats(stored_blob);
    ASSERT_EQ(stored_floats.size(), original.size());
    for (size_t i = 0; i < original.size(); i++) {
        EXPECT_NEAR(stored_floats[i], original[i], 0.001)
            << "Float mismatch at index " << i;
    }
}

TEST(Bridge, BlobVec0NearestNeighbors) {
    TempDB tmp{"bridge_vec0"};
    lattice::SchemaVector schemas = {
        make_schema("BridgeVec", {
            {"label", text_prop("label")},
            {"embedding", blob_prop("embedding")},
        })
    };

    lattice::swift_configuration config(tmp.str());
    auto* lattice_ref = lattice::swift_lattice_ref::create(config, schemas);
    auto& db = *lattice_ref->get();

    // Insert documents with embeddings via the bridge
    auto insert_doc = [&](const std::string& label, const std::vector<float>& vec) {
        auto sdo = make_sdo("BridgeVec", schemas[0].properties);
        sdo.values["label"] = label;
        sdo.values["embedding"] = pack_floats(vec);
        { lattice::dynamic_object obj(sdo); db.add(obj); }

        // Get globalId from the just-inserted row
        auto rows = db.db().query(
            "SELECT globalId FROM BridgeVec WHERE label = ?", {label});
        if (!rows.empty()) {
            auto gid = std::get<std::string>(rows[0].at("globalId"));
            db.upsert_vec0("BridgeVec", "embedding", gid, pack_floats(vec));
        }
    };

    insert_doc("doc_a", {1.0f, 0.0f, 0.0f});
    insert_doc("doc_b", {0.0f, 1.0f, 0.0f});
    insert_doc("doc_c", {0.0f, 0.0f, 1.0f});

    // KNN query through the bridge
    auto qvec = pack_floats({1.0f, 0.0f, 0.0f});
    auto results = db.knn_query("BridgeVec", "embedding", qvec, 3,
                                 lattice::lattice_db::distance_metric::l2);
    ASSERT_EQ(results.size(), 3u);

    // First result should be doc_a (exact match)
    auto first_row = db.db().query(
        "SELECT label FROM BridgeVec WHERE globalId = ?",
        {results[0].global_id});
    ASSERT_EQ(first_row.size(), 1u);
    EXPECT_EQ(std::get<std::string>(first_row[0].at("label")), "doc_a");
    EXPECT_NEAR(results[0].distance, 0.0, 0.001);
}

// ============================================================================
// receive_sync_data — the complete sync ingest path through the bridge
// ============================================================================

TEST(Bridge, ReceiveSyncDataInsert) {
    TempDB tmp{"bridge_sync"};
    lattice::SchemaVector schemas = {
        make_schema("BridgePerson", {
            {"name", text_prop("name")},
            {"age", int_prop("age")},
        })
    };

    lattice::swift_configuration config(tmp.str());
    auto* lattice_ref = lattice::swift_lattice_ref::create(config, schemas);
    auto& db = *lattice_ref->get();

    // Build a sync event JSON
    lattice::audit_log_entry entry;
    entry.global_id = "sync-entry-1";
    entry.table_name = "BridgePerson";
    entry.operation = "INSERT";
    entry.row_id = 1;
    entry.global_row_id = "bridge-person-uuid";
    entry.changed_fields = {
        {"name", lattice::any_property("Synced Person")},
        {"age", lattice::any_property(42)}
    };
    entry.changed_fields_names = {"name", "age"};
    entry.timestamp = "2024-01-01T00:00:00Z";

    auto sse = lattice::server_sent_event::make_audit_log({entry});
    auto json = sse.to_json();
    lattice::ByteVector data(json.begin(), json.end());

    // Call receive_sync_data — the entry point Swift uses
    auto applied = db.receive_sync_data(data);
    EXPECT_FALSE(applied.empty());
    EXPECT_FALSE(db.last_receive_error().has_value());

    // Verify object was created
    auto results = db.objects("BridgePerson");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].get_string("name"), "Synced Person");
    EXPECT_EQ(int(results[0].get_int("age")), 42);
}

TEST(Bridge, ReceiveSyncDataWithBlob) {
    // THE KEY TEST: BLOB data through the full sync bridge path
    TempDB tmp{"bridge_sync_blob"};
    lattice::SchemaVector schemas = {
        make_schema("BridgeVec", {
            {"label", text_prop("label")},
            {"embedding", blob_prop("embedding")},
        })
    };

    lattice::swift_configuration config(tmp.str());
    auto* lattice_ref = lattice::swift_lattice_ref::create(config, schemas);
    auto& db = *lattice_ref->get();

    // Simulate the source side: object with BLOB → AuditLog → hex encoding
    std::vector<float> original = {1.0f, 2.0f, 3.0f, 4.5f, -0.5f};
    auto blob = pack_floats(original);

    // The AuditLog trigger stores BLOB as hex(embedding) → string_kind
    std::string hex_str;
    for (auto byte : blob) {
        char buf[3];
        snprintf(buf, sizeof(buf), "%02X", byte);
        hex_str += buf;
    }

    lattice::audit_log_entry entry;
    entry.global_id = "blob-sync-1";
    entry.table_name = "BridgeVec";
    entry.operation = "INSERT";
    entry.row_id = 1;
    entry.global_row_id = "bridge-vec-uuid";
    // The embedding is hex-encoded (string_kind), matching what the audit trigger produces
    entry.changed_fields = {
        {"label", lattice::any_property("synced vec")},
        {"embedding", lattice::any_property(hex_str)}  // hex string, not raw bytes
    };
    entry.changed_fields_names = {"label", "embedding"};
    entry.timestamp = "2024-01-01T00:00:00Z";

    // Full round-trip through SSE JSON
    auto sse = lattice::server_sent_event::make_audit_log({entry});
    auto json = sse.to_json();
    lattice::ByteVector data(json.begin(), json.end());

    auto applied = db.receive_sync_data(data);
    EXPECT_FALSE(applied.empty()) << "receive_sync_data should succeed";

    auto err = db.last_receive_error();
    EXPECT_FALSE(err.has_value()) << "No error expected, got: " << (err ? *err : "");

    // Verify the object was created with correct BLOB data
    auto results = db.objects("BridgeVec");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].get_string("label"), "synced vec");

    // Read back BLOB and verify it matches original
    // managed<swift_dynamic_object> doesn't expose get_data directly —
    // read BLOB from raw SQL to verify storage
    auto blob_rows = db.db().query(
        "SELECT embedding FROM BridgeVec WHERE globalId = ?",
        {std::string(results[0].global_id())});
    ASSERT_EQ(blob_rows.size(), 1u);
    auto stored_blob = std::get<std::vector<uint8_t>>(blob_rows[0].at("embedding"));
    auto stored_floats = unpack_floats(stored_blob);
    ASSERT_EQ(stored_floats.size(), original.size())
        << "BLOB size mismatch after sync: expected " << original.size()
        << " floats, got " << stored_floats.size();
    for (size_t i = 0; i < original.size(); i++) {
        EXPECT_NEAR(stored_floats[i], original[i], 0.001)
            << "Float mismatch at index " << i << " after sync round-trip";
    }
}

TEST(Bridge, ReceiveSyncDataBlobWithRawBytes) {
    // Test what happens if BLOB comes as data_kind (raw bytes) instead of
    // string_kind (hex). This is what json_to_any_property produces when
    // deserializing {kind:6, value:"hex_str"}.
    TempDB tmp{"bridge_sync_blob_raw"};
    lattice::SchemaVector schemas = {
        make_schema("BridgeVec", {
            {"label", text_prop("label")},
            {"embedding", blob_prop("embedding")},
        })
    };

    lattice::swift_configuration config(tmp.str());
    auto* lattice_ref = lattice::swift_lattice_ref::create(config, schemas);
    auto& db = *lattice_ref->get();

    std::vector<float> original = {1.0f, 2.0f, 3.0f};
    auto blob = pack_floats(original);

    // Build entry with data_kind (raw bytes) — this is what happens
    // after a from_json round-trip of a {kind:6, value:"hex"} property
    lattice::audit_log_entry entry;
    entry.global_id = "blob-raw-1";
    entry.table_name = "BridgeVec";
    entry.operation = "INSERT";
    entry.row_id = 1;
    entry.global_row_id = "bridge-raw-uuid";
    entry.changed_fields = {
        {"label", lattice::any_property("raw bytes test")},
        {"embedding", lattice::any_property(blob)}  // data_kind with raw bytes
    };
    entry.changed_fields_names = {"label", "embedding"};
    entry.timestamp = "2024-01-01T00:00:00Z";

    // Full SSE round-trip
    auto sse = lattice::server_sent_event::make_audit_log({entry});
    auto json = sse.to_json();
    lattice::ByteVector data(json.begin(), json.end());

    auto applied = db.receive_sync_data(data);
    auto err = db.last_receive_error();
    EXPECT_FALSE(err.has_value()) << "Error: " << (err ? *err : "");
    EXPECT_FALSE(applied.empty());

    // Verify BLOB survived
    auto results = db.objects("BridgeVec");
    ASSERT_EQ(results.size(), 1u);

    // managed<swift_dynamic_object> doesn't expose get_data directly —
    // read BLOB from raw SQL to verify storage
    auto blob_rows = db.db().query(
        "SELECT embedding FROM BridgeVec WHERE globalId = ?",
        {std::string(results[0].global_id())});
    ASSERT_EQ(blob_rows.size(), 1u);
    auto stored_blob = std::get<std::vector<uint8_t>>(blob_rows[0].at("embedding"));
    auto stored_floats = unpack_floats(stored_blob);
    ASSERT_EQ(stored_floats.size(), original.size())
        << "data_kind BLOB failed: expected " << original.size()
        << " floats, got " << stored_floats.size();
    for (size_t i = 0; i < original.size(); i++) {
        EXPECT_NEAR(stored_floats[i], original[i], 0.001);
    }
}

// ============================================================================
// events_after — the complete sync outbound path
// ============================================================================

TEST(Bridge, EventsAfterWithBlob) {
    // Verify that when an object with a BLOB is added, the AuditLog entry
    // produced by events_after() has the BLOB correctly hex-encoded.
    TempDB tmp{"bridge_events"};
    lattice::SchemaVector schemas = {
        make_schema("BridgeVec", {
            {"label", text_prop("label")},
            {"embedding", blob_prop("embedding")},
        })
    };

    lattice::swift_configuration config(tmp.str());
    auto* lattice_ref = lattice::swift_lattice_ref::create(config, schemas);
    auto& db = *lattice_ref->get();

    // Add object with BLOB
    std::vector<float> original = {1.0f, 2.0f, 3.0f};
    auto sdo = make_sdo("BridgeVec", schemas[0].properties);
    sdo.values["label"] = std::string("outbound test");
    sdo.values["embedding"] = pack_floats(original);

    { lattice::dynamic_object obj(sdo); db.add(obj); }

    // Get events
    auto events = db.events_after(std::nullopt);
    ASSERT_FALSE(events.empty());

    // Find the BridgeVec INSERT entry
    bool found = false;
    for (const auto& e : events) {
        if (e.table_name == "BridgeVec" && e.operation == "INSERT") {
            found = true;

            // The embedding field should be in changedFields as a hex string
            auto it = e.changed_fields.find("embedding");
            ASSERT_NE(it, e.changed_fields.end())
                << "embedding field missing from AuditLog changedFields";

            // Serialize to JSON and back (simulates send over wire)
            auto sse = lattice::server_sent_event::make_audit_log({e});
            auto json = sse.to_json();

            // Apply on a second instance
            TempDB tmp2{"bridge_events_target"};
            lattice::swift_configuration config2(tmp2.str());
            lattice::swift_lattice db2(config2, schemas);

            lattice::ByteVector data(json.begin(), json.end());
            auto applied = db2.receive_sync_data(data);
            EXPECT_FALSE(applied.empty());

            // Verify BLOB round-tripped correctly
            auto results = db2.objects("BridgeVec");
            ASSERT_EQ(results.size(), 1u);
            EXPECT_EQ(results[0].get_string("label"), "outbound test");

            auto blob_rows2 = db2.db().query(
                "SELECT embedding FROM BridgeVec WHERE globalId = ?",
                {std::string(results[0].global_id())});
            ASSERT_EQ(blob_rows2.size(), 1u);
            auto stored = std::get<std::vector<uint8_t>>(blob_rows2[0].at("embedding"));
            auto floats = unpack_floats(stored);
            ASSERT_EQ(floats.size(), original.size());
            for (size_t i = 0; i < original.size(); i++) {
                EXPECT_NEAR(floats[i], original[i], 0.001);
            }
            break;
        }
    }
    EXPECT_TRUE(found) << "No BridgeVec INSERT entry in events_after()";
}

// ============================================================================
// dynamic_object_ref (Swift's primary handle)
// ============================================================================

TEST(Bridge, DynamicObjectRefCRUD) {
    TempDB tmp{"bridge_ref"};
    lattice::SchemaVector schemas = {
        make_schema("BridgePerson", {
            {"name", text_prop("name")},
            {"age", int_prop("age")},
        })
    };

    lattice::swift_configuration config(tmp.str());
    auto* lattice_ref = lattice::swift_lattice_ref::create(config, schemas);
    auto& db = *lattice_ref->get();

    // Create via ref with properties (what Swift does)
    auto sdo = make_sdo("BridgePerson", schemas[0].properties);
    sdo.values["name"] = std::string("RefPerson");
    sdo.values["age"] = int64_t(28);
    auto* ref = new lattice::dynamic_object_ref(sdo);

    db.add(ref);

    // Query — verify it was inserted
    auto results = db.objects("BridgePerson");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].get_string("name"), "RefPerson");
    EXPECT_EQ(int(results[0].get_int("age")), 28);

    // Cleanup
    delete ref;
}

// ============================================================================
// Observation through the bridge
// ============================================================================

TEST(Bridge, TableObserverViaBridge) {
    TempDB tmp{"bridge_obs"};
    lattice::SchemaVector schemas = {
        make_schema("BridgePerson", {
            {"name", text_prop("name")},
            {"age", int_prop("age")},
        })
    };

    lattice::swift_configuration config(tmp.str());
    auto* lattice_ref = lattice::swift_lattice_ref::create(config, schemas);
    auto& db = *lattice_ref->get();

    // Use the lattice_db parent's observer API (std::function based)
    int callback_count = 0;
    auto obs_id = static_cast<lattice::lattice_db&>(db).add_table_observer("BridgePerson",
        [&](const std::string& op, int64_t row_id, const std::string& gid) {
            callback_count++;
        });

    auto sdo = make_sdo("BridgePerson", schemas[0].properties);
    sdo.values["name"] = std::string("Observed");
    sdo.values["age"] = int64_t(30);

    { lattice::dynamic_object obj(sdo); db.add(obj); }

    EXPECT_GE(callback_count, 1);

    static_cast<lattice::lattice_db&>(db).remove_table_observer("BridgePerson", obs_id);
}

// ============================================================================
// swift_lattice_ref (cached factory, Swift's primary entry point)
// ============================================================================

TEST(Bridge, SwiftLatticeRefCreate) {
    TempDB tmp{"bridge_lattice_ref"};
    lattice::SchemaVector schemas = {
        make_schema("BridgePerson", {
            {"name", text_prop("name")},
            {"age", int_prop("age")},
        })
    };

    lattice::swift_configuration config(tmp.str());
    auto* ref = lattice::swift_lattice_ref::create(config, schemas);
    ASSERT_NE(ref, nullptr);

    // Add object through the ref
    auto sdo = make_sdo("BridgePerson", schemas[0].properties);
    sdo.values["name"] = std::string("ViaRef");
    sdo.values["age"] = int64_t(40);

    { lattice::dynamic_object obj(sdo); ref->get()->add(obj); }

    auto results = ref->get()->objects("BridgePerson");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].get_string("name"), "ViaRef");

    // Cleanup
    delete ref;
}

#endif // !__linux__
