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

// Deterministic regression for the close-during-read crash: evict() (which
// swift_lattice::close() calls) must NOT erase ptr_cache_ while the instance is
// still alive — a live managed object resolves its lattice via get_by_pointer(),
// and erasing the entry returns null → use-after-free during a concurrent read.
// (The Swift-level repro, CloseGuardTests.test_ConcurrentReadsDuringClose, is
// timing-based; this asserts the invariant directly.)
TEST(Bridge, EvictPreservesPtrCacheWhileInstanceAlive) {
    TempDB tmp{"bridge_evict"};
    lattice::SchemaVector schemas = {
        make_schema("EvictPerson", {
            {"name", text_prop("name")},
            {"age", int_prop("age")},
        })
    };
    lattice::swift_configuration config(tmp.str());
    auto* lattice_ref = lattice::swift_lattice_ref::create(config, schemas);
    lattice::swift_lattice* ptr = lattice_ref->get();

    // Resolvable before eviction.
    ASSERT_NE(lattice::detail::LatticeCache::instance().get_by_pointer(ptr), nullptr);

    // Evict — exactly what swift_lattice::close() does. The instance is STILL
    // alive (lattice_ref holds the shared_ptr), so it must still resolve.
    lattice::detail::LatticeCache::instance().evict(ptr);

    EXPECT_NE(lattice::detail::LatticeCache::instance().get_by_pointer(ptr), nullptr)
        << "evict() must not erase ptr_cache_ for a still-alive instance";
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
    lattice::dynamic_object_ref ref(sdo);

    db.add(ref);

    // Query — verify it was inserted
    auto results = db.objects("BridgePerson");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].get_string("name"), "RefPerson");
    EXPECT_EQ(int(results[0].get_int("age")), 28);
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

    // Use the lattice_db parent's observer API (std::function based,
    // batched). One fire per WAL flush; the single-row insert below
    // produces a count=1 batch.
    int callback_count = 0;
    auto obs_id = static_cast<lattice::lattice_db&>(db).add_table_observer("BridgePerson",
        [&](const std::vector<lattice::lattice_db::change_event>&) {
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

// ============================================================================
// LatticeCache concurrency — on-disk get_or_create builds outside the lock and
// de-dups concurrent opens of the same key to a single instance.
// ============================================================================

TEST(Bridge, CacheConcurrentSamePathDedup) {
    TempDB tmp{"bridge_cache_dedup"};
    lattice::SchemaVector schemas = {
        make_schema("BridgePerson", {
            {"name", text_prop("name")},
            {"age", int_prop("age")},
        })
    };

    constexpr int kThreads = 16;
    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    std::vector<lattice::swift_lattice_ref*> refs(kThreads, nullptr);

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            // All threads target the SAME on-disk path. Release into the lock
            // simultaneously to maximize the chance of racing construction.
            lattice::swift_configuration config(tmp.str());
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
            refs[i] = lattice::swift_lattice_ref::create(config, schemas);
        });
    }
    go.store(true, std::memory_order_release);
    for (auto& t : threads) t.join();

    // Every concurrent open of the same key must resolve to ONE instance —
    // no duplicate SQLite connection to the same file.
    ASSERT_NE(refs[0], nullptr);
    auto* first = refs[0]->get();
    ASSERT_NE(first, nullptr);
    for (int i = 0; i < kThreads; ++i) {
        ASSERT_NE(refs[i], nullptr) << "create() returned null on thread " << i;
        EXPECT_EQ(refs[i]->get(), first)
            << "thread " << i << " got a different swift_lattice instance";
        EXPECT_EQ(refs[i]->hash_value(), refs[0]->hash_value());
    }

    // The shared instance is usable.
    auto sdo = make_sdo("BridgePerson", schemas[0].properties);
    sdo.values["name"] = std::string("Concurrent");
    sdo.values["age"] = int64_t(1);
    { lattice::dynamic_object obj(sdo); first->add(obj); }
    EXPECT_EQ(first->objects("BridgePerson").size(), 1u);

    for (auto* r : refs) delete r;
}

TEST(Bridge, CacheConcurrentCreateResolveStress) {
    // TSan target: hammer get_or_create's cache-hit path and get_by_pointer
    // (the per-object hot path) concurrently. A seed ref keeps the cached
    // instance alive so every create() resolves to it.
    TempDB tmp{"bridge_cache_stress"};
    lattice::SchemaVector schemas = {
        make_schema("BridgePerson", {
            {"name", text_prop("name")},
            {"age", int_prop("age")},
        })
    };

    auto* seed = lattice::swift_lattice_ref::create(
        lattice::swift_configuration(tmp.str()), schemas);
    ASSERT_NE(seed, nullptr);
    auto seed_hash = seed->hash_value();

    constexpr int kThreads = 12;
    constexpr int kIters = 25;
    std::atomic<bool> go{false};
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&] {
            lattice::swift_configuration config(tmp.str());
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
            for (int j = 0; j < kIters; ++j) {
                auto* ref = lattice::swift_lattice_ref::create(config, schemas);
                if (!ref || ref->hash_value() != seed_hash) { failures++; if (ref) delete ref; continue; }
                // Reverse lookup exercises get_by_pointer under contention.
                auto back = lattice::swift_lattice_ref::shared_for_lattice(ref->get());
                if (!back || back.get() != ref->get()) failures++;
                delete ref;
            }
        });
    }
    go.store(true, std::memory_order_release);
    for (auto& t : threads) t.join();

    EXPECT_EQ(failures.load(), 0);
    delete seed;
}

// ============================================================================
// Bridge fingerprint fast path — write-free reopen at the swift_lattice layer
// ============================================================================

TEST(Bridge, ReopenIsWriteFree) {
    TempDB tmp{"bridge_writefree"};
    lattice::SchemaVector schemas = {
        make_schema("BridgeFpPerson", {
            {"name", text_prop("name")},
            {"age", int_prop("age")},
        })
    };

    // NOTE: direct swift_lattice construction (no swift_lattice_ref) — safe
    // for SQL + objects() but NOT for dynamic_object add(), which requires a
    // registered ref. Writes below go through raw SQL so the audit triggers
    // (and their sync_disabled() call) still exercise the trigger path.

    // Opens #1-#3 settle the schema + close-time stats; each open really
    // re-runs the ensure path (no LatticeCache involved).
    {
        lattice::swift_configuration config(tmp.str());
        lattice::swift_lattice db(config, schemas);
        db.db().execute(
            "INSERT INTO BridgeFpPerson(name, age) VALUES('Alice', 30)");
    }
    {
        lattice::swift_configuration config(tmp.str());
        lattice::swift_lattice db(config, schemas);
    }
    {
        lattice::swift_configuration config(tmp.str());
        lattice::swift_lattice db(config, schemas);
    }

    // Open #4 must be write-free AND fully functional from in-memory state.
    {
        lattice::swift_configuration config(tmp.str());
        lattice::swift_lattice db(config, schemas);
        EXPECT_EQ(sqlite3_total_changes(db.db().handle()), 0);

        // Hydration works (schemas_ populated by the fast path)
        auto results = db.objects("BridgeFpPerson");
        ASSERT_EQ(results.size(), 1u);
        EXPECT_EQ(results[0].get_string("name"), "Alice");

        // Writes still work: the INSERT fires the audit trigger, which calls
        // sync_disabled() — this throws if the per-connection SQL function
        // wasn't registered on the fast path.
        db.db().execute(
            "INSERT INTO BridgeFpPerson(name, age) VALUES('Bob', 25)");
        EXPECT_EQ(db.objects("BridgeFpPerson").size(), 2u);
    }
}

// ============================================================================
// Link-list position coherence — dangling junction rows must not shift
// positions off the element cache (EXC_BAD_ACCESS via ListSlice.first)
// ============================================================================

static lattice::property_descriptor list_prop(const std::string& name,
                                              const std::string& target) {
    lattice::property_descriptor desc;
    desc.name = name;
    desc.kind = lattice::property_kind::list;
    desc.target_table = target;
    desc.type = lattice::column_type::text;
    return desc;
}

TEST(Bridge, ListPositionsSkipDanglingLinks) {
    TempDB tmp{"bridge_dangling"};
    lattice::SchemaVector schemas = {
        make_schema("FpBand", {
            {"name", text_prop("name")},
            {"albums", list_prop("albums", "FpAlbum")},
        }),
        make_schema("FpAlbum", {
            {"title", text_prop("title")},
            {"year", int_prop("year")},
        }),
    };

    auto* lattice_ref = lattice::swift_lattice_ref::create(
        lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *lattice_ref->get();

    db.db().execute("INSERT INTO FpBand(globalId, name) VALUES('band-1', 'Zeppelin')");
    db.db().execute("INSERT INTO FpAlbum(globalId, title, year) VALUES('al-1', 'IV', 1971)");
    db.db().execute("INSERT INTO FpAlbum(globalId, title, year) VALUES('al-2', 'Houses', 1973)");
    db.db().execute("INSERT INTO FpAlbum(globalId, title, year) VALUES('al-3', 'Graffiti', 1975)");
    db.db().execute("INSERT INTO _FpBand_FpAlbum_albums(lhs, rhs) VALUES('band-1', 'al-1')");
    db.db().execute("INSERT INTO _FpBand_FpAlbum_albums(lhs, rhs) VALUES('band-1', 'al-2')");
    db.db().execute("INSERT INTO _FpBand_FpAlbum_albums(lhs, rhs) VALUES('band-1', 'al-3')");

    // Dangle the FIRST link: target row deleted, junction row lingers (what
    // upsert churn / cross-process deletes leave behind). The element cache
    // skips it, so positions numbered over ALL link rows would shift by one —
    // hydrating the wrong element or walking off the end of the cache.
    db.db().execute("DELETE FROM FpAlbum WHERE globalId = 'al-1'");

    auto bands = db.objects("FpBand");
    ASSERT_EQ(bands.size(), 1u);
    auto field = bands[0].get_managed_field<std::vector<lattice::swift_dynamic_object*>>("albums");
    lattice::link_list list(field);

    EXPECT_EQ(list.size(), 2u);  // live membership only

    // Newest-first positions — the exact ListSlice.sortedBy(...).first shape
    // that crashed in the field.
    auto positions = list.find_indices("", "year", /*ascending=*/false);
    ASSERT_EQ(positions.size(), 2u);
    for (auto pos : positions) {
        EXPECT_LT(pos, list.size()) << "position indexes past the element cache";
    }

    auto top = list[positions[0]];
    EXPECT_EQ(int(top.object->get_int("year")), 1975);
    auto next = list[positions[1]];
    EXPECT_EQ(int(next.object->get_int("year")), 1973);

    // Out-of-range access is a catchable error, not undefined behavior.
    EXPECT_THROW(list[99].object->get_int("year"), std::out_of_range);

    delete lattice_ref;
}

TEST(Bridge, UpsertRebindsToExistingRowAndAppendIsIdempotent) {
    TempDB tmp{"bridge_upsert_rebind"};
    lattice::swift_schema_entry sig_entry = make_schema("FpSig", {
        {"key", text_prop("key")},
        {"val", int_prop("val")},
    });
    sig_entry.constraints.push_back(lattice::swift_constraint({"key"}, /*upsert=*/true));
    lattice::SchemaVector schemas = {
        make_schema("FpOwner", {
            {"name", text_prop("name")},
            {"sigs", list_prop("sigs", "FpSig")},
        }),
        sig_entry,
    };

    auto* lattice_ref = lattice::swift_lattice_ref::create(
        lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *lattice_ref->get();

    db.db().execute("INSERT INTO FpOwner(globalId, name) VALUES('own-1', 'O')");

    // First write: plain insert.
    {
        auto sdo = make_sdo("FpSig", schemas[1].properties);
        sdo.values["key"] = std::string("k1");
        sdo.values["val"] = int64_t(1);
        lattice::dynamic_object obj(sdo);
        db.add(obj);
    }
    auto gid_rows = db.db().query("SELECT globalId FROM FpSig");
    ASSERT_EQ(gid_rows.size(), 1u);
    auto original_gid = std::get<std::string>(gid_rows[0].at("globalId"));

    // Second write: same unique key — ON CONFLICT DO UPDATE. The object MUST
    // rebind to the surviving row's identity; binding to the freshly
    // generated globalId points every later link write at a row that doesn't
    // exist (this is how signal lists accumulated dangling junction rows).
    auto sdo2 = make_sdo("FpSig", schemas[1].properties);
    sdo2.values["key"] = std::string("k1");
    sdo2.values["val"] = int64_t(2);
    lattice::dynamic_object obj2(sdo2);
    db.add(obj2);

    EXPECT_EQ(obj2.get_string("globalId"), original_gid)
        << "upsert did not rebind to the existing row";
    auto rows = db.db().query("SELECT COUNT(*) AS c, MAX(val) AS v FROM FpSig");
    EXPECT_EQ(std::get<int64_t>(rows[0].at("c")), 1);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("v")), 2);

    // Append the rebound object to the owner's list — buildSignalHistory's
    // add-then-append flow. The junction row must reference the REAL row.
    auto owners = db.objects("FpOwner");
    ASSERT_EQ(owners.size(), 1u);
    auto field = owners[0].get_managed_field<std::vector<lattice::swift_dynamic_object*>>("sigs");
    lattice::link_list list(field);
    auto* obj2_ref = lattice::dynamic_object_ref::wrap(obj2);
    list.push_back(*obj2_ref);

    auto junction = db.db().query(
        "SELECT rhs FROM _FpOwner_FpSig_sigs WHERE lhs = 'own-1'");
    ASSERT_EQ(junction.size(), 1u);
    EXPECT_EQ(std::get<std::string>(junction[0].at("rhs")), original_gid)
        << "junction row references a phantom globalId (dangling link)";

    // Re-append is a set-semantics no-op, not a PRIMARY KEY violation.
    list.push_back(*obj2_ref);
    delete obj2_ref;
    auto recount = db.db().query(
        "SELECT COUNT(*) AS c FROM _FpOwner_FpSig_sigs WHERE lhs = 'own-1'");
    EXPECT_EQ(std::get<int64_t>(recount[0].at("c")), 1);

    delete lattice_ref;
}

// ============================================================================
// Row cache — materialized reads (see dynamic_object.hpp contract)
// ============================================================================
// Stays inside the !__linux__ guard: these tests exercise the Swift bridge
// (swift_lattice_ref/dynamic_object), which the test target only links on
// Apple platforms — same constraint as everything above.

namespace {
lattice::property_descriptor nullable_text_prop(const std::string& name) {
    lattice::property_descriptor desc;
    desc.name = name;
    desc.type = lattice::column_type::text;
    desc.kind = lattice::property_kind::primitive;
    desc.nullable = true;
    return desc;
}
}  // namespace

TEST(Bridge, RowCacheMaterializedReads) {
    TempDB tmp{"rowcache"};
    lattice::SchemaVector schemas = {
        make_schema("RcPerson", {
            {"name", text_prop("name")},
            {"age", int_prop("age")},
        })
    };
    auto* ref = lattice::swift_lattice_ref::create(
        lattice::swift_configuration(tmp.str()), schemas);
    ASSERT_NE(ref, nullptr);
    auto& db = *ref->get();

    {
        auto sdo = make_sdo("RcPerson", schemas[0].properties);
        sdo.values["name"] = std::string("Alice");
        sdo.values["age"] = int64_t(30);
        lattice::dynamic_object obj(sdo);
        db.add(obj);
    }

    auto results = db.objects("RcPerson");
    ASSERT_EQ(results.size(), 1u);

    // Materialized reads: the query already hydrated the row — ZERO further
    // SQL for any number of property reads.
    lattice::dynamic_object cached(results[0]);
    cached.enable_row_cache();
    const auto base = lattice::database::total_statement_count();
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(cached.get_string("name"), "Alice");
        EXPECT_EQ(cached.get_int("age"), 30);
    }
    EXPECT_EQ(lattice::database::total_statement_count() - base, 0u)
        << "materialized reads issued SQL";

    // The live path (default) pays one statement per read — the recall
    // N-statements pathology this exists to kill. Documents the contrast.
    lattice::dynamic_object live(results[0]);
    const auto live_base = lattice::database::total_statement_count();
    for (int i = 0; i < 5; ++i) {
        (void)live.get_string("name");
        (void)live.get_int("age");
    }
    EXPECT_GE(lattice::database::total_statement_count() - live_base, 10u)
        << "expected the live path to issue one statement per read";

    // Write-through: DB row updates AND the cached read sees the new value.
    cached.set_int("age", 31);
    const auto after_write = lattice::database::total_statement_count();
    EXPECT_EQ(cached.get_int("age"), 31);
    EXPECT_EQ(lattice::database::total_statement_count() - after_write, 0u)
        << "read-your-writes should be served from the cache";
    auto verify = db.objects("RcPerson");
    EXPECT_EQ(verify[0].get_int("age"), 31) << "write-through missed the DB";

    // Atomic SQL-side increment: lands in the DB, drops the cached key so
    // the next materialized read falls through live (no stale value).
    cached.increment_int_field("age", 1);
    EXPECT_EQ(cached.get_int("age"), 32);
    auto verify2 = db.objects("RcPerson");
    EXPECT_EQ(verify2[0].get_int("age"), 32) << "increment missed the DB";

    delete ref;
}

TEST(Bridge, RowCacheSnapshotAndRefresh) {
    TempDB tmp{"rowcache_refresh"};
    lattice::SchemaVector schemas = {
        make_schema("RcSnap", {
            {"name", text_prop("name")},
            {"age", int_prop("age")},
        })
    };
    auto* ref = lattice::swift_lattice_ref::create(
        lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *ref->get();
    {
        auto sdo = make_sdo("RcSnap", schemas[0].properties);
        sdo.values["name"] = std::string("Snap");
        sdo.values["age"] = int64_t(1);
        lattice::dynamic_object obj(sdo);
        db.add(obj);
    }
    auto results = db.objects("RcSnap");
    lattice::dynamic_object cached(results[0]);
    cached.enable_row_cache();
    ASSERT_EQ(cached.get_int("age"), 1);

    // External write behind the object's back: the materialized object is a
    // SNAPSHOT — stale until explicitly refreshed. That is the documented
    // trade (and exactly what recall wants).
    db.db().execute("UPDATE RcSnap SET age = 99");
    EXPECT_EQ(cached.get_int("age"), 1) << "snapshot unexpectedly saw an external write";
    cached.refresh_row_cache();
    EXPECT_EQ(cached.get_int("age"), 99) << "refreshRowCache did not pick up the external write";

    // Disabled → live reads again.
    cached.disable_row_cache();
    db.db().execute("UPDATE RcSnap SET age = 100");
    EXPECT_EQ(cached.get_int("age"), 100);

    delete ref;
}

TEST(Bridge, RowCacheNullRoundTrip) {
    TempDB tmp{"rowcache_null"};
    lattice::SchemaVector schemas = {
        make_schema("RcOpt", {
            {"name", text_prop("name")},
            {"email", nullable_text_prop("email")},
        })
    };
    auto* ref = lattice::swift_lattice_ref::create(
        lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *ref->get();
    {
        auto sdo = make_sdo("RcOpt", schemas[0].properties);
        sdo.values["name"] = std::string("Opt");
        sdo.values["email"] = std::string("a@b.c");
        lattice::dynamic_object obj(sdo);
        db.add(obj);
    }
    auto results = db.objects("RcOpt");
    lattice::dynamic_object cached(results[0]);
    cached.enable_row_cache();

    const auto base = lattice::database::total_statement_count();
    EXPECT_TRUE(cached.has_value("email"));
    EXPECT_EQ(lattice::database::total_statement_count() - base, 0u);

    // set_nil must write through: a cached read after setNil returning the
    // stale value would be silent wrong-value corruption for optionals.
    cached.set_nil("email");
    EXPECT_FALSE(cached.has_value("email"))
        << "cache returned stale non-nil after setNil";
    // And back:
    cached.set_field<std::string>("email", "x@y.z");
    const auto reread = lattice::database::total_statement_count();
    EXPECT_TRUE(cached.has_value("email"));
    EXPECT_EQ(cached.get_string("email"), "x@y.z");
    EXPECT_EQ(lattice::database::total_statement_count() - reread, 0u);

    delete ref;
}

#endif // !__linux__
