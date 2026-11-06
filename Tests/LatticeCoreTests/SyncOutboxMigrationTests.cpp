#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
#include "TestHelpers.hpp"
#include <lattice.hpp>
#include <lattice/sync_storage.hpp>
#include <lattice/sync_delivery.hpp>

namespace {
// Literal storage shapes from Core64 stage_sync_delivery and Q2 epoch 8.
constexpr const char* six_column_outbox =
    "CREATE TABLE _lattice_sync_outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "sync_id TEXT NOT NULL, source_audit_id INTEGER, source_global_id TEXT, "
    "event_global_id TEXT NOT NULL, payload TEXT NOT NULL, UNIQUE(sync_id,event_global_id))";
constexpr const char* ten_column_outbox =
    "CREATE TABLE _lattice_sync_outbox (sync_id TEXT NOT NULL,source_audit_id INTEGER NOT NULL,"
    "source_global_id TEXT NOT NULL,wire_global_id TEXT NOT NULL,table_name TEXT NOT NULL,"
    "global_row_id TEXT NOT NULL,membership INTEGER NOT NULL,wire_json TEXT NOT NULL,"
    "routing_key TEXT NOT NULL,encoded_charge INTEGER NOT NULL,PRIMARY KEY(sync_id,source_audit_id),"
    "UNIQUE(sync_id,wire_global_id),UNIQUE(sync_id,table_name,global_row_id))";

struct predecessor_store {
    TempDB path{"outbox_migration"};
    std::unique_ptr<lattice::database> raw;
    lattice::audit_log_entry source;
    explicit predecessor_store(bool six = true) {
        {
            lattice::swift_schema_entry no_history;
            no_history.table_name="OldNoHistory";
            lattice::property_descriptor value;
            value.name="value";value.type=lattice::column_type::text;
            value.kind=lattice::property_kind::primitive;value.no_history=true;
            no_history.properties["value"]=value;
            lattice::swift_lattice seed{lattice::swift_configuration(path.str()), {no_history}};
            seed.db().execute("INSERT INTO OldNoHistory(globalId,value) VALUES('old-row','initial')");
            static_cast<lattice::lattice_db&>(seed).add(TestPerson{"source", 1, std::nullopt});
            source = lattice::query_audit_log_for_sync(seed.db(), "migration").back();
        }
        raw = std::make_unique<lattice::database>(path.str());
        // Reconstruct the predecessor's bookkeeping from its literal schema.
        // No production writer is allowed to bypass the epoch capability.
        const auto guards = raw->query("SELECT name FROM sqlite_schema WHERE type='trigger' "
                                       "AND name GLOB '_lattice_writer_v9_*'");
        for (const auto& guard : guards)
            raw->execute("DROP TRIGGER " + lattice::detail::sync_storage_quote(std::get<std::string>(guard.at("name"))));
        raw->execute("DROP TABLE _lattice_sync_outbox");
        raw->execute(six ? six_column_outbox : ten_column_outbox);
        raw->execute("DELETE FROM _lattice_meta WHERE key LIKE 'schema_fingerprint:%'");

    }
    lattice::audit_log_entry wire(const std::string& id, const std::string& operation = "INSERT") const {
        auto result = source;
        result.id = 0; result.global_id = id; result.operation = operation;
        return result;
    }
    void legacy(int64_t order, const lattice::audit_log_entry& value,
                lattice::column_value_t source_id = nullptr, lattice::column_value_t source_uuid = nullptr) {
        // Whitespace is intentional: migration and sending must keep original bytes.
        raw->execute("INSERT INTO _lattice_sync_outbox VALUES(?,?,?,?,?,?)",
            {order, std::string("migration"), source_id, source_uuid, value.global_id, " \n" + value.to_json() + " "});
    }
    std::unique_ptr<lattice::lattice_db> open() {
        return std::make_unique<lattice::lattice_db>(lattice::configuration(path.str()));
    }
};

struct replay_owner {
    lattice::lattice_db* db;
    lattice::mock_sync_transport* wire;
    std::unique_ptr<lattice::synchronizer> sync;
    explicit replay_owner(std::unique_ptr<lattice::lattice_db> owned, bool finite = false) : db(owned.get()) {
        lattice::sync_config config;
        config.sync_id = "migration"; config.all_active_sync_ids = {config.sync_id};
        config.chunk_size = 1; config.upload_coalesce_ms = 0; config.ack_timeout_base_ms = 60'000;
        config.checkpoint_passive_interval_ms = 0; config.checkpoint_truncate_interval_ms = 0;
        config.finite_cycle = finite;
        if (finite) config.websocket_url = "wss://migration-fixture.invalid"; // injected transport; never dialed
        auto transport = std::make_unique<lattice::mock_sync_transport>(); wire = transport.get();
        sync = std::make_unique<lattice::synchronizer>(std::move(owned), config, std::move(transport));
        sync->connect();
    }
    ~replay_owner() { sync->disconnect(); }
    void ack(const std::string& id) {
        wire->simulate_message(lattice::transport_message::from_string(lattice::server_sent_event::make_ack({id}).to_json()));
        sync->sync_now();
    }
    std::vector<std::string> uploaded_ids() const {
        std::vector<std::string> result;
        for (const auto& message : wire->get_sent_messages()) {
            auto event = lattice::server_sent_event::from_json(message.as_string());
            if (event && event->event_type == lattice::server_sent_event::type::audit_log)
                for (const auto& item : event->audit_logs) result.push_back(item.global_id);
        }
        return result;
    }
};
}

TEST(SyncOutboxMigration, SixColumnReopenPreservesBytesNullProvenanceAndOpaqueAck) {
    for (bool live_partial_provenance : {false,true}) {
    predecessor_store fixture;
    const auto delivery = fixture.wire("Opaque-Delivery-ID");
    fixture.legacy(17, delivery, live_partial_provenance ? lattice::column_value_t(fixture.source.id) : lattice::column_value_t(nullptr));
    if (!live_partial_provenance) fixture.raw->execute("DELETE FROM AuditLog");
    else fixture.raw->execute("INSERT INTO _lattice_sync_state(audit_entry_id,sync_id,is_synchronized) "
        "SELECT id,'migration',1 FROM AuditLog");
    const auto source_before = fixture.raw->query("SELECT * FROM AuditLog");
    const auto original = fixture.raw->query("SELECT payload FROM _lattice_sync_outbox");
    {
        auto migrated = fixture.open();
        const auto rows = migrated->db().query("SELECT * FROM _lattice_sync_outbox");
        ASSERT_EQ(rows.size(), 1u);
        EXPECT_EQ(std::get<int64_t>(rows[0].at("receipt_id")), 17);
        if (live_partial_provenance) EXPECT_EQ(std::get<int64_t>(rows[0].at("source_audit_id")),fixture.source.id);
        else EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(rows[0].at("source_audit_id")));
        EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(rows[0].at("source_global_id")));
        EXPECT_EQ(rows[0].at("wire_json"), original[0].at("payload"));
    }
    replay_owner replay(fixture.open());
    ASSERT_EQ(replay.uploaded_ids(), std::vector<std::string>{delivery.global_id});
    const auto sent = replay.wire->get_sent_messages();
    ASSERT_EQ(sent.size(), 1u);
    EXPECT_NE(sent[0].as_string().find(std::get<std::string>(original[0].at("payload"))), std::string::npos);
    replay.ack("opaque-delivery-id");
    EXPECT_EQ(replay.db->db().query("SELECT 1 FROM _lattice_sync_outbox").size(), 1u);
    replay.ack(delivery.global_id);
    EXPECT_TRUE(replay.db->db().query("SELECT 1 FROM _lattice_sync_outbox").empty());
    EXPECT_EQ(replay.db->db().query("SELECT * FROM AuditLog"),source_before);
    }
}

TEST(SyncOutboxMigration, LegacyFifoIsIndependentOfSourceOrderAndSameEntityWaitsForAck) {
    predecessor_store fixture;
    auto first = fixture.wire("First-Delivery", "DELETE");
    auto second = fixture.wire("Second-Delivery");
    fixture.legacy(7, first, int64_t{900}, std::string("compacted-first"));
    fixture.legacy(41, second, int64_t{100}, std::string("compacted-second"));
    fixture.raw->execute("DELETE FROM AuditLog");
    replay_owner replay(fixture.open());
    ASSERT_EQ(replay.uploaded_ids(), std::vector<std::string>{first.global_id});
    replay.sync->sync_now();
    EXPECT_EQ(replay.uploaded_ids().size(), 1u);
    replay.ack(first.global_id);
    EXPECT_EQ(replay.uploaded_ids(), (std::vector<std::string>{first.global_id, second.global_id}));
    replay.ack(second.global_id);
    EXPECT_TRUE(replay.db->db().query("SELECT 1 FROM _lattice_sync_outbox").empty());
}

TEST(SyncOutboxMigration, TenColumnPreservesWireAndMetadataAcrossSecondOpen) {
    predecessor_store fixture(false);
    auto delivery = fixture.source;
    const std::string bytes = "\n" + delivery.to_json() + " ";
    constexpr int64_t envelope = sizeof("{\"kind\":\"auditLog\",\"auditLog\":[") - 1 + 2;
    fixture.raw->execute("INSERT INTO _lattice_sync_outbox VALUES(?,?,?,?,?,?,?,?,?,?)",
        {std::string("migration"), delivery.id, delivery.global_id, delivery.global_id, delivery.table_name,
         delivery.global_row_id, int64_t{1}, bytes, std::string("null"), int64_t(bytes.size()) + envelope});
    const auto before = fixture.raw->query("SELECT * FROM _lattice_sync_outbox");
    for (int pass = 0; pass != 2; ++pass) {
        auto migrated = fixture.open();
        auto after = migrated->db().query("SELECT * FROM _lattice_sync_outbox");
        ASSERT_EQ(after.size(), 1u);
        after[0].erase("receipt_id"); after[0].erase("legacy_receipt");
        EXPECT_EQ(after, before);
        const auto loaded = lattice::detail::read_sync_outbox(migrated->db(), "migration");
        ASSERT_EQ(loaded.size(), 1u);
        EXPECT_FALSE(loaded[0].legacy_receipt);
        EXPECT_EQ(loaded[0].wire_json, bytes);
    }
}

TEST(SyncOutboxMigration, LateMalformedReceiptRollsBackSchemaAndEveryEarlierRow) {
    predecessor_store fixture;
    fixture.legacy(1, fixture.wire("good"));
    fixture.legacy(2, fixture.wire("bad"));
    fixture.raw->execute("UPDATE _lattice_sync_outbox SET payload='{' WHERE id=2");
    const auto before = fixture.raw->query("SELECT * FROM _lattice_sync_outbox ORDER BY id");
    const auto schema = fixture.raw->query("SELECT sql FROM sqlite_schema WHERE name='_lattice_sync_outbox'");
    EXPECT_THROW(fixture.open(), std::exception);
    EXPECT_EQ(fixture.raw->query("SELECT * FROM _lattice_sync_outbox ORDER BY id"), before);
    EXPECT_EQ(fixture.raw->query("SELECT sql FROM sqlite_schema WHERE name='_lattice_sync_outbox'"), schema);
    EXPECT_TRUE(fixture.raw->query("SELECT 1 FROM sqlite_schema WHERE name='_lattice_sync_outbox_v9_migration'").empty());
}

TEST(SyncOutboxMigration, NullMetadataAndOversizeAreRefusedBeforePayloadHydration) {
    for (bool null_metadata : {false, true}) {
        predecessor_store fixture(false);
        // Reproduce a corrupt predecessor without its NOT NULL declarations.
        fixture.raw->execute("DROP TABLE _lattice_sync_outbox");
        fixture.raw->execute("CREATE TABLE _lattice_sync_outbox(sync_id,source_audit_id,source_global_id,wire_global_id,"
            "table_name,global_row_id,membership,wire_json,routing_key,encoded_charge)");
        const auto payload = fixture.source.to_json();
        constexpr int64_t envelope = sizeof("{\"kind\":\"auditLog\",\"auditLog\":[") - 1 + 2;
        fixture.raw->execute("INSERT INTO _lattice_sync_outbox VALUES(?,?,?,?,?,?,?,?,?,?)",
            {std::string("migration"), fixture.source.id, fixture.source.global_id, fixture.source.global_id,
             null_metadata ? lattice::column_value_t(nullptr) : lattice::column_value_t(fixture.source.table_name),
             fixture.source.global_row_id, int64_t{0}, payload,
             null_metadata ? std::string("null") : std::string(65'537, 'x'), int64_t(payload.size()) + envelope});
        const auto before = fixture.raw->query("SELECT * FROM _lattice_sync_outbox");
        const auto schema = fixture.raw->query("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY name");
        try { auto unexpected = fixture.open(); FAIL() << "invalid predecessor was accepted"; }
        catch (const lattice::db_error& error) {
            EXPECT_STREQ(error.what(),"Sync outbox predecessor has an invalid or oversized receipt");
        }
        EXPECT_EQ(fixture.raw->query("SELECT * FROM _lattice_sync_outbox"), before);
        EXPECT_EQ(fixture.raw->query("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY name"), schema);
        EXPECT_EQ(lattice::detail::sync_outbox_columns(*fixture.raw).size(), 10u);
    }
}

TEST(SyncOutboxMigration, PreCutoverHandleCannotWriteOrdinaryNoHistoryOrLazyMetadata) {
    predecessor_store fixture;
    // An old Lattice handle has the audit helper but not the new writer
    // capability. Reach the capability guard rather than failing SQL prepare
    // merely because this fixture uses a bare SQLite connection.
    ASSERT_EQ(sqlite3_create_function_v2(fixture.raw->handle(), "sync_disabled", 0,
        SQLITE_UTF8, fixture.raw->handle(), [](sqlite3_context* context, int, sqlite3_value**) {
            auto* db = static_cast<sqlite3*>(sqlite3_user_data(context));
            sqlite3_stmt* statement = nullptr;
            int disabled = 0;
            if (sqlite3_prepare_v2(db, "SELECT disabled FROM _SyncControl WHERE id=1", -1,
                                   &statement, nullptr) == SQLITE_OK) {
                if (sqlite3_step(statement) == SQLITE_ROW) disabled = sqlite3_column_int(statement, 0);
                sqlite3_finalize(statement);
            }
            sqlite3_result_int(context, disabled);
        }, nullptr, nullptr, nullptr), SQLITE_OK);
    // These tables did not exist on a store that never used either API.
    fixture.raw->execute("DROP TABLE _lattice_local_metadata_v1");
    fixture.raw->execute("DROP TABLE _lattice_applied_receipts");
    auto current = fixture.open();
    for (const char* sql : {
        "UPDATE TestPerson SET age=99", "UPDATE OldNoHistory SET value='old'",
        "INSERT INTO _lattice_local_metadata_v1 VALUES('scope','key','old')",
        "INSERT INTO _lattice_applied_receipts VALUES('old')",
        "UPDATE _SyncControl SET disabled=1 WHERE id=1"}) {
        try { fixture.raw->execute(sql); FAIL() << sql; }
        catch (const lattice::db_error& error) {
            EXPECT_NE(std::string(error.what()).find("lattice_storage_writer_epoch"), std::string::npos);
        }
    }
    EXPECT_NO_THROW(current->db().execute("UPDATE TestPerson SET age=2"));
    EXPECT_NO_THROW(current->db().execute("UPDATE OldNoHistory SET value='current'"));
    EXPECT_EQ(current->db().query("SELECT 1 FROM TestPerson WHERE age=2").size(), 1u);
    EXPECT_EQ(current->db().query("SELECT 1 FROM OldNoHistory WHERE value='current'").size(), 1u);
    const auto history=current->db().query("SELECT changedFields FROM AuditLog WHERE tableName='OldNoHistory' AND operation='UPDATE'");
    ASSERT_EQ(history.size(),1u);
    EXPECT_EQ(std::get<std::string>(history[0].at("changedFields")).find("current"),std::string::npos);
    EXPECT_TRUE(current->db().query("SELECT 1 FROM _lattice_local_metadata_v1").empty());
}

TEST(SyncOutboxMigration, FiniteCancellationKeepsMigratedObligationForNextOwner) {
    predecessor_store fixture;
    auto delivery = fixture.wire("Cancelled-Then-Replayed");
    fixture.legacy(5, delivery);
    fixture.raw->execute("DELETE FROM AuditLog");
    {
        replay_owner first(fixture.open(), true);
        ASSERT_EQ(first.uploaded_ids(), std::vector<std::string>{delivery.global_id});
        first.sync->cancel_finite_cycle();
        first.ack(delivery.global_id);
        EXPECT_EQ(first.db->db().query("SELECT 1 FROM _lattice_sync_outbox").size(), 1u);
        EXPECT_EQ(first.sync->get_finite_cycle_status().phase, lattice::synchronizer_base::finite_cycle_phase::cancelled);
    }
    replay_owner next(fixture.open(), true);
    ASSERT_EQ(next.uploaded_ids(), std::vector<std::string>{delivery.global_id});
    next.ack(delivery.global_id);
    EXPECT_TRUE(next.db->db().query("SELECT 1 FROM _lattice_sync_outbox").empty());
}
