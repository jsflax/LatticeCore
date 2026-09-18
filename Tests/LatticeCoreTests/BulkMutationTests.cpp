#include "TestHelpers.hpp"
#include <lattice.hpp>
#include <bulk_mutation.hpp>
#include <limits>

namespace {
using namespace lattice;

SchemaVector batch_schemas() {
    swift_schema_entry entry;
    entry.table_name = "BulkItem";
    property_descriptor count;
    count.name = "count";
    count.type = column_type::integer;
    property_descriptor date;
    date.name = "accessedAt";
    date.type = column_type::real;
    entry.properties["count"] = count;
    entry.properties["accessedAt"] = date;
    return {entry};
}

std::unique_ptr<swift_lattice_ref> make_batch_ref(const swift_configuration& config,
                                                  const SchemaVector& schemas) {
#if LATTICE_HAS_FRT
    return std::unique_ptr<swift_lattice_ref>(swift_lattice_ref::create(config, schemas));
#else
    return std::make_unique<swift_lattice_ref>(swift_lattice_ref::create(config, schemas));
#endif
}

struct BatchDB {
    TempDB path;
    SchemaVector schemas = batch_schemas();
    std::unique_ptr<swift_lattice_ref> ref;
    explicit BatchDB(const std::string& name)
        : path(name), ref(make_batch_ref(swift_configuration(path.str()), schemas)) {}
    BatchDB(const std::string& name, const std::filesystem::path& actual_path) : path(name) {
        path.path = actual_path;
        ref = make_batch_ref(swift_configuration(path.str()), schemas);
    }
    lattice::swift_lattice& core() { return *ref->get(); }
    void add(int64_t value = 1, const std::string& global_id = "") {
        swift_dynamic_object source;
        source.table_name = "BulkItem";
        source.properties = schemas[0].properties;
        source.values["count"] = value;
        source.values["accessedAt"] = 10.0;
        dynamic_object object(source);
        if (global_id.empty()) core().add(object);
        else core().add_preserving_global_id(object, global_id);
    }
    std::vector<std::unique_ptr<dynamic_object_ref>> rows() {
        auto values = core().objects("BulkItem", std::nullopt, std::string("count ASC"));
        std::vector<std::unique_ptr<dynamic_object_ref>> result;
        for (auto& row : values) result.push_back(std::make_unique<dynamic_object_ref>(row));
        return result;
    }
    int64_t count() {
        return std::get<int64_t>(core().db().query("SELECT count FROM main.BulkItem")[0].at("count"));
    }
    int64_t audit_updates() {
        return std::get<int64_t>(core().db().query(
            "SELECT COUNT(*) AS n FROM main.AuditLog WHERE tableName = 'BulkItem' AND operation = 'UPDATE'")[0].at("n"));
    }
};

struct VariableLimit {
    sqlite3* connection;
    int previous;
    VariableLimit(database& db, int limit) : connection(db.handle()),
        previous(sqlite3_limit(connection, SQLITE_LIMIT_VARIABLE_NUMBER, limit)) {}
    ~VariableLimit() { sqlite3_limit(connection, SQLITE_LIMIT_VARIABLE_NUMBER, previous); }
};

selected_mutation_batch increment(const dynamic_object_ref& row, int64_t by = 1) {
    selected_mutation_batch batch;
    batch.add_object(row);
    batch.increment_int64("count", by);
    return batch;
}
} // namespace

TEST(BulkMutation, LocalDedupChunksAuditAndMaterializedSnapshots) {
    BatchDB fixture("batch_local");
    for (int64_t i = 1; i <= 12; ++i) fixture.add(i);
    auto rows = fixture.rows();
    ASSERT_EQ(rows.size(), 12u);
    rows[0]->enable_row_cache();
    selected_mutation_batch batch;
    for (const auto& row : rows) batch.add_object(*row);
    batch.add_object(*rows[0]);
    batch.set("accessedAt", column_value_t(42.5));
    batch.increment_int64("count", 3);
    const auto old_audit = fixture.audit_updates();
    fixture.core().begin_transaction();
    {
        // Two assignment binds + one guard leave two (id, UUID) pairs/chunk.
        VariableLimit limit(fixture.core().db(), 8);
        EXPECT_EQ(fixture.ref->apply_selected_mutations(batch), 12);
        EXPECT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    }
    EXPECT_TRUE(rows[0]->is_row_cache_enabled());
    EXPECT_EQ(rows[0]->get_int("count"), 1) << "bulk SQL must preserve materialized snapshots";
    EXPECT_EQ(rows[1]->get_int("count"), 5) << "live reads use the current writer transaction";
    fixture.core().commit();
    EXPECT_EQ(fixture.audit_updates() - old_audit, 12);
    auto persisted = fixture.core().db().query("SELECT count, accessedAt FROM main.BulkItem ORDER BY id");
    ASSERT_EQ(persisted.size(), 12u);
    for (size_t i = 0; i < persisted.size(); ++i) {
        EXPECT_EQ(std::get<int64_t>(persisted[i].at("count")), static_cast<int64_t>(i) + 4);
        EXPECT_EQ(std::get<double>(persisted[i].at("accessedAt")), 42.5);
    }
    rows[0]->refresh_row_cache();
    EXPECT_EQ(rows[0]->get_int("count"), 4);
}

TEST(BulkMutation, RequiresExactCoreTransactionOwnerAndRecordsBridgeError) {
    BatchDB fixture("batch_txn");
    fixture.add();
    auto rows = fixture.rows();
    auto batch = increment(*rows[0]);
    EXPECT_EQ(fixture.ref->apply_selected_mutations(batch), 0);
    EXPECT_FALSE(last_bridge_error().empty());
    fixture.core().db().execute("BEGIN IMMEDIATE");
    EXPECT_THROW(fixture.core().apply_selected_mutations(batch), std::runtime_error);
    fixture.core().db().execute("ROLLBACK");
    fixture.core().begin_transaction();
    std::string other_thread_error;
    std::thread other([&] {
        EXPECT_EQ(fixture.ref->apply_selected_mutations(batch), 0);
        other_thread_error = last_bridge_error();
    });
    other.join();
    EXPECT_FALSE(other_thread_error.empty());
    EXPECT_EQ(fixture.ref->apply_selected_mutations(batch), 1);
    EXPECT_TRUE(last_bridge_error().empty());
    fixture.core().rollback();
    EXPECT_EQ(fixture.count(), 1);
}

TEST(BulkMutation, PreflightsAllRowsBeforeAnyWriteAndRejectsIntegerOverflow) {
    BatchDB fixture("batch_preflight");
    fixture.add(1);
    fixture.add(std::numeric_limits<int64_t>::max());
    auto rows = fixture.rows();
    selected_mutation_batch batch;
    for (const auto& row : rows) batch.add_object(*row);
    batch.increment_int64("count", 1);
    fixture.core().begin_transaction();
    EXPECT_THROW(fixture.core().apply_selected_mutations(batch), std::runtime_error);
    EXPECT_EQ(fixture.count(), 1);
    EXPECT_EQ(fixture.audit_updates(), 0);
    fixture.core().rollback();

    fixture.core().db().execute("UPDATE BulkItem SET count = ? WHERE id = 2",
                              {std::numeric_limits<int64_t>::min()});
    auto negative = increment(*rows[1], -1);
    fixture.core().begin_transaction();
    EXPECT_THROW(fixture.core().apply_selected_mutations(negative), std::runtime_error);
    fixture.core().rollback();

    fixture.core().db().execute("UPDATE BulkItem SET count = 1.5 WHERE id = 2");
    fixture.core().begin_transaction();
    EXPECT_THROW(fixture.core().apply_selected_mutations(batch), std::runtime_error);
    EXPECT_EQ(fixture.count(), 1);
    fixture.core().rollback();

    fixture.core().db().execute("DELETE FROM BulkItem WHERE id = 2");
    fixture.core().begin_transaction();
    EXPECT_THROW(fixture.core().apply_selected_mutations(batch), std::runtime_error);
    EXPECT_EQ(fixture.count(), 1);
    fixture.core().rollback();
}

TEST(BulkMutation, RejectsDuplicateReservedWrongTypeAndForeignHandles) {
    BatchDB fixture("batch_invalid"), foreign("batch_foreign");
    fixture.add(); foreign.add();
    auto rows = fixture.rows(), others = foreign.rows();
    fixture.core().begin_transaction();
    auto duplicate = increment(*rows[0]);
    duplicate.increment_int64("count", 2);
    EXPECT_THROW(fixture.core().apply_selected_mutations(duplicate), std::runtime_error);
    auto foreign_batch = increment(*others[0]);
    EXPECT_THROW(fixture.core().apply_selected_mutations(foreign_batch), std::runtime_error);
    swift_dynamic_object unmanaged;
    dynamic_object_ref unmanaged_ref(unmanaged);
    auto unmanaged_batch = increment(unmanaged_ref);
    EXPECT_THROW(fixture.core().apply_selected_mutations(unmanaged_batch), std::runtime_error);
    for (const auto& column : {"id", "globalId", "_source", "_lattice_attach_token", "missing", "accessedAt"}) {
        selected_mutation_batch invalid;
        invalid.add_object(*rows[0]);
        invalid.increment_int64(column, 1);
        EXPECT_THROW(fixture.core().apply_selected_mutations(invalid), std::runtime_error) << column;
    }
    selected_mutation_batch wrong_set;
    wrong_set.add_object(*rows[0]);
    wrong_set.set("count", column_value_t(5.0));
    EXPECT_THROW(fixture.core().apply_selected_mutations(wrong_set), std::runtime_error);
    selected_mutation_batch nonfinite;
    nonfinite.add_object(*rows[0]);
    nonfinite.set("accessedAt", column_value_t(std::numeric_limits<double>::infinity()));
    EXPECT_THROW(fixture.core().apply_selected_mutations(nonfinite), std::runtime_error);
    EXPECT_EQ(fixture.count(), 1);
    fixture.core().rollback();
}

TEST(BulkMutation, AttachedRowsUsePhysicalRoutesAndAmbiguousUUIDIsRejected) {
    BatchDB main("batch_main"), attached("batch_arm");
    main.add(1); attached.add(2);
    const auto before = database::thread_statement_count();
    EXPECT_FALSE(main.ref->has_attached_stores());
    EXPECT_EQ(database::thread_statement_count(), before);
    main.core().attach(attached.core());
    EXPECT_TRUE(main.ref->has_attached_stores());
    auto rows = main.rows();
    ASSERT_EQ(rows.size(), 2u);
    ASSERT_EQ(rows[0]->managed_primary_key(), rows[1]->managed_primary_key());
    selected_mutation_batch batch;
    for (const auto& row : rows) batch.add_object(*row);
    batch.add_object(*rows[1]);
    batch.increment_int64("count", 10);
    main.core().begin_transaction();
    EXPECT_EQ(main.core().apply_selected_mutations(batch), 2);
    main.core().commit();
    EXPECT_EQ(main.count(), 11);
    EXPECT_EQ(attached.count(), 12);
    EXPECT_EQ(main.audit_updates(), 1);
    EXPECT_EQ(attached.audit_updates(), 1);
    main.core().detach(attached.core());
    EXPECT_FALSE(main.ref->has_attached_stores());
    main.core().begin_transaction();
    EXPECT_THROW(main.core().apply_selected_mutations(batch), std::runtime_error);
    main.core().rollback();

    const std::string shared_uuid = "00000000-0000-0000-0000-000000000123";
    main.add(3, shared_uuid); attached.add(4, shared_uuid);
    main.core().attach(attached.core());
    auto ambiguous_rows = main.rows();
    selected_mutation_batch ambiguous;
    for (const auto& row : ambiguous_rows) ambiguous.add_object(*row);
    ambiguous.increment_int64("count", 1);
    main.core().begin_transaction();
    EXPECT_THROW(main.core().apply_selected_mutations(ambiguous), std::runtime_error);
    main.core().rollback();
}

TEST(BulkMutation, ReattachingSameAliasNeverRevivesOldHandleEvenWithSameIdentity) {
    BatchDB main("batch_rebind_main"), attached("batch_rebind_arm");
    attached.add(2, "00000000-0000-0000-0000-000000000456");
    main.core().attach(attached.core());
    auto old_rows = main.rows();
    ASSERT_EQ(old_rows.size(), 1u);
    auto stale = increment(*old_rows[0]);
    main.core().detach(attached.core());
    // Even the identical file with unchanged UUID and physical row id starts
    // a new attachment lifetime. File/UUID/id checks alone cannot detect this.
    main.core().attach(attached.core());
    main.core().begin_transaction();
    EXPECT_THROW(main.core().apply_selected_mutations(stale), std::runtime_error);
    EXPECT_EQ(attached.count(), 2);
    auto fresh = main.rows();
    auto current = increment(*fresh[0]);
    EXPECT_EQ(main.core().apply_selected_mutations(current), 1);
    main.core().commit();
    EXPECT_EQ(attached.count(), 3);
}

TEST(BulkMutation, MidChunkTriggerFailureCanRollBackEarlierChunksAndAuditRows) {
    BatchDB fixture("batch_rollback");
    for (int64_t i = 1; i <= 7; ++i) fixture.add(i);
    auto rows = fixture.rows();
    selected_mutation_batch batch;
    for (const auto& row : rows) batch.add_object(*row);
    batch.increment_int64("count", 10);
    fixture.core().db().execute(
        "CREATE TRIGGER abort_late_batch BEFORE UPDATE ON BulkItem WHEN OLD.id = 5 "
        "BEGIN SELECT RAISE(ABORT, 'late batch failure'); END");
    fixture.core().begin_transaction();
    {
        VariableLimit limit(fixture.core().db(), 6); // 2 (id, UUID) pairs after assignment + guard
        EXPECT_EQ(fixture.ref->apply_selected_mutations(batch), 0);
        EXPECT_FALSE(last_bridge_error().empty());
    }
    EXPECT_EQ(fixture.count(), 11) << "the caller owns rollback of earlier successful chunks";
    fixture.core().rollback();
    EXPECT_EQ(fixture.count(), 1);
    EXPECT_EQ(fixture.audit_updates(), 0);
}

TEST(BulkMutation, TriggerIdentityReplacementIsDetectedAndRolledBack) {
    for (const int variable_limit : {6, 20}) {
        BatchDB fixture("batch_trigger_identity");
        for (int64_t i = 1; i <= 4; ++i) fixture.add(i);
        auto rows = fixture.rows();
        selected_mutation_batch batch;
        for (const auto& row : rows) batch.add_object(*row);
        batch.increment_int64("count", 10);
        fixture.core().db().execute(
            "CREATE TRIGGER replace_selected_identity AFTER UPDATE ON BulkItem WHEN OLD.id = 1 "
            "BEGIN UPDATE BulkItem SET globalId = 'replacement' WHERE id = 4; END");
        fixture.core().begin_transaction();
        {
            // Six binds gives two-row chunks (a later chunk replacement);
            // twenty binds puts all rows in one statement (trigger timing).
            VariableLimit limit(fixture.core().db(), variable_limit);
            EXPECT_EQ(fixture.ref->apply_selected_mutations(batch), 0);
            EXPECT_FALSE(last_bridge_error().empty());
        }
        fixture.core().rollback();
        EXPECT_EQ(fixture.count(), 1);
        EXPECT_EQ(fixture.audit_updates(), 0);
        EXPECT_NE(std::get<std::string>(fixture.core().db().query(
            "SELECT globalId FROM BulkItem WHERE id = 4")[0].at("globalId")), "replacement");
    }
}

TEST(BulkMutation, NegativePhysicalPrimaryKeyIsAccepted) {
    BatchDB fixture("batch_negative_id");
    fixture.add(5);
    fixture.core().db().execute("UPDATE BulkItem SET id = -7 WHERE id = 1");
    auto rows = fixture.rows();
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0]->managed_primary_key(), -7);
    auto batch = increment(*rows[0]);
    fixture.core().begin_transaction();
    EXPECT_EQ(fixture.core().apply_selected_mutations(batch), 1);
    fixture.core().commit();
    EXPECT_EQ(fixture.count(), 6);
}

TEST(BulkMutation, AttachedOnlyTableUsesCapturedSourceSchema) {
    TempDB main_path("batch_attached_only_main");
    auto main = make_batch_ref(swift_configuration(main_path.str()), SchemaVector{});
    BatchDB attached("batch_attached_only_arm");
    attached.add(9);
    ASSERT_TRUE(main->get()->attach(attached.core()));
    auto rows = main->get()->objects("BulkItem");
    ASSERT_EQ(rows.size(), 1u);
    dynamic_object_ref row(rows[0]);
    selected_mutation_batch batch;
    batch.add_object(row);
    batch.set("accessedAt", column_value_t(73.25));
    batch.increment_int64("count", 2);
    main->get()->begin_transaction();
    EXPECT_EQ(main->apply_selected_mutations(batch), 1);
    EXPECT_TRUE(last_bridge_error().empty()) << last_bridge_error();
    main->get()->commit();
    database check(attached.path.str());
    auto persisted = check.query("SELECT count, accessedAt FROM BulkItem");
    ASSERT_EQ(persisted.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(persisted[0].at("count")), 11);
    EXPECT_EQ(std::get<double>(persisted[0].at("accessedAt")), 73.25);
    EXPECT_FALSE(main->get()->db().table_exists("BulkItem")) << "attached schema must not create a local model table";
}

TEST(BulkMutation, DifferentStoreAtSameAliasAndIdentityRejectsHeldRows) {
    BatchDB main("batch_alias_main"), original("batch_alias_arm");
    const std::string gid = "00000000-0000-0000-0000-000000000789";
    original.add(2, gid);
    ASSERT_TRUE(main.core().attach(original.core()));
    auto rows = main.rows();
    ASSERT_EQ(rows.size(), 1u);
    auto stale = increment(*rows[0]);
    ASSERT_TRUE(main.core().detach(original.core()));
    struct Directory {
        std::filesystem::path path;
        explicit Directory(std::filesystem::path value) : path(std::move(value)) {
            std::filesystem::create_directory(path);
        }
        ~Directory() { std::filesystem::remove_all(path); }
    } directory(original.path.str() + "-rebound");
    BatchDB replacement("batch_replacement", directory.path / original.path.path.filename());
    replacement.add(2, gid);
    ASSERT_TRUE(main.core().attach(replacement.core()));
    auto fresh = main.rows();
    ASSERT_EQ(fresh.size(), 1u);
    ASSERT_EQ(rows[0]->managed_primary_key(), fresh[0]->managed_primary_key());
    main.core().begin_transaction();
    EXPECT_THROW(main.core().apply_selected_mutations(stale), std::runtime_error);
    EXPECT_EQ(main.core().apply_selected_mutations(increment(*fresh[0])), 1);
    main.core().commit();
    EXPECT_EQ(original.count(), 2);
    EXPECT_EQ(replacement.count(), 3);
    ASSERT_TRUE(main.core().detach(replacement.core()));
}

TEST(BulkMutation, FailedDetachInvalidatesOldRouteAndRetryRestoresFreshRows) {
    BatchDB main("batch_detach_fail_main"), attached("batch_detach_fail_arm");
    attached.add(8);
    ASSERT_TRUE(main.core().attach(attached.core()));
    auto rows = main.rows();
    ASSERT_EQ(rows.size(), 1u);
    auto old_batch = increment(*rows[0]);
    sqlite3_set_authorizer(main.core().db().handle(),
        [](void*, int action, const char*, const char*, const char*, const char*) {
            return action == SQLITE_DETACH ? SQLITE_DENY : SQLITE_OK;
        }, nullptr);
    EXPECT_FALSE(main.core().detach(attached.core()));
    sqlite3_set_authorizer(main.core().db().handle(), nullptr, nullptr);
    main.core().begin_transaction();
    EXPECT_THROW(main.core().apply_selected_mutations(old_batch), std::runtime_error);
    main.core().rollback();
    ASSERT_TRUE(main.core().detach(attached.core()));
    ASSERT_TRUE(main.core().attach(attached.core()));
    auto fresh = main.rows();
    main.core().begin_transaction();
    EXPECT_EQ(main.core().apply_selected_mutations(increment(*fresh[0])), 1);
    main.core().commit();
    EXPECT_EQ(attached.count(), 9);
}

TEST(BulkMutation, AttachmentRejectsReservedMetadataColumnsBeforeSideEffects) {
    for (const auto& reserved : {"_source", "_lattice_attach_token"}) {
        BatchDB main("batch_reserved_main"), attached("batch_reserved_arm");
        attached.core().db().execute(std::string("ALTER TABLE BulkItem ADD COLUMN ") + reserved + " INTEGER");
        EXPECT_FALSE(main.core().attach(attached.core()));
        EXPECT_FALSE(main.ref->has_attached_stores());
        auto databases = main.core().db().query("PRAGMA database_list");
        for (const auto& database : databases) {
            auto name = std::get<std::string>(database.at("name"));
            EXPECT_TRUE(name == "main" || name == "temp");
        }
    }
}


TEST(BulkMutation, FailureInAttachedStoreRollsBackPriorLocalUpdateAndAudit) {
    BatchDB main("batch_cross_store_rollback"), attached("zz_batch_failure_arm");
    main.add(1); attached.add(2);
    attached.core().db().execute(
        "CREATE TRIGGER reject_attached_batch BEFORE UPDATE ON BulkItem "
        "BEGIN SELECT RAISE(ABORT, 'attached mutation rejected'); END");
    ASSERT_TRUE(main.core().attach(attached.core()));
    auto rows = main.rows();
    ASSERT_EQ(rows.size(), 2u);
    selected_mutation_batch batch;
    for (const auto& row : rows) batch.add_object(*row);
    batch.increment_int64("count", 10);
    main.core().begin_transaction();
    EXPECT_EQ(main.ref->apply_selected_mutations(batch), 0);
    EXPECT_FALSE(last_bridge_error().empty());
    EXPECT_EQ(main.count(), 11) << "main was updated before the later attached-store failure";
    main.core().rollback();
    EXPECT_EQ(main.count(), 1);
    EXPECT_EQ(attached.count(), 2);
    EXPECT_EQ(main.audit_updates(), 0);
    EXPECT_EQ(attached.audit_updates(), 0);
}

TEST(BulkMutation, FailedViewCreationInvalidatesMetadataAndDetachAllowsRetry) {
    BatchDB main("batch_attach_fail_main"), attached("batch_attach_fail_arm");
    attached.add(8);
    sqlite3_set_authorizer(main.core().db().handle(),
        [](void*, int action, const char*, const char*, const char*, const char*) {
            return action == SQLITE_CREATE_TEMP_VIEW ? SQLITE_DENY : SQLITE_OK;
        }, nullptr);
    EXPECT_FALSE(main.core().attach(attached.core()));
    sqlite3_set_authorizer(main.core().db().handle(), nullptr, nullptr);
    EXPECT_FALSE(main.core().attach(attached.core())) << "an incomplete attachment cannot be treated as idempotent success";
    ASSERT_TRUE(main.core().detach(attached.core()));
    ASSERT_TRUE(main.core().attach(attached.core()));
    auto rows = main.rows();
    ASSERT_EQ(rows.size(), 1u);
    main.core().begin_transaction();
    EXPECT_EQ(main.core().apply_selected_mutations(increment(*rows[0])), 1);
    main.core().commit();
    EXPECT_EQ(attached.count(), 9);
}

TEST(BulkMutation, DetachRevalidatesCapturedTokenAfterAliasRebind) {
    struct Probe : lattice_db {
        explicit Probe(const configuration& config) : lattice_db(config) {}
        int64_t token_for(const std::string& alias) {
            std::lock_guard<std::mutex> lock(attach_mutex_);
            return attached_route_tokens_.at(alias);
        }
        void detach_expected(const std::string& alias, const std::string& path, int64_t token) {
            detach_alias_if_current(alias, path, token);
        }
    };
    TempDB main_path("batch_detach_probe");
    Probe main(configuration(main_path.str()));
    BatchDB attached("batch_detach_probe_arm");
    attached.add(1);
    main.attach(attached.core());
    auto alias = attached.path.path.filename().replace_extension().string();
    const auto token = main.token_for(alias);
    main.detach(attached.core());
    main.attach(attached.core());
    ASSERT_NE(main.token_for(alias), token);
    // Deterministically models a detach that resolved its path/token before
    // another thread detached and rebound that alias, without timing sleeps.
    main.detach_expected(alias, attached.path.str(), token);
    EXPECT_TRUE(main.has_attached_stores());
    EXPECT_EQ(std::get<int64_t>(main.db().query("SELECT count FROM BulkItem")[0].at("count")), 1);
    main.detach(attached.core());
}
