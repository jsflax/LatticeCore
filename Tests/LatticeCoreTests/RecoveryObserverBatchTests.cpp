#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"
#include <set>

namespace {
using access = lattice::detail::recovery_writer_access;
using state = lattice::detail::recovery_install_state;
std::shared_ptr<lattice::lattice_db> observer_store(const std::string& path = ":memory:") {
    lattice::configuration config(path);
    config.audit_retention_seconds = 0;
    config.busy_timeout_ms = 100;
    auto owner = std::make_shared<lattice::lattice_db>(config);
    owner->add(TestPerson{"seed", 1, std::nullopt});
    if (!config.is_in_memory()) {
        auto* notifier = lattice::instance_registry::instance().get_or_create_notifier(path);
        if (notifier) notifier->stop_listening();
    }
    return owner;
}
int64_t physical_id(lattice::database& writer, const std::string& table, const std::string& gid) {
    // Only fixed test table names are accepted here.
    return std::get<int64_t>(writer.query("SELECT id FROM " + table + " WHERE globalId=?", {gid}).at(0).at("id"));
}
int64_t audit_head(lattice::database& writer) {
    return std::get<int64_t>(writer.query("SELECT COALESCE(MAX(id),0) AS n FROM AuditLog").at(0).at("n"));
}
std::vector<int64_t> audit_ids(lattice::database& writer, int64_t after) {
    std::vector<int64_t> ids;
    for (const auto& row : writer.query("SELECT id FROM AuditLog WHERE id>? ORDER BY id", {after}))
        ids.push_back(std::get<int64_t>(row.at("id")));
    return ids;
}
std::set<std::string> fields(const std::string& value) {
    const auto names = nlohmann::json::parse(value);
    if (!names.is_array()) throw std::runtime_error("expected a field-name array");
    std::set<std::string> result;
    for (const auto& name : names) {
        // Ordinary generated UPDATE audits include NULL for unchanged columns.
        // Recovery's new refresh arrays themselves contain only strings.
        if (!name.is_null()) result.insert(name.get<std::string>());
    }
    return result;
}
void suppressed_fields(const std::string& path) {
    auto owner = observer_store(path);
    auto held = owner->add(TestPerson{"before", 10, std::nullopt});
    auto held_name = held.name;
    held.age = 11; // The last genuine audit describes age, not recovery's name/email.
    const auto gid = held.global_id();
    const auto id = physical_id(owner->db(), "TestPerson", gid);
    const auto before = owner->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto stale = owner->db().query("SELECT changedFieldsNames FROM AuditLog WHERE globalRowId=? ORDER BY id DESC LIMIT 1", {gid});
    ASSERT_EQ(fields(std::get<std::string>(stale.at(0).at("changedFieldsNames"))), (std::set<std::string>{"age"}));
    int object_calls = 0, table_calls = 0, audit_calls = 0, invalidations = 0;
    auto object = owner->add_object_observer("TestPerson", id, [&](const auto& names) {
        ++object_calls;
        EXPECT_EQ(fields(names), (std::set<std::string>{"name", "age", "email"}));
        EXPECT_EQ(std::string(held_name), "canonical");
        EXPECT_EQ(access::active_writer(*owner), nullptr);
        EXPECT_FALSE(owner->db().is_in_transaction());
    });
    auto table = owner->add_table_observer("TestPerson", [&](const auto& changes) {
        ++table_calls;
        ASSERT_EQ(changes.size(), 1u);
        EXPECT_EQ(std::get<2>(changes[0]), id);
        const auto row = owner->db().query("SELECT name,email FROM TestPerson WHERE id=?", {id}).at(0);
        EXPECT_EQ(std::get<std::string>(row.at("name")), "canonical");
        EXPECT_EQ(std::get<std::string>(row.at("email")), "local-overlay");
    });
    auto audit = owner->add_table_observer("AuditLog", [&](const auto&) { ++audit_calls; });
    auto invalidation = owner->add_invalidation_hook_detailed([&](const auto& changes, auto reason) {
        if (reason != lattice::lattice_db::invalidation_reason::commit) return;
        ++invalidations;
        ASSERT_EQ(changes.size(), 1u);
        EXPECT_EQ(changes[0].table, "TestPerson");
        EXPECT_TRUE(changes[0].changed_fields.empty()); // No false disjointness proof.
    });
    const auto result = access::install(owner, [&](auto& writer) {
        writer.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        writer.execute("UPDATE TestPerson SET name='canonical',email='local-overlay' WHERE id=?", {id});
        writer.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
        EXPECT_EQ(object_calls + table_calls + audit_calls + invalidations, 0);
    });
    EXPECT_EQ(result.state, state::committed);
    EXPECT_EQ(result.primary_error, nullptr);
    EXPECT_EQ(result.postcommit_error, nullptr);
    EXPECT_EQ(object_calls, 1);
    EXPECT_EQ(table_calls, 1);
    EXPECT_EQ(invalidations, 1);
    EXPECT_EQ(audit_calls, 0);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog ORDER BY id"), before);
    EXPECT_EQ(physical_id(owner->db(), "TestPerson", gid), id);
    owner->remove_object_observer("TestPerson", id, object);
    owner->remove_table_observer("TestPerson", table);
    owner->remove_table_observer("AuditLog", audit);
    owner->remove_invalidation_hook(invalidation);
}
void actual_audits(const std::string& path) {
    auto owner = observer_store(path);
    auto held = owner->add(TestPerson{"before", 10, std::nullopt});
    const auto id = physical_id(owner->db(), "TestPerson", held.global_id());
    const auto before = audit_head(owner->db());
    std::vector<int64_t> delivered;
    int calls = 0;
    auto audit = owner->add_table_observer("AuditLog", [&](const auto& changes) {
        ++calls;
        EXPECT_FALSE(owner->db().is_in_transaction());
        for (const auto& change : changes) {
            EXPECT_EQ(std::get<1>(change), "INSERT");
            EXPECT_FALSE(std::get<3>(change).empty());
            delivered.push_back(std::get<2>(change));
        }
    });
    const auto result = access::install(owner, [&](auto& writer) {
        writer.execute("UPDATE TestPerson SET name='first' WHERE id=?", {id});
        writer.execute("UPDATE TestPerson SET name='second' WHERE id=?", {id});
        EXPECT_TRUE(delivered.empty());
    });
    EXPECT_EQ(result.state, state::committed);
    EXPECT_EQ(result.postcommit_error, nullptr);
    const auto actual = audit_ids(owner->db(), before);
    ASSERT_EQ(actual.size(), 2u);
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(delivered, actual); // Neither duplicate the last audit nor revive an old one.
    owner->remove_table_observer("AuditLog", audit);
}
}

TEST(RecoveryObserverBatch, SuppressedFinalFieldsRefreshHeldObjectInMemory) { suppressed_fields(":memory:"); }
TEST(RecoveryObserverBatch, SuppressedFinalFieldsRefreshHeldObjectInFile) {
    TempDB file{"recovery_fields"}; suppressed_fields(file.str());
}
TEST(RecoveryObserverBatch, GenuineAuditInsertsAreDeliveredExactlyOnceInMemory) { actual_audits(":memory:"); }
TEST(RecoveryObserverBatch, GenuineAuditInsertsAreDeliveredExactlyOnceInFile) {
    TempDB file{"recovery_actual_audits"}; actual_audits(file.str());
}

TEST(RecoveryObserverBatch, FailedSuppressedEffectsDoNotPublishOrChangeOrdinarySuccessor) {
    TempDB file{"recovery_observer_rollback"};
    for (const auto& path : {std::string(":memory:"), file.str()}) {
        auto owner = observer_store(path);
        auto held = owner->add(TestPerson{"before", 10, std::nullopt});
        const auto id = physical_id(owner->db(), "TestPerson", held.global_id());
        const auto before = audit_head(owner->db());
        int model_calls = 0, audit_calls = 0;
        auto model = owner->add_table_observer("TestPerson", [&](const auto&) { ++model_calls; });
        auto audit = owner->add_table_observer("AuditLog", [&](const auto&) { ++audit_calls; });
        const auto result = access::install(owner, [&](auto& writer) {
            writer.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
            writer.execute("UPDATE TestPerson SET name='discard' WHERE id=?", {id});
            throw std::runtime_error("reject complete install");
        });
        EXPECT_EQ(result.state, state::rolled_back);
        EXPECT_EQ(model_calls + audit_calls, 0);
        EXPECT_EQ(std::string(held.name), "before");
        EXPECT_EQ(owner->read_sync_disabled_flag(), 0);
        EXPECT_EQ(audit_head(owner->db()), before);
        held.name = "ordinary";
        EXPECT_EQ(model_calls, 1);
        EXPECT_EQ(audit_calls, 1);
        EXPECT_EQ(audit_ids(owner->db(), before).size(), 1u);
        owner->remove_table_observer("TestPerson", model);
        owner->remove_table_observer("AuditLog", audit);
    }
}

TEST(RecoveryObserverBatch, SameProcessSiblingSeesFinalHeldRowAndNoHistoricalAudit) {
    TempDB file{"recovery_observer_sibling"};
    auto writer = observer_store(file.str());
    auto model = writer->add(TestPerson{"before", 4, std::nullopt});
    auto sibling = observer_store(file.str());
    auto held = sibling->find_by_global_id<TestPerson>(model.global_id());
    ASSERT_TRUE(held);
    const auto id = physical_id(writer->db(), "TestPerson", model.global_id());
    int object_calls = 0, audit_calls = 0;
    auto object = sibling->add_object_observer("TestPerson", id, [&](const auto& names) {
        ++object_calls;
        EXPECT_TRUE(fields(names).contains("name"));
        EXPECT_EQ(std::string(held->name), "after");
        EXPECT_FALSE(writer->db().is_in_transaction());
    });
    auto audit = sibling->add_table_observer("AuditLog", [&](const auto&) { ++audit_calls; });
    auto result = access::install(writer, [&](auto& db) {
        db.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        db.execute("UPDATE TestPerson SET name='after' WHERE id=?", {id});
        db.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
    });
    EXPECT_EQ(result.state, state::committed);
    EXPECT_EQ(result.postcommit_error, nullptr);
    EXPECT_EQ(object_calls, 1);
    EXPECT_EQ(audit_calls, 0);
    sibling->remove_object_observer("TestPerson", id, object);
    sibling->remove_table_observer("AuditLog", audit);
}

TEST(RecoveryObserverBatch, SuppressedLinkDeleteRefreshesParentWithoutRevivingDeleteAudit) {
    TempDB file{"recovery_observer_link"};
    for (const auto& path : {std::string(":memory:"), file.str()}) {
        auto owner = observer_store(path);
        auto parent = owner->add(TestOwner{"parent", nullptr});
        auto pet = owner->add(TestPet{"pet", 2.0});
        owner->ensure_link_table("_TestOwner_TestPet_pet", "TestOwner:pet", "TestPet");
        auto add_link = [&] {
            owner->db().execute("INSERT INTO _TestOwner_TestPet_pet(lhs,rhs) VALUES(?,?)", {parent.global_id(), pet.global_id()});
        };
        add_link();
        owner->db().execute("DELETE FROM _TestOwner_TestPet_pet"); // Old matching DELETE audit.
        add_link();
        const auto before = audit_head(owner->db());
        ASSERT_FALSE(owner->db().query("SELECT id FROM AuditLog WHERE tableName='_TestOwner_TestPet_pet' AND operation='DELETE'").empty());
        const auto id = physical_id(owner->db(), "TestOwner", parent.global_id());
        int parent_calls = 0, audit_calls = 0;
        auto object = owner->add_object_observer("TestOwner", id, [&](const auto& names) {
            ++parent_calls;
            EXPECT_TRUE(fields(names).contains("pet"));
            EXPECT_TRUE(owner->db().query("SELECT lhs FROM _TestOwner_TestPet_pet").empty());
        });
        auto audit = owner->add_table_observer("AuditLog", [&](const auto&) { ++audit_calls; });
        auto result = access::install(owner, [&](auto& db) {
            db.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
            db.execute("DELETE FROM _TestOwner_TestPet_pet");
            db.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
        });
        EXPECT_EQ(result.state, state::committed);
        EXPECT_EQ(result.postcommit_error, nullptr);
        EXPECT_GT(parent_calls, 0);
        EXPECT_EQ(audit_calls, 0);
        EXPECT_EQ(audit_head(owner->db()), before);
        owner->remove_object_observer("TestOwner", id, object);
        owner->remove_table_observer("AuditLog", audit);
    }
}

TEST(RecoveryObserverBatch, FieldMetadataRefusalRollsBackBeforePublishingFinalView) {
    TempDB file{"recovery_observer_metadata"};
    for (const auto& path : {std::string(":memory:"), file.str()}) {
        auto owner = observer_store(path);
        auto held = owner->add(TestPerson{"before", 10, std::nullopt});
        const auto id = physical_id(owner->db(), "TestPerson", held.global_id());
        int calls = 0, denied = 0;
        auto model = owner->add_table_observer("TestPerson", [&](const auto&) { ++calls; });
        auto* handle = owner->db().handle();
        auto result = access::install(owner, [&](auto& db) {
            db.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
            db.execute("UPDATE TestPerson SET name='discard' WHERE id=?", {id});
            db.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
            EXPECT_EQ(sqlite3_set_authorizer(handle,
                [](void* context, int action, const char* table, const char*, const char*, const char*) {
                    if (action == SQLITE_READ && table && std::string(table) == "pragma_table_info") {
                        ++*static_cast<int*>(context);
                        return SQLITE_DENY;
                    }
                    return SQLITE_OK;
                }, &denied), SQLITE_OK);
        });
        ASSERT_EQ(sqlite3_set_authorizer(handle, nullptr, nullptr), SQLITE_OK);
        EXPECT_GT(denied, 0);
        EXPECT_EQ(result.state, state::rolled_back);
        EXPECT_NE(result.primary_error, nullptr);
        EXPECT_EQ(result.cleanup_error, nullptr);
        EXPECT_EQ(calls, 0);
        EXPECT_EQ(std::string(held.name), "before");
        EXPECT_EQ(owner->read_sync_disabled_flag(), 0);
        held.name = "ordinary";
        EXPECT_EQ(calls, 1);
        owner->remove_table_observer("TestPerson", model);
    }
}

TEST(RecoveryObserverBatch, RolledBackSavepointAuditCannotBecomeAnEventOrFrontierInput) {
    TempDB file{"recovery_observer_savepoint"};
    for (const auto& path : {std::string(":memory:"), file.str()}) {
        auto owner = observer_store(path);
        auto held = owner->add(TestPerson{"before", 10, std::nullopt});
        const auto id = physical_id(owner->db(), "TestPerson", held.global_id());
        const auto before = audit_head(owner->db());
        int audit_calls = 0;
        auto audit = owner->add_table_observer("AuditLog", [&](const auto&) { ++audit_calls; });
        auto result = access::install(owner, [&](auto& db) {
            db.execute("SAVEPOINT rejected_entry");
            db.execute("UPDATE TestPerson SET name='rejected' WHERE id=?", {id});
            EXPECT_GT(audit_head(db), before);
            db.execute("ROLLBACK TO rejected_entry");
            db.execute("RELEASE rejected_entry");
            EXPECT_EQ(audit_head(db), before);
            db.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
            db.execute("UPDATE TestPerson SET name='canonical' WHERE id=?", {id});
            db.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
        });
        EXPECT_EQ(result.state, state::committed);
        EXPECT_EQ(result.postcommit_error, nullptr);
        EXPECT_EQ(audit_head(owner->db()), before);
        EXPECT_EQ(audit_calls, 0);
        EXPECT_EQ(std::string(held.name), "canonical");
        owner->remove_table_observer("AuditLog", audit);
    }
}

TEST(RecoveryObserverBatch, SavepointAuditIdReuseDeliversOnlyTheSurvivingIdentityOnce) {
    TempDB file{"recovery_observer_reused_audit"};
    for (const auto& path : {std::string(":memory:"), file.str()}) {
        auto owner = observer_store(path);
        auto held = owner->add(TestPerson{"before", 10, std::nullopt});
        const auto id = physical_id(owner->db(), "TestPerson", held.global_id());
        int64_t rejected_id = 0;
        std::string rejected_uuid;
        std::vector<std::pair<int64_t, std::string>> delivered;
        auto audit = owner->add_table_observer("AuditLog", [&](const auto& changes) {
            for (const auto& change : changes) delivered.emplace_back(std::get<2>(change), std::get<3>(change));
        });
        auto result = access::install(owner, [&](auto& db) {
            db.execute("SAVEPOINT rejected_entry");
            db.execute("UPDATE TestPerson SET name='rejected' WHERE id=?", {id});
            const auto rejected = db.query("SELECT id,globalId FROM AuditLog ORDER BY id DESC LIMIT 1").at(0);
            rejected_id = std::get<int64_t>(rejected.at("id"));
            rejected_uuid = std::get<std::string>(rejected.at("globalId"));
            db.execute("ROLLBACK TO rejected_entry");
            db.execute("RELEASE rejected_entry");
            db.execute("UPDATE TestPerson SET name='surviving' WHERE id=?", {id});
        });
        EXPECT_EQ(result.state, state::committed);
        EXPECT_EQ(result.postcommit_error, nullptr);
        const auto actual = owner->db().query("SELECT id,globalId FROM AuditLog ORDER BY id DESC LIMIT 1").at(0);
        EXPECT_EQ(std::get<int64_t>(actual.at("id")), rejected_id);
        EXPECT_NE(std::get<std::string>(actual.at("globalId")), rejected_uuid);
        ASSERT_EQ(delivered.size(), 1u);
        EXPECT_EQ(delivered[0], (std::pair<int64_t, std::string>{rejected_id, std::get<std::string>(actual.at("globalId"))}));
        owner->remove_table_observer("AuditLog", audit);
    }
}

TEST(RecoveryObserverBatch, GeoQuartetAlsoRefreshesItsLogicalPropertyName) {
    TempDB file{"recovery_observer_geo"};
    for (const auto& path : {std::string(":memory:"), file.str()}) {
        auto owner = observer_store(path);
        auto held = owner->add(TestPlace{"place", lattice::geo_bounds::point(0, 0)});
        const auto id = physical_id(owner->db(), "TestPlace", held.global_id());
        const auto before = audit_head(owner->db());
        int calls = 0;
        auto object = owner->add_object_observer("TestPlace", id, [&](const auto& names) {
            ++calls;
            const auto changed = fields(names);
            for (const auto& field : {"name", "location", "location_minLat", "location_maxLat", "location_minLon", "location_maxLon"})
                EXPECT_TRUE(changed.contains(field));
            const auto location = held.location.detach();
            EXPECT_DOUBLE_EQ(location.min_lat, -1.0);
            EXPECT_DOUBLE_EQ(location.max_lat, 2.0);
            EXPECT_DOUBLE_EQ(location.min_lon, -3.0);
            EXPECT_DOUBLE_EQ(location.max_lon, 4.0);
        });
        auto result = access::install(owner, [&](auto& db) {
            db.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
            db.execute("UPDATE TestPlace SET location_minLat=-1,location_maxLat=2,location_minLon=-3,location_maxLon=4 WHERE id=?", {id});
            db.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
        });
        if (result.primary_error) {
            try { std::rethrow_exception(result.primary_error); }
            catch (const std::exception& error) { ADD_FAILURE() << "geographic recovery refusal: " << error.what(); }
        }
        EXPECT_EQ(result.state, state::committed);
        EXPECT_EQ(result.postcommit_error, nullptr);
        EXPECT_EQ(calls, 1);
        EXPECT_EQ(audit_head(owner->db()), before);
        owner->remove_object_observer("TestPlace", id, object);
    }
}
