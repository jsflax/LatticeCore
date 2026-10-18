#include "TestHelpers.hpp"
#include <LatticeCore.hpp>
#include <memory>
#include <optional>
#include <chrono>
#include <future>
#include <cerrno>
#include <thread>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef __linux__
#include <lattice.hpp>
#include <dynamic_object.hpp>
#include <list.hpp>
#include <error.hpp>
#include "../../Sources/LatticeSwiftCppBridge/src/mutation_operation_test_probe.hpp"
#include "../../Sources/LatticeCore/src/recovery_admission_test_probe.hpp"
#include <sstream>
#include <algorithm>
#include <array>
#include <cstring>
#include <utility>
#include "ObserverDeliveryCustodyTestAccess.hpp"
namespace lattice {
struct topology_admission_test_access {
    static std::shared_ptr<database> writer(lattice_db& owner) {
        std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
        return owner.db_;
    }
    static std::shared_ptr<database> reader(lattice_db& owner) {
        std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
        return owner.read_db_;
    }
    static sqlite3* raw(database& db) { return db.internal_handle(); }
    static size_t actual_waiters(const database& db) { return db.topology_waiter_count_(); }
    template<typename F> static void with_original_vec0_hold(lattice_db& owner, F&& body) {
        owner.with_vec0_serialization("topology-turn-test", false, std::forward<F>(body));
    }
    static bool valid(lattice_db& owner) {
        std::lock_guard<std::mutex> lock(owner.attach_mutex_);
        return owner.attachment_topology_valid_;
    }
    static size_t tokens(lattice_db& owner) {
        std::lock_guard<std::mutex> lock(owner.attach_mutex_);
        return owner.attached_route_tokens_.size();
    }
    static bool topology_unlocked(lattice_db& owner) {
        if (!owner.attach_mutex_.try_lock()) return false;
        owner.attach_mutex_.unlock(); return true;
    }
    static std::mutex& ownership_mutex(lattice_db& owner) { return owner.connection_ownership_mutex_; }
    static std::recursive_timed_mutex& mutation_store_gate(lattice_db& owner) {
        return owner.store_write_gate_?*owner.store_write_gate_:owner.vec0_memory_maintenance_gate_;
    }
    static std::mutex& topology_mutex(lattice_db& owner) { return owner.attach_mutex_; }
    static void retire_reader(lattice_db& owner) { owner.close_read_db(); }
    static void with_metadata(lattice_db& owner, lattice_db& arm, std::shared_ptr<const void> value) {
        owner.attach_with_metadata(arm, std::move(value));
    }
};
} // namespace lattice
namespace {
lattice::SchemaVector graph_schema() {
    lattice::swift_schema_entry item; item.table_name = "GraphItem";
    lattice::property_descriptor value; value.name = "value"; value.type = lattice::column_type::integer;
    item.properties[value.name] = value;
    auto parent = item; parent.table_name = "GraphParent";
    auto child = value; child.name = "child"; child.type = lattice::column_type::text;
    child.kind = lattice::property_kind::link; child.target_table = item.table_name;
    child.nullable = true; parent.properties[child.name] = child;
    child.name = "items"; child.kind = lattice::property_kind::list;
    parent.properties[child.name] = child;
    auto payload = child; payload.name = "payload"; payload.kind = lattice::property_kind::union_type;
    payload.is_union = true; payload.union_desc.union_table_name = "_GraphParentPayload";
    payload.union_desc.cases.push_back({"item", {{"target", lattice::column_type::text, true, "GraphItem"}}});
    parent.properties[payload.name] = payload;
    return {item, parent};
}
std::unique_ptr<lattice::swift_lattice_ref> graph_owner(const lattice::swift_configuration& config,
                                                       const lattice::SchemaVector& schema) {
#if LATTICE_HAS_FRT
    return std::unique_ptr<lattice::swift_lattice_ref>(lattice::swift_lattice_ref::create(config, schema));
#else
    return std::make_unique<lattice::swift_lattice_ref>(lattice::swift_lattice_ref::create(config, schema));
#endif
}
std::unique_ptr<lattice::dynamic_object_ref> graph_ref(const lattice::swift_dynamic_object& raw) {
#if LATTICE_HAS_FRT
    return std::unique_ptr<lattice::dynamic_object_ref>(lattice::dynamic_object_ref::wrap(lattice::dynamic_object(raw)));
#else
    return std::make_unique<lattice::dynamic_object_ref>(lattice::dynamic_object_ref::wrap(lattice::dynamic_object(raw)));
#endif
}
std::unique_ptr<lattice::link_list_ref> graph_list(const lattice::dynamic_object_ref& parent) {
#if LATTICE_HAS_FRT
    return std::unique_ptr<lattice::link_list_ref>(parent.get_link_list("items"));
#else
    return std::make_unique<lattice::link_list_ref>(parent.get_link_list("items"));
#endif
}
class ManagedGraphMutation : public ::testing::Test {
protected:
    TempDB path{"managed_graph_unit"};
    lattice::SchemaVector schema = graph_schema();
    std::unique_ptr<lattice::swift_lattice_ref> owner;
    std::vector<int64_t> inserted;
    lattice::lattice_db::observer_id observer = 0;
    void SetUp() override {
        owner = graph_owner(lattice::swift_configuration(path.str()), schema);
        observer = static_cast<lattice::lattice_db&>(*owner->get()).add_table_observer("GraphItem", [this](const auto& changes) {
            for (const auto& c : changes) if (std::get<1>(c) == "INSERT") inserted.push_back(std::get<2>(c));
        });
    }
    void TearDown() override {
        lattice::detail::mutation_operation_test_hooks::after_graph_prepared = nullptr;
        owner->get()->remove_table_observer("GraphItem", observer);
        if (!owner->get()->is_closed() && owner->get()->db().is_in_transaction()) owner->get()->rollback();
        owner->close();
    }
    lattice::swift_dynamic_object raw(int64_t value, size_t kind = 0) {
        lattice::swift_dynamic_object result; result.table_name = schema[kind].table_name;
        result.properties = schema[kind].properties; result.values["value"] = value;
        return result;
    }
    std::unique_ptr<lattice::dynamic_object_ref> object(int64_t value, size_t kind = 0) { return graph_ref(raw(value, kind)); }
    void add(lattice::dynamic_object_ref& object) {
        lattice::cxx_error error(std::runtime_error("old"));
        ASSERT_TRUE(owner->add_checked(object, error)) << error.msg;
    }
    int64_t count(const std::string& table) {
        const auto rows = owner->get()->db().query("SELECT count(*) AS n FROM " + table);
        return std::get<int64_t>(rows.at(0).at("n"));
    }
    void refuse(const std::string& table, const std::string& code = "graph late edge") {
        owner->get()->db().execute("CREATE TRIGGER graph_refuse BEFORE INSERT ON " + table +
            " BEGIN SELECT RAISE(ABORT, '" + code + "'); END");
    }
    void failure() {
        const auto value = lattice::borrow_bridge_outcome(); ASSERT_TRUE(value.failed);
        ASSERT_TRUE(value.diagnostic_available); EXPECT_NE(std::string(value.message_data, value.message_size).find("graph late"), std::string::npos);
    }
};

TEST_F(ManagedGraphMutation, OwnedLateParentLinkFailureRollsBackRowsAndBindings) {
    auto child = object(2); auto parent_raw = raw(1, 1); parent_raw.link_values["child"] = child->shared();
    auto parent = graph_ref(parent_raw); refuse("_GraphParent_GraphItem_child");
    lattice::cxx_error error(std::runtime_error("old")); EXPECT_FALSE(owner->add_checked(*parent, error)); failure();
    EXPECT_FALSE(parent->is_managed()); EXPECT_FALSE(child->is_managed());
    EXPECT_EQ(count("GraphParent"), 0); EXPECT_EQ(count("GraphItem"), 0); EXPECT_TRUE(inserted.empty());
    EXPECT_FALSE(owner->get()->db().is_in_transaction());
}
TEST_F(ManagedGraphMutation, CallerLateParentLinkFailurePreservesEarlierPrefixAndDelivery) {
    owner->get()->begin_transaction(); auto earlier = object(41); add(*earlier); EXPECT_TRUE(inserted.empty());
    auto child = object(2); auto parent_raw = raw(1, 1); parent_raw.link_values["child"] = child->shared();
    auto parent = graph_ref(parent_raw); refuse("_GraphParent_GraphItem_child");
    lattice::cxx_error error(std::runtime_error("old")); EXPECT_FALSE(owner->add_checked(*parent, error)); failure();
    ASSERT_TRUE(owner->get()->db().is_in_transaction()); EXPECT_EQ(count("GraphItem"), 1); EXPECT_EQ(count("GraphParent"), 0);
    EXPECT_FALSE(parent->is_managed()); EXPECT_FALSE(child->is_managed()); EXPECT_TRUE(inserted.empty());
    owner->get()->commit(); EXPECT_EQ(inserted.size(), 1u);
}
TEST_F(ManagedGraphMutation, BulkLateSecondRowLeavesEveryOfferedObjectUnmanaged) {
    owner->get()->db().execute("CREATE TRIGGER graph_refuse BEFORE INSERT ON GraphItem WHEN NEW.value=2 "
        "BEGIN SELECT RAISE(ABORT, 'graph late second row'); END");
    auto first = object(1); auto second = object(2); lattice::cxx_error error(std::runtime_error("old"));
#if LATTICE_HAS_FRT
    std::vector<lattice::dynamic_object_ref*> offered{first.get(), second.get()};
#else
    std::vector<lattice::dynamic_object_ref> offered{*first, *second};
#endif
    EXPECT_FALSE(owner->add_bulk_checked(offered, error)); failure();
    EXPECT_FALSE(first->is_managed()); EXPECT_FALSE(second->is_managed()); EXPECT_EQ(count("GraphItem"), 0); EXPECT_TRUE(inserted.empty());
}
TEST_F(ManagedGraphMutation, SharedChildHasOneInsertAndAllBindingsPrecedeDelivery) {
    auto child = object(2); auto a = raw(1, 1); auto b = raw(3, 1);
    a.link_values["child"] = child->shared(); b.link_values["child"] = child->shared();
    auto first = graph_ref(a); auto second = graph_ref(b); bool saw = false;
    auto token = static_cast<lattice::lattice_db&>(*owner->get()).add_table_observer("GraphParent", [&](const auto&) {
        saw = true; EXPECT_TRUE(first->is_managed()); EXPECT_TRUE(second->is_managed()); EXPECT_TRUE(child->is_managed());
    });
    std::vector<lattice::dynamic_object*> offered{first->shared().get(), second->shared().get()};
    owner->get()->add_bulk(offered);
    EXPECT_TRUE(saw); EXPECT_EQ(count("GraphItem"), 1); EXPECT_EQ(count("GraphParent"), 2); EXPECT_EQ(inserted.size(), 1u);
    owner->get()->remove_table_observer("GraphParent", token);
}
TEST_F(ManagedGraphMutation, ManagedLinkFailureDoesNotAdoptChildOrAlterParent) {
    auto parent = object(1, 1); add(*parent); auto child = object(2); refuse("_GraphParent_GraphItem_child");
    parent->set_object("child", *child); failure();
    EXPECT_FALSE(child->is_managed()); EXPECT_TRUE(parent->is_managed()); EXPECT_EQ(count("GraphItem"), 0);
    EXPECT_EQ(count("_GraphParent_GraphItem_child"), 0); EXPECT_EQ(count("GraphParent"), 1);
}
TEST_F(ManagedGraphMutation, ManagedListFailureDoesNotAdoptChildOrLeaveMembership) {
    auto parent = object(1, 1); add(*parent); auto list = graph_list(*parent); auto child = object(2);
    refuse("_GraphParent_GraphItem_items"); list->push_back(*child); failure();
    EXPECT_FALSE(child->is_managed()); EXPECT_EQ(count("GraphItem"), 0); EXPECT_EQ(list->size(), 0u);
}
TEST_F(ManagedGraphMutation, ListReplacementFailureKeepsOriginalMembershipAndChild) {
    auto parent = object(1, 1); add(*parent); auto list = graph_list(*parent); auto old = object(2); list->push_back(*old);
    ASSERT_FALSE(lattice::last_bridge_call_failed()); ASSERT_EQ(list->size(), 1u);
    auto next = object(3);
    owner->get()->db().execute("CREATE TRIGGER graph_refuse BEFORE UPDATE ON _GraphParent_GraphItem_items "
        "BEGIN SELECT RAISE(ABORT, 'graph late list replacement'); END");
    list->operator[](0).assign(*next); failure();
    EXPECT_FALSE(next->is_managed()); EXPECT_EQ(count("GraphItem"), 1); EXPECT_EQ(list->size(), 1u);
    EXPECT_EQ(list->operator[](0).object->managed_primary_key(), old->managed_primary_key());
}
TEST_F(ManagedGraphMutation, UnionLateParentUpdateFailureDoesNotAdoptLinkedChild) {
    auto parent = object(1, 1); add(*parent); auto child = object(2);
    owner->get()->db().execute("CREATE TRIGGER graph_refuse BEFORE UPDATE OF payload ON GraphParent "
        "BEGIN SELECT RAISE(ABORT, 'graph late union parent'); END");
    lattice::union_value value; value.case_name = "item"; value.set_link_ref("target", *child);
    parent->set_union("payload", value); failure();
    EXPECT_FALSE(child->is_managed()); EXPECT_EQ(count("GraphItem"), 0); EXPECT_EQ(count("_GraphParentPayload"), 0);
}
TEST_F(ManagedGraphMutation, UnionTwoLinksShareOneUnmanagedChildAndOnePersistedRow) {
    auto pair_schema = schema;
    pair_schema[1].properties.at("payload").union_desc.cases.push_back({"pair", {
        {"left", lattice::column_type::text, true, "GraphItem"},
        {"right", lattice::column_type::text, true, "GraphItem"}}});
    TempDB pair_path{"graph_union_shared_child"};
    auto pair_owner = graph_owner(lattice::swift_configuration(pair_path.str()), pair_schema);
    auto parent_raw = raw(1, 1); parent_raw.properties = pair_schema[1].properties;
    auto parent = graph_ref(parent_raw); auto child = object(2);
    lattice::cxx_error error(std::runtime_error("old")); ASSERT_TRUE(pair_owner->add_checked(*parent, error)) << error.msg;
    lattice::union_value value; value.case_name = "pair";
    value.set_link_ref("left", *child); value.set_link_ref("right", *child);
    parent->set_union("payload", value); ASSERT_FALSE(lattice::last_bridge_call_failed());
    ASSERT_TRUE(child->is_managed());
    const auto rows = pair_owner->get()->db().query("SELECT pair__left, pair__right FROM _GraphParentPayload");
    ASSERT_EQ(rows.size(), 1u);
    const auto ids = pair_owner->get()->db().query("SELECT globalId FROM GraphItem"); ASSERT_EQ(ids.size(), 1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("pair__left")), std::get<std::string>(ids[0].at("globalId")));
    EXPECT_EQ(std::get<std::string>(rows[0].at("pair__right")), std::get<std::string>(ids[0].at("globalId")));
    pair_owner->close();
}
TEST_F(ManagedGraphMutation, DuplicateBulkRootsStillRefuseWithoutAdoptionOrRows) {
    auto row = object(1); std::vector<lattice::dynamic_object*> offered{row->shared().get(), row->shared().get()};
    EXPECT_THROW(owner->get()->add_bulk(offered), lattice::db_error);
    EXPECT_FALSE(row->is_managed()); EXPECT_EQ(count("GraphItem"), 0); EXPECT_TRUE(inserted.empty());
}
TEST_F(ManagedGraphMutation, RawCallerRefusalDoesNotBorrowOrEndTransaction) {
    owner->get()->db().execute("BEGIN IMMEDIATE"); auto row = object(1);
    lattice::cxx_error error(std::runtime_error("old")); EXPECT_FALSE(owner->add_checked(*row, error));
    EXPECT_TRUE(lattice::last_bridge_call_failed()); EXPECT_FALSE(row->is_managed());
    EXPECT_TRUE(owner->get()->db().is_in_transaction()); EXPECT_EQ(count("GraphItem"), 0);
    owner->get()->db().execute("ROLLBACK");
}
TEST_F(ManagedGraphMutation, ForeignPhysicalTargetRefusesBeforeAnyParentRow) {
    TempDB foreign_path{"graph_foreign"}; auto other = graph_owner(lattice::swift_configuration(foreign_path.str()), schema);
    auto child = object(2); lattice::cxx_error error(std::runtime_error("old")); ASSERT_TRUE(other->add_checked(*child, error));
    auto source = raw(1, 1); source.link_values["child"] = child->shared(); auto parent = graph_ref(source);
    EXPECT_FALSE(owner->add_checked(*parent, error)); EXPECT_TRUE(lattice::last_bridge_call_failed());
    EXPECT_FALSE(parent->is_managed()); EXPECT_EQ(count("GraphParent"), 0); other->close();
}
TEST_F(ManagedGraphMutation, CloseBetweenCheckedPreflightAndActualAdmissionCannotAcceptNoop) {
    auto row = object(1);
    lattice::detail::mutation_operation_test_hooks::after_graph_prepared = [](lattice::swift_lattice& native) {
        lattice::detail::mutation_operation_test_hooks::after_graph_prepared = nullptr; native.close();
    };
    lattice::cxx_error error(std::runtime_error("old")); EXPECT_FALSE(owner->add_checked(*row, error));
    EXPECT_TRUE(owner->get()->is_closed()); EXPECT_FALSE(row->is_managed()); EXPECT_TRUE(lattice::last_bridge_call_failed());
}
TEST_F(ManagedGraphMutation, SuccessfulSavepointBindingDoesNotPromiseOuterRollbackRestoration) {
    auto row = object(1); owner->get()->begin_transaction(); add(*row);
    ASSERT_TRUE(row->is_managed()); EXPECT_TRUE(inserted.empty()); owner->get()->rollback();
    EXPECT_EQ(count("GraphItem"), 0); EXPECT_TRUE(inserted.empty());
    EXPECT_TRUE(row->is_managed()) << "immediate-call binding contract does not rewind a user-retained value later";
}

TEST_F(ManagedGraphMutation, UnmanagedLinkGetterPreservesSharedManagedChildMetadata) {
    auto child = object(2); lattice::cxx_error error(std::runtime_error("old"));
    ASSERT_TRUE(owner->add_checked(*child, error)) << error.msg;
    auto parent = object(1, 1); parent->set_object("child", *child);
    ASSERT_FALSE(lattice::last_bridge_call_failed());
    const auto actual = child->shared();
    const auto id = actual->managed_primary_key();
    const auto table = actual->get_table_name();
    const auto global = actual->get_string("globalId");
    ASSERT_GT(id, 0); ASSERT_FALSE(table.empty()); ASSERT_FALSE(global.empty());
    auto first = parent->shared()->get_object("child");
    EXPECT_EQ(first.get_table_name(), table);
    EXPECT_EQ(first.get_string("globalId"), global);
    EXPECT_EQ(first.get_int("value"), 2);
    EXPECT_EQ(actual->get_table_name(), table);
    EXPECT_EQ(actual->get_string("globalId"), global);
    auto second = parent->shared()->get_object("child");
    EXPECT_EQ(second.managed_primary_key(), id);
    EXPECT_EQ(second.get_int("value"), 2);
    EXPECT_EQ(actual->get_table_name(), table);
    EXPECT_EQ(actual->get_string("globalId"), global);
}
TEST_F(ManagedGraphMutation, ScalarAutocommitThenGraphAddPreservesActualTailAdmission) {
    auto child = object(2); lattice::cxx_error error(std::runtime_error("old"));
    ASSERT_TRUE(owner->add_checked(*child, error)) << error.msg;
    child->set_int("value", 3);
    ASSERT_FALSE(lattice::last_bridge_call_failed());
    ASSERT_EQ(child->get_int("value"), 3);
    auto parent = object(1, 1); parent->set_object("child", *child);
    ASSERT_FALSE(lattice::last_bridge_call_failed());
    ASSERT_TRUE(owner->add_checked(*parent, error)) << error.msg;
    EXPECT_EQ(count("GraphItem"), 1); EXPECT_EQ(count("GraphParent"), 1);
    EXPECT_EQ(count("_GraphParent_GraphItem_child"), 1);
    EXPECT_EQ(inserted.size(), 1u);
}
TEST_F(ManagedGraphMutation, ManagedLinkThenUnmanagedReadWriteAndAddKeepsOneChild) {
    auto parent = object(1, 1); lattice::cxx_error error(std::runtime_error("old"));
    ASSERT_TRUE(owner->add_checked(*parent, error)) << error.msg;
    auto child = object(2); parent->set_object("child", *child);
    ASSERT_FALSE(lattice::last_bridge_call_failed());
    ASSERT_TRUE(child->is_managed());
    const auto id = child->managed_primary_key();
    ASSERT_GT(id, 0);
    auto second_parent = object(3, 1); second_parent->set_object("child", *child);
    ASSERT_FALSE(lattice::last_bridge_call_failed());
    auto edited = second_parent->shared()->get_object("child");
    edited.set_field<int64_t>("value", 4);
    auto reread = second_parent->shared()->get_object("child");
    EXPECT_EQ(reread.get_int("value"), 4);
    EXPECT_EQ(reread.managed_primary_key(), id);
    EXPECT_EQ(child->get_int("value"), 4);
    ASSERT_TRUE(owner->add_checked(*second_parent, error)) << error.msg;
    EXPECT_EQ(count("GraphItem"), 1); EXPECT_EQ(count("GraphParent"), 2);
    EXPECT_EQ(count("_GraphParent_GraphItem_child"), 2);
    EXPECT_EQ(inserted.size(), 1u);
    auto persisted = second_parent->shared()->get_object("child");
    EXPECT_EQ(persisted.managed_primary_key(), id);
    EXPECT_EQ(persisted.get_int("value"), 4);
}


TEST_F(ManagedGraphMutation, FileCoreCommitDeliversEachActualAuditRowExactlyOnce) {
    lattice::lattice_db& native = *owner->get();
    ASSERT_FALSE(native.config().is_in_memory());
    const auto journal = native.db().query("PRAGMA main.journal_mode");
    ASSERT_EQ(journal.size(), 1u);
    ASSERT_EQ(std::get<std::string>(journal[0].at("journal_mode")), "wal");
    // This real fixture uses the production default synchronous scheduler.
    // No mocked callback counter or asynchronous-idle inference is involved.
    ASSERT_NE(dynamic_cast<lattice::immediate_scheduler*>(native.get_scheduler().get()), nullptr);
    const auto old_rows = native.db().query("SELECT id FROM AuditLog ORDER BY id");
    const int64_t prior = old_rows.empty() ? 0 : std::get<int64_t>(old_rows.back().at("id"));
    struct audit_capture {
        std::mutex mutex;
        std::vector<int64_t> ids;
        bool unexpected_operation = false;
    };
    auto captured = std::make_shared<audit_capture>();
    const auto audit_observer = native.add_table_observer("AuditLog", [captured](const auto& changes) {
        std::lock_guard<std::mutex> hold(captured->mutex);
        for (const auto& change : changes) {
            if (std::get<1>(change) != "INSERT") captured->unexpected_operation = true;
            captured->ids.push_back(std::get<2>(change));
        }
    });
    struct cancel_audit_observer {
        lattice::lattice_db& owner;
        lattice::lattice_db::observer_id token;
        ~cancel_audit_observer() { owner.remove_table_observer("AuditLog", token); }
    } cancel{native, audit_observer};
    auto child = object(2); auto parent_raw = raw(1, 1);
    parent_raw.link_values["child"] = child->shared();
    auto parent = graph_ref(parent_raw);
    native.begin_transaction();
    lattice::cxx_error error(std::runtime_error("old"));
    ASSERT_TRUE(owner->add_checked(*parent, error)) << error.msg;
    ASSERT_TRUE(native.db().is_in_transaction());
    {
        std::lock_guard<std::mutex> hold(captured->mutex);
        EXPECT_TRUE(captured->ids.empty()) << "a savepoint RELEASE is not this caller's COMMIT";
    }
    native.commit();
    ASSERT_FALSE(native.db().is_in_transaction());
    const auto durable = native.db().query(
        "SELECT id, tableName FROM AuditLog WHERE id > ? ORDER BY id", {prior});
    std::vector<int64_t> expected;
    bool child_audit = false, parent_audit = false, link_audit = false;
    for (const auto& row : durable) {
        expected.push_back(std::get<int64_t>(row.at("id")));
        const auto& table = std::get<std::string>(row.at("tableName"));
        child_audit = child_audit || table == "GraphItem";
        parent_audit = parent_audit || table == "GraphParent";
        link_audit = link_audit || table == "_GraphParent_GraphItem_child";
    }
    ASSERT_TRUE(child_audit); ASSERT_TRUE(parent_audit); ASSERT_TRUE(link_audit);
    std::vector<int64_t> observed;
    {
        std::lock_guard<std::mutex> hold(captured->mutex);
        observed = captured->ids;
        EXPECT_FALSE(captured->unexpected_operation);
    }
    // Compare full multiplicity, never deduplicate the actual delivered IDs.
    std::sort(observed.begin(), observed.end());
    EXPECT_EQ(observed, expected);
}


// Additive original-SDK-shaped reproductions. They require real success and
// preserve the ordinary refusal; snapshots diagnose an actual failure only.
std::string sdk_refusal_data(const lattice::detail::recovery_admission_test_hooks::mutation_refusal_observation& value) {
    std::ostringstream out;
    out<<"refusals="<<value.refusals
       <<" before_rows="<<value.before_call.rows<<" before_flushing="<<value.before_call.flushing
       <<" before_recovery="<<value.before_call.recovery_reserved<<" before_topology="<<value.before_call.topology_depth
       <<" before_mutation="<<value.before_call.mutation_depth<<" before_epoch="<<value.before_call.custody_epoch
       <<" after_rows="<<value.after_return.rows<<" after_flushing="<<value.after_return.flushing
       <<" after_recovery="<<value.after_return.recovery_reserved<<" after_topology="<<value.after_return.topology_depth
       <<" after_mutation="<<value.after_return.mutation_depth<<" after_epoch="<<value.after_return.custody_epoch
       <<" owned_reads="<<value.owned_reads<<" native_prefix="<<value.native_census_prefix
       <<" statement="<<value.statement_comparison<<" busy="<<value.busy<<" readonly="<<value.readonly
       <<" same_native="<<value.same_native<<" columns="<<value.columns
       <<" sql_present="<<value.sql_present<<" sql_truncated="<<value.sql_truncated<<" sql="<<value.sql;
    return out.str(); // OFF the actual Native/store/buffer leaves.
}
bool sdk_actual_add(lattice::swift_lattice_ref& owner,lattice::dynamic_object_ref& object,
    lattice::cxx_error& error,lattice::detail::recovery_admission_test_hooks::mutation_refusal_observation& observed) {
    observed.before_call=lattice::observer_delivery_custody_test_access::tail(*owner.get());
    const bool accepted=owner.add_checked(object,error); // genuine original API
    observed.after_return=lattice::observer_delivery_custody_test_access::tail(*owner.get());
    return accepted;
}
TEST_F(ManagedGraphMutation, SDKLinkManagedScalarThenSecondParentUsesOriginalSuccessfulBoundary) {
    using namespace lattice::detail::recovery_admission_test_hooks;
    mutation_refusal_observation observed;observed.expected_writer=&owner->get()->db();
    mutation_refusal_scope diagnostic(observed);
    auto first=object(1,1);lattice::cxx_error error(std::runtime_error("old"));
    ASSERT_TRUE(sdk_actual_add(*owner,*first,error,observed))<<"first parent: "<<error.msg<<"; "<<sdk_refusal_data(observed);
    auto child=object(2);
    first->set_object("child",*child);
    ASSERT_FALSE(lattice::borrow_bridge_outcome().failed)<<"actual live link: "<<lattice::last_bridge_error()<<"; "<<sdk_refusal_data(observed);
    ASSERT_TRUE(child->is_managed());
    auto second_raw=raw(3,1);second_raw.link_values["child"]=child->shared();
    auto second=graph_ref(second_raw);
    child->set_int("value",2); // SAME managed-value write in original test_Link.
    ASSERT_FALSE(lattice::borrow_bridge_outcome().failed)<<"actual scalar: "<<lattice::last_bridge_error()<<"; "<<sdk_refusal_data(observed);
    ASSERT_TRUE(sdk_actual_add(*owner,*second,error,observed))<<"second parent: "<<error.msg<<"; "<<sdk_refusal_data(observed);
    EXPECT_TRUE(first->is_managed());EXPECT_TRUE(second->is_managed());
    EXPECT_EQ(count("GraphItem"),1);EXPECT_EQ(count("GraphParent"),2);
    EXPECT_EQ(count("_GraphParent_GraphItem_child"),2);
    EXPECT_EQ(observed.refusals,0u)<<sdk_refusal_data(observed);
}
TEST(ManagedGraphMutationSDKSeed, ActualSwiftGeoCatalogSixAddsInsideOneCallerTransaction) {
    using namespace lattice::detail::recovery_admission_test_hooks;
    for(const bool file_backed:{false,true}) {
        SCOPED_TRACE(file_backed?"file":"named-memory");TempDB path("sdk_seed_geo_admission");
        lattice::swift_schema_entry entry;entry.table_name="SDKSeedSpatialMutation";
        lattice::property_descriptor text{};text.type=lattice::column_type::text;
        for(const auto* field:{"category","label"}){text.name=field;entry.properties[field]=text;}
        lattice::property_descriptor enabled{};enabled.name="enabled";enabled.type=lattice::column_type::integer;
        entry.properties[enabled.name]=enabled;
        lattice::property_descriptor location{};location.name="location";location.type=lattice::column_type::integer;location.is_geo_bounds=true;
        entry.properties[location.name]=location;
        auto stops=location;stops.name="stops";stops.kind=lattice::property_kind::list;entry.properties[stops.name]=stops;
        const auto requested=file_backed?path.str():"file:"+path.path.filename().string()+"?mode=memory&cache=shared";
        auto actual=graph_owner(lattice::swift_configuration(requested),lattice::SchemaVector{entry});
        ASSERT_NE(actual,nullptr);
        struct cleanup {
            lattice::swift_lattice_ref& actual;
            ~cleanup()noexcept {
                try{if(actual.get()->db().is_in_transaction())actual.get()->rollback();}
                catch(const std::exception& error){ADD_FAILURE()<<"real caller cleanup: "<<error.what();}
                catch(...){ADD_FAILURE()<<"real caller cleanup: unknown failure";}
                try{actual.close();}
                catch(const std::exception& error){ADD_FAILURE()<<"real owner close: "<<error.what();}
                catch(...){ADD_FAILURE()<<"real owner close: unknown failure";}
            }
        } retained{*actual};
        mutation_refusal_observation observed;observed.expected_writer=&actual->get()->db();
        mutation_refusal_scope diagnostic(observed);
        actual->get()->begin_transaction();ASSERT_TRUE(actual->get()->db().is_in_transaction());
        const char* categories[]{"alpha","alpha","beta","beta","outside","disabled"};
        const char* labels[]{"a","a","b","c","z","x"};const double coordinates[]{0,1,2,3,60,0};
        std::vector<std::unique_ptr<lattice::dynamic_object_ref>> held;
        for(std::size_t i=0;i<6;++i) {
            SCOPED_TRACE(i);lattice::swift_dynamic_object raw;raw.table_name=entry.table_name;raw.properties=entry.properties;
            raw.values["category"]=std::string(categories[i]);raw.values["label"]=std::string(labels[i]);
            raw.values["enabled"]=std::int64_t(i!=5);
            raw.set_geo_bounds("location",coordinates[i],coordinates[i],coordinates[i],coordinates[i]);
            held.push_back(graph_ref(raw));lattice::cxx_error error(std::runtime_error("old"));
            ASSERT_TRUE(sdk_actual_add(*actual,*held.back(),error,observed))<<error.msg<<"; "<<sdk_refusal_data(observed);
            ASSERT_TRUE(held.back()->is_managed());ASSERT_TRUE(actual->get()->db().is_in_transaction());
        }
        actual->get()->commit();ASSERT_FALSE(actual->get()->db().is_in_transaction());
        const auto rows=actual->get()->db().query("SELECT count(*) AS n FROM SDKSeedSpatialMutation");
        ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::int64_t>(rows.front().at("n")),6);
        EXPECT_EQ(actual->get()->count_within_bbox_shape(entry.table_name,"location",-1,5,-1,5,
            std::string("enabled = 1"),std::string("category"),std::string("label")),2);
        EXPECT_FALSE(lattice::borrow_bridge_outcome().failed)<<lattice::last_bridge_error();
        EXPECT_EQ(observed.refusals,0u)<<sdk_refusal_data(observed);
    }
}

// Genuine scalar-tail controls: the actual update hook's model row must end
// through the existing settled drain, including a file UPDATE with no new WAL.
struct sdk_scalar_tail_events {
    std::mutex mutex;
    std::vector<lattice::lattice_db::change_event> rows,audits;
    unsigned row_batches=0,audit_batches=0;
    void row_batch(const std::vector<lattice::lattice_db::change_event>& batch) {
        std::lock_guard<std::mutex> leaf(mutex);++row_batches;
        rows.insert(rows.end(),batch.begin(),batch.end());
    }
    void audit_batch(const std::vector<lattice::lattice_db::change_event>& batch) {
        std::lock_guard<std::mutex> leaf(mutex);++audit_batches;
        audits.insert(audits.end(),batch.begin(),batch.end());
    }
    struct data {
        std::vector<lattice::lattice_db::change_event> rows,audits;
        unsigned row_batches,audit_batches;
    };
    data snapshot() {std::lock_guard<std::mutex> leaf(mutex);return {rows,audits,row_batches,audit_batches};}
};
struct sdk_scalar_tail_cleanup {
    lattice::swift_lattice_ref& actual;
    lattice::lattice_db::observer_id row_observer,audit_observer;
    ~sdk_scalar_tail_cleanup()noexcept {
        try {
            actual.get()->remove_table_observer("GraphItem",row_observer);
            actual.get()->remove_table_observer("AuditLog",audit_observer);
        }catch(const std::exception& error){ADD_FAILURE()<<"real observer retirement: "<<error.what();}
        catch(...){ADD_FAILURE()<<"real observer retirement: unknown failure";}
        try{if(actual.get()->db().is_in_transaction())actual.get()->rollback();}
        catch(const std::exception& error){ADD_FAILURE()<<"real caller cleanup: "<<error.what();}
        catch(...){ADD_FAILURE()<<"real caller cleanup: unknown failure";}
        try{actual.close();}
        catch(const std::exception& error){ADD_FAILURE()<<"real owner close: "<<error.what();}
        catch(...){ADD_FAILURE()<<"real owner close: unknown failure";}
    }
};
void sdk_scalar_tail_control(bool file_backed,int ending) {
    using namespace lattice::detail::recovery_admission_test_hooks;
    SCOPED_TRACE(file_backed?"file":"named-memory");
    SCOPED_TRACE(ending);
    TempDB path("sdk_noop_scalar_tail");const auto catalog=graph_schema();
    const auto requested=file_backed?path.str():"file:"+path.path.filename().string()+"?mode=memory&cache=shared";
    auto actual=graph_owner(lattice::swift_configuration(requested),catalog);ASSERT_NE(actual,nullptr);
    sdk_scalar_tail_events observed;
    auto row_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer("GraphItem",[&](const auto& batch){observed.row_batch(batch);});
    auto audit_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer("AuditLog",[&](const auto& batch){observed.audit_batch(batch);});
    sdk_scalar_tail_cleanup retained{*actual,row_observer,audit_observer};
    const auto make_item=[&](std::int64_t value){lattice::swift_dynamic_object raw;raw.table_name=catalog[0].table_name;
        raw.properties=catalog[0].properties;raw.values["value"]=value;return graph_ref(raw);};
    const auto count_rows=[&](const std::string& table){const auto rows=actual->get()->db().query("SELECT count(*) AS n FROM "+table);
        return std::get<std::int64_t>(rows.at(0).at("n"));};
    mutation_refusal_observation refusal;refusal.expected_writer=&actual->get()->db();mutation_refusal_scope diagnostic(refusal);
    auto first=make_item(47);lattice::cxx_error error(std::runtime_error("old"));
    ASSERT_TRUE(sdk_actual_add(*actual,*first,error,refusal))<<error.msg<<"; "<<sdk_refusal_data(refusal);
    ASSERT_TRUE(first->is_managed());
    const auto before=actual->get()->db().query("SELECT id,globalId,value FROM GraphItem");ASSERT_EQ(before.size(),1u);
    const auto original_id=std::get<std::int64_t>(before[0].at("id"));
    const auto original_global=std::get<std::string>(before[0].at("globalId"));
    const auto audit_before=count_rows("AuditLog");const auto original_events=observed.snapshot();
    ASSERT_EQ(original_events.rows.size(),1u);ASSERT_EQ(original_events.audits.size(),1u);
    ASSERT_EQ(original_events.row_batches,1u);ASSERT_EQ(original_events.audit_batches,1u);
    ASSERT_EQ(lattice::observer_delivery_custody_test_access::tail(*actual->get()).rows,0u);
    if(ending!=0)actual->get()->begin_transaction();
    ASSERT_EQ(actual->get()->db().is_in_transaction(),ending!=0);
    first->set_int("value",47); // Real managed same-value UPDATE, not SQL/no-op simulation.
    ASSERT_FALSE(lattice::borrow_bridge_outcome().failed)<<lattice::last_bridge_error();
    EXPECT_EQ(count_rows("AuditLog"),audit_before); // Trigger created no new audit row.
    const auto after_scalar=observed.snapshot();
    if(ending==0) {
        ASSERT_EQ(after_scalar.rows.size(),2u);EXPECT_EQ(after_scalar.row_batches,2u);
        EXPECT_EQ(std::get<1>(after_scalar.rows.back()),"UPDATE");
        EXPECT_EQ(std::get<2>(after_scalar.rows.back()),original_id);
        EXPECT_EQ(std::get<3>(after_scalar.rows.back()),original_global);
        EXPECT_EQ(lattice::observer_delivery_custody_test_access::tail(*actual->get()).rows,0u);
    }else {
        EXPECT_EQ(after_scalar.rows.size(),1u);EXPECT_EQ(after_scalar.row_batches,1u);
        EXPECT_GT(lattice::observer_delivery_custody_test_access::tail(*actual->get()).rows,0u);
    }
    EXPECT_EQ(after_scalar.audits.size(),1u);EXPECT_EQ(after_scalar.audit_batches,1u);
    auto second=make_item(52);
    ASSERT_TRUE(sdk_actual_add(*actual,*second,error,refusal))<<error.msg<<"; "<<sdk_refusal_data(refusal);
    ASSERT_TRUE(second->is_managed());EXPECT_EQ(count_rows("GraphItem"),2);
    EXPECT_EQ(actual->get()->db().is_in_transaction(),ending!=0);
    if(ending!=0) {
        const auto pending=observed.snapshot();EXPECT_EQ(pending.rows.size(),1u);EXPECT_EQ(pending.audits.size(),1u);
        EXPECT_EQ(pending.row_batches,1u);EXPECT_EQ(pending.audit_batches,1u);
        if(ending==1)actual->get()->commit();else actual->get()->rollback();
    }
    ASSERT_FALSE(actual->get()->db().is_in_transaction());
    auto settled=observed.snapshot();
    if(ending==2) {
        EXPECT_EQ(count_rows("GraphItem"),1);EXPECT_EQ(count_rows("AuditLog"),audit_before);
        EXPECT_EQ(settled.rows.size(),1u);EXPECT_EQ(settled.audits.size(),1u);
        EXPECT_EQ(settled.row_batches,1u);EXPECT_EQ(settled.audit_batches,1u);
        auto after_rollback=make_item(61);
        ASSERT_TRUE(sdk_actual_add(*actual,*after_rollback,error,refusal))<<error.msg<<"; "<<sdk_refusal_data(refusal);
        settled=observed.snapshot();ASSERT_EQ(settled.rows.size(),2u);
        EXPECT_EQ(std::get<1>(settled.rows.back()),"INSERT");
        EXPECT_EQ(settled.row_batches,2u);EXPECT_EQ(settled.audits.size(),2u);EXPECT_EQ(settled.audit_batches,2u);
        EXPECT_EQ(count_rows("GraphItem"),2);EXPECT_EQ(count_rows("AuditLog"),audit_before+1);
    }else {
        ASSERT_EQ(settled.rows.size(),3u);EXPECT_EQ(settled.row_batches,ending==0?3u:2u);
        EXPECT_EQ(std::get<1>(settled.rows[1]),"UPDATE");EXPECT_EQ(std::get<2>(settled.rows[1]),original_id);
        EXPECT_EQ(std::get<3>(settled.rows[1]),original_global);EXPECT_EQ(std::get<1>(settled.rows[2]),"INSERT");
        EXPECT_EQ(settled.audits.size(),2u);EXPECT_EQ(settled.audit_batches,2u);
        EXPECT_EQ(count_rows("GraphItem"),2);EXPECT_EQ(count_rows("AuditLog"),audit_before+1);
    }
    const auto survivor=actual->get()->db().query("SELECT id,globalId,value FROM GraphItem WHERE id = ?",{original_id});
    ASSERT_EQ(survivor.size(),1u);EXPECT_EQ(survivor[0].at("globalId"),before[0].at("globalId"));
    EXPECT_EQ(std::get<std::int64_t>(survivor[0].at("value")),47);
    EXPECT_EQ(lattice::observer_delivery_custody_test_access::tail(*actual->get()).rows,0u);
    const auto after_reads=observed.snapshot();
    EXPECT_EQ(after_reads.rows,settled.rows);EXPECT_EQ(after_reads.audits,settled.audits);
    EXPECT_EQ(after_reads.row_batches,settled.row_batches);EXPECT_EQ(after_reads.audit_batches,settled.audit_batches);
    EXPECT_EQ(refusal.refusals,0u)<<sdk_refusal_data(refusal);
}
TEST(ManagedGraphMutationScalarTail, FileAndMemoryNoopUpdateDrainsBeforeNextCheckedGraphAdd) {
    for(const bool file_backed:{false,true})sdk_scalar_tail_control(file_backed,0);
}
TEST(ManagedGraphMutationScalarTail, FileAndMemoryCallerCommitDeliversNoopAndGraphRowsExactlyOnce) {
    for(const bool file_backed:{false,true})sdk_scalar_tail_control(file_backed,1);
}
TEST(ManagedGraphMutationScalarTail, FileAndMemoryCallerRollbackDropsOnlyUncommittedNoopAndGraphRows) {
    for(const bool file_backed:{false,true})sdk_scalar_tail_control(file_backed,2);
}
TEST(ManagedGraphMutationScalarTail, FileAndMemoryRepeatedNoopDoesNotRediscoverEarlierAuditRow) {
    for(const bool file_backed:{false,true}) {
        SCOPED_TRACE(file_backed?"file":"named-memory");TempDB path("sdk_noop_prior_audit");const auto catalog=graph_schema();
        const auto requested=file_backed?path.str():"file:"+path.path.filename().string()+"?mode=memory&cache=shared";
        auto actual=graph_owner(lattice::swift_configuration(requested),catalog);ASSERT_NE(actual,nullptr);
        sdk_scalar_tail_events observed;
        auto row_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer("GraphItem",[&](const auto& batch){observed.row_batch(batch);});
        auto audit_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer("AuditLog",[&](const auto& batch){observed.audit_batch(batch);});
        sdk_scalar_tail_cleanup retained{*actual,row_observer,audit_observer};
        lattice::swift_dynamic_object raw;raw.table_name=catalog[0].table_name;raw.properties=catalog[0].properties;raw.values["value"]=std::int64_t(47);
        auto first=graph_ref(raw);lattice::cxx_error error(std::runtime_error("old"));
        ASSERT_TRUE(actual->add_checked(*first,error))<<error.msg;ASSERT_TRUE(first->is_managed());
        const auto audit_before=actual->get()->db().query("SELECT id,globalId FROM AuditLog ORDER BY id");ASSERT_EQ(audit_before.size(),1u);
        first->set_int("value",48);ASSERT_FALSE(lattice::borrow_bridge_outcome().failed)<<lattice::last_bridge_error();
        const auto actual_audit=actual->get()->db().query("SELECT id,globalId FROM AuditLog ORDER BY id");ASSERT_EQ(actual_audit.size(),2u);
        const auto changed=observed.snapshot();ASSERT_EQ(changed.rows.size(),2u);ASSERT_EQ(changed.audits.size(),2u);
        EXPECT_EQ(changed.row_batches,2u);EXPECT_EQ(changed.audit_batches,2u);
        EXPECT_EQ(std::get<2>(changed.audits.back()),std::get<std::int64_t>(actual_audit.back().at("id")));
        EXPECT_EQ(std::get<3>(changed.audits.back()),std::get<std::string>(actual_audit.back().at("globalId")));
        for(unsigned repeat=0;repeat!=2;++repeat) {
            SCOPED_TRACE(repeat);first->set_int("value",48);
            ASSERT_FALSE(lattice::borrow_bridge_outcome().failed)<<lattice::last_bridge_error();
            const auto after=observed.snapshot();ASSERT_EQ(after.rows.size(),3u+repeat);EXPECT_EQ(after.row_batches,3u+repeat);
            EXPECT_EQ(std::get<1>(after.rows.back()),"UPDATE");
            EXPECT_EQ(after.audits,changed.audits);EXPECT_EQ(after.audit_batches,changed.audit_batches);
            EXPECT_EQ(actual->get()->db().query("SELECT id,globalId FROM AuditLog ORDER BY id"),actual_audit);
            EXPECT_EQ(lattice::observer_delivery_custody_test_access::tail(*actual->get()).rows,0u);
        }
    }
}

// Additional actual producer controls for direct MAIN audit insert custody.
TEST(ManagedGraphMutationScalarTail, FileAndMemoryOneStatementKeepsBothActualSameRowAuditInserts) {
    for(const bool file_backed:{false,true}) {
        SCOPED_TRACE(file_backed?"file":"named-memory");TempDB path("sdk_actual_audit_inserts");const auto catalog=graph_schema();
        const auto requested=file_backed?path.str():"file:"+path.path.filename().string()+"?mode=memory&cache=shared";
        auto actual=graph_owner(lattice::swift_configuration(requested),catalog);ASSERT_NE(actual,nullptr);
        lattice::swift_dynamic_object raw;raw.table_name=catalog[0].table_name;raw.properties=catalog[0].properties;raw.values["value"]=std::int64_t(47);
        auto first=graph_ref(raw);lattice::cxx_error error(std::runtime_error("old"));
        ASSERT_TRUE(actual->add_checked(*first,error))<<error.msg;ASSERT_TRUE(first->is_managed());
        const auto prior=actual->get()->db().query("SELECT id,globalId FROM AuditLog ORDER BY id");ASSERT_EQ(prior.size(),1u);
        actual->get()->db().execute("CREATE TRIGGER sdk_two_actual_updates AFTER UPDATE OF value ON GraphItem "
            "WHEN OLD.value = 47 AND NEW.value = 48 BEGIN UPDATE GraphItem SET value = 49 WHERE id = NEW.id; END");
        sdk_scalar_tail_events observed;
        auto row_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer("GraphItem",[&](const auto& batch){observed.row_batch(batch);});
        auto audit_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer("AuditLog",[&](const auto& batch){observed.audit_batch(batch);});
        sdk_scalar_tail_cleanup retained{*actual,row_observer,audit_observer};
        first->set_int("value",48); // One real scalar statement invokes two actual row UPDATEs.
        ASSERT_FALSE(lattice::borrow_bridge_outcome().failed)<<lattice::last_bridge_error();
        const auto points=actual->get()->db().query("SELECT id,globalId,value FROM GraphItem");ASSERT_EQ(points.size(),1u);
        EXPECT_EQ(std::get<std::int64_t>(points[0].at("value")),49);
        const auto actual_audits=actual->get()->db().query("SELECT id,globalId FROM AuditLog WHERE id > ? ORDER BY id",
            {std::get<std::int64_t>(prior[0].at("id"))});ASSERT_EQ(actual_audits.size(),2u);
        EXPECT_NE(actual_audits[0].at("id"),actual_audits[1].at("id"));
        EXPECT_NE(actual_audits[0].at("globalId"),actual_audits[1].at("globalId"));
        const auto delivered=observed.snapshot();ASSERT_EQ(delivered.rows.size(),2u);ASSERT_EQ(delivered.audits.size(),2u);
        EXPECT_EQ(delivered.row_batches,1u);EXPECT_EQ(delivered.audit_batches,1u);
        for(std::size_t i=0;i<2;++i) {
            SCOPED_TRACE(i);EXPECT_EQ(std::get<1>(delivered.rows[i]),"UPDATE");
            EXPECT_EQ(std::get<2>(delivered.rows[i]),std::get<std::int64_t>(points[0].at("id")));
            EXPECT_EQ(std::get<3>(delivered.rows[i]),std::get<std::string>(points[0].at("globalId")));
            EXPECT_EQ(std::get<1>(delivered.audits[i]),"INSERT");
            EXPECT_EQ(std::get<2>(delivered.audits[i]),std::get<std::int64_t>(actual_audits[i].at("id")));
            EXPECT_EQ(std::get<3>(delivered.audits[i]),std::get<std::string>(actual_audits[i].at("globalId")));
        }
        EXPECT_EQ(lattice::observer_delivery_custody_test_access::tail(*actual->get()).rows,0u);
        first->set_int("value",49);ASSERT_FALSE(lattice::borrow_bridge_outcome().failed)<<lattice::last_bridge_error();
        const auto after_noop=observed.snapshot();ASSERT_EQ(after_noop.rows.size(),3u);
        EXPECT_EQ(after_noop.row_batches,2u);EXPECT_EQ(after_noop.audits,delivered.audits);
        EXPECT_EQ(after_noop.audit_batches,delivered.audit_batches);
        EXPECT_EQ(actual->get()->db().query("SELECT id,globalId FROM AuditLog WHERE id > ? ORDER BY id",
            {std::get<std::int64_t>(prior[0].at("id"))}),actual_audits);
    }
}
TEST(ManagedGraphMutationScalarTail, FileAndMemoryMarkSyncedKeepsActualCallerAuditUpdateSemantics) {
    for(const bool file_backed:{false,true})for(const bool rollback_first:{false,true}) {
        SCOPED_TRACE(file_backed?"file":"named-memory");
        SCOPED_TRACE(rollback_first?"rollback-before-real-ack":"commit-real-ack");
        TempDB path("sdk_actual_audit_update");const auto catalog=graph_schema();
        const auto requested=file_backed?path.str():"file:"+path.path.filename().string()+"?mode=memory&cache=shared";
        auto actual=graph_owner(lattice::swift_configuration(requested),catalog);ASSERT_NE(actual,nullptr);
        lattice::swift_dynamic_object raw;raw.table_name=catalog[0].table_name;raw.properties=catalog[0].properties;raw.values["value"]=std::int64_t(47);
        auto first=graph_ref(raw);lattice::cxx_error error(std::runtime_error("old"));
        ASSERT_TRUE(actual->add_checked(*first,error))<<error.msg;ASSERT_TRUE(first->is_managed());
        const auto initial=actual->get()->db().query("SELECT id,globalId,isSynchronized FROM AuditLog ORDER BY id");ASSERT_EQ(initial.size(),1u);
        const auto audit_id=std::get<std::int64_t>(initial[0].at("id"));
        const auto audit_global=std::get<std::string>(initial[0].at("globalId"));
        ASSERT_EQ(std::get<std::int64_t>(initial[0].at("isSynchronized")),0);
        sdk_scalar_tail_events observed;
        auto row_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer("GraphItem",[&](const auto& batch){observed.row_batch(batch);});
        auto audit_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer("AuditLog",[&](const auto& batch){observed.audit_batch(batch);});
        sdk_scalar_tail_cleanup retained{*actual,row_observer,audit_observer};
        actual->get()->begin_transaction();ASSERT_TRUE(actual->get()->db().is_in_transaction());
        lattice::mark_audit_entries_synced(*actual->get(),{audit_global}); // Actual existing sync producer.
        ASSERT_TRUE(actual->get()->db().is_in_transaction());
        const auto inside=actual->get()->db().query("SELECT isSynchronized FROM AuditLog WHERE id = ?",{audit_id});
        ASSERT_EQ(inside.size(),1u);EXPECT_EQ(std::get<std::int64_t>(inside[0].at("isSynchronized")),1);
        const auto pending=observed.snapshot();EXPECT_TRUE(pending.rows.empty());EXPECT_TRUE(pending.audits.empty());
        EXPECT_EQ(pending.row_batches,0u);EXPECT_EQ(pending.audit_batches,0u);
        if(rollback_first) {
            actual->get()->rollback();ASSERT_FALSE(actual->get()->db().is_in_transaction());
            const auto reverted=actual->get()->db().query("SELECT isSynchronized FROM AuditLog WHERE id = ?",{audit_id});
            ASSERT_EQ(reverted.size(),1u);EXPECT_EQ(std::get<std::int64_t>(reverted[0].at("isSynchronized")),0);
            const auto rolled=observed.snapshot();EXPECT_TRUE(rolled.rows.empty());EXPECT_TRUE(rolled.audits.empty());
            EXPECT_EQ(rolled.row_batches,0u);EXPECT_EQ(rolled.audit_batches,0u);
            lattice::mark_audit_entries_synced(*actual->get(),{audit_global});
        }else actual->get()->commit();
        ASSERT_FALSE(actual->get()->db().is_in_transaction());
        const auto committed=observed.snapshot();EXPECT_TRUE(committed.rows.empty());EXPECT_EQ(committed.row_batches,0u);
        ASSERT_EQ(committed.audits.size(),1u);EXPECT_EQ(committed.audit_batches,1u);
        EXPECT_EQ(std::get<0>(committed.audits[0]),"AuditLog");EXPECT_EQ(std::get<1>(committed.audits[0]),"UPDATE");
        EXPECT_EQ(std::get<2>(committed.audits[0]),audit_id);EXPECT_EQ(std::get<3>(committed.audits[0]),audit_global);
        const auto final=actual->get()->db().query("SELECT id,globalId,isSynchronized FROM AuditLog ORDER BY id");
        ASSERT_EQ(final.size(),1u);EXPECT_EQ(final[0].at("id"),initial[0].at("id"));
        EXPECT_EQ(final[0].at("globalId"),initial[0].at("globalId"));EXPECT_EQ(std::get<std::int64_t>(final[0].at("isSynchronized")),1);
        lattice::mark_audit_entries_synced(*actual->get(),{audit_global}); // Existing idempotent ACK behavior.
        const auto repeated=observed.snapshot();EXPECT_EQ(repeated.rows,committed.rows);EXPECT_EQ(repeated.audits,committed.audits);
        EXPECT_EQ(repeated.row_batches,committed.row_batches);EXPECT_EQ(repeated.audit_batches,committed.audit_batches);
        EXPECT_EQ(lattice::observer_delivery_custody_test_access::tail(*actual->get()).rows,0u);
    }
}

TEST(ManagedGraphMutationScalarTail, FileAuditIdentityUsesActualWriterWhilePublicReaderPinsEarlierSnapshot) {
    TempDB path("sdk_audit_writer_snapshot");const auto catalog=graph_schema();
    auto actual=graph_owner(lattice::swift_configuration(path.str()),catalog);ASSERT_NE(actual,nullptr);
    lattice::swift_dynamic_object raw;raw.table_name=catalog[0].table_name;raw.properties=catalog[0].properties;raw.values["value"]=std::int64_t(47);
    auto first=graph_ref(raw);lattice::cxx_error error(std::runtime_error("old"));
    ASSERT_TRUE(actual->add_checked(*first,error))<<error.msg;ASSERT_TRUE(first->is_managed());
    const auto original=actual->get()->db().query("SELECT id,globalId FROM main.AuditLog ORDER BY id");ASSERT_EQ(original.size(),1u);
    sdk_scalar_tail_events observed;
    auto row_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer("GraphItem",[&](const auto& batch){observed.row_batch(batch);});
    auto audit_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer("AuditLog",[&](const auto& batch){observed.audit_batch(batch);});
    sdk_scalar_tail_cleanup retained{*actual,row_observer,audit_observer};
    auto& reader=actual->get()->read_db(); // Genuine public owned file reader, not a second fabricated owner.
    struct reader_ending {
        lattice::database& reader;
        ~reader_ending()noexcept {
            try{if(reader.is_in_transaction())reader.rollback();}
            catch(const std::exception& error){ADD_FAILURE()<<"actual reader rollback: "<<error.what();}
            catch(...){ADD_FAILURE()<<"actual reader rollback: unknown failure";}
        }
    } close_reader{reader};
    reader.begin_transaction();ASSERT_TRUE(reader.is_in_transaction());
    const auto pinned=reader.query("SELECT id,globalId FROM main.AuditLog ORDER BY id");ASSERT_EQ(pinned,original);
    first->set_int("value",48);ASSERT_FALSE(lattice::borrow_bridge_outcome().failed)<<lattice::last_bridge_error();
    ASSERT_TRUE(reader.is_in_transaction());
    EXPECT_EQ(reader.query("SELECT id,globalId FROM main.AuditLog ORDER BY id"),original);
    const auto committed=actual->get()->db().query("SELECT id,globalId FROM main.AuditLog ORDER BY id");ASSERT_EQ(committed.size(),2u);
    const auto delivered=observed.snapshot();ASSERT_EQ(delivered.rows.size(),1u);ASSERT_EQ(delivered.audits.size(),1u);
    EXPECT_EQ(delivered.row_batches,1u);EXPECT_EQ(delivered.audit_batches,1u);
    EXPECT_EQ(std::get<1>(delivered.rows[0]),"UPDATE");EXPECT_EQ(std::get<1>(delivered.audits[0]),"INSERT");
    EXPECT_EQ(std::get<2>(delivered.audits[0]),std::get<std::int64_t>(committed[1].at("id")));
    EXPECT_EQ(std::get<3>(delivered.audits[0]),std::get<std::string>(committed[1].at("globalId")));
    EXPECT_FALSE(std::get<3>(delivered.audits[0]).empty());
    first->set_int("value",48);ASSERT_FALSE(lattice::borrow_bridge_outcome().failed)<<lattice::last_bridge_error();
    const auto after_noop=observed.snapshot();EXPECT_EQ(after_noop.rows.size(),2u);EXPECT_EQ(after_noop.row_batches,2u);
    EXPECT_EQ(after_noop.audits,delivered.audits);EXPECT_EQ(after_noop.audit_batches,delivered.audit_batches);
    EXPECT_EQ(reader.query("SELECT id,globalId FROM main.AuditLog ORDER BY id"),original);
    EXPECT_EQ(actual->get()->db().query("SELECT id,globalId FROM main.AuditLog ORDER BY id"),committed);
    EXPECT_EQ(lattice::observer_delivery_custody_test_access::tail(*actual->get()).rows,0u);
    reader.rollback();ASSERT_FALSE(reader.is_in_transaction());
}

// Same actual five-property Swift geo producer used by the original seed.
lattice::swift_schema_entry sdk_module_geo_catalog() {
    lattice::swift_schema_entry entry;entry.table_name="SDKSeedSpatialMutation";
    lattice::property_descriptor text{};text.type=lattice::column_type::text;
    for(const auto* field:{"category","label"}){text.name=field;entry.properties[field]=text;}
    lattice::property_descriptor enabled{};enabled.name="enabled";enabled.type=lattice::column_type::integer;
    entry.properties[enabled.name]=enabled;
    lattice::property_descriptor location{};location.name="location";location.type=lattice::column_type::integer;location.is_geo_bounds=true;
    entry.properties[location.name]=location;
    auto stops=location;stops.name="stops";stops.kind=lattice::property_kind::list;entry.properties[stops.name]=stops;
    return entry;
}
std::unique_ptr<lattice::dynamic_object_ref> sdk_module_geo_object(const lattice::swift_schema_entry& entry,
    const char* category,const char* label,double coordinate) {
    lattice::swift_dynamic_object raw;raw.table_name=entry.table_name;raw.properties=entry.properties;
    raw.values["category"]=std::string(category);raw.values["label"]=std::string(label);raw.values["enabled"]=std::int64_t(1);
    raw.set_geo_bounds("location",coordinate,coordinate,coordinate,coordinate);return graph_ref(raw);
}
struct sdk_module_geo_ending {
    lattice::swift_lattice_ref& actual;const std::string table;
    lattice::lattice_db::observer_id row_observer,audit_observer;
    ~sdk_module_geo_ending()noexcept {
        try{actual.get()->remove_table_observer(table,row_observer);actual.get()->remove_table_observer("AuditLog",audit_observer);}
        catch(const std::exception& error){ADD_FAILURE()<<"actual geo observer retirement: "<<error.what();}
        catch(...){ADD_FAILURE()<<"actual geo observer retirement: unknown failure";}
        try{if(actual.get()->db().is_in_transaction())actual.get()->rollback();}
        catch(const std::exception& error){ADD_FAILURE()<<"actual geo caller rollback: "<<error.what();}
        catch(...){ADD_FAILURE()<<"actual geo caller rollback: unknown failure";}
        try{actual.close();}
        catch(const std::exception& error){ADD_FAILURE()<<"actual geo owner close: "<<error.what();}
        catch(...){ADD_FAILURE()<<"actual geo owner close: unknown failure";}
    }
};
struct sdk_module_once_denial {
    sqlite3* const native;lattice::lattice_db& owner;
    unsigned attempts=0,denials=0;bool active=false,observation_failed=false;
    int total_at_denial=0,transaction_at_denial=-1;std::uint64_t rows_at_denial=0;
    sdk_module_once_denial(sqlite3* actual,lattice::lattice_db& actual_owner):native(actual),owner(actual_owner) {
        const int rc=sqlite3_set_authorizer(native,[](void* pointer,int action,const char* operation,const char* name,const char*,const char*)noexcept {
            auto& self=*static_cast<sdk_module_once_denial*>(pointer);
            if(action==SQLITE_SAVEPOINT&&operation&&name&&std::strcmp(operation,"BEGIN")==0&&
               std::strcmp(name,"__lattice_mutation_module_settlement")==0) {
                ++self.attempts;
                if(self.denials==0) {
                    ++self.denials;self.total_at_denial=sqlite3_total_changes(self.native);
                    self.transaction_at_denial=sqlite3_txn_state(self.native,"main");
                    try{self.rows_at_denial=lattice::observer_delivery_custody_test_access::tail(self.owner).rows;}
                    catch(...){self.observation_failed=true;}
                    return SQLITE_DENY; // The only actual injected refusal; cleanup is allowed.
                }
            }
            return SQLITE_OK;
        },this);
        if(rc!=SQLITE_OK)throw std::runtime_error("actual module authorizer install refused");
        active=true;
    }
    int clear()noexcept {if(!active)return SQLITE_OK;const int rc=sqlite3_set_authorizer(native,nullptr,nullptr);if(rc==SQLITE_OK)active=false;return rc;}
    ~sdk_module_once_denial()noexcept {if(active)EXPECT_EQ(clear(),SQLITE_OK)<<"actual module authorizer retirement";}
};
std::vector<sqlite3_stmt*> sdk_module_native_census(sqlite3* native) {
    std::vector<sqlite3_stmt*> statements;
    for(auto* statement=sqlite3_next_stmt(native,nullptr);statement;statement=sqlite3_next_stmt(native,statement)) {
        if(statements.size()==4096)throw std::runtime_error("actual test Native census exceeds fixed limit");
        statements.push_back(statement);
    }
    return statements;
}
struct sdk_module_external_ending {
    sqlite3_stmt* reader=nullptr;sqlite3_blob* blob=nullptr;
    int finish_reader()noexcept {auto* actual=std::exchange(reader,nullptr);return actual?sqlite3_finalize(actual):SQLITE_OK;}
    int close_blob()noexcept {auto* actual=std::exchange(blob,nullptr);return actual?sqlite3_blob_close(actual):SQLITE_OK;}
    ~sdk_module_external_ending()noexcept {
        EXPECT_EQ(finish_reader(),SQLITE_OK)<<"actual owned reader finalization";
        EXPECT_EQ(close_blob(),SQLITE_OK)<<"actual owned blob close";
    }
};
TEST(ManagedGraphMutationModuleSettlement, FileAndMemoryOneShotBoundaryDenialKeepsCallerPrefixAndFirstError) {
    for(const bool file_backed:{false,true}) {
        SCOPED_TRACE(file_backed?"file":"named-memory");TempDB path("sdk_geo_module_denial");const auto entry=sdk_module_geo_catalog();
        const auto requested=file_backed?path.str():"file:"+path.path.filename().string()+"?mode=memory&cache=shared";
        auto actual=graph_owner(lattice::swift_configuration(requested),lattice::SchemaVector{entry});ASSERT_NE(actual,nullptr);
        sdk_scalar_tail_events observed;
        auto row_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer(entry.table_name,[&](const auto& batch){observed.row_batch(batch);});
        auto audit_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer("AuditLog",[&](const auto& batch){observed.audit_batch(batch);});
        sdk_module_geo_ending retained{*actual,entry.table_name,row_observer,audit_observer};
        const auto writer=lattice::topology_admission_test_access::writer(*actual->get());ASSERT_NE(writer,nullptr);
        ASSERT_EQ(writer.get(),&actual->get()->db());auto* native=lattice::topology_admission_test_access::raw(*writer);ASSERT_NE(native,nullptr);
        actual->get()->begin_transaction();ASSERT_TRUE(actual->get()->db().is_in_transaction());
        auto prefix=sdk_module_geo_object(entry,"alpha","prefix",0);lattice::cxx_error prefix_error(std::runtime_error("old"));
        ASSERT_TRUE(actual->add_checked(*prefix,prefix_error))<<prefix_error.msg;ASSERT_TRUE(prefix->is_managed());
        const auto prefix_rows=writer->query("SELECT * FROM main.SDKSeedSpatialMutation ORDER BY id");ASSERT_EQ(prefix_rows.size(),1u);
        const auto prefix_audits=writer->query("SELECT * FROM main.AuditLog ORDER BY id");ASSERT_EQ(prefix_audits.size(),1u);
        const auto prefix_tail=lattice::observer_delivery_custody_test_access::tail(*actual->get());
        const int prefix_changes=sqlite3_total_changes(native);
        const auto before_offer=observed.snapshot();EXPECT_TRUE(before_offer.rows.empty());EXPECT_TRUE(before_offer.audits.empty());
        sdk_module_once_denial fault(native,*actual->get());
        auto refused=sdk_module_geo_object(entry,"failed","refused",7);lattice::cxx_error failure(std::runtime_error("old"));
        ASSERT_FALSE(actual->add_checked(*refused,failure));
        const auto bridge=lattice::borrow_bridge_outcome();
        ASSERT_TRUE(bridge.failed);ASSERT_TRUE(bridge.diagnostic_available);ASSERT_NE(bridge.message_data,nullptr);
        const std::string first_message=failure.msg;
        EXPECT_EQ(first_message,"not authorized");EXPECT_NE(failure.name.find("mutation_control_error"),std::string::npos);
        EXPECT_EQ(std::string(bridge.message_data,bridge.message_size),first_message);
        EXPECT_EQ(fault.denials,1u);
        // The Bridge graph owns the outer savepoint and Core bulk the inner:
        // denied inner commit, then inner and outer rollback settlements.
        EXPECT_EQ(fault.attempts,3u);EXPECT_FALSE(fault.observation_failed);
        EXPECT_GT(fault.total_at_denial,prefix_changes);EXPECT_GT(fault.rows_at_denial,prefix_tail.rows);
        EXPECT_EQ(fault.transaction_at_denial,SQLITE_TXN_WRITE); // Actual writes preceded refusal.
        EXPECT_FALSE(refused->is_managed());ASSERT_TRUE(writer->is_in_transaction());
        EXPECT_EQ(writer->query("SELECT * FROM main.SDKSeedSpatialMutation ORDER BY id"),prefix_rows);
        EXPECT_EQ(writer->query("SELECT * FROM main.AuditLog ORDER BY id"),prefix_audits);
        const auto restored=lattice::observer_delivery_custody_test_access::tail(*actual->get());
        EXPECT_EQ(restored.rows,prefix_tail.rows);EXPECT_EQ(restored.custody_epoch,prefix_tail.custody_epoch);
        const auto after_refusal=observed.snapshot();EXPECT_TRUE(after_refusal.rows.empty());EXPECT_TRUE(after_refusal.audits.empty());
        EXPECT_EQ(after_refusal.row_batches,0u);EXPECT_EQ(after_refusal.audit_batches,0u);
        ASSERT_EQ(fault.clear(),SQLITE_OK);
        auto later=sdk_module_geo_object(entry,"beta","later",1);lattice::cxx_error later_error(std::runtime_error("old"));
        ASSERT_TRUE(actual->add_checked(*later,later_error))<<later_error.msg;ASSERT_TRUE(later->is_managed());
        EXPECT_EQ(failure.msg,first_message);ASSERT_TRUE(writer->is_in_transaction());
        const auto final_rows=writer->query("SELECT * FROM main.SDKSeedSpatialMutation ORDER BY id");ASSERT_EQ(final_rows.size(),2u);
        const auto final_audits=writer->query("SELECT id,globalId FROM main.AuditLog ORDER BY id");ASSERT_EQ(final_audits.size(),2u);
        actual->get()->commit();ASSERT_FALSE(writer->is_in_transaction());
        const auto delivered=observed.snapshot();ASSERT_EQ(delivered.rows.size(),2u);ASSERT_EQ(delivered.audits.size(),2u);
        EXPECT_EQ(delivered.row_batches,1u);EXPECT_EQ(delivered.audit_batches,1u);
        for(std::size_t i=0;i<2;++i) {
            SCOPED_TRACE(i);EXPECT_EQ(std::get<1>(delivered.rows[i]),"INSERT");EXPECT_EQ(std::get<1>(delivered.audits[i]),"INSERT");
            EXPECT_EQ(std::get<2>(delivered.rows[i]),std::get<std::int64_t>(final_rows[i].at("id")));
            EXPECT_EQ(std::get<3>(delivered.rows[i]),std::get<std::string>(final_rows[i].at("globalId")));
            EXPECT_EQ(std::get<2>(delivered.audits[i]),std::get<std::int64_t>(final_audits[i].at("id")));
            EXPECT_EQ(std::get<3>(delivered.audits[i]),std::get<std::string>(final_audits[i].at("globalId")));
        }
        EXPECT_EQ(actual->get()->count_within_bbox_shape(entry.table_name,"location",-1,2,-1,2,
            std::string("enabled = 1"),std::string("category"),std::string("label")),2);
        EXPECT_EQ(actual->get()->count_within_bbox_shape(entry.table_name,"location",6,8,6,8,
            std::string("enabled = 1"),std::string("category"),std::string("label")),0);
    }
}
TEST(ManagedGraphMutationModuleSettlement, FileAndMemoryOwnedExternalReaderAndNullSqlBlobStillRefuseThenEnd) {
    using namespace lattice::detail::recovery_admission_test_hooks;
    for(const bool file_backed:{false,true}) {
        SCOPED_TRACE(file_backed?"file":"named-memory");TempDB path("sdk_geo_external_vm");const auto entry=sdk_module_geo_catalog();
        const auto requested=file_backed?path.str():"file:"+path.path.filename().string()+"?mode=memory&cache=shared";
        auto actual=graph_owner(lattice::swift_configuration(requested),lattice::SchemaVector{entry});ASSERT_NE(actual,nullptr);
        actual->get()->db().execute("CREATE TABLE _sdk_module_blob (id INTEGER PRIMARY KEY, value BLOB)");
        actual->get()->db().execute("INSERT INTO _sdk_module_blob VALUES (1,zeroblob(32))");
        sdk_scalar_tail_events observed;
        auto row_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer(entry.table_name,[&](const auto& batch){observed.row_batch(batch);});
        auto audit_observer=static_cast<lattice::lattice_db&>(*actual->get()).add_table_observer("AuditLog",[&](const auto& batch){observed.audit_batch(batch);});
        sdk_module_geo_ending retained{*actual,entry.table_name,row_observer,audit_observer};
        const auto writer=lattice::topology_admission_test_access::writer(*actual->get());ASSERT_NE(writer,nullptr);
        ASSERT_EQ(writer.get(),&actual->get()->db());auto* native=lattice::topology_admission_test_access::raw(*writer);ASSERT_NE(native,nullptr);
        sdk_module_external_ending external;
        actual->get()->begin_transaction();ASSERT_TRUE(writer->is_in_transaction());
        auto prefix=sdk_module_geo_object(entry,"alpha","prefix",0);lattice::cxx_error error(std::runtime_error("old"));
        ASSERT_TRUE(actual->add_checked(*prefix,error))<<error.msg;
        const auto prefix_rows=writer->query("SELECT * FROM main.SDKSeedSpatialMutation ORDER BY id");ASSERT_EQ(prefix_rows.size(),1u);
        const auto prefix_audits=writer->query("SELECT * FROM main.AuditLog ORDER BY id");ASSERT_EQ(prefix_audits.size(),1u);
        const auto prefix_tail=lattice::observer_delivery_custody_test_access::tail(*actual->get());
        auto offered=sdk_module_geo_object(entry,"beta","later",1);
        ASSERT_EQ(sqlite3_prepare_v2(native,"SELECT category FROM main.SDKSeedSpatialMutation UNION ALL SELECT 'next'",-1,&external.reader,nullptr),SQLITE_OK);
        ASSERT_EQ(sqlite3_step(external.reader),SQLITE_ROW);ASSERT_NE(sqlite3_stmt_busy(external.reader),0);
        {
            mutation_refusal_observation refusal;refusal.expected_writer=writer.get();mutation_refusal_scope diagnostic(refusal);
            ASSERT_FALSE(sdk_actual_add(*actual,*offered,error,refusal));
            EXPECT_EQ(error.msg,"mutation operation refuses unregistered active engine statement");
            EXPECT_EQ(refusal.refusals,1u);EXPECT_EQ(refusal.statement_comparison,reinterpret_cast<std::uintptr_t>(external.reader));
            EXPECT_TRUE(refusal.sql_present);EXPECT_TRUE(refusal.busy);EXPECT_TRUE(refusal.readonly);
        }
        EXPECT_FALSE(offered->is_managed());ASSERT_TRUE(writer->is_in_transaction());
        EXPECT_NE(sqlite3_stmt_busy(external.reader),0);ASSERT_EQ(sqlite3_step(external.reader),SQLITE_ROW);
        EXPECT_STREQ(reinterpret_cast<const char*>(sqlite3_column_text(external.reader,0)),"next");
        ASSERT_EQ(external.finish_reader(),SQLITE_OK); // Owned exact finalization, no production reset.
        const auto before_blob=sdk_module_native_census(native);
        ASSERT_EQ(sqlite3_blob_open(native,"main","_sdk_module_blob","value",1,0,&external.blob),SQLITE_OK);
        const auto with_blob=sdk_module_native_census(native);std::vector<sqlite3_stmt*> added;
        for(auto* statement:with_blob)if(std::find(before_blob.begin(),before_blob.end(),statement)==before_blob.end())added.push_back(statement);
        ASSERT_EQ(added.size(),1u);auto* blob_vm=added[0];
        ASSERT_EQ(sqlite3_sql(blob_vm),nullptr);ASSERT_NE(sqlite3_stmt_busy(blob_vm),0);
        ASSERT_NE(sqlite3_stmt_readonly(blob_vm),0);ASSERT_EQ(sqlite3_column_count(blob_vm),0);
        {
            mutation_refusal_observation refusal;refusal.expected_writer=writer.get();mutation_refusal_scope diagnostic(refusal);
            ASSERT_FALSE(sdk_actual_add(*actual,*offered,error,refusal));
            EXPECT_EQ(error.msg,"mutation operation refuses unregistered active engine statement");
            EXPECT_EQ(refusal.refusals,1u);EXPECT_EQ(refusal.statement_comparison,reinterpret_cast<std::uintptr_t>(blob_vm));
            EXPECT_FALSE(refusal.sql_present);EXPECT_TRUE(refusal.busy);EXPECT_TRUE(refusal.readonly);EXPECT_EQ(refusal.columns,0);
        }
        EXPECT_FALSE(offered->is_managed());ASSERT_TRUE(writer->is_in_transaction());
        EXPECT_NE(sqlite3_stmt_busy(blob_vm),0);std::array<unsigned char,32> bytes{};
        ASSERT_EQ(sqlite3_blob_read(external.blob,bytes.data(),int(bytes.size()),0),SQLITE_OK);
        EXPECT_EQ(bytes,(std::array<unsigned char,32>{}));ASSERT_EQ(external.close_blob(),SQLITE_OK);
        const auto ended=sdk_module_native_census(native);EXPECT_EQ(std::find(ended.begin(),ended.end(),blob_vm),ended.end());
        EXPECT_EQ(writer->query("SELECT * FROM main.SDKSeedSpatialMutation ORDER BY id"),prefix_rows);
        EXPECT_EQ(writer->query("SELECT * FROM main.AuditLog ORDER BY id"),prefix_audits);
        const auto unchanged=lattice::observer_delivery_custody_test_access::tail(*actual->get());
        EXPECT_EQ(unchanged.rows,prefix_tail.rows);EXPECT_EQ(unchanged.custody_epoch,prefix_tail.custody_epoch);
        const auto refused_events=observed.snapshot();EXPECT_TRUE(refused_events.rows.empty());EXPECT_TRUE(refused_events.audits.empty());
        EXPECT_EQ(refused_events.row_batches,0u);EXPECT_EQ(refused_events.audit_batches,0u);
        ASSERT_TRUE(actual->add_checked(*offered,error))<<error.msg;ASSERT_TRUE(offered->is_managed());ASSERT_TRUE(writer->is_in_transaction());
        const auto final_rows=writer->query("SELECT id,globalId FROM main.SDKSeedSpatialMutation ORDER BY id");ASSERT_EQ(final_rows.size(),2u);
        const auto final_audits=writer->query("SELECT id,globalId FROM main.AuditLog ORDER BY id");ASSERT_EQ(final_audits.size(),2u);
        actual->get()->commit();ASSERT_FALSE(writer->is_in_transaction());
        const auto delivered=observed.snapshot();ASSERT_EQ(delivered.rows.size(),2u);ASSERT_EQ(delivered.audits.size(),2u);
        EXPECT_EQ(delivered.row_batches,1u);EXPECT_EQ(delivered.audit_batches,1u);
        for(std::size_t i=0;i<2;++i) {
            SCOPED_TRACE(i);EXPECT_EQ(std::get<1>(delivered.rows[i]),"INSERT");EXPECT_EQ(std::get<1>(delivered.audits[i]),"INSERT");
            EXPECT_EQ(std::get<2>(delivered.rows[i]),std::get<std::int64_t>(final_rows[i].at("id")));
            EXPECT_EQ(std::get<3>(delivered.rows[i]),std::get<std::string>(final_rows[i].at("globalId")));
            EXPECT_EQ(std::get<2>(delivered.audits[i]),std::get<std::int64_t>(final_audits[i].at("id")));
            EXPECT_EQ(std::get<3>(delivered.audits[i]),std::get<std::string>(final_audits[i].at("globalId")));
        }
        EXPECT_EQ(actual->get()->count_within_bbox_shape(entry.table_name,"location",-1,2,-1,2,
            std::string("enabled = 1"),std::string("category"),std::string("label")),2);
    }
}

} // namespace
#endif

#if defined(__APPLE__) || defined(__linux__)
namespace {
class ForkOperationOwner : public lattice::lattice_db {
public:
    explicit ForkOperationOwner(const lattice::configuration& config = lattice::configuration(":memory:")) : lattice_db(config) {}
    template<class F> void unit(F&& body) { with_mutation_operation(std::forward<F>(body)); }
};
void child_alarm() {
    struct sigaction action{}; action.sa_handler = SIG_DFL; sigemptyset(&action.sa_mask); sigaction(SIGALRM, &action, nullptr);
    sigset_t unblocked; sigemptyset(&unblocked); sigaddset(&unblocked, SIGALRM); sigprocmask(SIG_UNBLOCK, &unblocked, nullptr); alarm(5);
}
void fork_unwind_control(bool throw_in_child) {
    ForkOperationOwner owner; pid_t child = -1; bool in_child = false, refused = false;
    try {
        owner.unit([&](lattice::database&) {
            child = fork(); if (child < 0) throw std::runtime_error("fork control creation failed");
            if (!child) { in_child = true; child_alarm(); if (throw_in_child) throw std::runtime_error("child body refusal"); }
        });
    } catch (...) { refused = true; }
    if (in_child) _exit(refused ? 0 : 7); // actual operation shells already unwound
    ASSERT_GT(child, 0); int status = 0; pid_t observed = 0;
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(7);
    while (std::chrono::steady_clock::now() < end) {
        observed = waitpid(child, &status, WNOHANG);
        if (observed == child || (observed < 0 && errno != EINTR)) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (observed != child) {
        (void)kill(child, SIGKILL);
        const auto cleanup_end = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < cleanup_end) {
            observed = waitpid(child, &status, WNOHANG);
            if (observed == child || (observed < 0 && errno != EINTR)) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    if (observed != child) {
        static constexpr char debt[] = "mutation-operation child ending unproved; Native namespace retained\n";
        (void)::write(STDERR_FILENO, debt, sizeof(debt)-1); _exit(94);
    }
    ASSERT_TRUE(WIFEXITED(status)); EXPECT_EQ(WEXITSTATUS(status), 0);
    EXPECT_FALSE(refused); owner.unit([](lattice::database&) {}); owner.close();
}
TEST(ManagedMutationOperationGate, NamedSharedMemoryForeignCallerRefusesBeforeGateWait) {
    ForkOperationOwner owner(lattice::configuration("file:operation_foreign_gate?mode=memory&cache=shared"));
    owner.begin_transaction();
    std::promise<bool> result; auto future = result.get_future();
    std::thread foreign([&] {
        try { owner.unit([](lattice::database&) {}); result.set_value(false); }
        catch (...) { result.set_value(true); }
    });
    const auto before_release = future.wait_for(std::chrono::seconds(2));
    // ALWAYS retire the real parent gate before joining, including baseline
    // failure. The test may fail but may not deadlock its own cleanup.
    owner.rollback(); foreign.join();
    EXPECT_EQ(before_release, std::future_status::ready); EXPECT_TRUE(future.get()); owner.close();
}
TEST(ManagedMutationForkCustody, ReturnedBodyAbandonsInheritedNativeGateAndPayload) { fork_unwind_control(false); }
TEST(ManagedMutationForkCustody, ThrowingBodyAbandonsInheritedNativeGateAndPayload) { fork_unwind_control(true); }
}
#endif
