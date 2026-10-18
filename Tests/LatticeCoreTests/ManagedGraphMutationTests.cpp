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
