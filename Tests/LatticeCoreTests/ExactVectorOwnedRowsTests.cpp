#include "TestHelpers.hpp"
#include "ManagedAttachmentTestSupport.hpp"
#include <exact_vector.hpp>
#include <lattice.hpp>
#include <lattice/exact_vector_owned.hpp>
#include <lattice/exact_vector_rows.hpp>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <functional>
#include <future>
#include <limits>
#include <set>
#include <type_traits>
#if GTEST_HAS_DEATH_TEST && (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <csignal>
#include <unistd.h>
#endif

// Source-authored ownership/bridge tests. Ranking arithmetic and the strict
// single-statement row selector retain their separate ExactVectorRows tests.
// Future qualification must configure its temporary root under ~/localdev.
namespace lattice {
struct exact_vector_owned_test_access {
    static auto acquire(lattice_db& owner) { return owner.acquire_exact_vector_read(); }
    static size_t active(const lattice_db& owner) {
        return owner.active_managed_attachment_operations_.load(std::memory_order_acquire);
    }
    static auto topology(lattice_db& owner) { return std::unique_lock<std::mutex>(owner.attach_mutex_); }
    static void quiesce(swift_lattice& owner) {
        if (owner.vec0_reconcile_future_.valid()) owner.vec0_reconcile_future_.wait();
        owner.wait_for_vec0_training();
        owner.stop_audit_maintenance();
    }
    static managed<swift_dynamic_object> hydrate(lattice_db& owner, const database::row_t& row,
                                                const detail::exact_vector_read_lease& lease) {
        return owner.hydrate_exact_row<swift_dynamic_object>(row, "ExactOwnedDoc", lease);
    }
    // Corrupt one admission prerequisite, then restore it before returning.
    // No SQL runs under topology, and no old returned object is repaired.
    static exact_vector_live_result malformed_registration(swift_lattice_ref& ref,
                                                           const exact_vector_request& request, int mode) {
        auto& owner = *ref.get();
        auto lock = topology(owner);
        auto tokens = owner.attached_route_tokens_;
        auto metadata = owner.attached_route_metadata_;
        const bool valid = owner.attachment_topology_valid_;
        if (mode == 0) owner.attached_route_tokens_.begin()->second = 0;
        if (mode == 1) owner.attached_route_metadata_.clear();
        if (mode == 2) owner.attachment_topology_valid_ = false;
        lock.unlock();
        auto result = ref.exact_nearest_rows(request);
        lock.lock();
        owner.attached_route_tokens_ = std::move(tokens);
        owner.attached_route_metadata_ = std::move(metadata);
        owner.attachment_topology_valid_ = valid;
        return result;
    }
};
} // namespace lattice

namespace {
using namespace lattice;
using Metric = detail::exact_vector_metric;
constexpr int32_t success = 0, invalid_request = 1, invalid_schema = 2;
constexpr int32_t resource_busy = 3, database_failure = 4, bridge_failure = 5;
const std::string table = "ExactOwnedDoc";
const std::vector<std::string> columns{"id", "globalId", "embedding", "payload", "eligible", "tag", "note"};

std::string quote(const std::string& value) { return detail::exact_vector_identifier(value); }
std::string alias(const TempDB& path) { return path.path.stem().string(); }

SchemaVector schema(const std::string& name = table) {
    swift_schema_entry entry;
    entry.table_name = name;
    for (const auto& field : {"payload", "note"}) {
        property_descriptor p;
        p.name = field; p.type = column_type::text; p.nullable = true;
        entry.properties[p.name] = p;
    }
    property_descriptor eligible;
    eligible.name = "eligible"; eligible.type = column_type::integer;
    entry.properties[eligible.name] = eligible;
    property_descriptor blob;
    blob.name = "tag"; blob.type = column_type::blob; blob.nullable = true;
    entry.properties[blob.name] = blob;
    blob.name = "embedding"; blob.is_vector = true;
    entry.properties[blob.name] = blob;
    return {entry};
}

std::unique_ptr<swift_lattice_ref> open(const std::string& path, const SchemaVector& schemas = schema()) {
#if LATTICE_HAS_FRT
    auto result = std::unique_ptr<swift_lattice_ref>(swift_lattice_ref::create(swift_configuration(path), schemas));
#else
    auto result = std::make_unique<swift_lattice_ref>(swift_lattice_ref::create(swift_configuration(path), schemas));
#endif
    if (!result || !result->get()) throw std::runtime_error("exact fixture open failed");
    exact_vector_owned_test_access::quiesce(*result->get());
    return result;
}

struct Store {
    TempDB path;
    std::unique_ptr<swift_lattice_ref> ref;
    explicit Store(const std::string& suffix, const SchemaVector& schemas = schema())
        : path("exact_owned_" + suffix), ref(open(path.str(), schemas)) {}
    lattice::swift_lattice& core() { return *ref->get(); }
    database& db() { return core().db(); }
};

void insert(Store& store, int64_t id, const std::string& gid, const std::vector<float>& vector,
            const std::string& payload, int64_t eligible = 1) {
    store.db().execute("INSERT INTO main." + quote(table) +
        "(id,globalId,embedding,payload,eligible,tag,note) VALUES(?,?,?,?,?,?,?)",
        {id, gid, pack_floats(vector), payload, eligible, std::vector<uint8_t>{0, 255, 4}, nullptr});
}

exact_vector_request request(int64_t k = 8, Metric metric = Metric::l2,
                             const std::vector<float>& query = {0, 0, 0, 0}) {
    exact_vector_request value;
    value.set_table(table); value.set_column("embedding"); value.set_k(k);
    value.set_metric(static_cast<int32_t>(metric));
    for (const auto component : query) value.add_component(component);
    return value;
}

std::unique_ptr<dynamic_object_ref> object(exact_vector_live_result& result, int64_t index) {
#if LATTICE_HAS_FRT
    return std::unique_ptr<dynamic_object_ref>(result.object_at(index));
#else
    return std::make_unique<dynamic_object_ref>(result.object_at(index));
#endif
}

void expect_released(Store& store) {
    EXPECT_EQ(exact_vector_owned_test_access::active(store.core()), 0u);
    EXPECT_FALSE(detail::managed_route_scope::active_for(&store.db()));
    auto* mutex = sqlite3_db_mutex(store.db().handle());
    bool free = false;
    std::thread probe([&] {
        free = sqlite3_mutex_try(mutex) == SQLITE_OK;
        if (free) sqlite3_mutex_leave(mutex);
    });
    probe.join();
    EXPECT_TRUE(free) << "same-thread recursive acquisition would hide a leaked lease";
}

std::set<sqlite3_stmt*> statements(sqlite3* raw) {
    std::set<sqlite3_stmt*> result;
    for (auto* statement = sqlite3_next_stmt(raw, nullptr); statement;
         statement = sqlite3_next_stmt(raw, statement)) result.insert(statement);
    return result;
}

void bounded(const std::function<void()>& body) {
#if GTEST_HAS_DEATH_TEST && (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
    struct RestoreStyle {
        std::string previous = ::testing::FLAGS_gtest_death_test_style;
        ~RestoreStyle() { ::testing::FLAGS_gtest_death_test_style = std::move(previous); }
    } restore;
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT({
        sigset_t alarm_mask;
        sigemptyset(&alarm_mask); sigaddset(&alarm_mask, SIGALRM);
        if (std::signal(SIGALRM, SIG_DFL) == SIG_ERR ||
            sigprocmask(SIG_UNBLOCK, &alarm_mask, nullptr) != 0) _exit(2);
        alarm(15);
        try {
            body(); // fixture cleanup completes before process exit
            if (::testing::Test::HasFailure()) _exit(1);
            std::fputs("exact_owned_case_complete\n", stderr);
            _exit(0);
        } catch (const std::exception& error) {
            std::fprintf(stderr, "exact_owned_failure: %s\n", error.what()); _exit(1);
        } catch (...) { _exit(1); }
    }, ::testing::ExitedWithCode(0), "exact_owned_case_complete");
#else
    GTEST_SKIP() << "bounded deadlock oracle requires native fresh-exec death tests";
#endif
}
} // namespace

TEST(ExactVectorOwnedRows, PhysicalWinnerKeepsItsPayloadProvenanceAndLiveFields) {
    Store main("main"), arm("quoted_\"arm"), other("other");
    insert(main, 1, "shared", {9, 0, 0, 0}, "wrong-main");
    insert(main, 2, "second", {2, 0, 0, 0}, "second");
    // Different physical column order must not swap payloads at the UNION.
    arm.db().execute("DROP TABLE main.ExactOwnedDoc");
    arm.db().execute("CREATE TABLE main.ExactOwnedDoc(note TEXT,tag BLOB,eligible INTEGER,"
                     "payload TEXT,embedding BLOB,globalId TEXT UNIQUE,id INTEGER PRIMARY KEY AUTOINCREMENT)");
    const std::string original("right\0attached", 14);
    insert(arm, 1, "shared", {1, 0, 0, 0}, original);
    insert(other, 1, "shared", {0, 0, 0, 0}, "ineligible", 0);
    ASSERT_TRUE(main.core().attach(arm.core()));
    ASSERT_TRUE(main.core().attach(other.core()));
    auto input = request(2);
    input.set_predicate("m.eligible = ?"); input.add_parameter(int64_t(1));
    auto result = main.ref->exact_nearest_rows(input);
    ASSERT_EQ(result.status_code(), success) << result.error_message();
    ASSERT_EQ(result.row_count(), 2);
    auto first = object(result, 0), second = object(result, 1);
    ASSERT_TRUE(first && second);
    ASSERT_EQ(result.status_code(), success);
    EXPECT_EQ(first->get_string("payload"), original);
    EXPECT_EQ(second->get_string("payload"), "second");
    EXPECT_DOUBLE_EQ(result.distance_at(0), 1);
    EXPECT_DOUBLE_EQ(result.distance_at(1), 2);
    EXPECT_EQ(first->managed_primary_key(), 1);
    EXPECT_EQ(std::get<std::string>(first->query_row_value("_source")), quote(alias(arm.path)));
    EXPECT_GT(std::get<int64_t>(first->query_row_value("_lattice_attach_token")), 0);
    EXPECT_EQ(std::get<std::vector<uint8_t>>(first->query_row_value("embedding")), pack_floats({1, 0, 0, 0}));
    EXPECT_EQ(first->get_data("tag"), (std::vector<uint8_t>{0, 255, 4}));
    EXPECT_EQ(first->query_row_value_type("note"), 0);
    EXPECT_FALSE(first->is_row_cache_enabled());
    arm.db().execute("UPDATE main.ExactOwnedDoc SET payload='later' WHERE id=1");
    EXPECT_EQ(first->get_string("payload"), "later");
    EXPECT_EQ(std::get<std::string>(first->query_row_value("payload")), original);
    first->set_string("payload", "written-to-winner");
    EXPECT_EQ(std::get<std::string>(arm.db().query("SELECT payload FROM main.ExactOwnedDoc WHERE id=1")[0].at("payload")), "written-to-winner");
    EXPECT_EQ(std::get<std::string>(main.db().query("SELECT payload FROM main.ExactOwnedDoc WHERE id=1")[0].at("payload")), "wrong-main");
    expect_released(main);
    ASSERT_TRUE(main.core().detach(other.core()));
    ASSERT_TRUE(main.core().detach(arm.core()));
}

TEST(ExactVectorOwnedRows, AttachedOnlyModelUsesItsCapturedSchemaAndDoesNotRepairSidecars) {
    Store main("source_only_main", schema("OtherOwnedDoc")), arm("source_only_arm");
    insert(arm, 1, "remote", {1, 0, 0, 0}, "remote");
    ASSERT_TRUE(main.core().attach(arm.core()));
    const auto before = arm.db().query("SELECT name,sql FROM sqlite_master ORDER BY name");
    auto result = main.ref->exact_nearest_rows(request());
    ASSERT_EQ(result.status_code(), success) << result.error_message();
    ASSERT_EQ(result.row_count(), 1);
    auto row = object(result, 0);
    ASSERT_TRUE(row);
    EXPECT_EQ(row->get()->get_model_table_name(), table);
    EXPECT_EQ(row->get_data("embedding"), pack_floats({1, 0, 0, 0}));
    EXPECT_FALSE(row->has_value("note"));
    row->set_string("note", "source-schema-field");
    EXPECT_EQ(row->get_string("note"), "source-schema-field");
    EXPECT_EQ(arm.db().query("SELECT name,sql FROM sqlite_master ORDER BY name"), before);
    ASSERT_TRUE(main.core().detach(arm.core()));
}

TEST(ExactVectorOwnedRows, BridgeMetricsEmptySuccessAndStickyFailuresAreDistinct) {
    Store main("bridge_status");
    exact_vector_live_result default_result;
    EXPECT_EQ(default_result.status_code(), bridge_failure);
    auto empty = main.ref->exact_nearest_rows(request());
    EXPECT_EQ(empty.status_code(), success);
    EXPECT_EQ(empty.row_count(), 0);
    insert(main, 1, "one", {1, 2, 0, 0}, "one");
    for (auto metric : {Metric::l2, Metric::l1, Metric::cosine}) {
        auto result = main.ref->exact_nearest_rows(request(1, metric, {1, 0, 0, 0}));
        ASSERT_EQ(result.status_code(), success) << result.error_message();
        ASSERT_EQ(result.row_count(), 1);
        const double expected = metric == Metric::cosine ? 1 - 1 / std::sqrt(5.0) : 2;
        EXPECT_NEAR(result.distance_at(0), expected, 0.00001);
        auto copy = result;
        result = exact_vector_live_result{};
        auto row = object(copy, 0);
        ASSERT_TRUE(row);
        EXPECT_EQ(row->get_string("payload"), "one");
        (void)copy.distance_at(-1);
        EXPECT_EQ(copy.status_code(), invalid_request);
        (void)copy.error_message(); (void)copy.row_count(); (void)copy.distance_at(0);
        EXPECT_EQ(copy.status_code(), invalid_request);
    }
    auto zero = main.ref->exact_nearest_rows(request(0));
    EXPECT_EQ(zero.status_code(), success); EXPECT_EQ(zero.row_count(), 0);
    auto bad_index = main.ref->exact_nearest_rows(request(1));
    auto absent = object(bad_index, 100);
    EXPECT_EQ(bad_index.status_code(), invalid_request);
    (void)bad_index.error_message();
    EXPECT_EQ(bad_index.status_code(), invalid_request);
    expect_released(main);
}

TEST(ExactVectorOwnedRows, InvalidRequestAndRegisteredPropertyAreCheckedEvenAtZeroK) {
    Store main("invalid_request");
    auto unknown = request(0); unknown.set_table("Unregistered");
    EXPECT_EQ(main.ref->exact_nearest_rows(unknown).status_code(), invalid_schema);
    auto scalar = request(0); scalar.set_column("tag");
    EXPECT_EQ(main.ref->exact_nearest_rows(scalar).status_code(), invalid_schema);
    auto negative = request(-1);
    EXPECT_EQ(main.ref->exact_nearest_rows(negative).status_code(), invalid_request);
    auto metric = request(0); metric.set_metric(99);
    EXPECT_EQ(main.ref->exact_nearest_rows(metric).status_code(), invalid_request);
    auto nan = request(0, Metric::l2, {std::numeric_limits<float>::quiet_NaN()});
    EXPECT_EQ(main.ref->exact_nearest_rows(nan).status_code(), invalid_request);
    auto infinity = request(0, Metric::l2, {std::numeric_limits<float>::infinity()});
    EXPECT_EQ(main.ref->exact_nearest_rows(infinity).status_code(), invalid_request);
    EXPECT_EQ(main.ref->exact_nearest_rows(request(0, Metric::cosine)).status_code(), invalid_request);
    auto syntax = request(0); syntax.set_predicate("m.eligible = (");
    EXPECT_NE(main.ref->exact_nearest_rows(syntax).status_code(), success);
    auto bindings = request(); bindings.set_predicate("m.eligible = ?");
    EXPECT_NE(main.ref->exact_nearest_rows(bindings).status_code(), success);
    auto sticky = request(); sticky.set_metric(99); sticky.set_metric(0);
    EXPECT_EQ(main.ref->exact_nearest_rows(sticky).status_code(), invalid_request);
    expect_released(main);
}

TEST(ExactVectorOwnedRows, DimensionBoundaryAccepts8192AndRefuses8193Permanently) {
    Store main("dimension_boundary");
    auto boundary = request(0, Metric::l2, std::vector<float>(8192, 0));
    auto accepted = main.ref->exact_nearest_rows(boundary);
    EXPECT_EQ(accepted.status_code(), success) << accepted.error_message();
    EXPECT_EQ(accepted.row_count(), 0);
    boundary.add_component(1);
    EXPECT_EQ(main.ref->exact_nearest_rows(boundary).status_code(), invalid_request);
    boundary.set_k(0); boundary.set_metric(0); boundary.set_table(table);
    boundary.add_component(0); // later valid setter calls cannot erase overflow
    EXPECT_EQ(main.ref->exact_nearest_rows(boundary).status_code(), invalid_request);
    expect_released(main);
}

TEST(ExactVectorOwnedRows, RequestCopyPreservesBindingsAndStickyFailureWithoutChangingItsSource) {
    static_assert(std::is_nothrow_copy_constructible_v<exact_vector_request>);
    static_assert(std::is_nothrow_copy_assignable_v<exact_vector_request>);
    Store main("request_copy");
    insert(main, 1, "included", {1, 0, 0, 0}, "included", 1);
    insert(main, 2, "excluded", {0, 0, 0, 0}, "excluded", 0);
    auto source = request(); source.set_predicate("m.eligible = ?"); source.add_parameter(int64_t(1));
    auto copied = source;
    exact_vector_request assigned; assigned = source;
    source.set_metric(99);
    for (const auto* input : {&copied, &assigned}) {
        auto result = main.ref->exact_nearest_rows(*input);
        ASSERT_EQ(result.status_code(), success) << result.error_message();
        ASSERT_EQ(result.row_count(), 1);
        auto row = object(result, 0); ASSERT_TRUE(row);
        EXPECT_EQ(std::get<std::string>(row->query_row_value("payload")), "included");
    }
    auto failed_copy = source;
    assigned = source;
    source.set_metric(0); failed_copy.set_metric(0); assigned.set_metric(0);
    for (const auto* input : {&source, &failed_copy, &assigned})
        EXPECT_EQ(main.ref->exact_nearest_rows(*input).status_code(), invalid_request);
    EXPECT_EQ(main.ref->exact_nearest_rows(copied).status_code(), success);
    expect_released(main);
}

TEST(ExactVectorOwnedRows, SameColumnLayoutsWithoutSoleIntegerPrimaryKeyCannotHydrateLiveRows) {
    for (bool composite : {false, true}) {
        Store main(composite ? "composite_pk" : "nonunique_id");
        main.db().execute("DROP TABLE main.ExactOwnedDoc");
        main.db().execute("CREATE TABLE main.ExactOwnedDoc(id INTEGER,globalId TEXT,embedding BLOB,"
            "payload TEXT,eligible INTEGER,tag BLOB,note TEXT" +
            std::string(composite ? ",PRIMARY KEY(id,globalId))" : ")"));
        insert(main, 7, "winner", {0, 0, 0, 0}, "winner-payload");
        insert(main, 7, "different", {9, 0, 0, 0}, "wrong-live-payload");
        for (int64_t k : {int64_t(0), int64_t(1)}) {
            auto result = main.ref->exact_nearest_rows(request(k));
            EXPECT_EQ(result.status_code(), invalid_schema) << result.error_message();
            EXPECT_EQ(result.row_count(), 0);
        }
        expect_released(main);
    }
}

TEST(ExactVectorOwnedRows, WinningZeroIDFailsLiveHydrationButNegativeNonzeroIDRemainsLive) {
    Store main("managed_id_boundary");
    insert(main, 0, "zero", {0, 0, 0, 0}, "unbound-zero");
    insert(main, -7, "negative", {1, 0, 0, 0}, "negative-original");
    const auto before = statements(main.db().handle());
    auto failed = main.ref->exact_nearest_rows(request(1));
    EXPECT_EQ(failed.status_code(), database_failure);
    EXPECT_EQ(failed.row_count(), 0);
    EXPECT_EQ(statements(main.db().handle()), before);
    expect_released(main);
    main.db().execute("DELETE FROM main.ExactOwnedDoc WHERE id=0");
    auto accepted = main.ref->exact_nearest_rows(request(1));
    ASSERT_EQ(accepted.status_code(), success) << accepted.error_message();
    ASSERT_EQ(accepted.row_count(), 1);
    auto row = object(accepted, 0); ASSERT_TRUE(row);
    EXPECT_EQ(row->managed_primary_key(), -7);
    EXPECT_FALSE(row->is_row_cache_enabled());
    main.db().execute("UPDATE main.ExactOwnedDoc SET payload='negative-live' WHERE id=-7");
    EXPECT_EQ(row->get_string("payload"), "negative-live");
    EXPECT_EQ(std::get<std::string>(row->query_row_value("payload")), "negative-original");
    row->set_string("payload", "negative-write");
    EXPECT_EQ(std::get<std::string>(main.db().query(
        "SELECT payload FROM main.ExactOwnedDoc WHERE id=-7")[0].at("payload")), "negative-write");
    expect_released(main);
}

TEST(ExactVectorOwnedRows, SameThreadCoreTransactionIsReadableButForeignAndRawTransactionsAreRefused) {
    bounded([] {
        Store main("transaction_admission");
        main.core().begin_transaction();
        struct CoreRollback {
            lattice_db& owner;
            ~CoreRollback() { try { if (owner.db().is_in_transaction()) owner.rollback(); } catch (...) {} }
        } core_cleanup{main.core()};
        insert(main, 1, "core-pending", {1, 0, 0, 0}, "pending-core");
        auto own = main.ref->exact_nearest_rows(request());
        ASSERT_EQ(own.status_code(), success) << own.error_message();
        ASSERT_EQ(own.row_count(), 1);
        auto row = object(own, 0); ASSERT_TRUE(row);
        EXPECT_EQ(std::get<std::string>(row->query_row_value("payload")), "pending-core");
        auto foreign = std::async(std::launch::async, [&] {
            return main.ref->exact_nearest_rows(request());
        });
        auto refused = foreign.get();
        EXPECT_EQ(refused.status_code(), resource_busy);
        EXPECT_EQ(refused.row_count(), 0);
        EXPECT_TRUE(main.db().is_in_transaction());
        main.core().rollback();
        EXPECT_EQ(main.ref->exact_nearest_rows(request()).row_count(), 0);
        main.db().begin_transaction(); // raw database BEGIN has no Core owner
        struct RawRollback {
            database& db;
            ~RawRollback() { try { if (db.is_in_transaction()) db.rollback(); } catch (...) {} }
        } raw_cleanup{main.db()};
        insert(main, 2, "raw-pending", {1, 0, 0, 0}, "pending-raw");
        auto raw = main.ref->exact_nearest_rows(request());
        EXPECT_EQ(raw.status_code(), resource_busy);
        EXPECT_EQ(raw.row_count(), 0);
        EXPECT_TRUE(main.db().is_in_transaction());
        main.db().rollback();
        auto after = main.ref->exact_nearest_rows(request());
        EXPECT_EQ(after.status_code(), success); EXPECT_EQ(after.row_count(), 0);
        expect_released(main);
    });
}

TEST(ExactVectorOwnedRows, MissingTypedProvenanceAndUntrackedRawTopologyFailClosed) {
    Store main("authentication_main"), arm("authentication_arm");
    insert(arm, 1, "arm", {1, 0, 0, 0}, "arm");
    ASSERT_TRUE(main.core().attach(arm.core()));
    for (int mode : {0, 1, 2}) {
        auto failed = exact_vector_owned_test_access::malformed_registration(*main.ref, request(), mode);
        EXPECT_NE(failed.status_code(), success) << "bad prerequisite " << mode;
        expect_released(main);
        EXPECT_EQ(main.ref->exact_nearest_rows(request()).status_code(), success);
    }
    main.db().execute("ATTACH DATABASE ':memory:' AS untracked_exact");
    EXPECT_NE(main.ref->exact_nearest_rows(request()).status_code(), success);
    main.db().execute("DETACH DATABASE untracked_exact");
    ASSERT_TRUE(main.core().detach(arm.core()));
    EXPECT_EQ(main.ref->exact_nearest_rows(request()).status_code(), success);
}

TEST(ExactVectorOwnedRows, IncompatibleParticipatingPhysicalColumnsAreNotSilentlyOmitted) {
    Store main("columns_main"), arm("columns_arm");
    insert(main, 1, "main", {1, 0, 0, 0}, "main");
    insert(arm, 1, "arm", {2, 0, 0, 0}, "arm");
    ASSERT_TRUE(main.core().attach(arm.core()));
    main.db().execute("ALTER TABLE " + quote(alias(arm.path)) + ".ExactOwnedDoc ADD COLUMN unregistered_extra TEXT");
    auto failed = main.ref->exact_nearest_rows(request(1));
    EXPECT_EQ(failed.status_code(), invalid_schema);
    expect_released(main);
    ASSERT_TRUE(main.core().detach(arm.core()));
}

TEST(ExactVectorOwnedRows, DifferentFileRawRebindCannotBorrowTheOldTypedIdentity) {
    Store main("raw_rebind_main"), arm("raw_rebind_arm"), replacement("raw_replacement");
    insert(arm, 1, "same", {1, 0, 0, 0}, "original");
    insert(replacement, 1, "same", {0, 0, 0, 0}, "wrong-replacement");
    ASSERT_TRUE(main.core().attach(arm.core()));
    const auto source = quote(alias(arm.path));
    main.db().execute("DETACH DATABASE " + source);
    main.db().execute("ATTACH DATABASE ? AS " + source, {replacement.path.str()});
    EXPECT_NE(main.ref->exact_nearest_rows(request()).status_code(), success);
    expect_released(main);
    // Restore the fixture's physical topology before typed teardown. This is
    // a different-file mismatch oracle, not support for raw same-file rebind.
    main.db().execute("DETACH DATABASE " + source);
    main.db().execute("ATTACH DATABASE ? AS " + source, {arm.path.str()});
    ASSERT_TRUE(main.core().detach(arm.core()));
}

TEST(ExactVectorOwnedRows, HydrationRejectsForeignLeaseAndAlteredPhysicalProvenance) {
    Store main("hydrate_main"), arm("hydrate_arm"), foreign("hydrate_foreign");
    insert(arm, 1, "one", {1, 0, 0, 0}, "one");
    ASSERT_TRUE(main.core().attach(arm.core()));
    auto lease = exact_vector_owned_test_access::acquire(main.core());
    std::vector<detail::exact_vector_row_arm> arms;
    for (const auto& source : lease->arms()) arms.push_back({source.schema, source.attachment_token, {}});
    auto rows = detail::select_exact_vector_rows(lease->connection(), table, "embedding", columns,
                                                arms, {0, 0, 0, 0}, 1, Metric::l2);
    ASSERT_EQ(rows.size(), 1u);
    auto row = rows[0].row;
    EXPECT_THROW(exact_vector_owned_test_access::hydrate(foreign.core(), row, *lease), db_error);
    row["_lattice_attach_token"] = int64_t(0);
    EXPECT_THROW(exact_vector_owned_test_access::hydrate(main.core(), row, *lease), db_error);
    row = rows[0].row;
    row.erase("_source");
    EXPECT_THROW(exact_vector_owned_test_access::hydrate(main.core(), row, *lease), db_error);
    row = rows[0].row;
    row["id"] = int64_t(0);
    EXPECT_THROW(exact_vector_owned_test_access::hydrate(main.core(), row, *lease), db_error);
    row["id"] = std::string("1");
    EXPECT_THROW(exact_vector_owned_test_access::hydrate(main.core(), row, *lease), db_error);
    row.erase("id");
    EXPECT_THROW(exact_vector_owned_test_access::hydrate(main.core(), row, *lease), db_error);
    lease.reset();
    expect_released(main);
    ASSERT_TRUE(main.core().detach(arm.core()));
}

TEST(ExactVectorOwnedRows, CapturedLeaseCoversSelectionAndHydrationButRowsDoNotPinRetiredWriter) {
    bounded([] {
        Store main("lease_main"), arm("lease_arm");
        insert(arm, 1, "arm", {1, 0, 0, 0}, "original");
        ASSERT_TRUE(main.core().attach(arm.core()));
        auto lease = exact_vector_owned_test_access::acquire(main.core());
        ASSERT_TRUE(lease);
        ASSERT_EQ(exact_vector_owned_test_access::active(main.core()), 1u);
        EXPECT_THROW(main.core().reopen_write_db(), db_error);
        EXPECT_THROW(main.core().close_write_db(), db_error);
        std::vector<detail::exact_vector_row_arm> arms;
        for (const auto& source : lease->arms()) arms.push_back({source.schema, source.attachment_token, {}});
        auto selected = detail::select_exact_vector_rows(lease->connection(), table, "embedding", columns,
                                                        arms, {0, 0, 0, 0}, 1, Metric::l2);
        ASSERT_EQ(selected.size(), 1u);
        // Selection has returned, but hydration still uses the same owned lease.
        EXPECT_THROW(main.core().reopen_write_db(), db_error);
        auto hydrated = exact_vector_owned_test_access::hydrate(main.core(), selected[0].row, *lease);
        auto field = hydrated.get_managed_field<std::string>("payload");
        EXPECT_EQ(field.db, lease->writer().get());
        EXPECT_EQ(field.attachment_token, std::get<int64_t>(selected[0].row.at("_lattice_attach_token")));
        auto old_writer = field.attachment_writer;
        EXPECT_THROW(main.core().close_write_db(), db_error);
        lease.reset();
        expect_released(main);
        main.core().reopen_write_db();
        EXPECT_TRUE(old_writer.expired());
        EXPECT_THROW(field.detach(), db_error);
        auto fresh = main.ref->exact_nearest_rows(request());
        EXPECT_EQ(fresh.status_code(), success);
        ASSERT_TRUE(main.core().detach(arm.core()));
    });
}

TEST(ExactVectorOwnedRows, SameFileReattachDoesNotReviveReturnedWinner) {
    Store main("stale_main"), arm("stale_arm");
    insert(arm, 1, "same", {1, 0, 0, 0}, "before");
    ASSERT_TRUE(main.core().attach(arm.core()));
    auto result = main.ref->exact_nearest_rows(request());
    ASSERT_EQ(result.status_code(), success);
    auto held = object(result, 0); ASSERT_TRUE(held);
    auto field = managed_attachment_test_access::field<std::string>(*held, "payload");
    const auto original_token = field.attachment_token;
    ASSERT_TRUE(main.core().detach(arm.core()));
    arm.db().execute("UPDATE ExactOwnedDoc SET payload='after' WHERE id=1");
    ASSERT_TRUE(main.core().attach(arm.core()));
    EXPECT_THROW(field.detach(), db_error);
    EXPECT_THROW(field = std::string("stale-write"), db_error);
    EXPECT_EQ(std::get<std::string>(held->query_row_value("payload")), "before");
    auto current = main.ref->exact_nearest_rows(request());
    ASSERT_EQ(current.status_code(), success);
    auto fresh = object(current, 0); ASSERT_TRUE(fresh);
    EXPECT_EQ(fresh->get_string("payload"), "after");
    EXPECT_NE(managed_attachment_test_access::field<std::string>(*fresh, "payload").attachment_token, original_token);
    ASSERT_TRUE(main.core().detach(arm.core()));
}

TEST(ExactVectorOwnedRows, DetachAfterAdmissionCannotRedirectSelectionOrReviveItsHandle) {
    bounded([] {
        Store main("detach_main"), arm("detach_arm");
        insert(arm, 1, "one", {1, 0, 0, 0}, "admitted-original");
        ASSERT_TRUE(main.core().attach(arm.core()));
        auto initial = main.ref->exact_nearest_rows(request());
        ASSERT_EQ(initial.status_code(), success);
        auto original = object(initial, 0); ASSERT_TRUE(original);
        const auto token = managed_attachment_test_access::field<std::string>(*original, "payload").attachment_token;
        struct State {
            Store* main; Store* arm; int64_t token;
            std::thread detach;
            bool entered = false, invalidated = false, detached = false, failed = false;
        } state{&main, &arm, token};
        auto* raw = main.db().handle();
        struct Reset {
            sqlite3* raw; State& state;
            ~Reset() {
                if (state.detach.joinable()) state.detach.join();
                sqlite3_create_function_v2(raw, "exact_owned_detach", 1, SQLITE_UTF8,
                                           nullptr, nullptr, nullptr, nullptr, nullptr);
            }
        } reset{raw, state};
        ASSERT_EQ(sqlite3_create_function_v2(raw, "exact_owned_detach", 1, SQLITE_UTF8, &state,
            [](sqlite3_context* context, int, sqlite3_value**) noexcept {
                auto& s = *static_cast<State*>(sqlite3_user_data(context));
                if (!s.entered) {
                    s.entered = true;
                    try {
                        s.detach = std::thread([&s] { s.detached = s.main->core().detach(s.arm->core()); });
                        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
                        while (managed_attachment_test_access::live(s.main->core(), s.token) &&
                               std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
                        s.invalidated = !managed_attachment_test_access::live(s.main->core(), s.token);
                    } catch (...) { s.failed = true; }
                }
                sqlite3_result_int(context, 1);
            }, nullptr, nullptr, nullptr), SQLITE_OK);
        auto input = request(); input.set_predicate("exact_owned_detach(m.eligible) = 1");
        auto result = main.ref->exact_nearest_rows(input);
        if (state.detach.joinable()) state.detach.join();
        EXPECT_TRUE(state.entered && state.invalidated && state.detached);
        EXPECT_FALSE(state.failed);
        ASSERT_EQ(result.status_code(), success) << result.error_message();
        ASSERT_EQ(result.row_count(), 1);
        auto held = object(result, 0); ASSERT_TRUE(held);
        EXPECT_EQ(std::get<std::string>(held->query_row_value("payload")), "admitted-original");
        EXPECT_THROW(held->get()->get_string("payload"), db_error);
        EXPECT_THROW(held->get()->set_string("payload", "stale-write"), db_error);
        ASSERT_TRUE(main.core().attach(arm.core()));
        EXPECT_THROW(held->get()->get_string("payload"), db_error);
        expect_released(main);
        ASSERT_TRUE(main.core().detach(arm.core()));
    });
}

TEST(ExactVectorOwnedRows, WriterContentionWaitsButTopologyContentionFailsBusy) {
    bounded([] {
        Store main("contention");
        insert(main, 1, "one", {1, 0, 0, 0}, "one");
        auto* mutex = sqlite3_db_mutex(main.db().handle());
        std::promise<void> started;
        auto entered = started.get_future();
        sqlite3_mutex_enter(mutex);
        auto pending = std::async(std::launch::async, [&] {
            started.set_value(); return main.ref->exact_nearest_rows(request());
        });
        entered.wait();
        const auto while_held = pending.wait_for(std::chrono::milliseconds(100));
        sqlite3_mutex_leave(mutex);
        EXPECT_EQ(while_held, std::future_status::timeout); // bounded scheduling check, not a formal scheduler proof
        EXPECT_EQ(pending.get().status_code(), success);
        auto topology = exact_vector_owned_test_access::topology(main.core());
        auto busy = std::async(std::launch::async, [&] { return main.ref->exact_nearest_rows(request()); });
        const auto ready = busy.wait_for(std::chrono::seconds(2));
        topology.unlock(); // release even if a broken blocking implementation waited
        EXPECT_EQ(ready, std::future_status::ready);
        EXPECT_EQ(busy.get().status_code(), resource_busy);
        expect_released(main);
    });
}

TEST(ExactVectorOwnedRows, ScalarFunctionAllowsNestedScalarButSealsTopologyMaintenanceAndVectorReentry) {
    bounded([] {
        Store main("callback_main"), arm("callback_arm");
        insert(main, 1, "main", {1, 0, 0, 0}, "main");
        ASSERT_TRUE(main.core().attach(arm.core()));
        auto initial = main.ref->exact_nearest_rows(request());
        ASSERT_EQ(initial.status_code(), success);
        auto row = object(initial, 0); ASSERT_TRUE(row);
        auto vector = managed_attachment_test_access::field<std::vector<uint8_t>>(*row, "embedding");
        vector.is_vector_column = true;
        struct State {
            Store* main; Store* arm; dynamic_object_ref* row; managed<std::vector<uint8_t>>* vector;
            bool entered = false, scalar = false, topology = false, reopen = false, mutation = false, escaped = false;
        } state{&main, &arm, row.get(), &vector};
        auto* raw = main.db().handle();
        struct Reset {
            sqlite3* raw;
            ~Reset() { sqlite3_create_function_v2(raw, "exact_owned_reentry", 1, SQLITE_UTF8,
                                                nullptr, nullptr, nullptr, nullptr, nullptr); }
        } reset{raw};
        ASSERT_EQ(sqlite3_create_function_v2(raw, "exact_owned_reentry", 1, SQLITE_UTF8, &state,
            [](sqlite3_context* context, int, sqlite3_value**) noexcept {
                auto& s = *static_cast<State*>(sqlite3_user_data(context));
                if (!s.entered) {
                    s.entered = true;
                    try {
                        s.scalar = s.row->get()->get_string("payload") == "main";
                        s.topology = !s.main->core().detach(s.arm->core());
                        try { s.main->core().reopen_write_db(); } catch (const db_error&) { s.reopen = true; }
                        try { s.vector->set_value(pack_floats({9, 0, 0, 0})); } catch (const db_error&) { s.mutation = true; }
                    } catch (...) { s.escaped = true; }
                }
                sqlite3_result_int(context, 1); // no C++ exception crosses SQLite's callback frame
            }, nullptr, nullptr, nullptr), SQLITE_OK);
        auto input = request(); input.set_predicate("exact_owned_reentry(m.eligible) = 1");
        auto result = main.ref->exact_nearest_rows(input);
        EXPECT_EQ(result.status_code(), success) << result.error_message();
        EXPECT_TRUE(state.entered && state.scalar && state.topology && state.reopen && state.mutation);
        EXPECT_FALSE(state.escaped);
        last_bridge_error().clear();
        expect_released(main);
        ASSERT_TRUE(main.core().detach(arm.core()));
    });
}

TEST(ExactVectorOwnedRows, PrepareAndStepFailuresReleaseStatementsAndDoNotBecomeEmptySuccess) {
    Store main("sql_failure");
    insert(main, 1, "one", {1, 0, 0, 0}, "one");
    auto* raw = main.db().handle();
    const auto before = statements(raw);
    struct Reset { sqlite3* raw; ~Reset() { sqlite3_set_authorizer(raw, nullptr, nullptr); } } reset{raw};
    ASSERT_EQ(sqlite3_set_authorizer(raw,
        [](void*, int action, const char* name, const char*, const char*, const char*) noexcept {
            return action == SQLITE_READ && name && std::strcmp(name, "ExactOwnedDoc") == 0 ? SQLITE_DENY : SQLITE_OK;
        }, nullptr), SQLITE_OK);
    auto denied = main.ref->exact_nearest_rows(request());
    ASSERT_EQ(sqlite3_set_authorizer(raw, nullptr, nullptr), SQLITE_OK);
    EXPECT_EQ(denied.status_code(), database_failure);
    EXPECT_EQ(statements(raw), before);
    expect_released(main);
    ASSERT_EQ(sqlite3_create_function_v2(raw, "exact_owned_fail", 1, SQLITE_UTF8, nullptr,
        [](sqlite3_context* context, int, sqlite3_value**) noexcept {
            sqlite3_result_error(context, "owned exact test step failure", -1);
        }, nullptr, nullptr, nullptr), SQLITE_OK);
    auto input = request(); input.set_predicate("exact_owned_fail(m.eligible) = 1");
    auto failed = main.ref->exact_nearest_rows(input);
    EXPECT_EQ(failed.status_code(), database_failure);
    EXPECT_EQ(statements(raw), before);
    expect_released(main);
    EXPECT_EQ(main.ref->exact_nearest_rows(request()).status_code(), success);
}

TEST(ExactVectorOwnedRows, ReadLeaseDoesNotDrainPreexistingDirtyNotifications) {
    auto ref = open(":memory:");
    auto& owner = *ref->get();
    auto& db = owner.db();
    int calls = 0;
    struct Clear { database& db; ~Clear() { db.set_txn_hooks({}, {}); } } clear{db};
    db.set_txn_hooks([&] { ++calls; }, [] {});
    db.mark_txn_dirty();
    auto result = ref->exact_nearest_rows(request());
    EXPECT_EQ(result.status_code(), success) << result.error_message();
    EXPECT_EQ(calls, 0) << "a read must not deliver notifications from earlier work";
    EXPECT_EQ(exact_vector_owned_test_access::active(owner), 0u);
    EXPECT_FALSE(detail::managed_route_scope::active_for(&db));
    db.query("SELECT 1 AS n");
    EXPECT_EQ(calls, 1) << "dirty work was deferred, not erased";
}
