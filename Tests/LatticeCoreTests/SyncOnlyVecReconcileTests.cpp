#include "TestHelpers.hpp"
#ifndef __linux__
#include <lattice.hpp>          // swift_lattice, swift_lattice_ref
#include <dynamic_object.hpp>
#include "Vec0MaintenanceTestSupport.hpp"

// ============================================================================
// Sync-only databases must become vector-searchable.
//
// Lifecycle under test: a database is CREATED empty (open's vec0 ensure has
// no rows to infer dimensions from, so no vec0 table is made), then hydrated
// by sync-apply — raw INSERTs that never touch the lazy vec0 create path and
// never fire vec0 triggers (none exist). Result: rows with embeddings and NO
// index. Two heal paths must both handle that state:
//
//   1. reconcile (dispatched on every open, including the fingerprint fast
//      path) must CREATE the index from a sampled row's dimensions and then
//      backfill — not bail at `table_exists`.
//   2. vacuum_vec0 (the explicit maintenance op) must likewise build from
//      scratch — its dimension probe must survive the missing `_info` table.
//
// Red on the pre-fix code: reconcile returned at the missing-table guard and
// vacuum_vec0's `_info` query threw straight to its catch (-1), so the DB
// stayed semantically unsearchable forever.
// ============================================================================

namespace {

lattice::swift_schema_entry recon_schema(const std::string& table) {
    lattice::swift_schema_entry entry;
    entry.table_name = table;

    lattice::property_descriptor label;
    label.name = "label";
    label.type = lattice::column_type::text;
    label.kind = lattice::property_kind::primitive;
    entry.properties["label"] = label;

    lattice::property_descriptor embedding;
    embedding.name = "embedding";
    embedding.type = lattice::column_type::blob;
    embedding.kind = lattice::property_kind::primitive;
    embedding.is_vector = true;
    entry.properties["embedding"] = embedding;

    return entry;
}

// Establish the real Swift vector schema while EMPTY, then let the seed's
// owned reconcile future drain during destruction before hydrating anything.
// The existing one-argument constructor reopens the persisted schema without
// dispatching another Swift-schema reconcile worker. These tests exercise the
// exact public maintenance operations, independently of dispatch scheduling.
std::unique_ptr<lattice::swift_lattice> quiescent_recon_fixture(const TempDB& path) {
    lattice::swift_configuration cfg(path.str());
    const lattice::SchemaVector schemas = {recon_schema("ReconDoc")};
    {
        lattice::swift_lattice seed(cfg, schemas);
    }
    return std::make_unique<lattice::swift_lattice>(
        lattice::swift_configuration(path.str()));
}

// Sync-apply shape: plain SQL INSERT — no add(), no vec0 artifacts involved.
void raw_hydrate(lattice::swift_lattice& db, const std::string& table,
                 const std::string& label, const std::vector<float>& vec) {
    db.db().execute(
        "INSERT INTO " + table + " (label, embedding) VALUES (?, ?)",
        {label, pack_floats(vec)});
}

int64_t vec_index_count(lattice::swift_lattice& db, const std::string& table) {
    if (!db.db().table_exists("_" + table + "_embedding_vec_rowids")) return -1;
    auto rows = db.db().query(
        "SELECT COUNT(*) AS c FROM _" + table + "_embedding_vec_rowids");
    return rows.empty() ? -1 : std::get<int64_t>(rows[0].at("c"));
}

void expect_index_identities_match_model(lattice::swift_lattice& db) {
    const auto model_rows = db.db().query("SELECT globalId FROM main.ReconDoc ORDER BY globalId");
    const auto index_rows = db.db().query(
        "SELECT id AS globalId FROM _ReconDoc_embedding_vec_rowids ORDER BY id");
    std::vector<std::string> model_ids, index_ids;
    for (const auto& row : model_rows) model_ids.push_back(std::get<std::string>(row.at("globalId")));
    for (const auto& row : index_rows) index_ids.push_back(std::get<std::string>(row.at("globalId")));
    EXPECT_EQ(index_ids, model_ids) << "vec0 must contain exactly the model global IDs";
}

} // namespace

// NOTE: this calls reconcile_vec0_gaps_for directly rather than reopening
// and relying on the open-path dispatch. In THIS test binary the fingerprint
// fast path never engages: TestHelpers' LATTICE_SCHEMA globals make every
// open re-run the DDL pass, whose Phase 6b would heal the index first and
// mask the reconcile guard under test. Production Swift apps have no C++
// global schemas, take the fast path, and reach reconcile — the path proven
// here.
TEST(SyncOnlyVecReconcile, ReconcileBuildsTheMissingIndex) {
    TempDB path{"vec_recon_direct"};
    auto l = quiescent_recon_fixture(path);
    raw_hydrate(*l, "ReconDoc", "r1", {1.0f, 0.0f, 0.0f, 0.0f});
    raw_hydrate(*l, "ReconDoc", "r2", {0.0f, 1.0f, 0.0f, 0.0f});
    ASSERT_FALSE(l->db().table_exists("_ReconDoc_embedding_vec"))
        << "fixture failed: raw INSERT must not create the vec0 index";

    l->reconcile_vec0_gaps_for("ReconDoc", "embedding");

    EXPECT_TRUE(l->db().table_exists("_ReconDoc_embedding_vec"))
        << "reconcile must CREATE the missing index for a sync-hydrated DB";
    EXPECT_EQ(vec_index_count(*l, "ReconDoc"), 2)
        << "reconcile must backfill every embedded row";

}

TEST(SyncOnlyVecReconcile, VacuumVec0BuildsTheIndexFromScratch) {
    TempDB path{"vec_recon_vacuum"};
    auto l = quiescent_recon_fixture(path);
    raw_hydrate(*l, "ReconDoc", "v1", {1.0f, 0.0f, 0.0f, 0.0f});
    raw_hydrate(*l, "ReconDoc", "v2", {0.0f, 1.0f, 0.0f, 0.0f});
    ASSERT_FALSE(l->db().table_exists("_ReconDoc_embedding_vec"));

    EXPECT_EQ(l->vacuum_vec0("ReconDoc", "embedding"), 2)
        << "vacuum_vec0 must build the index from scratch, not fail (-1)";
    EXPECT_EQ(vec_index_count(*l, "ReconDoc"), 2);

    // One heal makes the DB self-maintaining: ensure_vec0_table installed the
    // triggers, so subsequent sync-applied rows index themselves.
    raw_hydrate(*l, "ReconDoc", "v3", {0.0f, 0.0f, 1.0f, 0.0f});
    EXPECT_EQ(vec_index_count(*l, "ReconDoc"), 3)
        << "vec0 triggers must index rows applied after the heal";

}

namespace {

void expect_serialized_overlap(bool vacuum_first) {
    using namespace vec0_maintenance_test;
    TempDB path{vacuum_first ? "vec_vacuum_before_reconcile" : "vec_reconcile_before_vacuum"};
    auto l = quiescent_recon_fixture(path);
    raw_hydrate(*l, "ReconDoc", "one", {1.0f, 0.0f, 0.0f, 0.0f});
    raw_hydrate(*l, "ReconDoc", "two", {0.0f, 1.0f, 0.0f, 0.0f});
    ASSERT_FALSE(l->db().table_exists("_ReconDoc_embedding_vec"));

    auto* writer_mutex = sqlite3_db_mutex(l->db().handle());
    ASSERT_NE(writer_mutex, nullptr);
    const std::string incumbent = vacuum_first ? "vacuum" : "reconcile";
    const std::string contender = vacuum_first ? "reconcile" : "vacuum";
    const std::string hold_phase = vacuum_first ? "after-drop" : "before-fill";
    event_signal held, attempted, release;
    std::atomic<bool> held_once{false};
    std::atomic<bool> attempted_once{false};
    std::atomic<bool> released{false};
    std::atomic<bool> hold_timed_out{false};
    std::atomic<bool> acquired_before_release{false};
    std::atomic<int> contender_acquisitions{0};
    std::atomic<int> admission_mutex_result{-1};
    std::atomic<int64_t> vacuum_result{-2};

    lattice::vec0_maintenance_test_access::set(*l,
        [&](const char* operation, const char* phase) {
            if (incumbent == operation && hold_phase == phase && !held_once.exchange(true)) {
                held.send();
                if (!release.wait(3 * wait_budget)) hold_timed_out.store(true);
            }
            if (contender == operation && std::string(phase) == "attempt" &&
                !attempted_once.exchange(true)) {
                // This is the actual maintenance admission path, not a signal
                // before the public call. BUSY positively proves the incumbent
                // still owns SQLite across its multi-statement critical window.
                admission_mutex_result.store(try_writer_mutex(writer_mutex));
                attempted.send();
            }
            if (contender == operation && std::string(phase) == "acquired") {
                contender_acquisitions.fetch_add(1);
                if (!released.load()) acquired_before_release.store(true);
            }
        });

    auto run = [&](const std::string& operation) {
        if (operation == "vacuum") {
            vacuum_result.store(l->vacuum_vec0("ReconDoc", "embedding"));
        } else {
            l->reconcile_vec0_gaps_for("ReconDoc", "embedding");
        }
    };
    joined_task<void> first([&] { run(incumbent); });
    std::unique_ptr<joined_task<void>> second;
    release_on_exit cleanup{release};
    const bool incumbent_paused = held.wait();
    EXPECT_TRUE(incumbent_paused) << "incumbent did not reach the selected critical window";
    if (incumbent_paused) {
        second = std::make_unique<joined_task<void>>([&] { run(contender); });
        EXPECT_TRUE(attempted.wait()) << "contender did not reach maintenance admission";
        EXPECT_EQ(admission_mutex_result.load(), SQLITE_BUSY);
        EXPECT_EQ(contender_acquisitions.load(), 0);
    }

    // No fatal assertion or early return can strand the holder. Release even
    // after a failed expectation, then unconditionally join every started task.
    released.store(true);
    release.send();
    EXPECT_TRUE(first.ready()) << "incumbent did not complete after release";
    if (second) EXPECT_TRUE(second->ready()) << "contender did not complete after release";
    first.join();
    if (second) second->join();
    lattice::vec0_maintenance_test_access::set(*l, {});
    first.get();
    if (second) second->get();

    EXPECT_FALSE(hold_timed_out.load());
    EXPECT_FALSE(acquired_before_release.load());
    EXPECT_EQ(contender_acquisitions.load(), 1);
    EXPECT_EQ(vacuum_result.load(), 2);
    EXPECT_TRUE(l->db().table_exists("_ReconDoc_embedding_vec"));
    EXPECT_EQ(vec_index_count(*l, "ReconDoc"), 2);
    expect_index_identities_match_model(*l);
    raw_hydrate(*l, "ReconDoc", "three", {0.0f, 0.0f, 1.0f, 0.0f});
    EXPECT_EQ(vec_index_count(*l, "ReconDoc"), 3)
        << "triggers must remain installed after both overlapping maintenance operations";
    expect_index_identities_match_model(*l);
}

} // namespace

TEST(SyncOnlyVecReconcile, VacuumWaitsForReconcileFullBackfill) {
    expect_serialized_overlap(false);
}

TEST(SyncOnlyVecReconcile, ReconcileWaitsForVacuumFullRebuild) {
    expect_serialized_overlap(true);
}

TEST(SyncOnlyVecReconcile, FileObserverReentryRejectsWithoutSqlAndRecovers) {
    using namespace vec0_maintenance_test;
    TempDB path{"vec_reconcile_observer_reentry"};
    auto l = quiescent_recon_fixture(path);
    raw_hydrate(*l, "ReconDoc", "one", {1.0f, 0.0f, 0.0f, 0.0f});
    raw_hydrate(*l, "ReconDoc", "two", {0.0f, 1.0f, 0.0f, 0.0f});
    ASSERT_FALSE(l->db().table_exists("_ReconDoc_embedding_vec"));

    int callbacks = 0;
    int64_t nested_result = -2;
    uint64_t nested_statement_count = 1;
    std::thread::id callback_thread;
    std::string callback_error;
    auto& core = static_cast<lattice::lattice_db&>(*l);
    const auto observer = core.add_table_observer("_ReconDoc_embedding_vec_rowids",
        [&](const std::vector<lattice::lattice_db::change_event>&) {
            if (++callbacks != 1) return;
            callback_thread = std::this_thread::get_id();
            const auto before = lattice::database::thread_statement_count();
            try {
                nested_result = l->vacuum_vec0("ReconDoc", "embedding");
            } catch (const std::exception& e) {
                callback_error = e.what();
            }
            nested_statement_count = lattice::database::thread_statement_count() - before;
        });

    joined_task<void> worker([&] {
        l->reconcile_vec0_gaps_for("ReconDoc", "embedding");
    });
    const auto worker_id = worker.id();
    EXPECT_TRUE(worker.ready()) << "same-worker observer maintenance reentry deadlocked";
    worker.join();
    worker.get();
    core.remove_table_observer("_ReconDoc_embedding_vec_rowids", observer);

    EXPECT_GE(callbacks, 1) << "the real vec0 shadow-table observer must execute";
    EXPECT_EQ(callback_thread, worker_id);
    EXPECT_EQ(callback_error, "");
    EXPECT_EQ(nested_result, -1) << "file callback maintenance must reject recursive destruction";
    EXPECT_EQ(nested_statement_count, 0u) << "rejection must occur before any nested SQL";
    EXPECT_EQ(vec_index_count(*l, "ReconDoc"), 2);
    EXPECT_EQ(l->vacuum_vec0("ReconDoc", "embedding"), 2) << "the outer scope must release";
    raw_hydrate(*l, "ReconDoc", "three", {0.0f, 0.0f, 1.0f, 0.0f});
    EXPECT_EQ(vec_index_count(*l, "ReconDoc"), 3);
}

#endif  // __linux__
