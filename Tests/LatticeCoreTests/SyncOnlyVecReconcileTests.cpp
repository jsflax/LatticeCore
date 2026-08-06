#include "TestHelpers.hpp"
#ifndef __linux__
#include <lattice.hpp>          // swift_lattice, swift_lattice_ref
#include <dynamic_object.hpp>

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
    lattice::SchemaVector schemas = {recon_schema("ReconDoc")};

    lattice::swift_configuration cfg(path.str());
    auto* ref = lattice::swift_lattice_ref::create(cfg, schemas);
    auto* l = ref->get();
    raw_hydrate(*l, "ReconDoc", "r1", {1.0f, 0.0f, 0.0f, 0.0f});
    raw_hydrate(*l, "ReconDoc", "r2", {0.0f, 1.0f, 0.0f, 0.0f});
    ASSERT_FALSE(l->db().table_exists("_ReconDoc_embedding_vec"))
        << "fixture failed: raw INSERT must not create the vec0 index";

    l->reconcile_vec0_gaps_for("ReconDoc", "embedding");

    EXPECT_TRUE(l->db().table_exists("_ReconDoc_embedding_vec"))
        << "reconcile must CREATE the missing index for a sync-hydrated DB";
    EXPECT_EQ(vec_index_count(*l, "ReconDoc"), 2)
        << "reconcile must backfill every embedded row";

    delete ref;
}

TEST(SyncOnlyVecReconcile, VacuumVec0BuildsTheIndexFromScratch) {
    TempDB path{"vec_recon_vacuum"};
    lattice::SchemaVector schemas = {recon_schema("ReconDoc")};

    lattice::swift_configuration cfg(path.str());
    auto* ref = lattice::swift_lattice_ref::create(cfg, schemas);
    auto* l = ref->get();
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

    delete ref;
}

#endif  // __linux__
