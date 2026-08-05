#include "TestHelpers.hpp"
#ifndef __linux__
#include <lattice.hpp>          // swift_lattice, swift_lattice_ref
#include <dynamic_object.hpp>

// ============================================================================
// Attach × combined_nearest_query — cross-arm candidate identity.
//
// The multi-arm KNN/FTS candidate CTEs must key candidates by globalId, not
// per-database rowid: every attached arm's rowids start at 1, so an id-keyed
// candidate set lets one arm's candidate "match" a DIFFERENT memory in every
// other arm — unrelated rows surface carrying a borrowed distance, and
// distinct-by-globalId cannot suppress them (their globalIds differ).
//
// Fixture shape (deliberate rowid asymmetry so id-keying and gid-keying
// produce DIFFERENT result sets, not just different orderings):
//   arm A (main):    a1(id=1) a2(id=2) a3(id=3) — embeddings near the query
//   arm B (attached): b1(id=2 ONLY — dummy row inserted first then deleted)
//                     — embedding orthogonal/far
// k=1 ⇒ candidates are a1 (A's top) and b1 (B's top). Under id-keying that
// set is {1, 2}, and id 2 ALSO matches a2 in arm A — a2 was never a
// candidate, but surfaces with b1's distance attached.
// ============================================================================

namespace {

lattice::swift_schema_entry cross_arm_schema(const std::string& table) {
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

void insert_doc(lattice::swift_lattice& db, const lattice::SwiftSchema& props,
                const std::string& table, const std::string& label,
                const std::vector<float>& vec) {
    lattice::swift_dynamic_object sdo;
    sdo.table_name = table;
    sdo.properties = props;
    sdo.values["label"] = label;
    sdo.values["embedding"] = pack_floats(vec);
    { lattice::dynamic_object obj(sdo); db.add(obj); }
    auto rows = db.db().query(
        "SELECT globalId FROM " + table + " WHERE label = ?", {label});
    ASSERT_EQ(rows.size(), 1u) << "insert failed for " << label;
    auto gid = std::get<std::string>(rows[0].at("globalId"));
    db.upsert_vec0(table, "embedding", gid, pack_floats(vec));
}

int64_t rowid_of(lattice::swift_lattice& db, const std::string& table,
                 const std::string& label) {
    auto rows = db.db().query(
        "SELECT id FROM main." + table + " WHERE label = ?", {label});
    return rows.empty() ? -1 : std::get<int64_t>(rows[0].at("id"));
}

struct CrossArmFixture {
    lattice::swift_lattice_ref* a_ref;
    lattice::swift_lattice_ref* b_ref;
    lattice::swift_lattice* a;
    lattice::swift_lattice* b;
    lattice::SchemaVector schemas;

    CrossArmFixture(TempDB& a_path, TempDB& b_path, const std::string& table) {
        schemas = {cross_arm_schema(table)};
        lattice::swift_configuration a_cfg(a_path.str());
        lattice::swift_configuration b_cfg(b_path.str());
        a_ref = lattice::swift_lattice_ref::create(a_cfg, schemas);
        b_ref = lattice::swift_lattice_ref::create(b_cfg, schemas);
        a = a_ref->get();
        b = b_ref->get();

        // Arm A: three rows near the query vector, rowids 1..3.
        insert_doc(*a, schemas[0].properties, table, "a1", {1.0f, 0.0f, 0.0f, 0.0f});
        insert_doc(*a, schemas[0].properties, table, "a2", {0.9f, 0.1f, 0.0f, 0.0f});
        insert_doc(*a, schemas[0].properties, table, "a3", {0.8f, 0.2f, 0.0f, 0.0f});

        // Arm B: ONE far row, forced to rowid 2 (dummy at rowid 1, deleted).
        insert_doc(*b, schemas[0].properties, table, "b_dummy", {0.0f, 0.0f, 0.0f, 1.0f});
        insert_doc(*b, schemas[0].properties, table, "b1", {0.0f, 1.0f, 0.0f, 0.0f});
        auto dummy_rows = b->db().query(
            "SELECT globalId FROM " + table + " WHERE label = 'b_dummy'");
        EXPECT_EQ(dummy_rows.size(), 1u);
        b->db().execute("DELETE FROM " + table + " WHERE label = 'b_dummy'");
        b->db().execute("DELETE FROM _" + table + "_embedding_vec WHERE global_id = ?",
                        {std::get<std::string>(dummy_rows[0].at("globalId"))});

        EXPECT_EQ(rowid_of(*a, table, "a2"), 2);
        EXPECT_EQ(rowid_of(*b, table, "b1"), 2) << "fixture requires the rowid collision";

        a->attach(*b);
    }
};

std::vector<std::string> result_labels(
    const lattice::CombinedQueryResultVector& results) {
    std::vector<std::string> labels;
    for (const auto& r : results) labels.push_back(r.object.get_string("label"));
    return labels;
}

} // namespace

TEST(AttachKnnCrossArm, KnnDoesNotBorrowDistancesAcrossArms) {
    TempDB a_path{"xarm_knn_a"}, b_path{"xarm_knn_b"};
    CrossArmFixture fx(a_path, b_path, "CrossArmDoc");

    auto qvec = pack_floats({1.0f, 0.0f, 0.0f, 0.0f});
    lattice::VectorConstraintVector vecs = {
        lattice::vector_constraint("embedding", qvec, /*k=*/1, /*metric=*/0)  // L2
    };
    auto results = fx.a->combined_nearest_query(
        "CrossArmDoc", {}, vecs, {}, {}, std::nullopt,
        lattice::sort_descriptor{}, /*limit=*/10);

    auto labels = result_labels(results);

    // Exactly the two true candidates: A's top-1 and B's top-1.
    ASSERT_EQ(results.size(), 2u)
        << "expected exactly {a1, b1}; got: " << ::testing::PrintToString(labels);
    std::set<std::string> label_set(labels.begin(), labels.end());
    EXPECT_TRUE(label_set.count("a1"));
    EXPECT_TRUE(label_set.count("b1"));
    // a2 shares b1's ROWID but was never a candidate — id-keyed candidates
    // resurrect it with b1's distance attached.
    EXPECT_FALSE(label_set.count("a2"))
        << "a2 surfaced via cross-arm rowid collision";

    // Each row carries ITS OWN distance, not a borrowed one.
    for (const auto& r : results) {
        ASSERT_EQ(r.distances.size(), 1u);
        auto label = r.object.get_string("label");
        if (label == "a1") EXPECT_NEAR(r.distances[0].distance, 0.0, 0.01);
        if (label == "b1") EXPECT_GT(r.distances[0].distance, 1.0)
            << "b1 carried a borrowed near distance";
    }
}

TEST(AttachKnnCrossArm, KnnCountMatchesTrueCandidates) {
    TempDB a_path{"xarm_cnt_a"}, b_path{"xarm_cnt_b"};
    CrossArmFixture fx(a_path, b_path, "CrossArmCnt");

    auto qvec = pack_floats({1.0f, 0.0f, 0.0f, 0.0f});
    lattice::VectorConstraintVector vecs = {
        lattice::vector_constraint("embedding", qvec, /*k=*/1, /*metric=*/0)
    };
    auto n = fx.a->combined_nearest_query_count(
        "CrossArmCnt", {}, vecs, {}, {}, std::nullopt,
        lattice::sort_descriptor{}, /*limit=*/10);
    EXPECT_EQ(n, 2) << "count must match the true candidate rows (a1, b1)";
}

TEST(AttachKnnCrossArm, FtsCandidatesRespectArmOfOrigin) {
    TempDB a_path{"xarm_fts_a"}, b_path{"xarm_fts_b"};
    const std::string table = "CrossArmFts";
    lattice::SchemaVector schemas = {cross_arm_schema(table)};
    lattice::swift_configuration a_cfg(a_path.str());
    lattice::swift_configuration b_cfg(b_path.str());
    auto* a_ref = lattice::swift_lattice_ref::create(a_cfg, schemas);
    auto* b_ref = lattice::swift_lattice_ref::create(b_cfg, schemas);
    auto& a = *a_ref->get();
    auto& b = *b_ref->get();

    // FTS sidecars in both arms BEFORE inserts so triggers populate them.
    a.ensure_fts5_table(table, "label");
    b.ensure_fts5_table(table, "label");

    // Arm A rowids 1..2; only "quaternion camera" matches the query text.
    insert_doc(a, schemas[0].properties, table, "quaternion camera", {1.0f, 0.0f, 0.0f, 0.0f});
    insert_doc(a, schemas[0].properties, table, "unrelated alpha", {0.0f, 0.0f, 1.0f, 0.0f});

    // Arm B: "sourdough hydration" at rowid 1 — collides with A's matching row.
    insert_doc(b, schemas[0].properties, table, "sourdough hydration", {0.0f, 1.0f, 0.0f, 0.0f});

    a.attach(b);

    lattice::TextConstraintVector texts = {
        lattice::text_constraint("label", "quaternion", /*limit=*/10)
    };
    auto results = a.combined_nearest_query(
        table, {}, {}, {}, texts, std::nullopt,
        lattice::sort_descriptor{}, /*limit=*/10);

    auto labels = result_labels(results);
    ASSERT_EQ(results.size(), 1u)
        << "only arm A's row matches 'quaternion'; got: "
        << ::testing::PrintToString(labels);
    EXPECT_EQ(labels[0], "quaternion camera");
}

TEST(AttachKnnCrossArm, ReplicaSameGlobalIdInBothArmsStillReturnsBoth) {
    TempDB a_path{"xarm_rep_a"}, b_path{"xarm_rep_b"};
    const std::string table = "CrossArmRep";
    lattice::SchemaVector schemas = {cross_arm_schema(table)};
    lattice::swift_configuration a_cfg(a_path.str());
    lattice::swift_configuration b_cfg(b_path.str());
    auto* a_ref = lattice::swift_lattice_ref::create(a_cfg, schemas);
    auto* b_ref = lattice::swift_lattice_ref::create(b_cfg, schemas);
    auto& a = *a_ref->get();
    auto& b = *b_ref->get();

    insert_doc(a, schemas[0].properties, table, "shared", {1.0f, 0.0f, 0.0f, 0.0f});
    auto gid_rows = a.db().query("SELECT globalId FROM " + table + " WHERE label='shared'");
    ASSERT_EQ(gid_rows.size(), 1u);
    auto gid = std::get<std::string>(gid_rows[0].at("globalId"));

    // The hub/spoke replica case: same memory (same globalId) in the other
    // arm — possibly with drifted content/embedding.
    insert_doc(b, schemas[0].properties, table, "shared-replica", {0.9f, 0.1f, 0.0f, 0.0f});
    b.db().execute("UPDATE " + table + " SET globalId = ? WHERE label='shared-replica'", {gid});
    b.db().execute("DELETE FROM _" + table + "_embedding_vec");
    b.upsert_vec0(table, "embedding", gid, pack_floats({0.9f, 0.1f, 0.0f, 0.0f}));

    a.attach(b);

    auto qvec = pack_floats({1.0f, 0.0f, 0.0f, 0.0f});
    lattice::VectorConstraintVector vecs = {
        lattice::vector_constraint("embedding", qvec, /*k=*/2, /*metric=*/0)
    };
    auto results = a.combined_nearest_query(
        table, {}, vecs, {}, {}, std::nullopt,
        lattice::sort_descriptor{}, /*limit=*/10);

    // gid-keyed candidates legitimately match every replica of the SAME
    // memory — both copies return (Swift's distinct-by-globalId collapses
    // them downstream). The fix must not over-suppress this case.
    EXPECT_EQ(results.size(), 2u)
        << "both replicas of the same globalId should survive pre-distinct";
    for (const auto& r : results) {
        EXPECT_EQ(r.object.get_string("globalId"), gid);
    }
}

#endif  // __linux__
