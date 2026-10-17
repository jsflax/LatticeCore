#include "TestHelpers.hpp"
#ifndef __linux__
#include <lattice.hpp>          // swift_lattice, swift_lattice_ref
#include <dynamic_object.hpp>

// ============================================================================
// Attach integrity: filters over the union view and KNN under filters.
//
// Two independent defects, both observed live as "recall silently drops
// every attached-spoke row":
//
// 1. VIEW SCRAMBLING. The attach() union view used `SELECT *` arms, and
//    UNION ALL maps columns BY POSITION — but each database file's physical
//    column order is whatever its create-time schema iteration produced
//    (an unordered map: effectively random per file). Same-named columns
//    then carry DIFFERENT files' values, so a WHERE over the view reads an
//    attached row's modifiedAt as deletedAt (etc.) and filters it out.
//    id/globalId always lead the DDL, so joins kept working and property
//    reads (routed to the _source arm's real table) looked fine — only
//    SQL-level filters over the view corrupted. The view must project every
//    arm's columns BY NAME in one canonical order.
//
// 2. KNN PLANNER INVERSION. With `embedding MATCH ? AND k = ?` in the same
//    SELECT as the model-table join and WHERE pre-filters, SQLite is free
//    to drive the join with the model table outer (it will, when an arm's
//    table is small) — and vec0's KNN evaluated as the inner side returns
//    garbage or nothing. The KNN must live alone in a LIMIT-guarded
//    subquery (LIMIT blocks flattening), joined and filtered outside.
// ============================================================================

namespace {

lattice::swift_schema_entry integrity_schema(const std::string& table) {
    lattice::swift_schema_entry entry;
    entry.table_name = table;

    lattice::property_descriptor label;
    label.name = "label";
    label.type = lattice::column_type::text;
    label.kind = lattice::property_kind::primitive;
    entry.properties["label"] = label;

    lattice::property_descriptor flag;
    flag.name = "flag";
    flag.type = lattice::column_type::integer;
    flag.kind = lattice::property_kind::primitive;
    entry.properties["flag"] = flag;

    lattice::property_descriptor embedding;
    embedding.name = "embedding";
    embedding.type = lattice::column_type::blob;
    embedding.kind = lattice::property_kind::primitive;
    embedding.is_vector = true;
    entry.properties["embedding"] = embedding;

    return entry;
}

void insert_row(lattice::swift_lattice& db, const lattice::SwiftSchema& props,
                const std::string& table, const std::string& label,
                int64_t flag, const std::vector<float>& vec) {
    lattice::swift_dynamic_object sdo;
    sdo.table_name = table;
    sdo.properties = props;
    sdo.values["label"] = label;
    sdo.values["flag"] = flag;
    sdo.values["embedding"] = pack_floats(vec);
    { lattice::dynamic_object obj(sdo); db.add(obj); }
    auto rows = db.db().query(
        "SELECT globalId FROM main." + table + " WHERE label = ?", {label});
    ASSERT_EQ(rows.size(), 1u) << "insert failed for " << label;
    db.upsert_vec0(table, "embedding", std::get<std::string>(rows[0].at("globalId")),
                   pack_floats(vec));
}

} // namespace

// Arms whose files disagree on physical column order: a plain filtered
// SELECT through the union view must still see every arm's rows with the
// RIGHT values under each column name.
TEST(AttachViewIntegrity, FilterOverViewSurvivesDivergentColumnOrders) {
    TempDB a_path{"view_order_a"}, b_path{"view_order_b"};
    lattice::SchemaVector schemas = {integrity_schema("OrderDoc")};

    // Pre-create the two files with DELIBERATELY different property column
    // orders (id/globalId lead both, as create_model_table guarantees).
    {
        lattice::database raw_a(a_path.str());
        raw_a.execute("CREATE TABLE OrderDoc ("
                      "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                      "globalId TEXT UNIQUE NOT NULL, "
                      "label TEXT, flag INTEGER, embedding BLOB)");
        lattice::database raw_b(b_path.str());
        raw_b.execute("CREATE TABLE OrderDoc ("
                      "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                      "globalId TEXT UNIQUE NOT NULL, "
                      "embedding BLOB, flag INTEGER, label TEXT)");
    }

    lattice::swift_configuration a_cfg(a_path.str());
    lattice::swift_configuration b_cfg(b_path.str());
    auto* a_ref = lattice::swift_lattice_ref::create(a_cfg, schemas);
    auto* b_ref = lattice::swift_lattice_ref::create(b_cfg, schemas);
    auto* a = a_ref->get();
    auto* b = b_ref->get();

    insert_row(*a, schemas[0].properties, "OrderDoc", "a1", 1, {1.0f, 0.0f, 0.0f, 0.0f});
    insert_row(*b, schemas[0].properties, "OrderDoc", "b1", 1, {0.0f, 1.0f, 0.0f, 0.0f});
    insert_row(*b, schemas[0].properties, "OrderDoc", "b2", 0, {0.0f, 0.0f, 1.0f, 0.0f});

    a->attach(*b);

    // The union view must be name-correct: flag=1 admits a1 AND b1 (b2
    // stays out), and each admitted row carries ITS OWN label.
    auto rows = a->db().query("SELECT label, flag FROM OrderDoc WHERE flag = 1 ORDER BY label");
    ASSERT_EQ(rows.size(), 2u)
        << "positional view scrambling filtered attached-arm rows out";
    EXPECT_EQ(std::get<std::string>(rows[0].at("label")), "a1");
    EXPECT_EQ(std::get<std::string>(rows[1].at("label")), "b1");

    // And the KNN path with the same filter surfaces the attached arm.
    auto qvec = pack_floats({0.0f, 1.0f, 0.0f, 0.0f});
    lattice::VectorConstraintVector vecs = {
        lattice::vector_constraint("embedding", qvec, /*k=*/4, /*metric=*/0)};
    auto results = a->combined_nearest_query(
        "OrderDoc", {}, vecs, {}, {}, std::string("flag = 1"),
        lattice::sort_descriptor{}, /*limit=*/10);
    std::set<std::string> labels;
    for (const auto& r : results) labels.insert(r.object.get_string("label"));
    EXPECT_TRUE(labels.count("b1"))
        << "attached arm missing from filtered KNN over divergent column orders";
    EXPECT_FALSE(labels.count("b2")) << "flag=0 row leaked through the filter";

    delete a_ref;
    delete b_ref;
}

// A tiny arm plus WHERE pre-filters: the planner must never drive vec0's
// KNN as the inner side of the join (it returns nothing there), no matter
// how small an arm's model table is. NOTE: the pre-fix failure was
// planner-cost dependent — it reproduced live (17-column model tables,
// two-condition filter, 1-row main arm) but not in this minimal fixture,
// so this test pins the required behavior rather than proving red-first;
// the structural guarantee is the LIMIT-guarded KNN subquery, which the
// planner cannot flatten into the join regardless of stats.
TEST(AttachViewIntegrity, FilteredKnnSurvivesTinyArms) {
    TempDB a_path{"knn_tiny_a"}, b_path{"knn_tiny_b"};
    lattice::SchemaVector schemas = {integrity_schema("TinyDoc")};

    lattice::swift_configuration a_cfg(a_path.str());
    lattice::swift_configuration b_cfg(b_path.str());
    auto* a_ref = lattice::swift_lattice_ref::create(a_cfg, schemas);
    auto* b_ref = lattice::swift_lattice_ref::create(b_cfg, schemas);
    auto* a = a_ref->get();
    auto* b = b_ref->get();

    // main: ONE row (small enough to tempt the planner into model-outer).
    insert_row(*a, schemas[0].properties, "TinyDoc", "a1", 1, {0.0f, 0.0f, 0.0f, 1.0f});
    // attached: the rows the query is actually looking for.
    insert_row(*b, schemas[0].properties, "TinyDoc", "b1", 1, {1.0f, 0.0f, 0.0f, 0.0f});
    insert_row(*b, schemas[0].properties, "TinyDoc", "b2", 1, {0.9f, 0.1f, 0.0f, 0.0f});
    insert_row(*b, schemas[0].properties, "TinyDoc", "b3", 1, {0.8f, 0.2f, 0.0f, 0.0f});

    a->attach(*b);

    auto qvec = pack_floats({1.0f, 0.0f, 0.0f, 0.0f});
    lattice::VectorConstraintVector vecs = {
        lattice::vector_constraint("embedding", qvec, /*k=*/4, /*metric=*/0)};
    auto results = a->combined_nearest_query(
        "TinyDoc", {}, vecs, {}, {}, std::string("flag = 1"),
        lattice::sort_descriptor{}, /*limit=*/10);

    std::set<std::string> labels;
    for (const auto& r : results) labels.insert(r.object.get_string("label"));
    EXPECT_TRUE(labels.count("b1") && labels.count("b2") && labels.count("b3"))
        << "attached arm vanished from filtered KNN (planner drove vec0 as join inner)";
    // Every returned row carries its own distance: b1 is the exact match.
    for (const auto& r : results) {
        if (r.object.get_string("label") == "b1") {
            ASSERT_FALSE(r.distances.empty());
            EXPECT_NEAR(r.distances[0].distance, 0.0, 0.01)
                << "b1 carried a borrowed distance";
        }
    }

    delete a_ref;
    delete b_ref;
}

#endif  // __linux__
