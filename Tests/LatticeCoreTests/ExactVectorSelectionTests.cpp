#include <gtest/gtest.h>
#include <lattice/exact_vector_selection.hpp>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>

namespace {
using Metric = lattice::detail::exact_vector_metric;
using Candidate = lattice::detail::exact_vector_candidate;
using Predicate = lattice::detail::exact_vector_predicate;
using lattice::detail::select_exact_vector_candidates;

struct Row {
    std::string gid;
    std::vector<float> vector;
    bool eligible = true;
};

// Independent scalar oracle: no SQLite metric functions, candidate shortlist,
// helper-generated SQL, or production result is used to choose expected IDs.
long double distance(const std::vector<float>& a, const std::vector<float>& b, Metric metric) {
    long double squared = 0, l1 = 0, dot = 0, aa = 0, bb = 0;
    for (size_t i = 0; i < a.size(); ++i) {
        const auto x = static_cast<long double>(a[i]);
        const auto y = static_cast<long double>(b[i]);
        squared += (x - y) * (x - y);
        l1 += std::abs(x - y);
        dot += x * y; aa += x * x; bb += y * y;
    }
    if (metric == Metric::l1) return l1;
    if (metric == Metric::cosine) return 1 - dot / std::sqrt(aa * bb);
    return std::sqrt(squared);
}

std::vector<Candidate> oracle(const std::vector<Row>& rows, const std::vector<float>& query,
                              Metric metric, size_t k, bool eligible_only = true) {
    std::map<std::string, long double> best;
    for (const auto& row : rows) {
        if (eligible_only && !row.eligible) continue;
        const auto d = distance(row.vector, query, metric);
        auto [it, inserted] = best.emplace(row.gid, d);
        if (!inserted) it->second = std::min(it->second, d);
    }
    std::vector<Candidate> result;
    for (const auto& [gid, d] : best) result.push_back({gid, static_cast<double>(d)});
    std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
        return a.distance < b.distance || (a.distance == b.distance && a.global_id < b.global_id);
    });
    if (result.size() > k) result.resize(k);
    return result;
}

class ExactVectorSelection : public ::testing::Test {
protected:
    lattice::database db{":memory:"};
    const std::string model = "ExactDoc";
    const std::string column = "embedding";

    void create(const std::string& schema = "main") {
        const auto s = lattice::detail::exact_vector_identifier(schema);
        db.execute("CREATE TABLE " + s + ".ExactDoc(globalId TEXT PRIMARY KEY, label TEXT, eligible INTEGER)");
        db.execute("CREATE VIRTUAL TABLE " + s +
            "._ExactDoc_embedding_vec USING vec0(global_id TEXT PRIMARY KEY, embedding float[4])");
    }

    void insert(const Row& row, const std::string& schema = "main", const std::string& label = "keep") {
        const auto s = lattice::detail::exact_vector_identifier(schema);
        std::vector<uint8_t> bytes(row.vector.size() * sizeof(float));
        std::memcpy(bytes.data(), row.vector.data(), bytes.size());
        db.execute("INSERT INTO " + s + ".ExactDoc VALUES (?, ?, ?)",
                   {row.gid, label, int64_t(row.eligible)});
        db.execute("INSERT INTO " + s + "._ExactDoc_embedding_vec VALUES (?, ?)", {row.gid, bytes});
    }

    std::vector<Candidate> select(const std::vector<float>& query, Metric metric, size_t k,
                                  const std::vector<std::string>& schemas = {"main"},
                                  const Predicate& predicate = {"m.eligible = ?", {int64_t(1)}}) {
        const auto before = lattice::database::thread_statement_count();
        auto result = select_exact_vector_candidates(db, model, column, schemas, query, k, metric, predicate);
        EXPECT_EQ(lattice::database::thread_statement_count(), before + 1);
        return result;
    }

    void matches(const std::vector<Candidate>& actual, const std::vector<Candidate>& expected) {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            EXPECT_EQ(actual[i].global_id, expected[i].global_id) << i;
            EXPECT_NEAR(actual[i].distance, expected[i].distance, 0.00001) << i;
        }
    }

    void load(const std::vector<Row>& rows) { create(); for (const auto& row : rows) insert(row); }
};

TEST_F(ExactVectorSelection, FilteredL2FindsEligibleRowOutsideTopTwo) {
    const std::vector<Row> rows = {
        {"excluded-nearest", {1, 0, 0, 0}, false},
        {"excluded-second", {2, 0, 0, 0}, false},
        {"eligible-best", {3, 0, 0, 0}}
    };
    const std::vector<float> query{0, 0, 0, 0};
    const auto shortlist = oracle(rows, query, Metric::l2, 2, false);
    ASSERT_EQ(shortlist.size(), 2u);
    EXPECT_NE(shortlist[0].global_id, "eligible-best");
    EXPECT_NE(shortlist[1].global_id, "eligible-best");
    load(rows);
    matches(select(query, Metric::l2, 1), oracle(rows, query, Metric::l2, 1));
    matches(select(query, Metric::l2, 5), oracle(rows, query, Metric::l2, 5));
}

TEST_F(ExactVectorSelection, L1FindsWinnerOutsideL2TopFour) {
    const std::vector<Row> rows = {
        {"d1", {.875f, .875f, .875f, .875f}}, {"d2", {1, 1, 1, 1}},
        {"d3", {1.125f, 1.125f, 1.125f, 1.125f}}, {"d4", {1.25f, 1.25f, 1.25f, 1.25f}},
        {"l1-best", {3, 0, 0, 0}}
    };
    const std::vector<float> query{0, 0, 0, 0};
    const auto expected = oracle(rows, query, Metric::l1, 1);
    ASSERT_EQ(expected.front().global_id, "l1-best");
    for (const auto& candidate : oracle(rows, query, Metric::l2, 4))
        EXPECT_NE(candidate.global_id, expected.front().global_id);
    load(rows);
    matches(select(query, Metric::l1, 1, {"main"}, {}), expected);
    matches(select(query, Metric::l1, 5, {"main"}, {}), oracle(rows, query, Metric::l1, 5));
}

std::vector<Row> cosine_rows() {
    return {{"d1", {1, 1, 0, 0}}, {"d2", {1, 2, 0, 0}}, {"d3", {1, 3, 0, 0}},
            {"d4", {1, 4, 0, 0}}, {"cosine-best", {10, 0, 0, 0}}};
}

TEST_F(ExactVectorSelection, NonNormalizedCosineFindsWinnerOutsideL2TopFour) {
    const auto rows = cosine_rows();
    const std::vector<float> query{1, 0, 0, 0};
    const auto expected = oracle(rows, query, Metric::cosine, 1);
    ASSERT_EQ(expected.front().global_id, "cosine-best");
    for (const auto& candidate : oracle(rows, query, Metric::l2, 4))
        EXPECT_NE(candidate.global_id, expected.front().global_id);
    load(rows);
    matches(select(query, Metric::cosine, 1, {"main"}, {}), expected);
    matches(select(query, Metric::cosine, 5, {"main"}, {}), oracle(rows, query, Metric::cosine, 5));
}

TEST_F(ExactVectorSelection, NormalizedCosineControl) {
    auto rows = cosine_rows();
    for (auto& row : rows) {
        long double norm = 0;
        for (const auto v : row.vector) norm += static_cast<long double>(v) * v;
        for (auto& v : row.vector) v = static_cast<float>(v / std::sqrt(norm));
    }
    const std::vector<float> query{1, 0, 0, 0};
    load(rows);
    matches(select(query, Metric::cosine, 1), oracle(rows, query, Metric::cosine, 1));
    matches(select(query, Metric::cosine, 5), oracle(rows, query, Metric::cosine, 5));
}

TEST_F(ExactVectorSelection, AttachedReplicaUsesMinimumEligibleDistanceBeforeGlobalLimit) {
    create(); db.execute("ATTACH DATABASE ':memory:' AS replica"); create("replica");
    const Row main{"shared", {8, 0, 0, 0}};
    Row replica{"shared", {1, 0, 0, 0}, false};
    const Row other{"other", {2, 0, 0, 0}};
    insert(main); insert(other); insert(replica, "replica");
    const std::vector<float> query{0, 0, 0, 0};
    matches(select(query, Metric::l2, 2, {"main", "replica"}), oracle({main, replica, other}, query, Metric::l2, 2));
    db.execute("UPDATE replica.ExactDoc SET eligible = 1 WHERE globalId = ?", {replica.gid});
    replica.eligible = true;
    matches(select(query, Metric::l2, 1, {"main", "replica"}), oracle({main, replica, other}, query, Metric::l2, 1));
    matches(select(query, Metric::l2, 5, {"replica", "main"}), oracle({main, replica, other}, query, Metric::l2, 5));
}

TEST_F(ExactVectorSelection, RepeatsBindingsAcrossArmsWithoutParsingQuestionMarks) {
    create(); db.execute("ATTACH DATABASE ':memory:' AS \"replica\"\"arm\""); create("replica\"arm");
    const std::string label = "quoted ' value ? --";
    insert({"from-main", {2, 0, 0, 0}}, "main", label);
    insert({"from-replica", {1, 0, 0, 0}}, "replica\"arm", label);
    insert({"wrong-label", {0, 0, 0, 0}}, "main", "other");
    const Predicate predicate{"m.eligible = ? AND m.label = ? AND '?' = '?' /* ? */ -- ?",
                              {int64_t(1), label}};
    matches(select({0, 0, 0, 0}, Metric::l2, 2, {"main", "replica\"arm"}, predicate),
            {{"from-replica", 1}, {"from-main", 2}});
}

TEST_F(ExactVectorSelection, TiesUseGlobalIdAndSidecarOrphansAreNotCandidates) {
    load({{"z", {1, 0, 0, 0}}, {"a", {-1, 0, 0, 0}}});
    insert({"orphan", {0, 0, 0, 0}});
    db.execute("DELETE FROM main.ExactDoc WHERE globalId = ?", {std::string("orphan")});
    matches(select({0, 0, 0, 0}, Metric::l2, 1), {{"a", 1}});
    matches(select({0, 0, 0, 0}, Metric::l2, 9), {{"a", 1}, {"z", 1}});
}

TEST_F(ExactVectorSelection, UndefinedReplicaOutsideTopKRejectsInsteadOfBeingHiddenByMin) {
    create(); db.execute("ATTACH DATABASE ':memory:' AS replica"); create("replica");
    insert({"best", {1, 0, 0, 0}});
    insert({"undefined-replica", {0, 0, 0, 0}});
    insert({"undefined-replica", {0, 1, 0, 0}}, "replica");
    EXPECT_THROW(select({1, 0, 0, 0}, Metric::cosine, 1, {"main", "replica"}), lattice::db_error);
    db.execute("UPDATE main.ExactDoc SET eligible = 0 WHERE globalId = ?", {std::string("undefined-replica")});
    matches(select({1, 0, 0, 0}, Metric::cosine, 1, {"main", "replica"}), {{"best", 0}});
}

TEST_F(ExactVectorSelection, NonfiniteEligibleDistanceOutsideTopKRejects) {
    const auto maximum = std::numeric_limits<float>::max();
    load({{"best", {1, 0, 0, 0}}, {"overflow", {maximum, maximum, maximum, maximum}}});
    EXPECT_THROW(select({0, 0, 0, 0}, Metric::l2, 1), lattice::db_error);
}

TEST_F(ExactVectorSelection, InternalInputPreconditionsAndNoWorkCases) {
    const auto before = lattice::database::thread_statement_count();
    EXPECT_TRUE(select_exact_vector_candidates(db, model, column, {"main"}, {}, 1, Metric::l2).empty());
    EXPECT_TRUE(select_exact_vector_candidates(db, model, column, {"main"}, {1}, 0, Metric::l2).empty());
    EXPECT_TRUE(select_exact_vector_candidates(db, model, column, {}, {1}, 1, Metric::l2).empty());
    EXPECT_THROW(select_exact_vector_candidates(db, model, column, {"main"}, {0}, 1, Metric::cosine), std::invalid_argument);
    EXPECT_THROW(select_exact_vector_candidates(db, model, column, {"main"},
        {std::numeric_limits<float>::quiet_NaN()}, 1, Metric::l2), std::invalid_argument);
    EXPECT_THROW(select_exact_vector_candidates(db, model, column, {"main"},
        {std::numeric_limits<float>::infinity()}, 1, Metric::l2), std::invalid_argument);
    EXPECT_THROW(select_exact_vector_candidates(db, model, column, {"main"},
        std::vector<float>(8193, 1), 1, Metric::l2), std::invalid_argument);
    if constexpr (sizeof(size_t) >= sizeof(int64_t)) {
        EXPECT_THROW(select_exact_vector_candidates(db, model, column, {"main"}, {1},
            static_cast<size_t>(std::numeric_limits<int64_t>::max()) + 1, Metric::l2), std::invalid_argument);
    }
    EXPECT_THROW(select_exact_vector_candidates(db, model, column, {"main"}, {1}, 1,
        static_cast<Metric>(99)), std::invalid_argument);
    EXPECT_THROW(select_exact_vector_candidates(db, "", column, {"main"}, {1}, 1, Metric::l2), std::invalid_argument);
    EXPECT_THROW(select_exact_vector_candidates(db, model, column, {"main"}, {1}, 1, Metric::l2,
        {"", {int64_t(1)}}), std::invalid_argument);
    EXPECT_EQ(lattice::database::thread_statement_count(), before);
    create();
    insert({"one", {1, 0, 0, 0}});
    EXPECT_THROW(select({1, 0, 0}, Metric::l2, 1), lattice::db_error);
}

} // namespace
