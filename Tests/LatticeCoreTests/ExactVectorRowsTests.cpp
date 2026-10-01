#include <gtest/gtest.h>
#include <lattice/exact_vector_rows.hpp>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <set>
#include <tuple>

namespace {
using Metric = lattice::detail::exact_vector_metric;
using Arm = lattice::detail::exact_vector_row_arm;
using Match = lattice::detail::exact_vector_row;
using Predicate = lattice::detail::exact_vector_predicate;
using lattice::detail::exact_vector_identifier;
using lattice::detail::select_exact_vector_rows;
using lattice::detail::collect_exact_vector_rows;

std::vector<uint8_t> bytes(const std::vector<float>& value) {
    std::vector<uint8_t> result(value.size() * sizeof(float));
    if (!result.empty()) std::memcpy(result.data(), value.data(), result.size());
    return result;
}

struct Physical {
    int64_t id;
    std::string gid;
    std::vector<float> vector;
    std::string payload;
    std::string schema = "main";
    int64_t token = 0;
    bool eligible = true;
};

// Independent scalar oracle: no SQL, sqlite-vec, generated plan or returned
// score selects the expected physical winner. Tests use separated distances
// and exact integer-vector ties so float32 rounding cannot decide the oracle.
long double oracle_distance(const std::vector<float>& a, const std::vector<float>& b, Metric metric) {
    long double l1 = 0, square = 0, dot = 0, aa = 0, bb = 0;
    for (size_t i = 0; i < a.size(); ++i) {
        const long double x = a[i], y = b[i];
        l1 += std::abs(x-y); square += (x-y)*(x-y);
        dot += x*y; aa += x*x; bb += y*y;
    }
    if (metric == Metric::l1) return l1;
    if (metric == Metric::cosine) return 1-dot/std::sqrt(aa*bb);
    return std::sqrt(square);
}

std::string source_label(const Physical& value) {
    return value.schema == "main" ? "main" : exact_vector_identifier(value.schema);
}

std::vector<std::pair<Physical, long double>> oracle(const std::vector<Physical>& input,
    const std::vector<float>& query, size_t k, Metric metric) {
    std::map<std::string, std::pair<Physical, long double>> winners;
    auto key = [](const auto& value) {
        return std::make_tuple(value.second, value.first.schema == "main" ? 0 : 1,
                               source_label(value.first), value.first.id);
    };
    for (const auto& row : input) {
        if (!row.eligible) continue;
        const auto value = std::make_pair(row, oracle_distance(row.vector, query, metric));
        const auto found = winners.find(row.gid);
        if (found == winners.end() || key(value) < key(found->second)) winners[row.gid] = value;
    }
    std::vector<std::pair<Physical, long double>> result;
    for (const auto& [gid, value] : winners) result.push_back(value);
    std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
        return a.second < b.second || (a.second == b.second && a.first.gid < b.first.gid);
    });
    if (result.size() > k) result.resize(k);
    return result;
}

class ExactVectorRows : public ::testing::Test {
protected:
    lattice::database db{":memory:"};
    const std::vector<std::string> columns{"id", "globalId", "embedding", "payload", "eligible", "tag", "note"};

    void create(const std::string& schema = "main", bool reversed = false) {
        if (schema != "main") db.execute("ATTACH DATABASE ':memory:' AS " + exact_vector_identifier(schema));
        db.execute("CREATE TABLE " + exact_vector_identifier(schema) + ".Doc(" +
            (reversed ? "note TEXT, tag BLOB, eligible INTEGER, payload TEXT, embedding BLOB, globalId TEXT COLLATE NOCASE, id INTEGER PRIMARY KEY)"
                      : "id INTEGER PRIMARY KEY, globalId TEXT COLLATE NOCASE, embedding BLOB, payload TEXT, eligible INTEGER, tag BLOB, note TEXT)"));
    }

    void insert(const Physical& row, std::vector<uint8_t> tag = {}, lattice::column_value_t note = nullptr) {
        db.execute("INSERT INTO " + exact_vector_identifier(row.schema) +
            ".Doc(id,globalId,embedding,payload,eligible,tag,note) VALUES (?,?,?,?,?,?,?)",
            {row.id, row.gid, bytes(row.vector), row.payload, int64_t(row.eligible), tag, note});
    }

    Arm arm(const std::string& schema = "main", int64_t token = 0) {
        return {schema, token, {"m.eligible = ?", {int64_t(1)}}};
    }

    std::vector<Match> select(const std::vector<Arm>& arms, const std::vector<float>& query,
                              size_t k, Metric metric = Metric::l2) {
        const auto before = lattice::database::thread_statement_count();
        auto result = select_exact_vector_rows(db, "Doc", "embedding", columns, arms, query, k, metric);
        EXPECT_EQ(lattice::database::thread_statement_count(), before + 1);
        return result;
    }

    void matches(const std::vector<Match>& actual, const std::vector<std::pair<Physical, long double>>& expected) {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            const auto& row = actual[i].row;
            const auto& physical = expected[i].first;
            EXPECT_EQ(row.size(), columns.size() + 2);
            EXPECT_EQ(std::get<int64_t>(row.at("id")), physical.id);
            EXPECT_EQ(std::get<std::string>(row.at("globalId")), physical.gid);
            EXPECT_EQ(std::get<std::string>(row.at("payload")), physical.payload);
            EXPECT_EQ(std::get<std::vector<uint8_t>>(row.at("embedding")), bytes(physical.vector));
            EXPECT_EQ(std::get<std::string>(row.at("_source")), source_label(physical));
            EXPECT_EQ(std::get<int64_t>(row.at("_lattice_attach_token")), physical.token);
            EXPECT_NEAR(actual[i].distance, expected[i].second, 0.00001);
        }
    }

    std::set<sqlite3_stmt*> statements() {
        std::set<sqlite3_stmt*> result;
        for (auto* statement = sqlite3_next_stmt(db.handle(), nullptr); statement;
             statement = sqlite3_next_stmt(db.handle(), statement)) result.insert(statement);
        return result;
    }
};

TEST_F(ExactVectorRows, BestEligiblePhysicalPayloadAndProvenanceSurviveWithoutGidRehydration) {
    create(); create("replica", true);
    const std::vector<Physical> rows{
        {7,"shared",{8,0,0,0},"wrong-main"},
        {8,"other",{2,0,0,0},"other-main"},
        {7,"shared",{1,0,0,0},std::string("right\0attached",14),"replica",101},
        {9,"shared",{0,0,0,0},"excluded-attached","replica",101,false}
    };
    for (const auto& row : rows) insert(row);
    const std::vector<float> query{0,0,0,0};
    auto result = select({arm(),arm("replica",101)}, query, 2);
    matches(result, oracle(rows,query,2,Metric::l2));
    matches(select({arm("replica",101),arm()},query,2),oracle(rows,query,2,Metric::l2));
    db.execute("UPDATE replica.Doc SET payload = 'later' WHERE id = 7");
    EXPECT_EQ(std::get<std::string>(result.front().row.at("payload")), rows[2].payload);
}

TEST_F(ExactVectorRows, MainSourceAndPhysicalIdTiesAreIndependentOfArmOrder) {
    create(); create("zeta"); create("alpha",true);
    const std::vector<Physical> rows{
        {10,"main-tie",{1,0,0,0},"main"},
        {1,"main-tie",{-1,0,0,0},"attached","alpha",102},
        {9,"attached-tie",{2,0,0,0},"zeta","zeta",103},
        {8,"attached-tie",{-2,0,0,0},"alpha-high-id","alpha",102},
        {4,"attached-tie",{2,0,0,0},"alpha-low-id","alpha",102}
    };
    for (const auto& row : rows) insert(row);
    const std::vector<float> query{0,0,0,0};
    const auto expected=oracle(rows,query,9,Metric::l2);
    matches(select({arm("zeta",103),arm("alpha",102),arm()},query,9),expected);
    matches(select({arm(),arm("alpha",102),arm("zeta",103)},query,9),expected);
}

TEST_F(ExactVectorRows, BinaryGlobalIdsRemainDistinctAndDedupPrecedesTopK) {
    create(); create("replica");
    const std::vector<Physical> rows{
        {1,"A",{1,0,0,0},"upper"}, {2,"a",{-1,0,0,0},"lower"},
        {3,"A",{1,0,0,0},"duplicate","replica",12},
        {4,"A",{1,0,0,0},"duplicate2","replica",12},
        {5,"z",{2,0,0,0},"third"}
    };
    for (const auto& row : rows) insert(row);
    matches(select({arm(),arm("replica",12)},{0,0,0,0},3),oracle(rows,{0,0,0,0},3,Metric::l2));
    matches(select({arm(),arm("replica",12)},{0,0,0,0},2),oracle(rows,{0,0,0,0},2,Metric::l2));
}

TEST_F(ExactVectorRows, RequestedMetricsMatchIndependentFullPhysicalOracle) {
    create(); create("replica");
    const std::vector<Physical> rows{
        {1,"diagonal",{1,1,1,1},"diagonal"},
        {2,"axis",{3,0,0,0},"axis"},
        {3,"far-aligned",{10,0,0,0},"cosine"},
        {4,"diagonal",{2,1,0,0},"replica","replica",41},
        {5,"excluded",{1,0,0,0},"excluded","replica",41,false}
    };
    for (const auto& row : rows) insert(row);
    for (const auto metric : {Metric::l2,Metric::l1,Metric::cosine}) {
        const std::vector<float> query = metric==Metric::cosine ? std::vector<float>{1,0,0,0} : std::vector<float>{0,0,0,0};
        matches(select({arm(),arm("replica",41)},query,2,metric),oracle(rows,query,2,metric));
        matches(select({arm(),arm("replica",41)},query,9,metric),oracle(rows,query,9,metric));
    }
}

TEST_F(ExactVectorRows, CanonicalVectorsIgnoreMissingAndStaleSidecars) {
    create();
    insert({1,"one",{9,0,0,0},"canonical"}); insert({2,"two",{2,0,0,0},"other"});
    EXPECT_EQ(std::get<std::string>(select({arm()},{0,0,0,0},1)[0].row.at("globalId")),"two");
    db.execute("CREATE VIRTUAL TABLE _Doc_embedding_vec USING vec0(global_id TEXT PRIMARY KEY, embedding float[4])");
    db.execute("INSERT INTO _Doc_embedding_vec VALUES (?,?)",{std::string("one"),bytes({0,0,0,0})});
    db.execute("INSERT INTO _Doc_embedding_vec VALUES (?,?)",{std::string("orphan"),bytes({0,0,0,0})});
    db.execute("UPDATE Doc SET embedding=? WHERE id=1",{bytes({1,0,0,0})});
    const auto result=select({arm()},{0,0,0,0},2);
    ASSERT_EQ(result.size(),2u);
    EXPECT_EQ(std::get<std::string>(result[0].row.at("globalId")),"one");
    EXPECT_DOUBLE_EQ(result[0].distance,1.0);
    db.execute("DROP TABLE _Doc_embedding_vec");
    EXPECT_DOUBLE_EQ(select({arm()},{0,0,0,0},1)[0].distance,1.0);
}

TEST_F(ExactVectorRows, InvalidLosingReplicaOutsideTopKRejectsWholeRead) {
    create(); create("replica");
    insert({1,"best",{1,0,0,0},"best"});
    insert({2,"hidden",{0,1,0,0},"finite"});
    insert({3,"hidden",{0,0,0,0},"invalid","replica",7});
    const auto before=statements();
    EXPECT_THROW(select({arm(),arm("replica",7)},{1,0,0,0},1,Metric::cosine),lattice::db_error);
    // Unlike NULL (which sorts first), +Inf loses to this gid's finite main
    // replica and also falls outside global top-k. Neither stage may hide it.
    const auto maximum=std::numeric_limits<float>::max();
    db.execute("UPDATE replica.Doc SET embedding=? WHERE id=3",{bytes({maximum,maximum,maximum,maximum})});
    EXPECT_THROW(select({arm(),arm("replica",7)},{0,0,0,0},1),lattice::db_error);
    EXPECT_EQ(statements(),before);
    db.execute("UPDATE replica.Doc SET eligible=0 WHERE id=3");
    const auto result=select({arm(),arm("replica",7)},{1,0,0,0},1,Metric::cosine);
    ASSERT_EQ(result.size(),1u); EXPECT_EQ(std::get<std::string>(result[0].row.at("globalId")),"best");
}

TEST_F(ExactVectorRows, MalformedAndNonfiniteCanonicalValuesOutsideTopKReject) {
    create(); insert({1,"best",{1,0,0,0},"best"}); insert({2,"bad",{2,0,0,0},"bad"});
    const auto maximum=std::numeric_limits<float>::max();
    const std::vector<lattice::column_value_t> invalid{
        std::string("[2,0,0,0]"), std::vector<uint8_t>{}, std::vector<uint8_t>{1}, bytes({2,0,0}),
        bytes({std::numeric_limits<float>::quiet_NaN(),0,0,0}),
        bytes({std::numeric_limits<float>::infinity(),0,0,0}), bytes({maximum,maximum,maximum,maximum})
    };
    for (const auto& value : invalid) {
        db.execute("UPDATE Doc SET embedding=? WHERE id=2",{value});
        const auto before=statements();
        EXPECT_THROW(select({arm()},{0,0,0,0},1),lattice::db_error);
        EXPECT_EQ(statements(),before);
    }
}

TEST_F(ExactVectorRows, ExcludedMalformedAndAbsentOptionalVectorsAreNotCandidates) {
    create(); insert({1,"valid",{1,0,0,0},"valid"});
    insert({2,"excluded",{2,0,0,0},"excluded","main",0,false});
    insert({3,"absent",{3,0,0,0},"absent"});
    db.execute("UPDATE Doc SET embedding='not a float32 blob' WHERE id=2");
    db.execute("UPDATE Doc SET embedding=NULL WHERE id=3");
    const auto result=select({arm()},{0,0,0,0},9);
    ASSERT_EQ(result.size(),1u); EXPECT_EQ(std::get<std::string>(result[0].row.at("globalId")),"valid");
}

TEST_F(ExactVectorRows, PerArmBindingsPreserveBlobNullCommentsAndPhysicalColumnOrder) {
    create(); create("replica\"arm",true);
    const std::string main_label="main ' ? --", replica_label="attached ' ? --";
    const std::vector<uint8_t> tag{0,42,255};
    insert({1,"main",{2,0,0,0},main_label},tag);
    insert({2,"attached",{1,0,0,0},replica_label,"replica\"arm",77},tag);
    insert({3,"wrong",{0,0,0,0},"wrong","replica\"arm",77},tag);
    const std::string sql="m.payload=? AND m.tag=? AND m.note IS ? AND '?'='?' /* ? */ -- ?";
    const auto result=select({{"main",0,{sql,{main_label,tag,nullptr}}},
                              {"replica\"arm",77,{sql,{replica_label,tag,nullptr}}}},{0,0,0,0},9);
    ASSERT_EQ(result.size(),2u);
    EXPECT_EQ(std::get<std::string>(result[0].row.at("payload")),replica_label);
    EXPECT_EQ(std::get<std::string>(result[0].row.at("_source")),"\"replica\"\"arm\"");
    EXPECT_EQ(std::get<int64_t>(result[0].row.at("_lattice_attach_token")),77);
    EXPECT_EQ(std::get<std::vector<uint8_t>>(result[0].row.at("tag")),tag);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(result[0].row.at("note")));
    EXPECT_EQ(std::get<std::string>(result[1].row.at("payload")),main_label);
}

TEST_F(ExactVectorRows, QuotedModelAndVectorPropertyNamesProduceExactRowImage) {
    const std::string model="Odd model\"", vector="float\"vector";
    db.execute("CREATE TABLE " + exact_vector_identifier(model) +
        "(id INTEGER PRIMARY KEY, globalId TEXT, " + exact_vector_identifier(vector) + " BLOB, payload TEXT)");
    db.execute("INSERT INTO " + exact_vector_identifier(model) + " VALUES (?,?,?,?)",
               {int64_t(1),std::string("gid"),bytes({1,0}),std::string("payload")});
    const auto result=select_exact_vector_rows(db,model,vector,{"payload",vector,"globalId","id"},
                                               {{"main",0,{}}},{0,0},1,Metric::l2);
    ASSERT_EQ(result.size(),1u);
    EXPECT_EQ(result[0].row.size(),6u);
    EXPECT_EQ(std::get<std::string>(result[0].row.at("payload")),"payload");
    EXPECT_EQ(std::get<std::vector<uint8_t>>(result[0].row.at(vector)),bytes({1,0}));
}

TEST_F(ExactVectorRows, InvalidIdentityCannotDisappearThroughWinnerPayloadJoin) {
    create(); insert({1,"valid",{1,0,0,0},"valid"}); insert({2,"bad",{2,0,0,0},"bad"});
    for (const auto& value : std::vector<lattice::column_value_t>{nullptr,std::vector<uint8_t>{1,2}}) {
        db.execute("UPDATE Doc SET globalId=? WHERE id=2",{value});
        EXPECT_THROW(select({arm()},{0,0,0,0},1),lattice::db_error);
    }
}

TEST_F(ExactVectorRows, RequestAndSQLValidationPrecedeZeroOrEmptyResults) {
    create();
    EXPECT_TRUE(select({arm()},{0,0,0,0},1).empty());
    EXPECT_TRUE(select({arm()},{0,0,0,0},0).empty());
    EXPECT_THROW(select({arm()},{},0),std::invalid_argument);
    EXPECT_THROW(select({}, {0,0,0,0},0),std::invalid_argument);
    EXPECT_THROW(select({arm()},{0,0,0,0},0,Metric::cosine),std::invalid_argument);
    EXPECT_THROW(select({arm()},{1},0,static_cast<Metric>(99)),std::invalid_argument);
    EXPECT_THROW(select({arm()},{std::numeric_limits<float>::quiet_NaN()},0),std::invalid_argument);
    EXPECT_THROW(select({arm()},std::vector<float>(8193,1),0),std::invalid_argument);
    EXPECT_THROW(select({arm("main",1)},{1},0),std::invalid_argument);
    EXPECT_THROW(select({arm("replica",0)},{1},0),std::invalid_argument);
    EXPECT_THROW(select({arm(),arm()},{1},0),std::invalid_argument);
    EXPECT_THROW(select({arm("temp",1)},{1},0),std::invalid_argument);
    auto wrong=columns; wrong.push_back("PAYLOAD");
    EXPECT_THROW(select_exact_vector_rows(db,"Doc","embedding",wrong,{arm()},{1},0,Metric::l2),std::invalid_argument);
    wrong=columns; wrong.push_back("__lattice_exact_distance");
    EXPECT_THROW(select_exact_vector_rows(db,"Doc","embedding",wrong,{arm()},{1},0,Metric::l2),std::invalid_argument);
    EXPECT_THROW(select_exact_vector_rows(db,"missing","embedding",columns,{arm()},{1},0,Metric::l2),lattice::db_error);
    EXPECT_THROW(select({{"main",0,{"m.eligible=?",{}}}},{1},0),lattice::db_error);
    EXPECT_THROW(select({{"main",0,{"m.eligible=:value",{int64_t(1)}}}},{1},0),lattice::db_error);
    db.close();
    EXPECT_THROW(select({arm()},{1},0),lattice::db_error);
}

TEST_F(ExactVectorRows, StrictCollectorRejectsTailWritesAliasesAndBindingErrorsWithoutLeaking) {
    create(); insert({1,"row",{1,0,0,0},"unchanged"});
    const auto before=statements();
    EXPECT_THROW(collect_exact_vector_rows(db,{"SELECT 1; SELECT 2",{}}),lattice::db_error);
    EXPECT_THROW(collect_exact_vector_rows(db,{"UPDATE Doc SET payload='changed'",{}}),lattice::db_error);
    EXPECT_THROW(collect_exact_vector_rows(db,{"SELECT 1 AS duplicate,2 AS DUPLICATE",{}}),lattice::db_error);
    EXPECT_THROW(collect_exact_vector_rows(db,{"SELECT ?",{}}),lattice::db_error);
    EXPECT_THROW(collect_exact_vector_rows(db,{"SELECT ?1",{int64_t(1)}}),lattice::db_error);
    EXPECT_THROW(collect_exact_vector_rows(db,{std::string("SELECT 1\0;SELECT 2",18),{}}),std::invalid_argument);
    const int old_limit=sqlite3_limit(db.handle(),SQLITE_LIMIT_LENGTH,16);
    EXPECT_THROW(collect_exact_vector_rows(db,{"SELECT ?",{std::string(32,'x')}}),lattice::db_error);
    sqlite3_limit(db.handle(),SQLITE_LIMIT_LENGTH,old_limit);
    EXPECT_EQ(statements(),before);
    EXPECT_EQ(std::get<std::string>(select({arm()},{0,0,0,0},1)[0].row.at("payload")),"unchanged");
    db.execute("UPDATE Doc SET payload='' WHERE id=1");
    const auto empty=select({arm()},{0,0,0,0},1);
    ASSERT_EQ(empty.size(),1u);
    const auto& image=empty[0].row;
    ASSERT_TRUE(std::holds_alternative<std::vector<uint8_t>>(image.at("tag")));
    EXPECT_TRUE(std::get<std::vector<uint8_t>>(image.at("tag")).empty());
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(image.at("note")));
    ASSERT_TRUE(std::holds_alternative<std::string>(image.at("payload")));
    EXPECT_TRUE(std::get<std::string>(image.at("payload")).empty());
}

TEST_F(ExactVectorRows, AuthorizerFailureThrowsAndReadDoesNotDrainDeferredCallbacks) {
    create(); insert({1,"row",{1,0,0,0},"payload"});
    const auto before=statements();
    ASSERT_EQ(sqlite3_set_authorizer(db.handle(),[](void*,int action,const char*,const char*,const char*,const char*) {
        return action==SQLITE_READ ? SQLITE_DENY : SQLITE_OK;
    },nullptr),SQLITE_OK);
    EXPECT_THROW(select({arm()},{0,0,0,0},1),lattice::db_error);
    sqlite3_set_authorizer(db.handle(),nullptr,nullptr);
    EXPECT_EQ(statements(),before);
    int drains=0;
    db.set_txn_hooks([&] { ++drains; },[] {});
    db.mark_txn_dirty();
    EXPECT_EQ(select({arm()},{0,0,0,0},1).size(),1u);
    EXPECT_EQ(drains,0);
    (void)db.query("SELECT 1");
    EXPECT_EQ(drains,1);
    db.set_txn_hooks({},{});
}

} // namespace
