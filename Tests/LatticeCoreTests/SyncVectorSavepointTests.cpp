#include "TestHelpers.hpp"
#include <algorithm>
#include <map>
#include <string>
#include <vector>

namespace {

constexpr const char* model = "SavepointVectorDoc";
constexpr const char* failed_id = "00000000-0000-4000-8000-000000000401";
constexpr const char* successor_id = "00000000-0000-4000-8000-000000000402";
constexpr const char* channel = "vector-savepoint-receiver";
using Rows = std::vector<lattice::database::row_t>;
using Snapshot = std::map<std::string, Rows>;
using Contents = std::map<std::string, float>;
enum class Mutation { insert, update, erase };

lattice::configuration fixture_config(const TempDB& file) {
    lattice::configuration config(file.str());
    config.audit_retention_seconds = 0;
    config.busy_timeout_ms = 100;
    return config;
}

// The same modeled-table/audit-trigger creation used by the Swift bridge,
// without globally registering a fixture schema or dispatching a reconcile
// worker. On reopen this owner must not recreate/repair the fixture's index.
class VectorStore : public lattice::lattice_db {
public:
    using lattice::lattice_db::lattice_db;
    void create_fixture() {
        lattice::model_schema schema;
        schema.table_name = model;
        lattice::property_descriptor label;
        label.name = "label"; label.type = lattice::column_type::text;
        lattice::property_descriptor embedding;
        embedding.name = "embedding"; embedding.type = lattice::column_type::blob;
        embedding.is_vector = true;
        schema.properties = {label, embedding};
        create_model_table_public(schema);
        // Qualify ordinary flat vec0 DML, not unrelated IVF training commands.
        ensure_vec0_table(model, "embedding", 4);
    }
};

int64_t scalar(lattice::database& db, const std::string& sql,
               const std::vector<lattice::column_value_t>& values = {}) {
    return std::get<int64_t>(db.query(sql, values).at(0).at("n"));
}

void insert_row(lattice::database& db, const std::string& id, float value) {
    db.execute("INSERT INTO SavepointVectorDoc(globalId,label,embedding) VALUES(?,?,?)",
               {id, id, pack_floats({value, 0, 0, 0})});
}

int64_t row_id(lattice::database& db, const std::string& id) {
    return scalar(db, "SELECT id AS n FROM SavepointVectorDoc WHERE globalId=?", {id});
}

int64_t mutate(lattice::database& db, Mutation mutation) {
    if (mutation == Mutation::insert) {
        insert_row(db, "target", 1);
        return row_id(db, "target");
    }
    const auto id = row_id(db, "target");
    if (mutation == Mutation::update) {
        db.execute("UPDATE SavepointVectorDoc SET embedding=? WHERE globalId='target'",
                   {pack_floats({1, 0, 0, 0})});
    } else {
        db.execute("DELETE FROM SavepointVectorDoc WHERE globalId='target'");
    }
    return id;
}

const char* operation(Mutation mutation) {
    switch (mutation) {
        case Mutation::insert: return "INSERT";
        case Mutation::update: return "UPDATE";
        case Mutation::erase: return "DELETE";
    }
    return "invalid";
}

// Match remote apply's post-model SQL ordering: received AuditLog insertion,
// then receiving-channel loop-prevention state. This is a rollback-primitive
// fixture, not an invocation of apply_remote_changes_for or its ACK policy.
void bookkeeping(lattice::database& db, const std::string& id,
                 const std::string& row, int64_t local_id, const std::string& op) {
    db.execute(R"(
        INSERT INTO AuditLog(globalId,tableName,operation,rowId,globalRowId,
                             changedFields,changedFieldsNames,isFromRemote,isSynchronized,timestamp)
        VALUES(?,?,?,?,?,'{}','[]',1,0,1789819200)
    )", {id, std::string(model), op, local_id, row});
    db.execute(R"(
        INSERT INTO _lattice_sync_state(audit_entry_id,sync_id,is_synchronized)
        SELECT id,?,1 FROM AuditLog WHERE globalId=?
    )", {std::string(channel), id});
}

struct FailureCounter {
    sqlite3* handle;
    int hits = 0;
    int registration;
    explicit FailureCounter(lattice::database& db) : handle(db.handle()) {
        registration = sqlite3_create_function_v2(handle, "vector_failure_stage", 0,
            SQLITE_UTF8, &hits, [](sqlite3_context* context, int, sqlite3_value**) noexcept {
                ++*static_cast<int*>(sqlite3_user_data(context));
                sqlite3_result_int(context, 1);
            }, nullptr, nullptr, nullptr);
    }
    ~FailureCounter() {
        sqlite3_create_function_v2(handle, "vector_failure_stage", 0, SQLITE_UTF8,
                                   nullptr, nullptr, nullptr, nullptr, nullptr);
    }
    FailureCounter(const FailureCounter&) = delete;
    FailureCounter& operator=(const FailureCounter&) = delete;
};

Snapshot snapshot(lattice::database& db) {
    // Actual float32 bytes and shadow BLOBs, including validity/rowid slots,
    // not just a row-count oracle. Both physical and declared vector-chunk
    // rowids are included because this shadow table's PK is not a rowid alias.
    return {
        {"model", db.query("SELECT id,globalId,label,embedding FROM SavepointVectorDoc ORDER BY id")},
        {"virtual", db.query("SELECT global_id,embedding FROM _SavepointVectorDoc_embedding_vec ORDER BY global_id")},
        {"rowids", db.query("SELECT rowid,id,chunk_id,chunk_offset FROM _SavepointVectorDoc_embedding_vec_rowids ORDER BY rowid")},
        {"chunks", db.query("SELECT chunk_id,size,validity,rowids FROM _SavepointVectorDoc_embedding_vec_chunks ORDER BY chunk_id")},
        {"vectors", db.query("SELECT _rowid_ AS physical_rowid,rowid,vectors FROM _SavepointVectorDoc_embedding_vec_vector_chunks00 ORDER BY _rowid_")},
        {"sequences", db.query(R"(SELECT name,seq FROM sqlite_sequence WHERE name IN
            ('SavepointVectorDoc','_SavepointVectorDoc_embedding_vec_rowids',
             '_SavepointVectorDoc_embedding_vec_chunks','AuditLog') ORDER BY name)")},
        {"audit", db.query("SELECT * FROM AuditLog WHERE tableName='SavepointVectorDoc' ORDER BY id")},
        {"receiving", db.query(R"(SELECT s.* FROM _lattice_sync_state s JOIN AuditLog a
            ON a.id=s.audit_entry_id WHERE a.tableName='SavepointVectorDoc' ORDER BY s.audit_entry_id,s.sync_id)")}
    };
}

void expect_snapshot(lattice::database& db, const Snapshot& expected) {
    const auto actual = snapshot(db);
    ASSERT_EQ(actual.size(), expected.size());
    for (const auto& [name, rows] : expected) {
        // Avoid dumping entire vector chunks if an assertion fails.
        EXPECT_TRUE(actual.at(name) == rows) << "changed durable component: " << name;
    }
}

void expect_contents(lattice::database& db, const Contents& expected) {
    const auto rows = db.query("SELECT globalId,label,embedding FROM SavepointVectorDoc ORDER BY globalId");
    ASSERT_EQ(rows.size(), expected.size());
    size_t index = 0;
    for (const auto& [id, value] : expected) {
        EXPECT_EQ(std::get<std::string>(rows[index].at("globalId")), id);
        EXPECT_EQ(std::get<std::string>(rows[index].at("label")), id);
        EXPECT_EQ(std::get<std::vector<uint8_t>>(rows[index].at("embedding")), pack_floats({value, 0, 0, 0}));
        ++index;
    }
    const auto vectors = db.query("SELECT global_id,embedding FROM _SavepointVectorDoc_embedding_vec ORDER BY global_id");
    ASSERT_EQ(vectors.size(), expected.size());
    index = 0;
    for (const auto& [id, value] : expected) {
        EXPECT_EQ(std::get<std::string>(vectors[index].at("global_id")), id);
        EXPECT_EQ(std::get<std::vector<uint8_t>>(vectors[index].at("embedding")), pack_floats({value, 0, 0, 0}));
        ++index;
    }
}

void expect_public_search(VectorStore& store, const Contents& expected) {
    // Check bytes BEFORE querying and prove neither public query healed data.
    expect_contents(store.db(), expected);
    const auto before = snapshot(store.db());
    const auto changes = sqlite3_total_changes64(store.db().handle());
    std::vector<std::pair<std::string, float>> ordered(expected.begin(), expected.end());
    std::sort(ordered.begin(), ordered.end(), [](const auto& a, const auto& b) { return a.second < b.second; });
    for (const auto& predicate : {std::optional<std::string>{},
                                 std::optional<std::string>{"SavepointVectorDoc.label <> 'never-present'"}}) {
        // No filter uses native vec0 MATCH. A filter uses the public exhaustive
        // vec_distance_L2 path. Distances are distinct small exact integers.
        const auto results = store.knn_query(model, "embedding", pack_floats({0, 0, 0, 0}),
            10, lattice::lattice_db::distance_metric::l2, predicate);
        ASSERT_EQ(results.size(), ordered.size());
        for (size_t index = 0; index < ordered.size(); ++index) {
            EXPECT_EQ(results[index].global_id, ordered[index].first);
            EXPECT_NEAR(results[index].distance, ordered[index].second, 0.00001);
        }
    }
    EXPECT_EQ(sqlite3_total_changes64(store.db().handle()), changes) << "search must not repair this fixture";
    expect_snapshot(store.db(), before);
}

void expect_bookkeeping(lattice::database& db, const char* id, int64_t count) {
    EXPECT_EQ(scalar(db, "SELECT COUNT(*) AS n FROM AuditLog WHERE globalId=?", {std::string(id)}), count);
    EXPECT_EQ(scalar(db, R"(SELECT COUNT(*) AS n FROM _lattice_sync_state s JOIN AuditLog a
        ON a.id=s.audit_entry_id WHERE a.globalId=? AND s.sync_id=? AND s.is_synchronized=1)",
        {std::string(id), std::string(channel)}), count);
}

void run_vector_savepoint_case(Mutation mutation) {
    SCOPED_TRACE(operation(mutation));
    TempDB file{"sync_vector_savepoint"};
    Contents initial{{"anchor", 8}, {"far", 12}};
    if (mutation != Mutation::insert) initial.emplace("target", 4);
    auto attempted = initial;
    if (mutation == Mutation::erase) attempted.erase("target");
    else attempted["target"] = 1;
    auto after_successor = initial; after_successor["successor"] = 2;
    auto after_retry = attempted; after_retry["successor"] = 2;
    Snapshot committed, retried;
    {
        VectorStore store{fixture_config(file)};
        store.create_fixture();
        for (const auto& [id, value] : initial) insert_row(store.db(), id, value);
        expect_public_search(store, initial);
        ASSERT_FALSE(store.db().is_in_transaction());
        FailureCounter failure(store.db());
        ASSERT_EQ(failure.registration, SQLITE_OK);
        store.db().execute(R"(
            CREATE TRIGGER vector_reject_bookkeeping BEFORE INSERT ON _lattice_sync_state
            WHEN NEW.sync_id='vector-savepoint-receiver' AND EXISTS (
                SELECT 1 FROM AuditLog WHERE id=NEW.audit_entry_id
                AND globalId='00000000-0000-4000-8000-000000000401')
            BEGIN
                SELECT vector_failure_stage();
                SELECT RAISE(ABORT, 'vector receiving bookkeeping rejected');
            END
        )");
        store.begin_transaction();
        store.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        const auto before = snapshot(store.db());
        store.db().execute("SAVEPOINT vector_entry");
        const auto target_id = mutate(store.db(), mutation);
        expect_contents(store.db(), attempted);
        EXPECT_TRUE(snapshot(store.db()).at("model") != before.at("model"));
        EXPECT_TRUE(snapshot(store.db()).at("virtual") != before.at("virtual"));
        EXPECT_THROW(bookkeeping(store.db(), failed_id, "target", target_id, operation(mutation)), lattice::db_error);
        ASSERT_EQ(failure.hits, 1) << "failure must occur after model/vector SQL and received AuditLog insertion";
        ASSERT_TRUE(store.db().is_in_transaction()) << "ABORT must leave the owned outer transaction active";
        EXPECT_EQ(scalar(store.db(), "SELECT COUNT(*) AS n FROM AuditLog WHERE globalId=?", {std::string(failed_id)}), 1);
        store.db().execute("ROLLBACK TO vector_entry");
        store.db().execute("RELEASE vector_entry");
        ASSERT_TRUE(store.db().is_in_transaction());
        expect_snapshot(store.db(), before);
        expect_public_search(store, initial);
        expect_bookkeeping(store.db(), failed_id, 0);

        // An independent next entry succeeds in this SAME outer transaction.
        store.db().execute("SAVEPOINT vector_entry");
        insert_row(store.db(), "successor", 2);
        bookkeeping(store.db(), successor_id, "successor", row_id(store.db(), "successor"), "INSERT");
        store.db().execute("RELEASE vector_entry");
        store.db().execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
        store.commit();
        EXPECT_EQ(failure.hits, 1);
        ASSERT_FALSE(store.db().is_in_transaction());
        EXPECT_EQ(scalar(store.db(), "SELECT disabled AS n FROM _SyncControl WHERE id=1"), 0);
        expect_bookkeeping(store.db(), failed_id, 0);
        expect_bookkeeping(store.db(), successor_id, 1);
        expect_public_search(store, after_successor);
        committed = snapshot(store.db());
    }
    {
        // Plain database open cannot run a model migration/reconcile worker.
        // Inspect persisted bytes before constructing the reopened owner.
        lattice::database raw(file.str());
        expect_snapshot(raw, committed);
        expect_contents(raw, after_successor);
    }
    {
        VectorStore store{fixture_config(file)};
        expect_snapshot(store.db(), committed);
        expect_public_search(store, after_successor);
        store.db().execute("DROP TRIGGER vector_reject_bookkeeping");
        store.begin_transaction();
        store.db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        store.db().execute("SAVEPOINT vector_entry");
        const auto target_id = mutate(store.db(), mutation);
        bookkeeping(store.db(), failed_id, "target", target_id, operation(mutation));
        store.db().execute("RELEASE vector_entry");
        store.db().execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
        store.commit();
        ASSERT_FALSE(store.db().is_in_transaction());
        expect_bookkeeping(store.db(), failed_id, 1);
        expect_bookkeeping(store.db(), successor_id, 1);
        expect_public_search(store, after_retry);
        retried = snapshot(store.db());
    }
    {
        lattice::database raw(file.str());
        expect_snapshot(raw, retried);
        expect_contents(raw, after_retry);
    }
    {
        VectorStore reopened{fixture_config(file)};
        expect_snapshot(reopened.db(), retried);
        expect_bookkeeping(reopened.db(), failed_id, 1);
        expect_bookkeeping(reopened.db(), successor_id, 1);
        expect_public_search(reopened, after_retry);
    }
}
} // namespace

TEST(SyncVectorSavepoint, InsertRollbackRestoresShadowBytesAndSearchAcrossRetryAndReopen) {
    run_vector_savepoint_case(Mutation::insert);
}
TEST(SyncVectorSavepoint, UpdateRollbackRestoresShadowBytesAndSearchAcrossRetryAndReopen) {
    run_vector_savepoint_case(Mutation::update);
}
TEST(SyncVectorSavepoint, DeleteRollbackRestoresShadowBytesAndSearchAcrossRetryAndReopen) {
    run_vector_savepoint_case(Mutation::erase);
}
