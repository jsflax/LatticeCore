#include <gtest/gtest.h>
#ifndef __linux__
#include <lattice.hpp>
#include <dynamic_object.hpp>
#include <exception>
#include <filesystem>
#include <random>
#include <string_view>

namespace {

struct LinkSnapshotDatabase {
    std::filesystem::path path = std::filesystem::temp_directory_path() /
        ("bridge_link_snapshot_" + std::to_string(std::random_device{}()) + ".sqlite");
    ~LinkSnapshotDatabase() {
        std::filesystem::remove(path);
        std::filesystem::remove(path.string() + "-wal");
        std::filesystem::remove(path.string() + "-shm");
    }
    std::string str() const { return path.string(); }
};

// A different database connection can apply a replicated unlink after the
// target row has been read but before get_object copies the managed value.
// PROFILE fires when that SELECT completes, making the interleave exact and
// independent of scheduling, sleeps, or a production-only test hook.
struct UnlinkAfterTargetRead {
    lattice::database& writer;
    int targetReads = 0;
    bool unlinked = false;
    std::exception_ptr error;

    static int trace(unsigned event, void* context, void* statement, void*) noexcept {
        if (event != SQLITE_TRACE_PROFILE) return 0;
        auto& state = *static_cast<UnlinkAfterTargetRead*>(context);
        const char* sql = sqlite3_sql(static_cast<sqlite3_stmt*>(statement));
        if (!sql || std::string_view(sql) !=
                "SELECT * FROM SnapshotAgent WHERE globalId = ?") return 0;
        ++state.targetReads;
        if (state.unlinked || state.error) return 0;
        try {
            state.writer.execute(
                "DELETE FROM _SnapshotTodo_SnapshotAgent_assignee "
                "WHERE lhs = 'todo-1'");
            state.unlinked = true;
        } catch (...) {
            state.error = std::current_exception();
        }
        return 0;
    }
};

struct ScopedTrace {
    sqlite3* handle;
    ~ScopedTrace() { sqlite3_trace_v2(handle, 0, nullptr, nullptr); }
};

} // namespace

TEST(BridgeLinkSnapshot, RetainsResolvedTargetWhenAnotherConnectionUnlinksIt) {
    LinkSnapshotDatabase tmp;
    lattice::property_descriptor name;
    name.name = "name";
    name.kind = lattice::property_kind::primitive;
    name.type = lattice::column_type::text;
    lattice::property_descriptor assignee;
    assignee.name = "assignee";
    assignee.kind = lattice::property_kind::link;
    assignee.type = lattice::column_type::text;
    assignee.target_table = "SnapshotAgent";

    lattice::swift_schema_entry todoSchema;
    todoSchema.table_name = "SnapshotTodo";
    todoSchema.properties["assignee"] = assignee;
    lattice::swift_schema_entry agentSchema;
    agentSchema.table_name = "SnapshotAgent";
    agentSchema.properties["name"] = name;
    lattice::SchemaVector schemas{todoSchema, agentSchema};
    std::unique_ptr<lattice::swift_lattice_ref> owner(
        lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas));
    auto& db = *owner->get();
    db.db().execute("INSERT INTO SnapshotTodo(globalId) VALUES('todo-1')");
    db.db().execute(
        "INSERT INTO SnapshotAgent(globalId, name) VALUES('agent-1', 'Willow')");
    db.db().execute(
        "INSERT INTO _SnapshotTodo_SnapshotAgent_assignee(lhs, rhs) "
        "VALUES('todo-1', 'agent-1')");

    auto todos = db.objects("SnapshotTodo");
    ASSERT_EQ(todos.size(), 1u);
    lattice::dynamic_object todo(todos[0]);
    lattice::lattice_db foreignWriter{lattice::configuration(tmp.str())};
    UnlinkAfterTargetRead state{foreignWriter.db()};
    auto* readHandle = db.read_db().handle();
    ASSERT_EQ(sqlite3_trace_v2(readHandle, SQLITE_TRACE_PROFILE,
                             &UnlinkAfterTargetRead::trace, &state), SQLITE_OK);
    ScopedTrace trace{readHandle};

    // Previously the schema population and return each resolved the link
    // again; the first such read after the unlink dereferenced nullptr.
    auto resolved = todo.get_object("assignee");
    ASSERT_FALSE(state.error) << "The foreign unlink must commit during the target read";
    ASSERT_TRUE(state.unlinked);
    EXPECT_EQ(state.targetReads, 1);
    ASSERT_NE(resolved.lattice, nullptr);
    EXPECT_EQ(resolved.get_string("globalId"), "agent-1");
    EXPECT_EQ(resolved.get_string("name"), "Willow");

    // Retaining a resolved value must not turn the relationship into a stale
    // cross-call cache: the next lookup sees the now-empty relationship.
    EXPECT_EQ(todo.get_object("assignee").lattice, nullptr);
    EXPECT_TRUE(foreignWriter.db().query(
        "SELECT rhs FROM _SnapshotTodo_SnapshotAgent_assignee "
        "WHERE lhs = 'todo-1'").empty());
    EXPECT_EQ(db.objects("SnapshotAgent").size(), 1u);
}
#endif
