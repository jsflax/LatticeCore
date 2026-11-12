#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
#include "TestHelpers.hpp"
#include <lattice/sync.hpp>
#include <fstream>

// ============================================================================
// attach/detach — ATT-1 (idempotence, mutex, null-tolerant handles) and
// ATT-2 (view regeneration, detach). Exit tests from the 1.0.0 plan.
// ============================================================================

namespace {
int64_t temp_view_count(lattice::database& db) {
    auto rows = db.query("SELECT COUNT(*) AS c FROM sqlite_temp_master WHERE type='view'");
    return rows.empty() ? -1 : std::get<int64_t>(rows[0].at("c"));
}

int64_t person_count(lattice::lattice_db& db) {
    auto rows = db.db().query("SELECT COUNT(*) AS c FROM TestPerson");
    return rows.empty() ? -1 : std::get<int64_t>(rows[0].at("c"));
}

std::string quoted_alias_for(const TempDB& file) {
    std::string quoted = "\"";
    for (char c : file.path.stem().string()) {
        quoted += c;
        if (c == '"') quoted += '"';
    }
    return quoted + '"';
}

std::vector<lattice::database::row_t> attachment_topology(lattice::database& db) {
    db.query("SELECT name FROM sqlite_temp_master");
    return db.query("PRAGMA database_list");
}

struct DenyAttach {
    sqlite3* handle;
    int attempts = 0;
    explicit DenyAttach(sqlite3* value) : handle(value) {
        if (sqlite3_set_authorizer(handle,
                [](void* context, int action, const char*, const char*, const char*, const char*) {
                    if (action != SQLITE_ATTACH) return SQLITE_OK;
                    ++static_cast<DenyAttach*>(context)->attempts;
                    return SQLITE_DENY;
                }, this) != SQLITE_OK)
            throw std::runtime_error("Cannot install attachment refusal");
    }
    ~DenyAttach() { sqlite3_set_authorizer(handle, nullptr, nullptr); }
};
} // namespace

TEST(Attach, AttachTwiceIsNoOp) {
    TempDB a_path{"attach_a"}, b_path{"attach_b"};
    lattice::lattice_db a{lattice::configuration(a_path.str())};
    lattice::lattice_db b{lattice::configuration(b_path.str())};
    a.add(TestPerson{"main-1", 1, std::nullopt});
    b.add(TestPerson{"other-1", 2, std::nullopt});

    a.attach(b);
    const auto views_after_first = temp_view_count(a.db());
    const auto count_after_first = person_count(a);

    a.attach(b);  // must be a clean no-op, not an SQLite duplicate-alias error
    EXPECT_EQ(temp_view_count(a.db()), views_after_first);
    EXPECT_EQ(person_count(a), count_after_first);
    EXPECT_EQ(count_after_first, 2);
}

TEST(Attach, WritableSourceAdoptsAnExistingSQLiteAlias) {
    for (bool reader_already_attached : {false, true}) {
        SCOPED_TRACE(reader_already_attached);
        TempDB main_file{"writable_existing_main"}, source_file{"writable_existing_source"};
        lattice::lattice_db receiver{lattice::configuration(main_file.str())};
        lattice::lattice_db source{lattice::configuration(source_file.str())};
        receiver.add(TestPerson{"main", 1, std::nullopt});
        source.add(TestPerson{"source", 2, std::nullopt});
        const auto alias = quoted_alias_for(source_file);
        receiver.db().execute("ATTACH DATABASE ? AS " + alias, {source_file.str()});
        if (reader_already_attached)
            receiver.read_db().execute("ATTACH DATABASE ? AS " + alias, {source_file.str()});

        ASSERT_NO_THROW(receiver.attach(source));
        EXPECT_EQ(person_count(receiver), 2);
        EXPECT_EQ(std::get<int64_t>(receiver.read_db().query(
            "SELECT COUNT(*) AS c FROM TestPerson")[0].at("c")), 2);
        receiver.db().execute("UPDATE " + alias + ".TestPerson SET name='updated'");
        EXPECT_EQ(std::get<std::string>(source.db().query(
            "SELECT name FROM TestPerson")[0].at("name")), "updated");

        const auto writer_before = attachment_topology(receiver.db());
        const auto reader_before = attachment_topology(receiver.read_db());
        ASSERT_NO_THROW(receiver.attach(source));
        EXPECT_EQ(attachment_topology(receiver.db()), writer_before);
        EXPECT_EQ(attachment_topology(receiver.read_db()), reader_before);
        receiver.detach(source);
        EXPECT_EQ(person_count(receiver), 1);
        const auto name = source_file.path.stem().string();
        EXPECT_EQ(sqlite3_db_readonly(receiver.db().handle(), name.c_str()), -1);
        EXPECT_EQ(sqlite3_db_readonly(receiver.read_db().handle(), name.c_str()), -1);
    }
}

TEST(Attach, WritableReservedAliasToleranceDoesNotExtendToReaderReopen) {
    TempDB directory{"writable_reserved_alias_dir"};
    ASSERT_TRUE(std::filesystem::create_directory(directory.path));
    // The child is destroyed before its directory holder, including on failure.
    TempDB source_file;
    source_file.path = directory.path / "main.sqlite";
    struct RemoveFixtureSignal {
        std::filesystem::path path;
        ~RemoveFixtureSignal() {
            std::error_code ignored;
            std::filesystem::remove(path, ignored);
        }
    } source_signal{source_file.str() + "-signal"};
    TempDB main_file{"writable_reserved_alias_receiver"};
    lattice::lattice_db receiver{lattice::configuration(main_file.str())};
    lattice::lattice_db source{lattice::configuration(source_file.str())};
    receiver.add(TestPerson{"main", 1, std::nullopt});
    source.add(TestPerson{"source", 2, std::nullopt});
    ASSERT_EQ(quoted_alias_for(source_file), "\"main\"");

    // Explicit writable attach retains the released duplicate-alias tolerance.
    // Restoring that tracked alias onto a fresh reader never had that tolerance.
    ASSERT_NO_THROW(receiver.attach(source));
    receiver.close_read_db();
    std::string error;
    try { receiver.reopen_read_db(); }
    catch (const lattice::db_error& failure) { error = failure.what(); }
    EXPECT_NE(error.find("database main is already in use"), std::string::npos);
    EXPECT_EQ(std::get<std::string>(receiver.db().query(
        "SELECT name FROM main.TestPerson")[0].at("name")), "main");
}

TEST(Attach, WritableAttachFailureUnwindsOnlyNewAliases) {
    for (bool writer_already_attached : {false, true}) {
        for (bool reader_already_attached : {false, true}) {
            SCOPED_TRACE(writer_already_attached);
            SCOPED_TRACE(reader_already_attached);
            TempDB main_file{"writable_unwind_main"}, source_file{"writable_unwind_source"};
            lattice::lattice_db receiver{lattice::configuration(main_file.str())};
            lattice::lattice_db source{lattice::configuration(source_file.str())};
            receiver.add(TestPerson{"main", 1, std::nullopt});
            source.add(TestPerson{"source", 2, std::nullopt});
            const auto alias = quoted_alias_for(source_file);
            if (writer_already_attached)
                receiver.db().execute("ATTACH DATABASE ? AS " + alias, {source_file.str()});
            if (reader_already_attached)
                receiver.read_db().execute("ATTACH DATABASE ? AS " + alias, {source_file.str()});
            const auto writer_before = attachment_topology(receiver.db());
            const auto reader_before = attachment_topology(receiver.read_db());
            const auto views_before = temp_view_count(receiver.db());

            {
                // The authorizer refuses ATTACH even when the reader's alias
                // already exists. Neither that alias nor an existing writer
                // alias belongs to this failed attempt's unwind.
                DenyAttach refusal(receiver.read_db().handle());
                EXPECT_THROW(receiver.attach(source), lattice::db_error);
                EXPECT_EQ(refusal.attempts, 1);
            }
            EXPECT_EQ(attachment_topology(receiver.db()), writer_before);
            EXPECT_EQ(attachment_topology(receiver.read_db()), reader_before);
            EXPECT_EQ(temp_view_count(receiver.db()), views_before);
            EXPECT_EQ(person_count(receiver), 1);
            if (writer_already_attached)
                EXPECT_EQ(std::get<std::string>(receiver.db().query(
                    "SELECT name FROM " + alias + ".TestPerson")[0].at("name")), "source");
            if (reader_already_attached)
                EXPECT_EQ(std::get<std::string>(receiver.read_db().query(
                    "SELECT name FROM " + alias + ".TestPerson")[0].at("name")), "source");

            // A retry must still build the union: the failed attempt must not
            // leave attachment bookkeeping that turns it into a no-op.
            ASSERT_NO_THROW(receiver.attach(source));
            EXPECT_EQ(person_count(receiver), 2);
            EXPECT_EQ(std::get<int64_t>(receiver.read_db().query(
                "SELECT COUNT(*) AS c FROM TestPerson")[0].at("c")), 2);
            receiver.detach(source);
        }
    }
}

// Pins the null-deref fix: sync-enabled lattices have no read_db_; the old
// implementation dereferenced it unconditionally.
TEST(Attach, AttachOnSyncEnabledLatticeDoesNotCrash) {
    TempDB a_path{"attach_sync_a"}, b_path{"attach_sync_b"};
    // A sync-enabled configuration (websocket_url + token) opens WITHOUT a
    // read_db_ — exactly the shape whose attach null-dereferenced before.
    lattice::configuration cfg(a_path.str());
    cfg.websocket_url = "ws://localhost:9/sync";
    cfg.authorization_token = "t";
    lattice::lattice_db a{cfg};
    a.add(TestPerson{"main-1", 1, std::nullopt});

    lattice::lattice_db b{lattice::configuration(b_path.str())};
    b.add(TestPerson{"other-1", 2, std::nullopt});

    a.attach(b);
    EXPECT_EQ(person_count(a), 2);
    a.detach(b);
    EXPECT_EQ(person_count(a), 1);
}

TEST(Attach, SchemaMismatchThrowsCleanlyWithNoSideEffects) {
    TempDB a_path{"attach_mm_a"}, b_path{"attach_mm_b"};
    lattice::lattice_db a{lattice::configuration(a_path.str())};
    lattice::lattice_db b{lattice::configuration(b_path.str())};
    a.add(TestPerson{"main-1", 1, std::nullopt});
    // Diverge b's TestPerson schema AFTER ensure ran.
    b.db().execute("ALTER TABLE TestPerson ADD COLUMN extra TEXT");

    const auto views_before = temp_view_count(a.db());
    const auto dbs_before = a.db().query("PRAGMA database_list").size();
    EXPECT_THROW(a.attach(b), std::runtime_error);
    // No dangling ATTACH, no half-created views, main still readable.
    EXPECT_EQ(temp_view_count(a.db()), views_before);
    EXPECT_EQ(person_count(a), 1);
    // The failed attach left no alias behind (database_list includes the
    // 'temp' schema — compare against the pre-attach baseline).
    EXPECT_EQ(a.db().query("PRAGMA database_list").size(), dbs_before)
        << "failed attach left the database attached";
}

TEST(Attach, DetachRestoresMainVisibility) {
    TempDB a_path{"attach_dv_a"}, b_path{"attach_dv_b"};
    lattice::lattice_db a{lattice::configuration(a_path.str())};
    lattice::lattice_db b{lattice::configuration(b_path.str())};
    a.add(TestPerson{"main-1", 1, std::nullopt});
    b.add(TestPerson{"other-1", 2, std::nullopt});
    b.add(TestPerson{"other-2", 3, std::nullopt});

    a.attach(b);
    EXPECT_EQ(person_count(a), 3);  // union view: main + attached

    a.detach(b);
    EXPECT_EQ(person_count(a), 1);  // view gone — main table visible again
    // detach of something not attached: idempotent no-op
    a.detach(b);
    EXPECT_EQ(person_count(a), 1);
}

// The old CREATE ... IF NOT EXISTS meant the SECOND alias sharing a table
// name never joined the union view. Regeneration makes multi-alias
// attach/detach order-independent.
TEST(Attach, MultiAliasUnionAndPartialDetach) {
    TempDB a_path{"attach_ma_a"}, b_path{"attach_ma_b"}, c_path{"attach_ma_c"};
    lattice::lattice_db a{lattice::configuration(a_path.str())};
    lattice::lattice_db b{lattice::configuration(b_path.str())};
    lattice::lattice_db c{lattice::configuration(c_path.str())};
    a.add(TestPerson{"main-1", 1, std::nullopt});
    b.add(TestPerson{"b-1", 2, std::nullopt});
    c.add(TestPerson{"c-1", 3, std::nullopt});
    c.add(TestPerson{"c-2", 4, std::nullopt});

    a.attach(b);
    a.attach(c);
    EXPECT_EQ(person_count(a), 4) << "second alias must join the union view";

    a.detach(b);
    EXPECT_EQ(person_count(a), 3) << "remaining alias views must be regenerated";
    a.detach(c);
    EXPECT_EQ(person_count(a), 1);
}

TEST(Attach, ConcurrentAttachRaceIsSafe) {
    TempDB a_path{"attach_cc_a"}, b_path{"attach_cc_b"};
    lattice::lattice_db a{lattice::configuration(a_path.str())};
    lattice::lattice_db b{lattice::configuration(b_path.str())};
    a.add(TestPerson{"main-1", 1, std::nullopt});
    b.add(TestPerson{"other-1", 2, std::nullopt});

    std::atomic<int> failures{0};
    std::thread t1([&] { try { a.attach(b); } catch (...) { failures.fetch_add(1); } });
    std::thread t2([&] { try { a.attach(b); } catch (...) { failures.fetch_add(1); } });
    t1.join(); t2.join();

    EXPECT_EQ(failures.load(), 0) << "concurrent attach of the same db must be idempotent";
    EXPECT_EQ(person_count(a), 2);
}

// P0 fix (Jul 11 review): attaching a NAMED-MEMORY lattice to a FILE-backed
// lattice must resolve the memory URI, not create a literal disk file named
// "file:...". Requires SQLITE_OPEN_URI on the receiver's connections.
TEST(Attach, FileLatticeAttachesNamedMemoryLattice) {
    TempDB main_path{"attach_uri_recv"};
    lattice::lattice_db main_db{lattice::configuration(main_path.str())};
    lattice::lattice_db mem{lattice::configuration("file:attach_mem_src?mode=memory&cache=shared")};

    mem.add(TestPerson{"memory-row", 42, std::nullopt});
    main_db.add(TestPerson{"file-row", 1, std::nullopt});

    main_db.attach(mem);
    EXPECT_EQ(person_count(main_db), 2)
        << "memory lattice's rows must be visible through the union view";

    // And no junk literal file appeared in cwd.
    EXPECT_NE(std::ifstream("file:attach_mem_src?mode=memory&cache=shared").good(), true);

    main_db.detach(mem);
    EXPECT_EQ(person_count(main_db), 1);
}

// Names/paths containing a single quote must not break the ATTACH statement.
TEST(Attach, AttachPathWithSingleQuoteIsEscaped) {
    TempDB main_path{"attach_quote_recv"};
    lattice::lattice_db main_db{lattice::configuration(main_path.str())};
    lattice::lattice_db mem{lattice::configuration("file:qu'ote?mode=memory&cache=shared")};
    mem.add(TestPerson{"quoted", 7, std::nullopt});

    EXPECT_NO_THROW(main_db.attach(mem));
    EXPECT_EQ(person_count(main_db), 1);
    main_db.detach(mem);
}
