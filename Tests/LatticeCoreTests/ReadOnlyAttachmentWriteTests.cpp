#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
#include "TestHelpers.hpp"
#include <fstream>
#include <iterator>

namespace {
using Rows = std::vector<lattice::database::row_t>;

std::string quoted(const std::string& value) {
    std::string result = "\"";
    for (char c : value) { result += c; if (c == '"') result += c; }
    return result + '"';
}
std::string alias_for(const std::string& path) {
    return std::filesystem::path(path).filename().replace_extension().string();
}
struct RawFixture {
    sqlite3* db = nullptr;
    explicit RawFixture(const std::string& path) {
        const int rc = sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READWRITE, nullptr);
        if (rc != SQLITE_OK) {
            const std::string message = db ? sqlite3_errmsg(db) : "No fixture handle";
            if (db) sqlite3_close(db);
            throw std::runtime_error(message);
        }
    }
    ~RawFixture() { sqlite3_close(db); }
    void execute(const std::string& sql) {
        if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr) != SQLITE_OK)
            throw std::runtime_error(sqlite3_errmsg(db));
    }
};

void seed(const TempDB& file, bool legacy = false) {
    { lattice::lattice_db owner{lattice::configuration(file.str())}; }
    // Create the indexed fixture after Lattice teardown, so the receiver's
    // close-time optimize has genuinely missing statistics to consider.
    RawFixture raw(file.str());
    raw.execute("CREATE TABLE AliasItem(id INTEGER PRIMARY KEY,globalId TEXT UNIQUE NOT NULL,"
                "value TEXT NOT NULL" + std::string(legacy ? "" : ",note TEXT DEFAULT 'default'") + ")");
    raw.execute("CREATE INDEX alias_item_value ON AliasItem(value)");
    raw.execute("INSERT INTO AliasItem(id,globalId,value) VALUES(1,'fixture-id','original')");
}

struct FileImage {
    bool exists;
    std::string bytes;
    explicit FileImage(const std::string& path) : exists(std::filesystem::exists(path)) {
        if (!exists) return;
        std::ifstream input(path, std::ios::binary);
        if (!input) throw std::runtime_error("Cannot read fixture image");
        bytes.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    }
    bool operator==(const FileImage&) const = default;
};
struct SourceSnapshot {
    FileImage main, wal;
    Rows schema, rows;
    explicit SourceSnapshot(const TempDB& file) : main(file.str()), wal(file.str() + "-wal") {
        lattice::database reader(file.str(), lattice::database::open_mode::read_only);
        schema = reader.query("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name");
        rows = reader.query("SELECT * FROM AliasItem ORDER BY id");
    }
    bool operator==(const SourceSnapshot&) const = default;
};

void expect_read_only(lattice::database& connection, const std::string& alias) {
    ASSERT_EQ(sqlite3_db_readonly(connection.handle(), alias.c_str()), 1);
    const std::string table = quoted(alias) + ".AliasItem";
    for (const auto& sql : {
        "UPDATE " + table + " SET value='changed' WHERE id=1",
        "INSERT INTO " + table + "(id,globalId,value) VALUES(2,'new-id','changed')",
        "DELETE FROM " + table + " WHERE id=1",
        "CREATE TABLE " + quoted(alias) + ".RejectedWrite(id INTEGER)"}) {
        const int rc = sqlite3_exec(connection.handle(), sql.c_str(), nullptr, nullptr, nullptr);
        EXPECT_EQ(rc & 0xff, SQLITE_READONLY) << sql << ": " << sqlite3_errmsg(connection.handle());
    }
    const auto rows = connection.query("SELECT value FROM " + table);
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("value")), "original");
}

Rows topology(lattice::database& db) {
    // Establish the TEMP schema before comparing database_list.
    db.query("SELECT name FROM sqlite_temp_master");
    return db.query("PRAGMA database_list");
}
} // namespace

TEST(ReadOnlyAttachmentWrites, ProtectsBothHandlesAndRestoresPolicyAfterSourceCloses) {
    for (bool legacy : {false, true}) {
        SCOPED_TRACE(legacy);
        TempDB main_file{"readonly_alias_main"}, source_file{"readonly_alias_source"};
        seed(main_file); seed(source_file, legacy);
        auto source_config = lattice::configuration(source_file.str());
        source_config.read_only = true;
        auto source = std::make_unique<lattice::lattice_db>(source_config);
        const SourceSnapshot before(source_file);
        const auto alias = alias_for(source_file.str());
        {
            lattice::lattice_db receiver{lattice::configuration(main_file.str())};
            receiver.attach(*source);
            const auto attached = topology(receiver.db());
            ASSERT_NO_THROW(receiver.attach(*source));
            EXPECT_EQ(topology(receiver.db()), attached);
            expect_read_only(receiver.db(), alias);
            expect_read_only(receiver.read_db(), alias);
            receiver.db().execute("UPDATE main.AliasItem SET value='main-write' WHERE id=1");
            EXPECT_EQ(std::get<std::string>(receiver.db().query(
                "SELECT value FROM main.AliasItem")[0].at("value")), "main-write");
            if (legacy) {
                const auto rows = receiver.db().query("SELECT note FROM AliasItem WHERE _source<> 'main'");
                ASSERT_EQ(rows.size(), 1u);
                EXPECT_EQ(std::get<std::string>(rows[0].at("note")), "default");
            }
            source->close(); source.reset();
            receiver.close_read_db(); receiver.reopen_read_db();
            expect_read_only(receiver.read_db(), alias);
            receiver.close_write_db(); receiver.reopen_write_db();
            expect_read_only(receiver.db(), alias);
            receiver.db().execute("UPDATE main.AliasItem SET value='after-reopen' WHERE id=1");
            // Deliberately retain the alias through close and destruction.
            receiver.close();
            EXPECT_EQ(SourceSnapshot(source_file), before);
        }
        EXPECT_EQ(SourceSnapshot(source_file), before);
    }
}

TEST(ReadOnlyAttachmentWrites, WritableAttachmentsRemainWritableAndMixedModesAreRefused) {
    for (bool first_read_only : {false, true}) {
        TempDB main_file{"alias_mode_main"}, source_file{"alias_mode_source"};
        seed(main_file); seed(source_file);
        lattice::lattice_db receiver{lattice::configuration(main_file.str())};
        lattice::lattice_db writable{lattice::configuration(source_file.str())};
        auto config = lattice::configuration(source_file.str()); config.read_only = true;
        lattice::lattice_db read_only{config};
        auto& first = first_read_only ? read_only : writable;
        auto& second = first_read_only ? writable : read_only;
        receiver.attach(first);
        const auto before = topology(receiver.db());
        EXPECT_THROW(receiver.attach(second), std::runtime_error);
        EXPECT_EQ(topology(receiver.db()), before);
        const auto alias = alias_for(source_file.str());
        EXPECT_EQ(sqlite3_db_readonly(receiver.db().handle(), alias.c_str()), first_read_only ? 1 : 0);
        if (!first_read_only) {
            receiver.db().execute("UPDATE " + quoted(alias) + ".AliasItem SET value='writable' WHERE id=1");
            EXPECT_EQ(std::get<std::string>(writable.db().query(
                "SELECT value FROM AliasItem")[0].at("value")), "writable");
        }
        receiver.detach(first);
        EXPECT_EQ(sqlite3_db_readonly(receiver.db().handle(), alias.c_str()), -1);
    }
}

TEST(ReadOnlyAttachmentWrites, QuotesFilePathsAndKeepsConfiguredIdentityForDetach) {
    for (bool uri : {false, true}) {
        TempDB main_file{"readonly_path_main"}, source_file{"readonly_%?#'\"é"};
        seed(main_file); seed(source_file, true);
        std::string configured = source_file.str();
        if (uri) {
            // Independently spell the punctuation escapes in the URI fixture.
            for (const auto& [plain, escaped] : std::vector<std::pair<std::string, std::string>>{
                     {"%", "%25"}, {"?", "%3F"}, {"#", "%23"}, {"'", "%27"}, {"\"", "%22"}, {"é", "%C3%A9"}}) {
                size_t offset = 0;
                while ((offset = configured.find(plain, offset)) != std::string::npos) {
                    configured.replace(offset, plain.size(), escaped); offset += escaped.size();
                }
            }
            const auto* vfs = sqlite3_vfs_find(nullptr);
            ASSERT_NE(vfs, nullptr);
            configured = "file:" + configured + "?mode=ro&cache=private&vfs=" + vfs->zName;
        }
        auto config = lattice::configuration(configured); config.read_only = true;
        lattice::lattice_db source{config}, receiver{lattice::configuration(main_file.str())};
        const SourceSnapshot before(source_file);
        receiver.attach(source); receiver.attach(source);
        expect_read_only(receiver.db(), alias_for(configured));
        receiver.close_write_db(); receiver.reopen_write_db();
        expect_read_only(receiver.db(), alias_for(configured));
        receiver.detach(source);
        EXPECT_EQ(sqlite3_db_readonly(receiver.db().handle(), alias_for(configured).c_str()), -1);
        EXPECT_EQ(SourceSnapshot(source_file), before);
    }
}

TEST(ReadOnlyAttachmentWrites, DoesNotAdoptAnUntrackedWritableAlias) {
    TempDB main_file{"manual_alias_main"}, source_file{"manual_alias_source"};
    seed(main_file); seed(source_file);
    lattice::lattice_db receiver{lattice::configuration(main_file.str())};
    auto config = lattice::configuration(source_file.str()); config.read_only = true;
    lattice::lattice_db source{config};
    const auto alias = alias_for(source_file.str());
    receiver.db().execute("ATTACH DATABASE ? AS " + quoted(alias), {source_file.str()});
    EXPECT_THROW(receiver.attach(source), lattice::db_error);
    EXPECT_EQ(sqlite3_db_readonly(receiver.db().handle(), alias.c_str()), 0);
    EXPECT_EQ(sqlite3_db_readonly(receiver.read_db().handle(), alias.c_str()), -1);
    receiver.db().execute("DETACH DATABASE " + quoted(alias));
    ASSERT_NO_THROW(receiver.attach(source));
    expect_read_only(receiver.db(), alias);
    receiver.detach(source);
}

TEST(ReadOnlyAttachmentWrites, UnwindsAnAttachRefusedByTheSecondHandle) {
    TempDB main_file{"unwind_alias_main"}, source_file{"unwind_alias_source"}, retained_file{"unwind_alias_retained"};
    seed(main_file); seed(source_file); seed(retained_file);
    lattice::lattice_db receiver{lattice::configuration(main_file.str())};
    auto config = lattice::configuration(source_file.str()); config.read_only = true;
    auto retained_config = lattice::configuration(retained_file.str()); retained_config.read_only = true;
    lattice::lattice_db source{config}, retained{retained_config};
    receiver.attach(retained);
    const auto writer_before = topology(receiver.db()), reader_before = topology(receiver.read_db());
    struct DenyAttach {
        sqlite3* handle;
        explicit DenyAttach(sqlite3* value) : handle(value) {
            if (sqlite3_set_authorizer(handle, [](void*, int action, const char*, const char*, const char*, const char*) {
                    return action == SQLITE_ATTACH ? SQLITE_DENY : SQLITE_OK;
                }, nullptr) != SQLITE_OK) throw std::runtime_error("Cannot install attachment refusal");
        }
        ~DenyAttach() { sqlite3_set_authorizer(handle, nullptr, nullptr); }
    };
    {
        DenyAttach refusal(receiver.read_db().handle());
        EXPECT_THROW(receiver.attach(source), lattice::db_error);
    }
    EXPECT_EQ(topology(receiver.db()), writer_before);
    EXPECT_EQ(topology(receiver.read_db()), reader_before);
    EXPECT_EQ(sqlite3_db_readonly(receiver.db().handle(), alias_for(source_file.str()).c_str()), -1);
    expect_read_only(receiver.db(), alias_for(retained_file.str()));
    ASSERT_NO_THROW(receiver.attach(source));
    expect_read_only(receiver.db(), alias_for(source_file.str()));
    receiver.detach(source); receiver.detach(retained);
}

TEST(ReadOnlyAttachmentWrites, RejectsUnsupportedSourceFormsBeforeAttaching) {
    TempDB main_file{"readonly_form_main"}, source_file{"readonly_form_source"};
    seed(main_file); seed(source_file);
    lattice::lattice_db receiver{lattice::configuration(main_file.str())};
    const auto before = topology(receiver.db());
    for (const auto& path : {std::string(":memory:"), std::string(""),
             "file:" + source_file.str() + "?immutable=0",
             "file:" + source_file.str() + "?cache=private&cache=private"}) {
        auto config = lattice::configuration(path); config.read_only = true;
        lattice::lattice_db source{config};
        EXPECT_THROW(receiver.attach(source), lattice::db_error) << path;
        EXPECT_EQ(topology(receiver.db()), before);
    }
    // A URI containing this substring is still a real file, not a memory exemption.
    TempDB misleading_file{"readonly_:memory:_source"}; seed(misleading_file);
    auto config = lattice::configuration("file:" + misleading_file.str()); config.read_only = true;
    lattice::lattice_db source{config};
    receiver.attach(source);
    expect_read_only(receiver.db(), alias_for(config.path));
    receiver.detach(source);
}

TEST(ReadOnlyAttachmentWrites, WritableNamedMemoryAttachmentsKeepTheirExistingBehavior) {
    TempDB main_file{"named_memory_alias_main"};
    lattice::lattice_db receiver{lattice::configuration(main_file.str())};
    const auto path = "file:" + main_file.path.stem().string() + "?mode=memory&cache=shared";
    lattice::lattice_db source{lattice::configuration(path)};
    receiver.add(TestPerson{"main", 1, std::nullopt});
    source.add(TestPerson{"memory", 2, std::nullopt});
    receiver.attach(source);
    EXPECT_EQ(receiver.db().query("SELECT name FROM TestPerson").size(), 2u);
    EXPECT_EQ(sqlite3_db_readonly(receiver.db().handle(), alias_for(path).c_str()), 0);
    receiver.detach(source);
    EXPECT_EQ(receiver.db().query("SELECT name FROM TestPerson").size(), 1u);
}

TEST(ReadOnlyAttachmentWrites, RetainedAliasTeardownDoesNotAnalyzeSource) {
    for (bool read_only : {false, true}) {
        SCOPED_TRACE(read_only);
        TempDB main_file{"optimize_alias_main"}, source_file{"optimize_alias_source"};
        seed(main_file); seed(source_file);
        auto config = lattice::configuration(source_file.str()); config.read_only = read_only;
        lattice::lattice_db source{config};
        const auto alias = alias_for(source_file.str());
        const SourceSnapshot before(source_file);
        {
            lattice::lattice_db receiver{lattice::configuration(main_file.str())};
            receiver.attach(source);
            ASSERT_EQ(receiver.db().query("SELECT value FROM " + quoted(alias) +
                ".AliasItem WHERE value='original'").size(), 1u);
            if (!read_only) {
                // Default optimize mask plus the debug bit: require actual
                // pending source analysis without executing it in this probe.
                const auto planned = receiver.db().query("PRAGMA " + quoted(alias) + ".optimize(65535)");
                bool analyzes_fixture = false;
                for (const auto& row : planned) for (const auto& [_, value] : row) {
                    if (const auto* text = std::get_if<std::string>(&value))
                        analyzes_fixture |= text->find("AliasItem") != std::string::npos;
                }
                ASSERT_TRUE(analyzes_fixture);
            }
            receiver.close();
        }
        const SourceSnapshot after(source_file);
        if (read_only) {
            EXPECT_EQ(after, before);
        } else {
            EXPECT_FALSE(after == before) << "Writable control did not exercise source cleanup";
            EXPECT_FALSE(source.db().query("SELECT * FROM sqlite_stat1 WHERE tbl='AliasItem'").empty());
        }
    }
}

TEST(ReadOnlyAttachmentWrites, RetainedAliasTeardownPreservesCommittedUncheckpointedWAL) {
    for (bool memory_receiver : {false, true}) {
        SCOPED_TRACE(memory_receiver);
        TempDB main_file{"wal_alias_main"}, source_file{"wal_alias_source"};
        seed(main_file); seed(source_file, true);
        {
            RawFixture writer(source_file.str());
            ASSERT_EQ(sqlite3_db_config(writer.db, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1, nullptr), SQLITE_OK);
            writer.execute("PRAGMA wal_autocheckpoint=0");
            writer.execute("UPDATE AliasItem SET value='committed-wal' WHERE id=1");
        }
        auto source_config = lattice::configuration(source_file.str()); source_config.read_only = true;
        auto source = std::make_unique<lattice::lattice_db>(source_config);
        const SourceSnapshot before(source_file);
        ASSERT_TRUE(before.wal.exists); ASSERT_FALSE(before.wal.bytes.empty());
        auto config = lattice::configuration(memory_receiver
            ? "file:" + main_file.path.stem().string() + "?mode=memory&cache=shared" : main_file.str());
        auto receiver = std::make_unique<lattice::lattice_db>(config);
        receiver->attach(*source);
        auto held = receiver->borrow_read_connection();
        const auto alias = alias_for(source_file.str());
        ASSERT_EQ(sqlite3_db_readonly(receiver->db().handle(), alias.c_str()), 1);
        const auto rows = held->query("SELECT value FROM " + quoted(alias) + ".AliasItem");
        ASSERT_EQ(rows.size(), 1u);
        EXPECT_EQ(std::get<std::string>(rows[0].at("value")), "committed-wal");
        source->close(); source.reset();
        receiver->close();
        EXPECT_EQ(SourceSnapshot(source_file), before);
        receiver.reset();
        EXPECT_EQ(SourceSnapshot(source_file), before);
        // For the memory receiver this is the last writer-wrapper reference.
        held.reset();
        EXPECT_EQ(SourceSnapshot(source_file), before);
    }
}

TEST(ReadOnlyAttachmentWrites, ReceiverTruncatePreservesFullyBackfilledReadOnlyWAL) {
    TempDB main_file{"checkpoint_main"}, source_file{"checkpoint_readonly"},
        writable_file{"checkpoint_writable_\"alias"};
    seed(main_file); seed(source_file, true); seed(writable_file);
    // Keep the source writer alive after FULL: its WAL is backfilled but is
    // still non-empty, so an unqualified receiver TRUNCATE would cut it.
    RawFixture source_writer(source_file.str());
    source_writer.execute("PRAGMA wal_autocheckpoint=0");
    source_writer.execute("UPDATE AliasItem SET value='backfilled' WHERE id=1");
    int log = -1, backfilled = -1;
    ASSERT_EQ(sqlite3_wal_checkpoint_v2(source_writer.db, "main", SQLITE_CHECKPOINT_FULL,
                                       &log, &backfilled), SQLITE_OK);
    ASSERT_GT(log, 0); ASSERT_EQ(backfilled, log);
    auto source_config = lattice::configuration(source_file.str()); source_config.read_only = true;
    lattice::lattice_db source{source_config}, writable{lattice::configuration(writable_file.str())},
        receiver{lattice::configuration(main_file.str())};
    receiver.attach(source); receiver.attach(writable);
    receiver.db().execute("PRAGMA wal_autocheckpoint=0");
    writable.db().execute("PRAGMA wal_autocheckpoint=0");
    receiver.db().execute("UPDATE main.AliasItem SET value='receiver-wal' WHERE id=1");
    writable.db().execute("UPDATE AliasItem SET value='writable-wal' WHERE id=1");
    ASSERT_GT(std::filesystem::file_size(main_file.str() + "-wal"), 0u);
    ASSERT_GT(std::filesystem::file_size(writable_file.str() + "-wal"), 0u);
    const SourceSnapshot before(source_file);
    ASSERT_TRUE(before.wal.exists); ASSERT_FALSE(before.wal.bytes.empty());

    const auto result = receiver.db().wal_checkpoint(true);

    EXPECT_EQ(result.rc, SQLITE_OK); EXPECT_EQ(result.busy, 0);
    EXPECT_EQ(result.log_frames, 0); EXPECT_EQ(result.checkpointed, 0);
    EXPECT_EQ(SourceSnapshot(source_file), before);
    // Writable aliases retain their previous checkpoint behavior, including
    // correct quoting of an alias containing an identifier quote.
    EXPECT_EQ(std::filesystem::file_size(main_file.str() + "-wal"), 0u);
    EXPECT_EQ(std::filesystem::file_size(writable_file.str() + "-wal"), 0u);
    receiver.detach(writable); receiver.detach(source);
}

TEST(ReadOnlyAttachmentWrites, ReceiverTruncateSucceedsWithUnbackfilledReadOnlyWAL) {
    TempDB main_file{"checkpoint_pending_main"}, source_file{"checkpoint_pending_source"};
    seed(main_file); seed(source_file, true);
    const FileImage source_main(source_file.str());
    {
        RawFixture writer(source_file.str());
        ASSERT_EQ(sqlite3_db_config(writer.db, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1, nullptr), SQLITE_OK);
        writer.execute("PRAGMA wal_autocheckpoint=0");
        writer.execute("UPDATE AliasItem SET value='pending-wal' WHERE id=1");
    }
    // The committed row is still in the WAL, not in the source main file.
    ASSERT_EQ(FileImage(source_file.str()), source_main);
    auto source_config = lattice::configuration(source_file.str()); source_config.read_only = true;
    lattice::lattice_db source{source_config}, receiver{lattice::configuration(main_file.str())};
    receiver.attach(source);
    receiver.db().execute("PRAGMA wal_autocheckpoint=0");
    receiver.db().execute("UPDATE main.AliasItem SET value='receiver-wal' WHERE id=1");
    const SourceSnapshot before(source_file);
    ASSERT_TRUE(before.wal.exists); ASSERT_FALSE(before.wal.bytes.empty());
    ASSERT_EQ(before.rows.size(), 1u);
    ASSERT_EQ(std::get<std::string>(before.rows[0].at("value")), "pending-wal");

    const auto result = receiver.db().wal_checkpoint(true);

    EXPECT_EQ(result.rc, SQLITE_OK); EXPECT_EQ(result.busy, 0);
    EXPECT_EQ(result.log_frames, 0); EXPECT_EQ(result.checkpointed, 0);
    EXPECT_EQ(std::filesystem::file_size(main_file.str() + "-wal"), 0u);
    EXPECT_EQ(SourceSnapshot(source_file), before);
    receiver.detach(source);
}
