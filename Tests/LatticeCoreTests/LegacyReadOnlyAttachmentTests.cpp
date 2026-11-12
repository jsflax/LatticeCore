#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
#include "TestHelpers.hpp"
#include <fstream>
#include <iterator>

namespace {
using Rows = std::vector<lattice::database::row_t>;

void seed_legacy_table(const TempDB& file, const std::string& columns,
                       const std::string& insert,
                       const std::string& initial_name = "LegacyItem") {
    lattice::lattice_db owner{lattice::configuration(file.str())};
    owner.db().execute("CREATE TABLE " + initial_name + " (" + columns + ")");
    owner.db().execute(insert);
    if (initial_name != "LegacyItem") {
        // Missing-identity fixtures use a link-style seed name so the model
        // update hook does not query their absent id/globalId before attachment.
        owner.db().execute("ALTER TABLE " + initial_name + " RENAME TO LegacyItem");
    }
    const auto checkpoint = owner.db().wal_checkpoint(true);
    if (checkpoint.rc != SQLITE_OK || checkpoint.busy != 0)
        throw std::runtime_error("Fixture checkpoint did not finish");
    owner.close();
}

std::string file_image(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("Cannot read fixture file");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

struct SourceSnapshot {
    std::string bytes;
    Rows schema;
    Rows rows;
};

SourceSnapshot source_snapshot(const TempDB& file) {
    lattice::database reader(file.str(), lattice::database::open_mode::read_only);
    auto schema = reader.query(
        "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name");
    auto rows = reader.query("SELECT * FROM LegacyItem ORDER BY rowid");
    return {file_image(file.str()), std::move(schema), std::move(rows)};
}

void expect_same_source(const SourceSnapshot& before, const SourceSnapshot& after) {
    ASSERT_FALSE(before.bytes.empty());
    ASSERT_FALSE(before.rows.empty());
    EXPECT_EQ(after.bytes, before.bytes);
    EXPECT_EQ(after.schema, before.schema);
    EXPECT_EQ(after.rows, before.rows);
}

struct ViewTopology {
    Rows views;
    Rows databases;
};

ViewTopology topology(lattice::database& db) {
    // Querying temp first establishes the same temp-schema baseline on both reads.
    auto views = db.query(
        "SELECT name,sql FROM sqlite_temp_master WHERE type='view' ORDER BY name");
    return {std::move(views), db.query("PRAGMA database_list")};
}

void expect_same_topology(const ViewTopology& before, const ViewTopology& after) {
    EXPECT_EQ(after.views, before.views);
    EXPECT_EQ(after.databases, before.databases);
}

struct AttachmentProbe {
    sqlite3* handle;
    int attaches = 0;
    int table_info_reads = 0;

    explicit AttachmentProbe(lattice::database& db) : handle(db.handle()) {
        if (sqlite3_set_authorizer(handle, authorize, this) != SQLITE_OK)
            throw std::runtime_error("Cannot observe attachment fixture");
    }
    ~AttachmentProbe() { sqlite3_set_authorizer(handle, nullptr, nullptr); }
    AttachmentProbe(const AttachmentProbe&) = delete;
    AttachmentProbe& operator=(const AttachmentProbe&) = delete;

    static int authorize(void* context, int action, const char* argument,
                         const char*, const char*, const char*) {
        auto& probe = *static_cast<AttachmentProbe*>(context);
        if (action == SQLITE_ATTACH) ++probe.attaches;
        if (action == SQLITE_PRAGMA && argument &&
            std::string(argument) == "table_info") ++probe.table_info_reads;
        return SQLITE_OK;
    }
};

Rows projected_rows(lattice::database& db) {
    return db.query(
        "SELECT id,globalId,label,rank,note,score,caption FROM LegacyItem ORDER BY label");
}

void expect_projected_rows(const Rows& rows) {
    ASSERT_EQ(rows.size(), 3u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("label")), "a-legacy");
    EXPECT_EQ(std::get<int64_t>(rows[0].at("rank")), 2);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(rows[0].at("note")));
    EXPECT_EQ(std::get<int64_t>(rows[0].at("score")), 17);
    EXPECT_EQ(std::get<std::string>(rows[0].at("caption")), "fallback'value");
    EXPECT_EQ(std::get<std::string>(rows[0].at("globalId")), "legacy-a");
    EXPECT_EQ(std::get<int64_t>(rows[0].at("id")), 41);

    EXPECT_EQ(std::get<std::string>(rows[1].at("label")), "b-legacy");
    EXPECT_EQ(std::get<int64_t>(rows[1].at("rank")), 3);
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(rows[1].at("note")));
    EXPECT_EQ(std::get<int64_t>(rows[1].at("score")), 93);
    // An existing NULL is data, not a request to use the newer schema's default.
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(rows[1].at("caption")));
    EXPECT_EQ(std::get<std::string>(rows[1].at("globalId")), "legacy-b");
    EXPECT_EQ(std::get<int64_t>(rows[1].at("id")), 52);

    EXPECT_EQ(std::get<std::string>(rows[2].at("label")), "main");
    EXPECT_EQ(std::get<int64_t>(rows[2].at("rank")), 11);
    EXPECT_EQ(std::get<std::string>(rows[2].at("note")), "retained");
    EXPECT_EQ(std::get<int64_t>(rows[2].at("score")), 101);
    EXPECT_EQ(std::get<std::string>(rows[2].at("caption")), "main-caption");
}
} // namespace

TEST(LegacyReadOnlyAttachment, ProjectsEachLegacyArmWithoutChangingItsFileOrRows) {
    TempDB main_file{"legacy_attach_main"};
    TempDB first_file{"legacy_attach_first"};
    TempDB second_file{"legacy_attach_second"};
    seed_legacy_table(main_file,
        "id INTEGER PRIMARY KEY,globalId TEXT UNIQUE NOT NULL,label TEXT NOT NULL,"
        "rank INTEGER NOT NULL,note TEXT,score INTEGER NOT NULL DEFAULT 17,"
        "caption TEXT DEFAULT 'fallback''value'",
        "INSERT INTO LegacyItem VALUES(1,'main-id','main',11,'retained',101,'main-caption')");
    // Deliberately use different physical orders, and different missing columns.
    seed_legacy_table(first_file,
        "label TEXT NOT NULL,globalId TEXT UNIQUE NOT NULL,id INTEGER PRIMARY KEY,"
        "rank INTEGER NOT NULL",
        "INSERT INTO LegacyItem VALUES('a-legacy','legacy-a',41,2)");
    seed_legacy_table(second_file,
        "score INTEGER NOT NULL,caption TEXT,rank INTEGER NOT NULL,label TEXT NOT NULL,"
        "id INTEGER PRIMARY KEY,globalId TEXT UNIQUE NOT NULL",
        "INSERT INTO LegacyItem VALUES(93,NULL,3,'b-legacy',52,'legacy-b')");
    const auto first_before = source_snapshot(first_file);
    const auto second_before = source_snapshot(second_file);

    {
        lattice::lattice_db main{lattice::configuration(main_file.str())};
        // An ordinary index does not turn an additive nullable column into a key.
        main.db().execute("CREATE INDEX legacy_optional_note ON LegacyItem(note)");
        auto first_config = lattice::configuration(first_file.str());
        auto second_config = lattice::configuration(second_file.str());
        first_config.read_only = second_config.read_only = true;
        lattice::lattice_db first{first_config}, second{second_config};
        const auto writer_before = topology(main.db());
        const auto reader_before = topology(main.read_db());
        AttachmentProbe first_probe(first.db()), second_probe(second.db());

        ASSERT_NO_THROW(main.attach(first));
        ASSERT_NO_THROW(main.attach(second));
        EXPECT_GT(first_probe.table_info_reads, 0);
        EXPECT_GT(second_probe.table_info_reads, 0);
        expect_projected_rows(projected_rows(main.db()));
        expect_projected_rows(projected_rows(main.read_db()));
        auto filtered = main.read_db().query(
            "SELECT label FROM LegacyItem WHERE rank=2 AND score=17 AND note IS NULL "
            "AND caption='fallback''value'");
        ASSERT_EQ(filtered.size(), 1u);
        EXPECT_EQ(std::get<std::string>(filtered[0].at("label")), "a-legacy");

        // Cached view SQL must retain each arm's projection when a reader is replaced.
        main.close_read_db();
        main.reopen_read_db();
        expect_projected_rows(projected_rows(main.read_db()));
        ASSERT_NO_THROW(main.detach(first));
        auto remaining = main.read_db().query("SELECT label FROM LegacyItem ORDER BY label");
        ASSERT_EQ(remaining.size(), 2u);
        EXPECT_EQ(std::get<std::string>(remaining[0].at("label")), "b-legacy");
        EXPECT_EQ(std::get<std::string>(remaining[1].at("label")), "main");
        ASSERT_NO_THROW(main.detach(second));
        expect_same_topology(writer_before, topology(main.db()));
        expect_same_topology(reader_before, topology(main.read_db()));
    }
    expect_same_source(first_before, source_snapshot(first_file));
    expect_same_source(second_before, source_snapshot(second_file));
}

TEST(LegacyReadOnlyAttachment, RejectsUnsafeDifferencesBeforeAnyAttachSideEffect) {
    struct Case {
        const char* name;
        const char* main_columns;
        const char* source_columns;
        bool read_only;
        const char* main_insert = "INSERT INTO LegacyItem(label) VALUES('main')";
        const char* main_index = nullptr;
        const char* source_seed_name = "LegacyItem";
    };
    const Case cases[] = {
        {"writable subset", "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT,optional TEXT",
            "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT", false},
        {"missing integer primary key", "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT",
            "globalId TEXT,label TEXT", true,
            "INSERT INTO LegacyItem(label) VALUES('main')", nullptr, "_LegacyItem"},
        {"missing global identity despite default",
            "id INTEGER PRIMARY KEY,globalId TEXT DEFAULT 'generated',label TEXT",
            "id INTEGER PRIMARY KEY,label TEXT", true,
            "INSERT INTO LegacyItem(label) VALUES('main')", nullptr, "_LegacyItem"},
        {"missing named identity despite default",
            "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT,primaryKey TEXT DEFAULT 'generated'",
            "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT", true},
        {"missing arbitrary SQL key despite default",
            "id INTEGER,globalId TEXT,label TEXT,keyPart TEXT PRIMARY KEY DEFAULT 'generated'",
            "id INTEGER,globalId TEXT,label TEXT", true},
        {"missing ordinary unique-index column",
            "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT,uniqueValue TEXT",
            "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT", true,
            "INSERT INTO LegacyItem(label,uniqueValue) VALUES('main','retained')",
            "CREATE UNIQUE INDEX legacy_unique_value ON LegacyItem(uniqueValue)"},
        {"missing composite unique-index component",
            "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT,uniqueValue TEXT DEFAULT 'unknown'",
            "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT", true,
            "INSERT INTO LegacyItem(label,uniqueValue) VALUES('main','retained')",
            "CREATE UNIQUE INDEX legacy_unique_pair ON LegacyItem(label,uniqueValue)"},
        {"unproven expression-key dependency",
            "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT,uniqueValue TEXT",
            "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT", true,
            "INSERT INTO LegacyItem(label,uniqueValue) VALUES('main','retained')",
            "CREATE UNIQUE INDEX legacy_unique_expression ON LegacyItem(lower(uniqueValue))"},
        {"required column without default",
            "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT,required TEXT NOT NULL",
            "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT", true,
            "INSERT INTO LegacyItem(label,required) VALUES('main','retained')"},
        {"extra column", "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT,optional TEXT",
            "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT,optional TEXT,extra TEXT", true},
        {"renamed column", "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT,optional TEXT",
            "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT,renamed TEXT", true},
    };
    for (const auto& item : cases) {
        SCOPED_TRACE(item.name);
        TempDB main_file{"legacy_reject_main"}, source_file{"legacy_reject_source"};
        seed_legacy_table(main_file, item.main_columns, item.main_insert);
        seed_legacy_table(source_file, item.source_columns,
                          "INSERT INTO " + std::string(item.source_seed_name) +
                              "(label) VALUES('source')",
                          item.source_seed_name);
        lattice::lattice_db main{lattice::configuration(main_file.str())};
        if (item.main_index) main.db().execute(item.main_index);
        auto config = lattice::configuration(source_file.str());
        config.read_only = item.read_only;
        lattice::lattice_db source{config};
        const auto source_before = source_snapshot(source_file);
        const auto writer_before = topology(main.db());
        const auto reader_before = topology(main.read_db());
        AttachmentProbe writer_probe(main.db()), reader_probe(main.read_db());
        AttachmentProbe source_probe(source.db());

        try {
            main.attach(source);
            ADD_FAILURE() << "Unsafe schema difference was admitted";
        } catch (const std::runtime_error& error) {
            EXPECT_EQ(std::string(error.what()),
                "Schema mismatch for table 'LegacyItem' between main database and attached database '" +
                source_file.path.stem().string() + "'");
        }
        EXPECT_EQ(writer_probe.attaches, 0);
        EXPECT_EQ(reader_probe.attaches, 0);
        EXPECT_GT(source_probe.table_info_reads, 0);
        expect_same_topology(writer_before, topology(main.db()));
        expect_same_topology(reader_before, topology(main.read_db()));
        auto visible = main.read_db().query("SELECT label FROM LegacyItem");
        ASSERT_EQ(visible.size(), 1u);
        EXPECT_EQ(std::get<std::string>(visible[0].at("label")), "main");
        expect_same_source(source_before, source_snapshot(source_file));
    }
}

TEST(LegacyReadOnlyAttachment, MatchingAttachedOnlyTablesKeepTheirNamedColumnValues) {
    TempDB main_file{"legacy_attached_only_main"};
    TempDB first_file{"legacy_attached_only_first"};
    TempDB second_file{"legacy_attached_only_second"};
    seed_legacy_table(first_file,
        "id INTEGER PRIMARY KEY,globalId TEXT,label TEXT,rank INTEGER",
        "INSERT INTO LegacyItem VALUES(11,'a-id','a-only',7)");
    seed_legacy_table(second_file,
        "rank INTEGER,label TEXT,globalId TEXT,id INTEGER PRIMARY KEY",
        "INSERT INTO LegacyItem VALUES(19,'b-only','b-id',22)");
    lattice::lattice_db main{lattice::configuration(main_file.str())};
    lattice::lattice_db first{lattice::configuration(first_file.str())};
    lattice::lattice_db second{lattice::configuration(second_file.str())};
    const auto writer_before = topology(main.db());
    const auto reader_before = topology(main.read_db());
    ASSERT_NO_THROW(main.attach(first));
    ASSERT_NO_THROW(main.attach(second));
    for (auto* db : {&main.db(), &main.read_db()}) {
        auto rows = db->query("SELECT id,globalId,label,rank FROM LegacyItem ORDER BY label");
        ASSERT_EQ(rows.size(), 2u);
        EXPECT_EQ(std::get<int64_t>(rows[0].at("id")), 11);
        EXPECT_EQ(std::get<std::string>(rows[0].at("globalId")), "a-id");
        EXPECT_EQ(std::get<std::string>(rows[0].at("label")), "a-only");
        EXPECT_EQ(std::get<int64_t>(rows[0].at("rank")), 7);
        EXPECT_EQ(std::get<int64_t>(rows[1].at("id")), 22);
        EXPECT_EQ(std::get<std::string>(rows[1].at("globalId")), "b-id");
        EXPECT_EQ(std::get<std::string>(rows[1].at("label")), "b-only");
        EXPECT_EQ(std::get<int64_t>(rows[1].at("rank")), 19);
    }
    ASSERT_NO_THROW(main.detach(first));
    ASSERT_NO_THROW(main.detach(second));
    expect_same_topology(writer_before, topology(main.db()));
    expect_same_topology(reader_before, topology(main.read_db()));
}
