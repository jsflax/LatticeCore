#include "TestHelpers.hpp"

// ============================================================================
// Schema Tests — DDL generation, migration, geo/vec0 table creation
// ============================================================================

TEST(Schema, CreateFromSchema) {
    auto* schema = lattice::schema_registry::instance().get_schema(typeid(TestTrip));
    ASSERT_NE(schema, nullptr);
    EXPECT_EQ(schema->table_name, "TestTrip");
    EXPECT_EQ(schema->properties.size(), 3u);
}

TEST(Schema, RegistryContainsAll) {
    auto* person = lattice::schema_registry::instance().get_schema(typeid(TestPerson));
    auto* dog = lattice::schema_registry::instance().get_schema(typeid(TestDog));
    ASSERT_NE(person, nullptr);
    ASSERT_NE(dog, nullptr);
    EXPECT_EQ(person->table_name, "TestPerson");
    EXPECT_EQ(dog->table_name, "TestDog");

    auto all = lattice::schema_registry::instance().all_schemas();
    EXPECT_GE(all.size(), 4u);
}

TEST(Schema, AddColumn) {
    TempDB tmp{"addcol"};

    // Create database with "old" schema missing email column
    {
        lattice::database old_db(tmp.str());
        old_db.execute(R"(
            CREATE TABLE TestPerson (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                globalId TEXT UNIQUE,
                name TEXT NOT NULL,
                age INTEGER NOT NULL
            )
        )");
        old_db.execute(
            "INSERT INTO TestPerson (globalId, name, age) VALUES ('test-uuid-1', 'OldPerson', 30)");
    }

    // Reopen with lattice_db — should add email column
    {
        lattice::lattice_db db(tmp.str());
        auto persons = db.objects<TestPerson>();
        ASSERT_EQ(persons.size(), 1u);
        EXPECT_EQ(std::string(persons[0].name), "OldPerson");

        auto new_person = db.add(TestPerson{"NewPerson", 25, "new@example.com"});
        auto found = db.find<TestPerson>(new_person.id());
        ASSERT_TRUE(found.has_value());
        EXPECT_EQ(found->email.detach(), std::optional<std::string>("new@example.com"));
    }
}

TEST(Schema, RemoveColumn) {
    TempDB tmp{"rmcol"};

    // Create database with extra column
    {
        lattice::database old_db(tmp.str());
        old_db.execute(R"(
            CREATE TABLE TestPerson (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                globalId TEXT UNIQUE,
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                email TEXT,
                legacy_field TEXT
            )
        )");
        old_db.execute(
            "INSERT INTO TestPerson (globalId, name, age, email, legacy_field) "
            "VALUES ('test-uuid-2', 'LegacyPerson', 40, 'legacy@example.com', 'old_data')");
    }

    // Reopen — should drop legacy_field
    {
        lattice::lattice_db db(tmp.str());
        auto persons = db.objects<TestPerson>();
        ASSERT_EQ(persons.size(), 1u);
        EXPECT_EQ(std::string(persons[0].name), "LegacyPerson");

        auto info = db.db().get_table_info("TestPerson");
        EXPECT_EQ(info.find("legacy_field"), info.end());
    }
}

TEST(Schema, EnsureTablesIdempotent) {
    lattice::lattice_db db;
    db.add(TestPerson{"A", 1, std::nullopt});

    // Creating tables again should not error
    // (The lattice_db constructor already calls ensure_tables)
    lattice::lattice_db db2;
    db2.add(TestPerson{"B", 2, std::nullopt});
    EXPECT_EQ(db2.objects<TestPerson>().size(), 1u);
}

TEST(Schema, ReopenPreservesSchema) {
    TempDB tmp{"reopen"};

    {
        lattice::lattice_db db(tmp.str());
        db.add(TestPerson{"Alice", 30, "alice@test.com"});
    }
    {
        lattice::lattice_db db(tmp.str());
        auto persons = db.objects<TestPerson>();
        ASSERT_EQ(persons.size(), 1u);

        auto info = db.db().get_table_info("TestPerson");
        EXPECT_NE(info.find("name"), info.end());
        EXPECT_NE(info.find("age"), info.end());
        EXPECT_NE(info.find("email"), info.end());
    }
}

TEST(Schema, LinkCreatesJunction) {
    lattice::lattice_db db;

    auto owner = db.add(TestOwner{"Owner", nullptr});
    owner.pet = TestPet{"Pet", 10.0};

    // Junction table should exist
    auto tables = db.db().query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='_TestOwner_TestPet_pet'");
    EXPECT_EQ(tables.size(), 1u);
}

namespace {
constexpr const char* reverse_link_index = "idx__ReverseNode_ReverseNode_parent_rhs_lhs";
struct ReverseLinkNodeTag {};

void register_reverse_link_schema() {
    // Supply the target table explicitly, as Swift/C ABI schema registration does.
    // The older native LATTICE_SCHEMA pointer helper leaves target_table empty.
    lattice::model_schema schema;
    schema.table_name = "ReverseNode";
    lattice::property_descriptor parent;
    parent.name = "parent"; parent.kind = lattice::property_kind::link;
    parent.type = lattice::column_type::integer; parent.nullable = true;
    parent.target_table = "ReverseNode";
    schema.properties.push_back(parent);
    lattice::schema_registry::instance().register_model(typeid(ReverseLinkNodeTag), std::move(schema));
}

// Reproduce the released epoch-6 fingerprint, rather than deleting all markers:
// an old cookie-valid marker must not skip the new index migration on reopen.
struct LegacyLinkFingerprint : lattice::lattice_db {
    static std::string key() {
        std::ostringstream out;
        out << "epoch:6\ntarget_schema_version:1\n";
        auto schemas = lattice::schema_registry::instance().all_schemas();
        std::sort(schemas.begin(), schemas.end(), [](const auto* a, const auto* b) {
            return a->table_name < b->table_name;
        });
        for (const auto* schema : schemas) {
            out << "table:" << schema->table_name << '\n';
            for (const auto& prop : schema->properties) serialize_property_for_fingerprint(out, prop);
        }
        std::ostringstream hex;
        hex << std::hex << std::setfill('0') << std::setw(16) << fnv1a_hash(out.str());
        return "schema_fingerprint:" + hex.str();
    }
};

void make_legacy_link_store(const std::string& path) {
    register_reverse_link_schema();
    {
        lattice::lattice_db db(path);
        db.db().execute("INSERT INTO ReverseNode(globalId) VALUES('parent'),('child')");
        db.db().execute("INSERT INTO _ReverseNode_ReverseNode_parent(lhs,rhs,globalId) "
                        "VALUES('child','parent','link')");
    }
    // Stamp after the normal close's possible PRAGMA optimize DDL, so the old
    // marker really matches the cookie at the next open.
    lattice::database raw(path);
    raw.execute(std::string("DROP INDEX ") + reverse_link_index);
    raw.execute("DELETE FROM _lattice_meta WHERE key LIKE 'schema_fingerprint:%'");
    auto cookie = raw.query("PRAGMA schema_version");
    raw.execute("INSERT INTO _lattice_meta(key,value) VALUES(?,?)",
                {LegacyLinkFingerprint::key(), std::to_string(std::get<int64_t>(cookie[0].at("schema_version")))});
}
} // namespace

TEST(Schema, ReverseLinkLookupUsesCoveringIndex) {
    register_reverse_link_schema();
    lattice::lattice_db db;
    const auto info = db.db().query(std::string("PRAGMA index_info('") + reverse_link_index + "')");
    ASSERT_EQ(info.size(), 2u);
    EXPECT_EQ(std::get<std::string>(info[0].at("name")), "rhs");
    EXPECT_EQ(std::get<std::string>(info[1].at("name")), "lhs");
    const auto plan = db.db().query("EXPLAIN QUERY PLAN SELECT lhs FROM _ReverseNode_ReverseNode_parent WHERE rhs = ?",
                                    {std::string("target")});
    ASSERT_EQ(plan.size(), 1u);
    const auto detail = std::get<std::string>(plan[0].at("detail"));
    EXPECT_NE(detail.find("SEARCH"), std::string::npos);
    EXPECT_NE(detail.find("COVERING INDEX"), std::string::npos);
    EXPECT_NE(detail.find(reverse_link_index), std::string::npos);
}

TEST(Schema, LegacyReverseLinkIndexMigratesWithoutChangingRows) {
    TempDB tmp{"reverse_link_upgrade"};
    make_legacy_link_store(tmp.str());
    lattice::database observer(tmp.str(), lattice::database::open_mode::read_only);
    const auto marker = observer.query("SELECT value FROM _lattice_meta WHERE key = ?", {LegacyLinkFingerprint::key()});
    const auto cookie = observer.query("PRAGMA schema_version");
    ASSERT_EQ(marker.size(), 1u);
    ASSERT_EQ(std::get<std::string>(marker[0].at("value")),
              std::to_string(std::get<int64_t>(cookie[0].at("schema_version"))));
    const auto links = observer.query("SELECT * FROM _ReverseNode_ReverseNode_parent ORDER BY globalId");
    const auto audits = observer.query("SELECT * FROM AuditLog ORDER BY id");
    {
        lattice::lattice_db db(tmp.str());
        EXPECT_EQ(db.db().query("SELECT * FROM _ReverseNode_ReverseNode_parent ORDER BY globalId"), links);
        EXPECT_EQ(db.db().query("SELECT * FROM AuditLog ORDER BY id"), audits);
        EXPECT_EQ(db.db().query(std::string("PRAGMA index_info('") + reverse_link_index + "')").size(), 2u);
    }
    // Repeat-open must return to the existing write-free schema fast path.
    const auto version = observer.query("PRAGMA data_version");
    {
        lattice::lattice_db db(tmp.str());
        EXPECT_EQ(sqlite3_total_changes(db.db().handle()), 0);
        EXPECT_EQ(observer.query("PRAGMA data_version"), version);
    }
}

TEST(Schema, ReadOnlyLegacyLinksDoNotAttemptIndexMigration) {
    TempDB tmp{"reverse_link_readonly"};
    make_legacy_link_store(tmp.str());
    lattice::database observer(tmp.str(), lattice::database::open_mode::read_only);
    const auto schema = observer.query("SELECT * FROM sqlite_master ORDER BY name");
    lattice::configuration config;
    config.path = tmp.str(); config.read_only = true;
    {
        lattice::lattice_db db(config);
        EXPECT_EQ(db.db().query("SELECT lhs FROM _ReverseNode_ReverseNode_parent").size(), 1u);
        EXPECT_EQ(db.db().query(std::string("PRAGMA index_info('") + reverse_link_index + "')").size(), 0u);
    }
    EXPECT_EQ(observer.query("SELECT * FROM sqlite_master ORDER BY name"), schema);
}

TEST(Schema, ReverseLinkIndexPreservesResultsWithBoundedQueryWork) {
    register_reverse_link_schema();
    lattice::lattice_db db;
    db.db().execute("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<2048) "
                    "INSERT INTO ReverseNode(globalId) SELECT 'node-'||x FROM n");
    db.db().execute("INSERT INTO _ReverseNode_ReverseNode_parent(lhs,rhs,globalId) "
                    "SELECT globalId, CASE WHEN globalId='node-2048' THEN 'node-1' ELSE 'node-2' END, "
                    "'link-'||globalId FROM ReverseNode");
    db.db().execute("DROP INDEX idx__ReverseNode_ReverseNode_parent_rhs_lhs");
    const char* sql = "SELECT globalId FROM ReverseNode WHERE globalId IN "
        "(SELECT lhs FROM _ReverseNode_ReverseNode_parent WHERE rhs IN "
        "(SELECT globalId FROM ReverseNode WHERE globalId = 'node-1')) ORDER BY globalId";
    struct Read { std::vector<std::string> rows; int steps; int scans; };
    auto read = [&]() -> Read {
        sqlite3_stmt* statement = nullptr;
        if (sqlite3_prepare_v2(db.db().handle(), sql, -1, &statement, nullptr) != SQLITE_OK)
            throw std::runtime_error(sqlite3_errmsg(db.db().handle()));
        std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> owned(statement, sqlite3_finalize);
        Read result{};
        int status;
        while ((status = sqlite3_step(statement)) == SQLITE_ROW)
            result.rows.emplace_back(reinterpret_cast<const char*>(sqlite3_column_text(statement, 0)));
        if (status != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(db.db().handle()));
        result.steps = sqlite3_stmt_status(statement, SQLITE_STMTSTATUS_VM_STEP, 0);
        result.scans = sqlite3_stmt_status(statement, SQLITE_STMTSTATUS_FULLSCAN_STEP, 0);
        return result;
    };
    const auto before = read();
    db.ensure_link_table("_ReverseNode_ReverseNode_parent", "ReverseNode", "ReverseNode");
    const auto after = read();
    EXPECT_EQ(before.rows, (std::vector<std::string>{"node-2048"}));
    EXPECT_EQ(after.rows, before.rows);
    EXPECT_GE(before.scans, 2047);
    EXPECT_EQ(after.scans, 0);
    EXPECT_LT(after.steps * 10, before.steps);
}

TEST(Schema, GeoCreatesRTree) {
    lattice::lattice_db db;
    db.add(TestPlace{"Test", lattice::geo_bounds::point(0, 0)});

    // R*Tree table should exist
    auto tables = db.db().query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='_TestPlace_location_rtree'");
    EXPECT_EQ(tables.size(), 1u);

    // Verify 4 geo columns
    auto info = db.db().get_table_info("TestPlace");
    EXPECT_NE(info.find("location_minLat"), info.end());
    EXPECT_NE(info.find("location_maxLat"), info.end());
    EXPECT_NE(info.find("location_minLon"), info.end());
    EXPECT_NE(info.find("location_maxLon"), info.end());

    // Verify triggers
    auto triggers = db.db().query(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '%_TestPlace_location_rtree%'");
    EXPECT_GE(triggers.size(), 3u);
}

TEST(Schema, VectorCreatesVec0) {
    lattice::lattice_db db;

    db.db().execute(
        "CREATE TABLE IF NOT EXISTS VecModel ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "globalId TEXT UNIQUE NOT NULL, "
        "name TEXT NOT NULL, "
        "embedding BLOB)");
    db.ensure_vec0_table("VecModel", "embedding", 3);

    auto tables = db.db().query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='_VecModel_embedding_vec'");
    EXPECT_EQ(tables.size(), 1u);
}

// ----------------------------------------------------------------------------
// Migration Block Callback
// ----------------------------------------------------------------------------

TEST(Schema, MigrationBlockCallback) {
    TempDB tmp{"migration"};

    // Create old schema with lat/lon columns
    {
        lattice::database old_db(tmp.str());
        old_db.execute(R"(
            CREATE TABLE TestPlace (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                globalId TEXT UNIQUE DEFAULT (lower(hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' || substr(hex(randomblob(2)),2) || '-' || substr('89ab',abs(random()) % 4 + 1, 1) || substr(hex(randomblob(2)),2) || '-' || hex(randomblob(6)))),
                name TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL
            )
        )");
        old_db.execute("INSERT INTO TestPlace (name, latitude, longitude) VALUES (?, ?, ?)",
                       {std::string("SF Coffee"), 37.78, -122.41});
        old_db.execute("INSERT INTO TestPlace (name, latitude, longitude) VALUES (?, ?, ?)",
                       {std::string("NYC Pizza"), 40.71, -74.01});
    }

    // Reopen with migration block
    bool called = false;
    int rows_migrated = 0;
    std::vector<std::string> removed_cols;

    {
        lattice::configuration config(tmp.str());
        config.migration_block = [&](lattice::migration_context& ctx) {
            called = true;
            auto* changes = ctx.changes_for("TestPlace");
            if (changes) {
                for (const auto& col : changes->removed_columns) {
                    removed_cols.push_back(col);
                }
            }
            ctx.enumerate_objects("TestPlace", [&](const lattice::migration_row& old_row,
                                                    lattice::migration_row& new_row) {
                rows_migrated++;
                double lat = 0, lon = 0;
                auto lat_it = old_row.find("latitude");
                if (lat_it != old_row.end() && std::holds_alternative<double>(lat_it->second))
                    lat = std::get<double>(lat_it->second);
                auto lon_it = old_row.find("longitude");
                if (lon_it != old_row.end() && std::holds_alternative<double>(lon_it->second))
                    lon = std::get<double>(lon_it->second);

                new_row["location_minLat"] = lat;
                new_row["location_maxLat"] = lat;
                new_row["location_minLon"] = lon;
                new_row["location_maxLon"] = lon;
            });
        };

        lattice::lattice_db db(config);

        EXPECT_TRUE(called);
        EXPECT_EQ(rows_migrated, 2);
        EXPECT_NE(std::find(removed_cols.begin(), removed_cols.end(), "latitude"), removed_cols.end());
        EXPECT_NE(std::find(removed_cols.begin(), removed_cols.end(), "longitude"), removed_cols.end());

        // Verify migrated data
        auto places = db.objects<TestPlace>();
        ASSERT_EQ(places.size(), 2u);

        bool found_sf = false;
        for (const auto& place : places) {
            if (std::string(place.name) == "SF Coffee") {
                auto loc = place.location.detach();
                EXPECT_NEAR(loc.min_lat, 37.78, 0.001);
                EXPECT_NEAR(loc.min_lon, -122.41, 0.001);
                found_sf = true;
            }
        }
        EXPECT_TRUE(found_sf);

        // Verify old columns removed
        auto info = db.db().get_table_info("TestPlace");
        EXPECT_EQ(info.find("latitude"), info.end());
        EXPECT_EQ(info.find("longitude"), info.end());
    }
}

// ----------------------------------------------------------------------------
// geo_bounds type tests
// ----------------------------------------------------------------------------

TEST(Schema, GeoBoundsType) {
    lattice::geo_bounds empty;
    EXPECT_DOUBLE_EQ(empty.min_lat, 0.0);
    EXPECT_TRUE(empty.is_point());

    lattice::geo_bounds bbox(37.0, 38.0, -123.0, -122.0);
    EXPECT_FALSE(bbox.is_point());
    EXPECT_NEAR(bbox.center_lat(), 37.5, 0.001);
    EXPECT_NEAR(bbox.center_lon(), -122.5, 0.001);
    EXPECT_NEAR(bbox.lat_span(), 1.0, 0.001);

    EXPECT_TRUE(bbox.contains(37.5, -122.5));
    EXPECT_FALSE(bbox.contains(36.0, -122.5));

    lattice::geo_bounds overlapping(37.5, 38.5, -122.5, -121.5);
    EXPECT_TRUE(bbox.intersects(overlapping));

    lattice::geo_bounds non_overlapping(39.0, 40.0, -123.0, -122.0);
    EXPECT_FALSE(bbox.intersects(non_overlapping));

    auto point = lattice::geo_bounds::point(37.78, -122.41);
    EXPECT_TRUE(point.is_point());
    EXPECT_DOUBLE_EQ(point.min_lat, 37.78);

    EXPECT_EQ(bbox, lattice::geo_bounds(37.0, 38.0, -123.0, -122.0));
    EXPECT_NE(bbox, overlapping);
}

// ============================================================================
// Schema fingerprint fast path — write-free reopen, invalidation, coverage
// ============================================================================

// Lift protected fingerprint statics into test scope via a derived probe.
namespace {
struct FingerprintProbe : lattice::lattice_db {
    using lattice::lattice_db::serialize_property_for_fingerprint;
    using lattice::lattice_db::fnv1a_hash;
};

std::string serialize_prop_for_test(const lattice::property_descriptor& p) {
    std::ostringstream out;
    FingerprintProbe::serialize_property_for_fingerprint(out, p);
    return out.str();
}

int64_t data_version_of(lattice::database& db) {
    auto rows = db.query("PRAGMA data_version");
    return std::get<int64_t>(rows[0].begin()->second);
}
} // namespace

TEST(Schema, ReopenIsWriteFree) {
    TempDB tmp{"writefree"};

    // Opens #1-#3: first open runs the full ensure pass; close-time
    // "PRAGMA optimize" may create sqlite_stat1 (DDL → cookie bump), which
    // forces at most one revalidation pass. By open #4 the state is stable.
    { lattice::lattice_db db(tmp.str()); db.add(TestPerson{"A", 1, std::nullopt}); }
    { lattice::lattice_db db(tmp.str()); }
    { lattice::lattice_db db(tmp.str()); }

    // Independent connection observes whether the final open commits anything.
    lattice::database observer(tmp.str(), lattice::database::open_mode::read_only);
    int64_t dv_before = data_version_of(observer);

    {
        lattice::lattice_db db(tmp.str());
        // Zero row writes on the opener's connection...
        EXPECT_EQ(sqlite3_total_changes(db.db().handle()), 0);
        // ...and no committed changes visible to an independent connection.
        EXPECT_EQ(data_version_of(observer), dv_before);
        // The fast-path marker exists.
        auto markers = db.db().query(
            "SELECT key FROM _lattice_meta WHERE key LIKE 'schema_fingerprint:%'");
        EXPECT_GE(markers.size(), 1u);
    }
}

TEST(Schema, FingerprintMarkerRefreshOnExternalDDL) {
    TempDB tmp{"fpinv"};
    { lattice::lattice_db db(tmp.str()); db.add(TestPerson{"A", 1, std::nullopt}); }
    { lattice::lattice_db db(tmp.str()); }

    auto read_marker = [&](lattice::database& raw) {
        auto rows = raw.query(
            "SELECT value FROM _lattice_meta WHERE key LIKE 'schema_fingerprint:%' LIMIT 1");
        return rows.empty() ? std::string{}
                            : std::get<std::string>(rows[0].at("value"));
    };

    std::string marker_before;
    {
        // External bare DDL bumps the schema cookie → marker goes stale.
        lattice::database raw(tmp.str());
        marker_before = read_marker(raw);
        ASSERT_FALSE(marker_before.empty());
        raw.execute("CREATE TABLE extraneous(x INTEGER)");
    }

    {
        // Next open revalidates (slow path), still works, and re-stamps the
        // marker with the advanced cookie.
        lattice::lattice_db db(tmp.str());
        db.add(TestPerson{"B", 2, std::nullopt});
        EXPECT_EQ(db.objects<TestPerson>().size(), 2u);

        lattice::database raw(tmp.str(), lattice::database::open_mode::read_only);
        std::string marker_after = read_marker(raw);
        ASSERT_FALSE(marker_after.empty());
        EXPECT_NE(marker_after, marker_before);
    }
}

TEST(Schema, FingerprintCoversDDLDrivingAttributes) {
    lattice::property_descriptor base;
    base.name = "col";
    base.type = lattice::column_type::text;
    base.kind = lattice::property_kind::primitive;
    const std::string baseline = serialize_prop_for_test(base);

    auto expect_differs = [&](auto mutate, const char* what) {
        auto p = base;
        mutate(p);
        EXPECT_NE(serialize_prop_for_test(p), baseline)
            << "fingerprint must change when " << what << " changes";
    };

    expect_differs([](auto& p) { p.is_indexed = true; }, "is_indexed");
    expect_differs([](auto& p) { p.is_full_text = true; }, "is_full_text");
    expect_differs([](auto& p) { p.is_vector = true; }, "is_vector");
    expect_differs([](auto& p) { p.is_geo_bounds = true; }, "is_geo_bounds");
    expect_differs([](auto& p) { p.is_unique = true; }, "is_unique");
    expect_differs([](auto& p) { p.nullable = true; }, "nullable");
    expect_differs([](auto& p) { p.type = lattice::column_type::integer; }, "type");
    expect_differs([](auto& p) {
        p.kind = lattice::property_kind::link;
        p.target_table = "Other";
    }, "kind/target_table");
    expect_differs([](auto& p) { p.column_name = "renamed"; }, "column_name");
    expect_differs([](auto& p) {
        p.is_union = true;
        p.union_desc.union_table_name = "_U";
    }, "union descriptor");
}

TEST(Schema, SchemaCookieVisibleInsideTransaction) {
    // The fast path stores PRAGMA schema_version read INSIDE the slow path's
    // transaction as the marker value. That only works if in-transaction DDL
    // bumps are visible to the same connection AND survive the commit
    // unchanged. Guard the platform assumption loudly.
    TempDB tmp{"cookievis"};
    lattice::database db(tmp.str());
    auto cookie = [&] {
        auto rows = db.query("PRAGMA schema_version");
        return std::get<int64_t>(rows[0].begin()->second);
    };

    int64_t before = cookie();
    db.begin_transaction();
    db.execute("CREATE TABLE cookie_probe(x INTEGER)");
    int64_t inside = cookie();
    db.commit();
    int64_t after = cookie();

    EXPECT_GT(inside, before);
    EXPECT_EQ(inside, after)
        << "schema cookie read inside the transaction must equal the "
           "post-commit value, or every fast-path open would miss";
}

TEST(Schema, MachineryShadowColumnIsNotARemoval) {
    // Phase 8 materializes `<link>__link_gid` shadow columns for @Unique
    // constraints that include a to-one link. They are NOT part of the user
    // schema — a binary opening the database without knowledge of the shadow
    // must not treat it as a removed column and rebuild the table (the
    // rebuild drops the shadow, the next pass re-adds it, and the cycle
    // repeats on every open while holding the write lock for a full table
    // copy).
    TempDB tmp{"shadowcol"};

    { lattice::lattice_db db(tmp.str()); db.add(TestPerson{"A", 1, std::nullopt}); }

    {
        // Simulate Phase 8: machinery column added outside the schema.
        // (Through a lattice_db handle — the audit trigger on UPDATE calls
        // the per-connection sync_disabled() function.)
        lattice::lattice_db db(tmp.str());
        db.db().execute("ALTER TABLE TestPerson ADD COLUMN boss__link_gid TEXT");
        db.db().execute("UPDATE TestPerson SET boss__link_gid = 'gid-1'");
    }

    {
        // Reopen with the schema that does NOT declare the shadow column.
        // detect/migrate must ignore it: no rebuild, data intact.
        lattice::lattice_db db(tmp.str());
        auto info = db.db().get_table_info("TestPerson");
        EXPECT_NE(info.find("boss__link_gid"), info.end())
            << "machinery column was dropped by a table rebuild";
        auto rows = db.db().query(
            "SELECT boss__link_gid FROM TestPerson WHERE name = 'A'");
        ASSERT_EQ(rows.size(), 1u);
        EXPECT_EQ(std::get<std::string>(rows[0].at("boss__link_gid")), "gid-1")
            << "shadow data lost — table was rebuilt";
    }
}

// ----------------------------------------------------------------------------
// Per-property UNIQUE indexes on the core table-DDL path
//
// `property_descriptor::is_unique` is how uniqueness arrives from the C ABI
// (and any core-registry user); it must produce the SAME schema objects the
// Swift bridge's constraint-driven pass ("Phase 8") creates — index name
// `unique_<table>_<ordinal>` — so a database created by one path and reopened
// by the other never grows a second unique index on the same column.
// ----------------------------------------------------------------------------

namespace {

struct CoreUniqueGadgetTag {};

lattice::model_schema core_unique_gadget_schema() {
    lattice::model_schema schema;
    schema.table_name = "CoreUniqueGadget";

    lattice::property_descriptor serial;
    serial.name = "serial";
    serial.type = lattice::column_type::text;
    serial.kind = lattice::property_kind::primitive;
    serial.nullable = true;
    serial.is_unique = true;

    lattice::property_descriptor label;
    label.name = "label";
    label.type = lattice::column_type::text;
    label.kind = lattice::property_kind::primitive;
    label.nullable = true;

    schema.properties = {serial, label};
    return schema;
}

// Registration is global (the table is created by every subsequent open in
// this process) — the DDL is tiny and fully guarded, so other tests are
// unaffected.
void register_core_unique_gadget() {
    lattice::schema_registry::instance().register_model(
        typeid(CoreUniqueGadgetTag), core_unique_gadget_schema());
}

}  // namespace

TEST(Schema, CorePathCreatesUniquePropertyIndexWithBridgeNaming) {
    register_core_unique_gadget();

    TempDB tmp{"coreunique"};
    lattice::lattice_db db(tmp.str());

    // Exactly one unique index, named as the bridge's Phase 8 would name the
    // equivalent single-column constraint.
    auto rows = db.db().query(
        "SELECT name FROM sqlite_master "
        "WHERE type='index' AND tbl_name='CoreUniqueGadget' AND name LIKE 'unique_%'");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("name")), "unique_CoreUniqueGadget_0");

    // Enforcement: duplicate value for the unique column must be rejected.
    db.db().execute(
        "INSERT INTO CoreUniqueGadget(globalId, serial, label) VALUES('g-1', 'S1', 'a')");
    EXPECT_THROW(
        db.db().execute(
            "INSERT INTO CoreUniqueGadget(globalId, serial, label) VALUES('g-2', 'S1', 'b')"),
        lattice::db_error);
    // Distinct values still insert.
    db.db().execute(
        "INSERT INTO CoreUniqueGadget(globalId, serial, label) VALUES('g-3', 'S2', 'c')");
    auto count = db.db().query("SELECT COUNT(*) AS c FROM CoreUniqueGadget");
    EXPECT_EQ(std::get<int64_t>(count[0].at("c")), 2);
}

TEST(Schema, CoreUniquePassDeduplicatesPreexistingRowsKeepingNewest) {
    register_core_unique_gadget();

    TempDB tmp{"coreuniquededup"};
    {
        // Table pre-exists with duplicate data (rows inserted before the
        // constraint could be enforced) — mirrors the bridge pass's recovery:
        // deduplicate keeping the newest row per unique value, then index.
        lattice::database old_db(tmp.str());
        old_db.execute(R"(
            CREATE TABLE CoreUniqueGadget (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                globalId TEXT UNIQUE,
                serial TEXT,
                label TEXT
            )
        )");
        old_db.execute(
            "INSERT INTO CoreUniqueGadget (globalId, serial, label) VALUES ('a', 'DUP', 'old')");
        old_db.execute(
            "INSERT INTO CoreUniqueGadget (globalId, serial, label) VALUES ('b', 'DUP', 'new')");
    }

    lattice::lattice_db db(tmp.str());

    auto rows = db.db().query("SELECT label FROM CoreUniqueGadget WHERE serial = 'DUP'");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("label")), "new")
        << "dedup must keep the newest row per unique value";

    auto idx = db.db().query(
        "SELECT name FROM sqlite_master "
        "WHERE type='index' AND name='unique_CoreUniqueGadget_0'");
    EXPECT_EQ(idx.size(), 1u);
}

namespace {

struct UnionMigrationDB : lattice::lattice_db {
    using lattice::lattice_db::lattice_db;
    using lattice::lattice_db::create_model_table_public;
};

lattice::union_descriptor migration_union(const std::string& table, bool expanded) {
    lattice::union_descriptor descriptor;
    descriptor.union_table_name = table;
    descriptor.cases.push_back({"text", {{"", lattice::column_type::text}}});
    if (expanded) descriptor.cases.push_back({"amount", {{"", lattice::column_type::integer}}});
    return descriptor;
}

void migration_parent(UnionMigrationDB& db, const std::string& table,
                      std::initializer_list<const char*> fields) {
    lattice::model_schema schema;
    schema.table_name = table;
    for (const auto* name : fields) {
        lattice::property_descriptor field;
        field.name = name;
        field.type = lattice::column_type::text;
        field.nullable = true;
        schema.properties.push_back(field);
    }
    db.create_model_table_public(schema);
}

void seed_union_migration(UnionMigrationDB& db) {
    // Apple SQLite may default to legacy rename semantics. Exercise the modern
    // reference-rewriting path used by Linux explicitly on every connection.
    db.db().execute("PRAGMA legacy_alter_table=OFF");
    migration_parent(db, "MigrationOwner", {"first", "second"});
    migration_parent(db, "MigrationSharedOwner", {"item"});
    migration_parent(db, "MigrationLateOwner", {"item"});
    db.ensure_union_table("_MigrationChoiceA", migration_union("_MigrationChoiceA", false), "MigrationOwner", "first");
    db.create_union_cascade_trigger("MigrationOwner", "first", "_MigrationChoiceA");
    db.ensure_union_table("_MigrationChoiceA", migration_union("_MigrationChoiceA", false), "MigrationSharedOwner", "item");
    db.create_union_cascade_trigger("MigrationSharedOwner", "item", "_MigrationChoiceA");
    db.ensure_union_table("_MigrationChoiceB", migration_union("_MigrationChoiceB", false), "MigrationOwner", "second");
    db.create_union_cascade_trigger("MigrationOwner", "second", "_MigrationChoiceB");
    db.db().execute("INSERT INTO _MigrationChoiceA(id,globalId,\"case\",text) VALUES "
                    "(41,'choice-a-1','text','first'),(42,'choice-a-2','text','shared')");
    db.db().execute("INSERT INTO _MigrationChoiceB(id,globalId,\"case\",text) VALUES "
                    "(61,'choice-b-1','text','second')");
    db.db().execute("INSERT INTO MigrationOwner(id,globalId,first,second) "
                    "VALUES(11,'owner-1','choice-a-1','choice-b-1')");
    db.db().execute("INSERT INTO MigrationSharedOwner(id,globalId,item) "
                    "VALUES(12,'owner-2','choice-a-2')");
    db.db().execute("INSERT INTO _lattice_sync_state(audit_entry_id,sync_id,is_synchronized) "
                    "SELECT id,'migration-channel',1 FROM AuditLog");
    db.db().execute("INSERT INTO _lattice_replication_slots(sync_id,confirmed_audit_id,last_active_at,upload_floor) "
                    "SELECT 'migration-channel',MAX(id),'2026-10-22T00:00:00Z',MAX(id) FROM AuditLog");
    db.db().execute("INSERT INTO _lattice_sync_set(sync_id,table_name,global_row_id) "
                    "VALUES('migration-channel','_MigrationChoiceA','choice-a-1')");
    db.db().execute("CREATE TABLE MigrationProbe(id INTEGER PRIMARY KEY AUTOINCREMENT,"
                    "globalId TEXT UNIQUE DEFAULT (lower(hex(randomblob(16)))),kind TEXT)");
    // One external reference and one trigger attached to the rebuilt union.
    // Neither is generated/known to Lattice; spaces require quoted DROP names.
    db.db().execute(R"(CREATE TRIGGER "custom union reference" AFTER UPDATE OF first ON MigrationOwner
        BEGIN INSERT INTO MigrationProbe(kind) SELECT 'parent'
        WHERE EXISTS(SELECT 1 FROM _MigrationChoiceA WHERE globalId=NEW.first); END)");
    db.db().execute(R"(CREATE TRIGGER "custom union insert" AFTER INSERT ON _MigrationChoiceA
        BEGIN INSERT INTO MigrationProbe(kind) VALUES('insert'); END)");
    // A similar-looking name is not a license to erase an unknown trigger.
    db.db().execute(R"(CREATE TRIGGER _cascade_delete_custom_union AFTER DELETE ON MigrationOwner
        BEGIN INSERT INTO MigrationProbe(kind) VALUES('custom'); END)");
}

using MigrationRows = std::map<std::string, std::vector<lattice::database::row_t>>;

MigrationRows union_migration_rows(lattice::database& db) {
    MigrationRows rows;
    const std::pair<const char*, const char*> queries[] = {
        {"owner", "SELECT id,globalId,first,second FROM MigrationOwner ORDER BY id"},
        {"shared", "SELECT id,globalId,item FROM MigrationSharedOwner ORDER BY id"},
        {"late", "SELECT id,globalId,item FROM MigrationLateOwner ORDER BY id"},
        {"first", "SELECT id,globalId,\"case\",text FROM _MigrationChoiceA ORDER BY id"},
        {"second", "SELECT id,globalId,\"case\",text FROM _MigrationChoiceB ORDER BY id"},
        {"probe", "SELECT * FROM MigrationProbe ORDER BY id"},
        {"audit", "SELECT * FROM AuditLog ORDER BY id"},
        {"sync", "SELECT * FROM _lattice_sync_state ORDER BY audit_entry_id,sync_id"},
        {"slots", "SELECT * FROM _lattice_replication_slots ORDER BY sync_id"},
        {"set", "SELECT * FROM _lattice_sync_set ORDER BY sync_id,table_name,global_row_id"},
        {"meta", "SELECT * FROM _lattice_meta ORDER BY key"}
    };
    for (const auto& [name, sql] : queries) rows.emplace(name, db.query(sql));
    return rows;
}

auto union_migration_schema(lattice::database& db) {
    return db.query("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name");
}

auto union_migration_cascades_and_custom(lattice::database& db) {
    return db.query("SELECT name,tbl_name,sql FROM sqlite_master WHERE type='trigger' "
                    "AND (name LIKE '_cascade_delete_%' OR name LIKE 'custom union %') ORDER BY name");
}

void expand_migration_unions(lattice::lattice_db& db) {
    db.ensure_union_table("_MigrationChoiceA", migration_union("_MigrationChoiceA", true), "MigrationOwner", "first");
    // On SQLite with modern ALTER TABLE semantics, this second rename used to
    // fail because the first union's parent cascades still referenced _old.
    db.ensure_union_table("_MigrationChoiceB", migration_union("_MigrationChoiceB", true), "MigrationOwner", "second");
}

struct UnionMigrationIndexDenial {
    sqlite3* connection;
    bool fired = false;
    static int authorize(void* context, int action, const char* name,
                         const char*, const char*, const char*) {
        auto& self = *static_cast<UnionMigrationIndexDenial*>(context);
        if (action == SQLITE_CREATE_INDEX && name && std::strcmp(name, "idx__MigrationChoiceA_case") == 0) {
            self.fired = true;
            return SQLITE_DENY;
        }
        return SQLITE_OK;
    }
    ~UnionMigrationIndexDenial() { sqlite3_set_authorizer(connection, nullptr, nullptr); }
};

} // namespace

TEST(Schema, UnionMigrationRestoresEveryParentAndCustomTrigger) {
    TempDB tmp{"union_cascades"};
    {
        UnionMigrationDB old(tmp.str());
        ASSERT_NO_THROW(seed_union_migration(old));
    }
    lattice::lattice_db db(tmp.str());
    db.db().execute("PRAGMA legacy_alter_table=OFF");
    const auto rows = union_migration_rows(db.db());
    const auto triggers = union_migration_cascades_and_custom(db.db());
    ASSERT_NO_THROW(expand_migration_unions(db));
    EXPECT_EQ(union_migration_rows(db.db()), rows) << "schema copies must preserve IDs, audit and sync state";
    EXPECT_EQ(union_migration_cascades_and_custom(db.db()), triggers);
    EXPECT_FALSE(db.db().table_exists("_MigrationChoiceA_old"));
    EXPECT_FALSE(db.db().table_exists("_MigrationChoiceB_old"));
    EXPECT_TRUE(db.db().get_table_info("_MigrationChoiceA").contains("amount"));
    EXPECT_TRUE(db.db().get_table_info("_MigrationChoiceB").contains("amount"));

    // Restored custom triggers remain functional, and were NOT fired by copy.
    ASSERT_NO_THROW(db.db().execute("UPDATE MigrationOwner SET first=first WHERE id=11"));
    ASSERT_NO_THROW(db.db().execute("INSERT INTO _MigrationChoiceA(globalId,\"case\",amount) "
                                    "VALUES('new-choice','amount',9)"));
    const auto probes = db.db().query("SELECT kind FROM MigrationProbe ORDER BY id");
    ASSERT_EQ(probes.size(), 2u);
    EXPECT_EQ(std::get<std::string>(probes[0].at("kind")), "parent");
    EXPECT_EQ(std::get<std::string>(probes[1].at("kind")), "insert");

    ASSERT_NO_THROW(db.db().execute("DELETE FROM MigrationOwner WHERE id=11"));
    EXPECT_TRUE(db.db().query("SELECT id FROM _MigrationChoiceA WHERE globalId='choice-a-1'").empty());
    EXPECT_TRUE(db.db().query("SELECT id FROM _MigrationChoiceB WHERE globalId='choice-b-1'").empty());
    EXPECT_EQ(db.db().query("SELECT id FROM _MigrationChoiceA WHERE globalId='choice-a-2'").size(), 1u);
    ASSERT_NO_THROW(db.db().execute("DELETE FROM MigrationSharedOwner WHERE id=12"));
    EXPECT_TRUE(db.db().query("SELECT id FROM _MigrationChoiceA WHERE globalId='choice-a-2'").empty());
    EXPECT_EQ(db.db().query("SELECT kind FROM MigrationProbe WHERE kind='custom'").size(), 1u);
}

TEST(Schema, UnionMigrationFailureRollsBackMetadataAndDDLThenRetries) {
    for (const bool outer_transaction : {false, true}) {
        SCOPED_TRACE(outer_transaction ? "caller transaction" : "direct Core call");
        TempDB tmp{"union_rollback"};
        UnionMigrationDB db(tmp.str());
        ASSERT_NO_THROW(seed_union_migration(db));
        const auto initial_rows = union_migration_rows(db.db());
        const auto initial_schema = union_migration_schema(db.db());
        if (outer_transaction) {
            db.begin_transaction();
            db.db().execute("INSERT INTO MigrationProbe(kind) VALUES('caller-owned')");
        }
        const auto rows = union_migration_rows(db.db());
        const auto schema = union_migration_schema(db.db());
        {
            // Deny the index recreation AFTER data copy, trigger restoration
            // and DROP _old. Also register a previously unseen parent: rollback
            // must undo metadata as well as the complete schema/data rebuild.
            UnionMigrationIndexDenial denial{db.db().handle()};
            ASSERT_EQ(sqlite3_set_authorizer(denial.connection, UnionMigrationIndexDenial::authorize, &denial), SQLITE_OK);
            EXPECT_THROW(db.ensure_union_table("_MigrationChoiceA", migration_union("_MigrationChoiceA", true),
                                               "MigrationLateOwner", "item"), lattice::db_error);
            EXPECT_TRUE(denial.fired);
        }
        EXPECT_EQ(db.db().is_in_transaction(), outer_transaction);
        EXPECT_EQ(union_migration_rows(db.db()), rows);
        EXPECT_EQ(union_migration_schema(db.db()), schema);
        ASSERT_NO_THROW(expand_migration_unions(db));
        EXPECT_EQ(db.db().is_in_transaction(), outer_transaction);
        if (outer_transaction) {
            // Releasing the migration savepoint must not commit the caller's
            // unrelated work, nor prevent rolling back the successful retry.
            db.rollback();
            EXPECT_EQ(union_migration_rows(db.db()), initial_rows);
            EXPECT_EQ(union_migration_schema(db.db()), initial_schema);
            ASSERT_NO_THROW(expand_migration_unions(db));
        }
        EXPECT_FALSE(db.db().is_in_transaction());
        EXPECT_EQ(union_migration_rows(db.db()), initial_rows);
        EXPECT_FALSE(db.db().table_exists("_MigrationChoiceA_old"));
        EXPECT_FALSE(db.db().table_exists("_MigrationChoiceB_old"));
    }
}

TEST(Schema, UnionMigrationRefusesRewrittenViewOrForeignKeyWithoutDataLoss) {
    for (const bool foreign_key : {false, true}) {
        SCOPED_TRACE(foreign_key ? "foreign key with delete cascade" : "view");
        TempDB tmp{"union_dependency"};
        UnionMigrationDB db(tmp.str());
        ASSERT_NO_THROW(seed_union_migration(db));
        ASSERT_EQ(std::get<int64_t>(db.db().query("PRAGMA foreign_keys")[0].at("foreign_keys")), 1);
        if (foreign_key) {
            db.db().execute("CREATE TABLE MigrationDependency(id INTEGER PRIMARY KEY, "
                            "globalId TEXT UNIQUE DEFAULT (lower(hex(randomblob(16)))), "
                            "ref TEXT REFERENCES _MigrationChoiceA(globalId) ON DELETE CASCADE)");
            db.db().execute("INSERT INTO MigrationDependency(id,ref) VALUES(7,'choice-a-1')");
        } else {
            db.db().execute("CREATE VIEW MigrationDependency AS SELECT id,globalId FROM _MigrationChoiceA");
        }
        const auto dependency_rows = db.db().query("SELECT * FROM MigrationDependency ORDER BY id");
        const auto rows = union_migration_rows(db.db());
        const auto schema = union_migration_schema(db.db());
        std::string migration_error;
        try {
            db.ensure_union_table("_MigrationChoiceA", migration_union("_MigrationChoiceA", true),
                                  "MigrationLateOwner", "item");
        } catch (const lattice::db_error& error) {
            migration_error = error.what();
        }
        EXPECT_NE(migration_error.find("external table or view"), std::string::npos);
        EXPECT_FALSE(db.db().is_in_transaction());
        EXPECT_EQ(union_migration_schema(db.db()), schema);
        EXPECT_EQ(union_migration_rows(db.db()), rows);
        EXPECT_EQ(db.db().query("SELECT * FROM MigrationDependency ORDER BY id"), dependency_rows)
            << "DROP of the renamed union must never reach foreign-key cascade actions";
        EXPECT_FALSE(db.db().table_exists("_MigrationChoiceA_old"));

        // The refusal leaves a valid, retryable store. Removing the unsupported
        // dependency is an explicit caller choice, never migration cleanup.
        db.db().execute(std::string("DROP ") + (foreign_key ? "TABLE " : "VIEW ") + "MigrationDependency");
        ASSERT_NO_THROW(expand_migration_unions(db));
        EXPECT_EQ(union_migration_rows(db.db()), rows);
    }
}
