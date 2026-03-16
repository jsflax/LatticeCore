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
