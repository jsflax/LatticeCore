#include "TestHelpers.hpp"

// ============================================================================
// Schema Tests — DDL generation, migration, geo/vec0 table creation
// ============================================================================

TEST(Schema, CreateFromSchema) {
    auto schema = lattice::schema_registry::instance().get_schema(typeid(TestTrip));
    ASSERT_NE(schema, nullptr);
    EXPECT_EQ(schema->table_name, "TestTrip");
    EXPECT_EQ(schema->properties.size(), 3u);
}

TEST(Schema, RegistryContainsAll) {
    auto person = lattice::schema_registry::instance().get_schema(typeid(TestPerson));
    auto dog = lattice::schema_registry::instance().get_schema(typeid(TestDog));
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

// Registry-generation controls use isolated real registries, not extra static
// application models. The final SQL callback control uses the actual global
// registry gate and replaces TestPerson with its exact original declaration.
#include <condition_variable>
#include <exception>
#include <atomic>
#if defined(__APPLE__) || defined(__linux__)
#include <sys/wait.h>
#include <signal.h>
#endif

#include "SchemaRegistryGenerationTestAccess.hpp"

namespace {
using RegistryAccess=lattice::detail::schema_registry_generation_test_access;
lattice::model_schema registry_declaration(std::string table,std::string property) {
    lattice::model_schema result;result.table_name=std::move(table);
    lattice::property_descriptor value{};value.name=std::move(property);value.type=lattice::column_type::text;
    result.properties.push_back(std::move(value));return result;
}
struct RegistryRendezvous {
    std::mutex mutex;std::condition_variable changed;bool ready=false,release=false,timed_out=false;
    void arrive() {std::lock_guard lock(mutex);ready=true;changed.notify_all();}
    bool wait_ready() {std::unique_lock lock(mutex);return changed.wait_for(lock,std::chrono::seconds(5),[&]{return ready;});}
    void wait_release() {std::unique_lock lock(mutex);if(!changed.wait_for(lock,std::chrono::seconds(5),[&]{return release;}))timed_out=true;}
    void resume() {std::lock_guard lock(mutex);release=true;changed.notify_all();}
};
struct RegistryThread {
    std::thread value;
    template<class F> explicit RegistryThread(F&& body):value(std::forward<F>(body)){}
    ~RegistryThread(){if(value.joinable())value.join();}
};
}

TEST(SchemaRegistryGeneration, RetainedReplacementAndTypeRemapSurviveRegistryDestruction) {
    lattice::schema_snapshot_v2 old;
    lattice::schema_handle_v2 original;
    RegistryAccess::isolated([&](auto& registry){
        registry.register_model(typeid(TestPerson),registry_declaration("RegistryAlpha","first"));
        old=registry.all_schemas();original=old.get_schema(typeid(TestPerson));
        registry.register_model(typeid(TestPerson),registry_declaration("RegistryAlpha","second"));
        const auto replaced=registry.get_schema(typeid(TestPerson));
        ASSERT_NE(replaced,nullptr);EXPECT_EQ(replaced->properties.at(0).name,"second");
        EXPECT_EQ(original->properties.at(0).name,"first");
        registry.register_model(typeid(TestPerson),registry_declaration("RegistryBeta","third"));
        const auto remapped=registry.all_schemas();
        EXPECT_EQ(remapped.get_schema(typeid(TestPerson))->table_name,"RegistryBeta");
        EXPECT_EQ(remapped.get_schema("RegistryAlpha")->properties.at(0).name,"second");
        EXPECT_EQ(remapped.size(),2u);EXPECT_GT(remapped.generation(),old.generation());
    });
    ASSERT_NE(original,nullptr);EXPECT_EQ(original->table_name,"RegistryAlpha");
    EXPECT_EQ(original->properties.at(0).name,"first");EXPECT_EQ(old.size(),1u);
}

TEST(SchemaRegistryGeneration, ConcurrentPublicationRebasesWithoutLosingEitherWholeModel) {
    RegistryAccess::isolated([&](auto& registry){
        registry.register_model(typeid(TestPerson),registry_declaration("RegistrySeed","seed"));
        const auto before=registry.all_schemas();RegistryRendezvous paused;
        struct context {RegistryRendezvous* rendezvous;std::atomic<bool> first{true};} state{&paused};
        std::exception_ptr worker_error;
        RegistryAccess::observe(registry,[](void* raw,auto&){
            auto& state=*static_cast<context*>(raw);
            if(state.first.exchange(false)){state.rendezvous->arrive();state.rendezvous->wait_release();}
        },&state,[&]{
            RegistryThread worker([&]{try{registry.register_model(typeid(TestDog),registry_declaration("RegistryLeft","left"));}catch(...){worker_error=std::current_exception();}});
            const bool ready=paused.wait_ready();EXPECT_TRUE(ready);
            if(ready) {
                EXPECT_EQ(registry.all_schemas().generation(),before.generation());
                registry.register_model(typeid(TestTrip),registry_declaration("RegistryRight","right"));
                const auto middle=registry.all_schemas();
                EXPECT_EQ(middle.get_schema(typeid(TestTrip))->properties.at(0).name,"right");
                EXPECT_EQ(middle.get_schema(typeid(TestDog)),nullptr);
            }
            paused.resume();
        });
        EXPECT_FALSE(paused.timed_out);EXPECT_EQ(worker_error,nullptr);
        const auto final=registry.all_schemas();EXPECT_EQ(final.size(),3u);
        EXPECT_EQ(final.get_schema(typeid(TestDog))->properties.at(0).name,"left");
        EXPECT_EQ(final.get_schema(typeid(TestTrip))->properties.at(0).name,"right");
        EXPECT_EQ(before.size(),1u);EXPECT_EQ(before.get_schema("RegistryLeft"),nullptr);
    });
}

TEST(SchemaRegistryGeneration, ReentrantPublicationRunsOffGateAndOldLeaseRetires) {
    RegistryAccess::isolated([&](auto& registry){
        registry.register_model(typeid(TestPerson),registry_declaration("RegistrySame","old"));
        auto retained=registry.all_schemas();auto handle=retained.get_schema(typeid(TestPerson));
        const auto old=RegistryAccess::weak(retained);bool entered=false,off_gate=false;
        struct context {bool* entered;bool* off_gate;} state{&entered,&off_gate};
        RegistryAccess::observe(registry,[](void* raw,auto& actual){
            auto& state=*static_cast<context*>(raw);if(*state.entered)return;
            *state.entered=true;*state.off_gate=RegistryAccess::gate_free(actual);
            actual.register_model(typeid(TestDog),registry_declaration("RegistryNested","nested"));
        },&state,[&]{registry.register_model(typeid(TestPerson),registry_declaration("RegistrySame","new"));});
        EXPECT_TRUE(entered);EXPECT_TRUE(off_gate);
        EXPECT_EQ(registry.get_schema(typeid(TestDog))->properties.at(0).name,"nested");
        EXPECT_EQ(registry.get_schema(typeid(TestPerson))->properties.at(0).name,"new");
        retained={};EXPECT_FALSE(old.expired());EXPECT_EQ(handle->properties.at(0).name,"old");
        handle={};EXPECT_TRUE(old.expired());
    });
}

TEST(SchemaRegistryGeneration, FailedCandidateKeepsActualFirstAndPriorGeneration) {
    RegistryAccess::isolated([&](auto& registry){
        registry.register_model(typeid(TestPerson),registry_declaration("RegistryFailure","before"));
        const auto before=registry.all_schemas();std::exception_ptr primary;
        try{throw std::runtime_error("actual candidate refusal");}catch(...){primary=std::current_exception();}
        std::exception_ptr caught;
        RegistryAccess::observe(registry,[](void* raw,auto&){std::rethrow_exception(*static_cast<std::exception_ptr*>(raw));},&primary,[&]{
            try{registry.register_model(typeid(TestPerson),registry_declaration("RegistryFailure","after"));}
            catch(...){caught=std::current_exception();}
        });
        EXPECT_EQ(caught,primary);EXPECT_EQ(registry.all_schemas().generation(),before.generation());
        EXPECT_EQ(registry.get_schema(typeid(TestPerson))->properties.at(0).name,"before");
        EXPECT_TRUE(RegistryAccess::gate_free(registry));
        registry.register_model(typeid(TestPerson),registry_declaration("RegistryFailure","after"));
        EXPECT_EQ(registry.get_schema(typeid(TestPerson))->properties.at(0).name,"after");
    });
}

TEST(SchemaRegistryGeneration, ActualSqlCallbackBorrowsGenerationWithoutWaitingPublicationGate) {
    auto& registry=lattice::schema_registry::instance();
    const auto person=registry.get_schema(typeid(TestPerson));ASSERT_NE(person,nullptr);
    lattice::database db(":memory:");RegistryRendezvous callback_entered,gate_held;
    struct context {RegistryRendezvous* entered;RegistryRendezvous* held;std::exception_ptr error;std::uint64_t seen=0;} state{&callback_entered,&gate_held};
    ASSERT_EQ(sqlite3_create_function_v2(db.handle(),"actual_registry_generation",0,SQLITE_UTF8,&state,
        [](sqlite3_context* result,int,sqlite3_value**){
            auto& state=*static_cast<context*>(sqlite3_user_data(result));
            try {
                state.entered->arrive();if(!state.held->wait_ready())throw std::runtime_error("publication gate rendezvous missing");
                const auto actual=lattice::detail::schema_read_scope_v2::lookup("TestPerson");
                if(!actual)throw std::runtime_error("actual registered model missing");
                state.seen=RegistryAccess::entered_generation();state.held->resume();
                sqlite3_result_int64(result,static_cast<sqlite3_int64>(state.seen));
            }catch(...){state.error=std::current_exception();state.held->resume();sqlite3_result_error(result,"actual registry callback failed",-1);}
        },nullptr,nullptr,nullptr),SQLITE_OK);
    std::exception_ptr worker_error;
    RegistryThread worker([&]{try{
        if(!callback_entered.wait_ready())throw std::runtime_error("actual SQL callback missing");
        RegistryAccess::gate(registry,[&]{gate_held.arrive();gate_held.wait_release();});
    }catch(...){worker_error=std::current_exception();gate_held.arrive();}});
    const auto before=registry.all_schemas().generation();
    std::vector<lattice::database::row_t> rows;
    try{rows=db.query("SELECT actual_registry_generation() AS generation");}catch(...){gate_held.resume();throw;}
    worker.value.join();EXPECT_EQ(state.error,nullptr);EXPECT_EQ(worker_error,nullptr);
    EXPECT_FALSE(gate_held.timed_out);ASSERT_EQ(rows.size(),1u);
    EXPECT_EQ(std::get<int64_t>(rows.at(0).at("generation")),static_cast<int64_t>(before));
    registry.register_model(typeid(TestPerson),*person); // exact declaration, no new catalog model
    const auto after=registry.all_schemas().generation();EXPECT_GT(after,before);
    // The next actual operation receives the successor generation. No old TLS
    // view survives the preceding physical entry's actual destructor.
    ASSERT_EQ(sqlite3_create_function_v2(db.handle(),"next_registry_generation",0,SQLITE_UTF8,&state,
        [](sqlite3_context* result,int,sqlite3_value**){
            auto& state=*static_cast<context*>(sqlite3_user_data(result));
            try{sqlite3_result_int64(result,static_cast<sqlite3_int64>(RegistryAccess::entered_generation()));}
            catch(...){state.error=std::current_exception();sqlite3_result_error(result,"actual next registry callback failed",-1);}
        },nullptr,nullptr,nullptr),SQLITE_OK);
    const auto next=db.query("SELECT next_registry_generation() AS generation");
    EXPECT_EQ(state.error,nullptr);
    ASSERT_EQ(next.size(),1u);EXPECT_EQ(std::get<int64_t>(next.at(0).at("generation")),static_cast<int64_t>(after));
}

#if defined(__APPLE__) || defined(__linux__)
TEST(SchemaRegistryGeneration, ForkRefusesInheritedGateAndRetainedDataBeforeSynchronization) {
    RegistryAccess::isolated([&](auto& registry){
        registry.register_model(typeid(TestPerson),registry_declaration("RegistryFork","parent"));
        const auto snapshot=registry.all_schemas();const auto model=snapshot.get_schema(typeid(TestPerson));
        RegistryRendezvous locked;
        RegistryThread holder([&]{RegistryAccess::gate(registry,[&]{locked.arrive();locked.wait_release();});});
        const bool ready=locked.wait_ready();EXPECT_TRUE(ready);
        if(!ready){locked.resume();return;}
        const pid_t child=::fork();
        if(child==0) {
            int refused=0;
            try{(void)registry.all_schemas();}catch(...){++refused;}
            try{(void)lattice::schema_registry::instance();}catch(...){++refused;}
            try{(void)snapshot.size();}catch(...){++refused;}
            if(!model)++refused;
            {const auto abandoned=snapshot;if(!abandoned.valid())++refused;}
            ::_exit(refused==5?0:41);
        }
        locked.resume();holder.value.join();ASSERT_GT(child,0);
        int status=0;bool ended=false;
        const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        while(std::chrono::steady_clock::now()<end){const auto rc=::waitpid(child,&status,WNOHANG);if(rc==child){ended=true;break;}if(rc<0)break;std::this_thread::sleep_for(std::chrono::milliseconds(1));}
        if(!ended){::kill(child,SIGKILL);::waitpid(child,&status,0);}
        EXPECT_TRUE(ended);ASSERT_TRUE(WIFEXITED(status));EXPECT_EQ(WEXITSTATUS(status),0);
        EXPECT_FALSE(locked.timed_out);EXPECT_EQ(model->properties.at(0).name,"parent");
    });
}
#endif

TEST(SchemaRegistryGeneration, ValidDeepCatalogDoesNotRetainUnusedFallbackAndManualRefusalRecaptures) {
    auto& registry=lattice::schema_registry::instance();auto original=registry.all_schemas();
    const auto retired=RegistryAccess::weak(original);
    lattice::model_schema exact_person=*original.get_schema(typeid(TestPerson));
    auto captured=lattice::detail::recovery_owner_schema::capture_native();ASSERT_TRUE(captured.valid());
    const auto count=captured.models.size();const auto original_generation=original.generation();original={};
    registry.register_model(typeid(TestPerson),std::move(exact_person)); // exact full same application value
    EXPECT_TRUE(retired.expired());EXPECT_EQ(captured.models.size(),count);
    EXPECT_EQ(captured.models.at("TestPerson").table_name,"TestPerson");
    captured.refuse();auto&& refused=std::move(captured).prepare_native_fallback_();
    const auto current=registry.all_schemas();
    EXPECT_EQ(refused.native_fallback_lease_().generation(),current.generation());
    EXPECT_GT(refused.native_fallback_lease_().generation(),original_generation);
    EXPECT_EQ(refused.native_fallback_lease_().size(),current.size());
}
