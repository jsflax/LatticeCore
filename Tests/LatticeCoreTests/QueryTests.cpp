#include "TestHelpers.hpp"

// ============================================================================
// Query Tests — CRUD, WHERE, ORDER BY, LIMIT, OFFSET, Geo, etc.
// ============================================================================

// ----------------------------------------------------------------------------
// Basic CRUD
// ----------------------------------------------------------------------------

TEST(Query, BasicCRUD) {
    lattice::lattice_db db;

    // CREATE
    TestPerson p{"John", 30, "john@example.com"};
    auto person = db.add(std::move(p));
    ASSERT_NE(person.id(), 0);
    EXPECT_FALSE(person.global_id().empty());

    // READ
    auto all = db.objects<TestPerson>();
    ASSERT_EQ(all.size(), 1u);
    EXPECT_EQ(std::string(all[0].name), "John");
    EXPECT_EQ(int(all[0].age), 30);
    EXPECT_EQ(all[0].email.detach(), std::optional<std::string>("john@example.com"));

    // READ by ID
    auto found = db.find<TestPerson>(person.id());
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(std::string(found->name), "John");

    // UPDATE
    person.age = 31;
    auto updated = db.find<TestPerson>(person.id());
    EXPECT_EQ(int(updated->age), 31);

    // DELETE
    db.remove(person);
    EXPECT_FALSE(db.find<TestPerson>(person.id()).has_value());
    EXPECT_EQ(db.objects<TestPerson>().size(), 0u);
}

TEST(Query, MultipleTypes) {
    lattice::lattice_db db;

    db.add(TestPerson{"Alice", 25, std::nullopt});
    db.add(TestPerson{"Bob", 30, std::nullopt});
    db.add(TestDog{"Max", 25.5, true});
    db.add(TestDog{"Bella", 18.2, true});

    EXPECT_EQ(db.objects<TestPerson>().size(), 2u);
    EXPECT_EQ(db.objects<TestDog>().size(), 2u);
}

TEST(Query, AllPropertyTypes) {
    lattice::lattice_db db;

    auto obj = db.add(TestAllTypes{
        42, 9223372036854775807LL, 3.14159265359, true,
        "Hello, World!", 100, std::string("Optional value")
    });

    auto found = db.find<TestAllTypes>(obj.id());
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(int(found->int_val), 42);
    EXPECT_EQ(int64_t(found->int64_val), 9223372036854775807LL);
    EXPECT_NEAR(double(found->double_val), 3.14159265359, 0.0001);
    EXPECT_EQ(bool(found->bool_val), true);
    EXPECT_EQ(std::string(found->string_val), "Hello, World!");
    EXPECT_EQ(found->optional_int.detach(), 100);
    EXPECT_EQ(found->optional_string.detach(), std::optional<std::string>("Optional value"));

    // Test null optionals
    obj.optional_int = std::nullopt;
    obj.optional_string = std::nullopt;
    found = db.find<TestAllTypes>(obj.id());
    EXPECT_FALSE(found->optional_int.detach().has_value());
    EXPECT_FALSE(found->optional_string.detach().has_value());
}

TEST(Query, AddFromStruct) {
    lattice::lattice_db db;

    TestTrip raw{"Japan", 14, "Cherry blossom season"};
    auto trip = db.add(raw);

    EXPECT_EQ(std::string(trip.name), "Japan");
    EXPECT_EQ(int(trip.days), 14);
    EXPECT_EQ(trip.notes.detach(), std::optional<std::string>("Cherry blossom season"));

    auto found = db.find<TestTrip>(trip.id());
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(std::string(found->name), "Japan");
}

TEST(Query, BulkInsert) {
    lattice::lattice_db db;

    db.write([&]() {
        for (int i = 0; i < 100; i++) {
            db.add(TestPerson{"Person" + std::to_string(i), 20 + (i % 50), std::nullopt});
        }
    });

    EXPECT_EQ(db.objects<TestPerson>().size(), 100u);
}

TEST(Query, GlobalId) {
    lattice::lattice_db db;

    auto p1 = db.add(TestPerson{"Person1", 20, std::nullopt});
    auto p2 = db.add(TestPerson{"Person2", 30, std::nullopt});

    EXPECT_FALSE(p1.global_id().empty());
    EXPECT_FALSE(p2.global_id().empty());
    EXPECT_NE(p1.global_id(), p2.global_id());

    // UUID format: 8-4-4-4-12
    auto gid = p1.global_id();
    EXPECT_EQ(gid.length(), 36u);
    EXPECT_EQ(gid[8], '-');
    EXPECT_EQ(gid[13], '-');
    EXPECT_EQ(gid[18], '-');
    EXPECT_EQ(gid[23], '-');

    // Find by global ID
    auto found = db.find_by_global_id<TestPerson>(p1.global_id());
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(std::string(found->name), "Person1");
}

TEST(Query, EmptyResults) {
    lattice::lattice_db db;

    auto empty = db.objects<TestPerson>();
    EXPECT_EQ(empty.size(), 0u);
    EXPECT_TRUE(empty.empty());

    EXPECT_FALSE(db.find<TestPerson>(999).has_value());
    EXPECT_FALSE(db.find_by_global_id<TestPerson>("00000000-0000-0000-0000-000000000000").has_value());
}

TEST(Query, DoubleBoolProperties) {
    lattice::lattice_db db;

    auto dog = db.add(TestDog{"Max", 25.75, true});
    auto found = db.find<TestDog>(dog.id());
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(std::string(found->name), "Max");
    EXPECT_NEAR(double(found->weight), 25.75, 0.001);
    EXPECT_EQ(bool(found->is_good_boy), true);

    dog.is_good_boy = false;
    found = db.find<TestDog>(dog.id());
    EXPECT_EQ(bool(found->is_good_boy), false);
}

// ----------------------------------------------------------------------------
// WHERE, SORT, LIMIT, OFFSET
// ----------------------------------------------------------------------------

TEST(Query, WhereEquals) {
    lattice::lattice_db db;
    db.add(TestPerson{"John", 30, std::nullopt});
    db.add(TestPerson{"Jane", 25, std::nullopt});
    db.add(TestPerson{"John", 45, std::nullopt});

    auto johns = db.objects<TestPerson>().where("name = 'John'");
    EXPECT_EQ(johns.size(), 2u);

    auto specific = db.objects<TestPerson>().where("name = 'John' AND age = 30");
    EXPECT_EQ(specific.size(), 1u);
    EXPECT_EQ(int(specific[0].age), 30);
}

TEST(Query, WhereComparison) {
    lattice::lattice_db db;
    db.add(TestPerson{"John", 30, std::nullopt});
    db.add(TestPerson{"Jane", 25, std::nullopt});
    db.add(TestPerson{"Tim", 22, std::nullopt});
    db.add(TestPerson{"Alice", 35, std::nullopt});

    auto over25 = db.objects<TestPerson>().where("age > 25");
    EXPECT_EQ(over25.size(), 2u);  // John(30), Alice(35)

    auto under30 = db.objects<TestPerson>().where("age < 30");
    EXPECT_EQ(under30.size(), 2u);  // Jane(25), Tim(22)
}

TEST(Query, WhereString) {
    lattice::lattice_db db;
    db.add(TestPerson{"John Smith", 30, std::nullopt});
    db.add(TestPerson{"Jane Doe", 25, std::nullopt});
    db.add(TestPerson{"Johnny Appleseed", 40, std::nullopt});

    auto likes = db.objects<TestPerson>().where("name LIKE 'John%'");
    EXPECT_EQ(likes.size(), 2u);  // John Smith, Johnny Appleseed
}

TEST(Query, WhereNull) {
    lattice::lattice_db db;
    db.add(TestPerson{"WithEmail", 30, std::string("a@b.com")});
    db.add(TestPerson{"NoEmail", 25, std::nullopt});

    auto with = db.objects<TestPerson>().where("email IS NOT NULL");
    EXPECT_EQ(with.size(), 1u);
    EXPECT_EQ(std::string(with[0].name), "WithEmail");

    auto without = db.objects<TestPerson>().where("email IS NULL");
    EXPECT_EQ(without.size(), 1u);
}

TEST(Query, WhereCompound) {
    lattice::lattice_db db;
    db.add(TestPerson{"John", 30, std::nullopt});
    db.add(TestPerson{"Jane", 25, std::nullopt});
    db.add(TestPerson{"John", 45, std::nullopt});

    auto result = db.objects<TestPerson>().where("name = 'John' OR name = 'Jane'");
    EXPECT_EQ(result.size(), 3u);
}

TEST(Query, SortAscDesc) {
    lattice::lattice_db db;
    db.add(TestPerson{"John", 30, std::nullopt});
    db.add(TestPerson{"Jane", 25, std::nullopt});
    db.add(TestPerson{"Tim", 22, std::nullopt});
    db.add(TestPerson{"Alice", 35, std::nullopt});

    auto asc = db.objects<TestPerson>().sort("age", true);
    EXPECT_EQ(int(asc[0].age), 22);
    EXPECT_EQ(int(asc[3].age), 35);

    auto desc = db.objects<TestPerson>().sort("age", false);
    EXPECT_EQ(int(desc[0].age), 35);
    EXPECT_EQ(int(desc[3].age), 22);
}

TEST(Query, SortByName) {
    lattice::lattice_db db;
    db.add(TestPerson{"John", 30, std::nullopt});
    db.add(TestPerson{"Jane", 25, std::nullopt});
    db.add(TestPerson{"Tim", 22, std::nullopt});
    db.add(TestPerson{"Alice", 35, std::nullopt});

    auto by_name = db.objects<TestPerson>().sort("name");
    EXPECT_EQ(std::string(by_name[0].name), "Alice");
    EXPECT_EQ(std::string(by_name[3].name), "Tim");
}

TEST(Query, LimitOffset) {
    lattice::lattice_db db;
    db.add(TestPerson{"John", 30, std::nullopt});
    db.add(TestPerson{"Jane", 25, std::nullopt});
    db.add(TestPerson{"Tim", 22, std::nullopt});
    db.add(TestPerson{"Alice", 35, std::nullopt});

    auto limited = db.objects<TestPerson>().sort("age", true).limit(2);
    EXPECT_EQ(limited.size(), 2u);
    EXPECT_EQ(int(limited[0].age), 22);  // Tim
    EXPECT_EQ(int(limited[1].age), 25);  // Jane

    auto with_offset = db.objects<TestPerson>().sort("age", true).offset(1).limit(2);
    EXPECT_EQ(with_offset.size(), 2u);
    EXPECT_EQ(int(with_offset[0].age), 25);  // Jane (skipped Tim)
}

TEST(Query, CombinedWhereSortLimit) {
    lattice::lattice_db db;
    db.add(TestPerson{"John", 30, std::nullopt});
    db.add(TestPerson{"Jane", 25, std::nullopt});
    db.add(TestPerson{"Tim", 22, std::nullopt});
    db.add(TestPerson{"Alice", 35, std::nullopt});

    auto combined = db.objects<TestPerson>().where("age >= 25").sort("name").limit(2);
    ASSERT_EQ(combined.size(), 2u);
    EXPECT_EQ(std::string(combined[0].name), "Alice");  // 35
    EXPECT_EQ(std::string(combined[1].name), "Jane");   // 25
}

TEST(Query, Count) {
    lattice::lattice_db db;
    db.add(TestPerson{"A", 1, std::nullopt});
    db.add(TestPerson{"B", 2, std::nullopt});
    db.add(TestPerson{"C", 3, std::nullopt});

    EXPECT_EQ(db.objects<TestPerson>().size(), 3u);
}

// ----------------------------------------------------------------------------
// Transactions
// ----------------------------------------------------------------------------

TEST(Query, TransactionCommit) {
    lattice::lattice_db db;
    db.write([&]() {
        db.add(TestPerson{"TransactionPerson", 50, std::nullopt});
    });
    EXPECT_EQ(db.objects<TestPerson>().size(), 1u);
}

TEST(Query, TransactionRollback) {
    lattice::lattice_db db;
    try {
        db.write([&]() {
            db.add(TestPerson{"WillBeRolledBack", 99, std::nullopt});
            throw std::runtime_error("Simulated error");
        });
    } catch (const std::runtime_error&) {
        // Expected
    }

    // Rolled back — count should be 0
    EXPECT_EQ(db.objects<TestPerson>().size(), 0u);
}

// ----------------------------------------------------------------------------
// geo_bounds CRUD & Queries
// ----------------------------------------------------------------------------

TEST(Query, GeoBoundsCreate) {
    lattice::lattice_db db;

    auto point = lattice::geo_bounds::point(37.78, -122.41);
    auto place = db.add(TestPlace{"Coffee Shop", point});

    ASSERT_NE(place.id(), 0);
    auto loc = place.location.detach();
    EXPECT_TRUE(loc.is_point());
    EXPECT_NEAR(loc.min_lat, 37.78, 0.0001);
    EXPECT_NEAR(loc.max_lon, -122.41, 0.0001);
}

TEST(Query, GeoBoundsUpdate) {
    lattice::lattice_db db;

    auto place = db.add(TestPlace{"Shop", lattice::geo_bounds::point(37.78, -122.41)});
    place.location = lattice::geo_bounds::point(40.71, -74.01);  // NYC

    auto updated = db.find<TestPlace>(place.id());
    auto loc = updated->location.detach();
    EXPECT_NEAR(loc.min_lat, 40.71, 0.0001);
    EXPECT_NEAR(loc.min_lon, -74.01, 0.0001);
}

TEST(Query, GeoBoundsDelete) {
    lattice::lattice_db db;
    auto place = db.add(TestPlace{"Shop", lattice::geo_bounds::point(0, 0)});
    db.remove(place);
    EXPECT_EQ(db.objects<TestPlace>().size(), 0u);
}

TEST(Query, GeoBoundsWithinBBox) {
    lattice::lattice_db db;

    db.add(TestPlace{"SF Coffee", lattice::geo_bounds::point(37.78, -122.41)});
    db.add(TestPlace{"SF Restaurant", lattice::geo_bounds::point(37.79, -122.40)});
    db.add(TestPlace{"Oakland Cafe", lattice::geo_bounds::point(37.80, -122.27)});
    db.add(TestPlace{"NYC Pizza", lattice::geo_bounds::point(40.71, -74.01)});
    db.add(TestPlace{"NYC Deli", lattice::geo_bounds::point(40.72, -74.00)});
    db.add(TestPlace{"LA Taco", lattice::geo_bounds::point(34.05, -118.24)});

    // SF area
    auto sf = db.query<TestPlace>()
        .within_bbox("location", 37.7, 37.85, -122.5, -122.2)
        .execute();
    EXPECT_EQ(sf.size(), 3u);

    // NYC area
    auto nyc = db.query<TestPlace>()
        .within_bbox("location", 40.5, 41.0, -74.5, -73.5)
        .execute();
    EXPECT_EQ(nyc.size(), 2u);

    // LA area
    auto la = db.query<TestPlace>()
        .within_bbox("location", 33.0, 35.0, -119.0, -117.0)
        .execute();
    EXPECT_EQ(la.size(), 1u);

    // Empty (ocean)
    auto ocean = db.query<TestPlace>()
        .within_bbox("location", 0.0, 1.0, -150.0, -149.0)
        .execute();
    EXPECT_EQ(ocean.size(), 0u);

    // Combined with WHERE
    auto sf_coffee = db.query<TestPlace>()
        .within_bbox("location", 37.7, 37.85, -122.5, -122.2)
        .where("name LIKE '%Coffee%'")
        .execute();
    ASSERT_EQ(sf_coffee.size(), 1u);
    EXPECT_EQ(std::string(sf_coffee[0].name), "SF Coffee");

    // Count with spatial
    auto sf_count = db.query<TestPlace>()
        .within_bbox("location", 37.7, 37.85, -122.5, -122.2)
        .count();
    EXPECT_EQ(sf_count, 3);
}

TEST(Query, OptionalGeoBounds) {
    lattice::lattice_db db;

    auto lm1 = db.add(TestLandmark{"Unknown", std::nullopt});
    auto lm2 = db.add(TestLandmark{"Central Park",
        lattice::geo_bounds(40.764, 40.800, -73.981, -73.949)});

    EXPECT_EQ(db.objects<TestLandmark>().size(), 2u);

    auto f1 = db.find<TestLandmark>(lm1.id());
    EXPECT_FALSE(f1->bounds.has_value());

    auto f2 = db.find<TestLandmark>(lm2.id());
    EXPECT_TRUE(f2->bounds.has_value());
    EXPECT_NEAR(f2->bounds.value().min_lat, 40.764, 0.001);

    // Set on null
    lm1.bounds = std::optional<lattice::geo_bounds>(lattice::geo_bounds::point(51.5, -0.1));
    auto f1b = db.find<TestLandmark>(lm1.id());
    EXPECT_TRUE(f1b->bounds.has_value());

    // Clear
    lm1.bounds = std::nullopt;
    f1b = db.find<TestLandmark>(lm1.id());
    EXPECT_FALSE(f1b->bounds.has_value());
}

TEST(Query, GeoBoundsList) {
    lattice::lattice_db db;

    TestRegion ca;
    ca.name = "California";
    ca.zones = {
        lattice::geo_bounds(32.5, 42.0, -124.5, -114.0),
        lattice::geo_bounds(37.0, 38.5, -123.0, -121.5),
        lattice::geo_bounds(33.5, 34.5, -118.5, -117.5)
    };
    auto managed_ca = db.add(ca);

    auto zones = managed_ca.zones.detach();
    ASSERT_EQ(zones.size(), 3u);
    EXPECT_DOUBLE_EQ(zones[0].min_lat, 32.5);
    EXPECT_DOUBLE_EQ(zones[1].min_lat, 37.0);

    // Push back
    managed_ca.zones.push_back(lattice::geo_bounds(36.5, 37.0, -122.0, -121.5));
    EXPECT_EQ(managed_ca.zones.detach().size(), 4u);

    // Erase
    managed_ca.zones.erase(1);
    EXPECT_EQ(managed_ca.zones.size(), 3u);

    // Clear
    managed_ca.zones.clear();
    EXPECT_EQ(managed_ca.zones.size(), 0u);
}

TEST(Query, GeoBoundsListSpatialQuery) {
    lattice::lattice_db db;

    TestRegion west;
    west.name = "West Coast";
    west.zones = {
        lattice::geo_bounds(32.5, 42.0, -124.5, -114.0),
        lattice::geo_bounds(42.0, 46.3, -124.6, -116.5),
        lattice::geo_bounds(46.0, 49.0, -124.8, -116.9)
    };
    db.add(west);

    TestRegion mountain;
    mountain.name = "Mountain";
    mountain.zones = {
        lattice::geo_bounds(37.0, 41.0, -114.1, -109.0),
        lattice::geo_bounds(37.0, 41.0, -109.1, -102.0)
    };
    db.add(mountain);

    TestRegion northeast;
    northeast.name = "Northeast";
    northeast.zones = {
        lattice::geo_bounds(40.5, 45.0, -80.0, -71.0),
        lattice::geo_bounds(41.0, 43.0, -73.5, -69.9)
    };
    db.add(northeast);

    // SF area → West Coast
    auto sf = db.query<TestRegion>()
        .within_bbox("zones", 37.5, 38.0, -122.5, -122.0)
        .execute();
    ASSERT_EQ(sf.size(), 1u);
    EXPECT_EQ(std::string(sf[0].name), "West Coast");

    // Denver → Mountain
    auto den = db.query<TestRegion>()
        .within_bbox("zones", 39.5, 40.0, -105.0, -104.5)
        .execute();
    ASSERT_EQ(den.size(), 1u);
    EXPECT_EQ(std::string(den[0].name), "Mountain");

    // Gulf of Mexico → nothing
    auto gulf = db.query<TestRegion>()
        .within_bbox("zones", 25.0, 26.0, -90.0, -89.0)
        .execute();
    EXPECT_EQ(gulf.size(), 0u);
}

// ----------------------------------------------------------------------------
// Type-erased API (for Swift interop)
// ----------------------------------------------------------------------------

TEST(Query, TypeErasedApi) {
    lattice::lattice_db db;
    auto person = db.add(TestPerson{"TypeErasedPerson", 42, "test@example.com"});

    lattice::model_base& row = person;
    EXPECT_EQ(row.table_name(), "TestPerson");
    EXPECT_TRUE(row.is_valid());
    EXPECT_FALSE(row.global_id().empty());
}
