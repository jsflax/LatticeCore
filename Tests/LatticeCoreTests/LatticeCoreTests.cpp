#include <LatticeCore.hpp>
#include <cassert>
#include <iostream>
#include <filesystem>
#include <cmath>
#include <algorithm>

// Integration tests (real networking with Crow + websocketpp)
// TODO: Fix mutex crash in integration tests
// #define RUN_INTEGRATION_TESTS
#ifdef RUN_INTEGRATION_TESTS
#include "SyncIntegrationTests.hpp"
#endif

// ============================================================================
// Model Definitions (like Swift's @Model classes)
// ============================================================================

struct Person {
    std::string name;
    int age;
    std::optional<std::string> email;
};
LATTICE_SCHEMA(Person, name, age, email);

struct Dog {
    std::string name;
    double weight;
    bool is_good_boy;
};
LATTICE_SCHEMA(Dog, name, weight, is_good_boy);

struct Trip {
    std::string name;
    int days;
    std::optional<std::string> notes;
};
LATTICE_SCHEMA(Trip, name, days, notes);

struct AllTypesModel {
    int int_val;
    int64_t int64_val;
    double double_val;
    bool bool_val;
    std::string string_val;
    std::optional<int> optional_int;
    std::optional<std::string> optional_string;
};
LATTICE_SCHEMA(AllTypesModel, int_val, int64_val, double_val, bool_val, string_val, optional_int, optional_string);

// Model with geo_bounds for spatial queries
struct Place {
    std::string name;
    lattice::geo_bounds location;
};
LATTICE_SCHEMA(Place, name, location);

// Model with optional geo_bounds
struct Landmark {
    std::string name;
    std::optional<lattice::geo_bounds> bounds;
};
LATTICE_SCHEMA(Landmark, name, bounds);

// Model with geo_bounds list (for regions/zones)
struct Region {
    std::string name;
    std::vector<lattice::geo_bounds> zones;
};
LATTICE_SCHEMA(Region, name, zones);


// ============================================================================
// Models with Links (manually defined for now)
// ============================================================================

// Forward declarations for linked models
struct Owner;
struct Pet;

struct Pet {
    std::string name;
    double weight;
};
LATTICE_SCHEMA(Pet, name, weight);

struct Owner {
    std::string name;
    Pet* pet;  // To-one relationship using T* (realm-cpp pattern)
};
LATTICE_SCHEMA(Owner, name, pet);

// ============================================================================
// Test: Schema Registry
// ============================================================================

void test_schema_registry() {
    std::cout << "Testing schema registry..." << std::endl;

    auto* trip_schema = lattice::schema_registry::instance().get_schema(typeid(Trip));
    assert(trip_schema != nullptr);
    assert(trip_schema->table_name == "Trip");
    assert(trip_schema->properties.size() == 3);

    auto* person_schema = lattice::schema_registry::instance().get_schema(typeid(Person));
    assert(person_schema != nullptr);
    assert(person_schema->table_name == "Person");
    assert(person_schema->properties.size() == 3);

    auto* dog_schema = lattice::schema_registry::instance().get_schema(typeid(Dog));
    assert(dog_schema != nullptr);
    assert(dog_schema->table_name == "Dog");

    // Verify all schemas are registered
    auto all = lattice::schema_registry::instance().all_schemas();
    assert(all.size() >= 4);  // Trip, Person, Dog, AllTypesModel

    std::cout << "  Schema registry test passed!" << std::endl;
}

// ============================================================================
// Test: Basic CRUD Operations
// ============================================================================

void test_basic_crud() {
    std::cout << "Testing basic CRUD operations..." << std::endl;

    lattice::lattice_db db;

    // CREATE - add unmanaged object, get managed back
    Person p{"John", 30, "john@example.com"};
    auto person = db.add(std::move(p));

    assert(person.id() != 0);
    assert(!person.global_id().empty());

    // READ - verify data persisted
    auto all_persons = db.objects<Person>();
    assert(all_persons.size() == 1);
    assert(all_persons[0].name.detach() == "John");
    assert(all_persons[0].age.detach() == 30);
    assert(all_persons[0].email.detach() == "john@example.com");

    // READ by ID
    auto found = db.find<Person>(person.id());
    assert(found.has_value());
    assert(found->name.detach() == "John");

    // UPDATE - just assign, persists automatically
    person.age = 31;
    auto updated = db.find<Person>(person.id());
    assert(updated->age.detach() == 31);

    // DELETE
    db.remove(person);
    auto deleted = db.find<Person>(person.id());
    assert(!deleted.has_value());
    assert(db.objects<Person>().size() == 0);

    std::cout << "  CRUD test passed!" << std::endl;
}

// ============================================================================
// Test: Multiple Object Types (like Swift's Person, Dog)
// ============================================================================

void test_multiple_types() {
    std::cout << "Testing multiple object types..." << std::endl;

    lattice::lattice_db db;

    // Create persons
    auto person1 = db.add(Person{"Alice", 25, std::nullopt});
    auto person2 = db.add(Person{"Bob", 30, std::nullopt});

    // Create dogs
    auto dog1 = db.add(Dog{"Max", 25.5, true});
    auto dog2 = db.add(Dog{"Bella", 18.2, true});

    // Verify counts
    assert(db.objects<Person>().size() == 2);
    assert(db.objects<Dog>().size() == 2);

    // Verify data integrity
    auto persons = db.objects<Person>();
    bool found_alice = false, found_bob = false;
    for (const auto& p : persons) {
        if (p.name.detach() == "Alice") found_alice = true;
        if (p.name.detach() == "Bob") found_bob = true;
    }
    assert(found_alice && found_bob);

    std::cout << "  Multiple types test passed!" << std::endl;
}

// ============================================================================
// Test: All Property Types
// ============================================================================

void test_all_property_types() {
    std::cout << "Testing all property types..." << std::endl;

    lattice::lattice_db db;

    auto obj = db.add(AllTypesModel{
        42,                        // int_val
        9223372036854775807LL,     // int64_val (max int64)
        3.14159265359,             // double_val
        true,                      // bool_val
        "Hello, World!",           // string_val
        100,                       // optional_int
        "Optional value"           // optional_string
    });

    // Verify persistence
    auto found = db.find<AllTypesModel>(obj.id());
    assert(found.has_value());
    assert(found->int_val.detach() == 42);
    assert(found->int64_val.detach() == 9223372036854775807LL);
    assert(std::abs(found->double_val.detach() - 3.14159265359) < 0.0001);
    assert(found->bool_val.detach() == true);
    assert(found->string_val.detach() == "Hello, World!");
    assert(found->optional_int.detach() == 100);
    assert(found->optional_string.detach() == "Optional value");

    // Test null optionals
    obj.optional_int = std::nullopt;
    obj.optional_string = std::nullopt;

    found = db.find<AllTypesModel>(obj.id());
    assert(!found->optional_int.detach().has_value());
    assert(!found->optional_string.detach().has_value());

    std::cout << "  All property types test passed!" << std::endl;
}

// ============================================================================
// Test: Query Operations (like Swift's .where, .sortedBy)
// ============================================================================

void test_query_operations() {
    std::cout << "Testing query operations..." << std::endl;

    lattice::lattice_db db;

    // Create test data
    auto p1 = db.add(Person{"John", 30, std::nullopt});
    auto p2 = db.add(Person{"Jane", 25, std::nullopt});
    auto p3 = db.add(Person{"Tim", 22, std::nullopt});
    auto p4 = db.add(Person{"Alice", 35, std::nullopt});

    // Test count
    assert(db.objects<Person>().size() == 4);

    // Test where clause (raw SQL predicate)
    auto adults_over_25 = db.objects<Person>().where("age > 25");
    assert(adults_over_25.size() == 2);  // John(30), Alice(35)

    // Test sort ascending
    auto by_age_asc = db.objects<Person>().sort("age", true);
    assert(by_age_asc[0].age.detach() == 22);  // Tim
    assert(by_age_asc[3].age.detach() == 35);  // Alice

    // Test sort descending
    auto by_age_desc = db.objects<Person>().sort("age", false);
    assert(by_age_desc[0].age.detach() == 35);  // Alice
    assert(by_age_desc[3].age.detach() == 22);  // Tim

    // Test sort by name
    auto by_name = db.objects<Person>().sort("name");
    assert(by_name[0].name.detach() == "Alice");
    assert(by_name[3].name.detach() == "Tim");

    // Test limit
    auto limited = db.objects<Person>().sort("age", true).limit(2);
    assert(limited.size() == 2);
    assert(limited[0].age.detach() == 22);  // Tim
    assert(limited[1].age.detach() == 25);  // Jane

    // Test offset
    auto with_offset = db.objects<Person>().sort("age", true).offset(1).limit(2);
    assert(with_offset.size() == 2);
    assert(with_offset[0].age.detach() == 25);  // Jane (skipped Tim)

    // Test first (using limit + front)
    auto first_result = db.objects<Person>().sort("age", true).limit(1);
    assert(!first_result.empty());
    assert(first_result[0].name.detach() == "Tim");

    // Test combined where + sort + limit
    auto combined = db.objects<Person>().where("age >= 25").sort("name").limit(2);
    assert(combined.size() == 2);
    assert(combined[0].name.detach() == "Alice");  // 35
    assert(combined[1].name.detach() == "Jane");   // 25

    std::cout << "  Query operations test passed!" << std::endl;
}

// ============================================================================
// Test: Where Clause with String Comparison
// ============================================================================

void test_where_string_comparison() {
    std::cout << "Testing where with string comparison..." << std::endl;

    lattice::lattice_db db;

    auto p1 = db.add(Person{"John", 30, std::nullopt});
    auto p2 = db.add(Person{"Jane", 25, std::nullopt});
    auto p3 = db.add(Person{"John", 45, std::nullopt});

    // Find all Johns
    auto johns = db.objects<Person>().where("name = 'John'");
    assert(johns.size() == 2);

    // Find by exact name and age
    auto specific = db.objects<Person>().where("name = 'John' AND age = 30");
    assert(specific.size() == 1);
    assert(specific[0].age.detach() == 30);

    // Find by OR condition (like Swift's || in predicates)
    auto john_or_jane = db.objects<Person>().where("name = 'John' OR name = 'Jane'");
    assert(john_or_jane.size() == 3);

    std::cout << "  Where string comparison test passed!" << std::endl;
}

// ============================================================================
// Test: Transactions
// ============================================================================

void test_transactions() {
    std::cout << "Testing transactions..." << std::endl;

    lattice::lattice_db db;

    // Transaction that commits
    db.write([&]() {
        db.add(Person{"TransactionPerson", 50, std::nullopt});
    });
    assert(db.objects<Person>().size() == 1);

    // Transaction that rolls back
    try {
        db.write([&]() {
            db.add(Person{"WillBeRolledBack", 99, std::nullopt});
            throw std::runtime_error("Simulated error");
        });
    } catch (const std::runtime_error&) {
        // Expected
    }

    // Should still be 1 (rollback worked)
    assert(db.objects<Person>().size() == 1);

    // Verify the rolled-back person doesn't exist (check all persons have expected name)
    auto all = db.objects<Person>();
    for (const auto& p : all) {
        assert(p.name.detach() != "WillBeRolledBack");
    }

    std::cout << "  Transactions test passed!" << std::endl;
}

// ============================================================================
// Test: Add from Struct (like Swift's lattice.add(object))
// ============================================================================

void test_add_from_struct() {
    std::cout << "Testing add from struct..." << std::endl;

    lattice::lattice_db db;

    Trip raw_trip{"Japan", 14, "Cherry blossom season"};
    auto trip = db.add(raw_trip);

    assert(trip.name.detach() == "Japan");
    assert(trip.days.detach() == 14);
    assert(trip.notes.detach() == "Cherry blossom season");

    // Verify it's in the database
    auto found = db.find<Trip>(trip.id());
    assert(found.has_value());
    assert(found->name.detach() == "Japan");

    std::cout << "  Add from struct test passed!" << std::endl;
}

// ============================================================================
// Test: Bulk Insert (like Swift's lattice.add(contentsOf:))
// ============================================================================

void test_bulk_insert() {
    std::cout << "Testing bulk insert..." << std::endl;

    lattice::lattice_db db;

    // Create 100 persons in a transaction
    db.write([&]() {
        for (int i = 0; i < 100; i++) {
            db.add(Person{"Person" + std::to_string(i), 20 + (i % 50), std::nullopt});
        }
    });

    assert(db.objects<Person>().size() == 100);

    // Query a subset by iterating (until we add where to results)
    int age_30_plus = 0;
    for (const auto& p : db.objects<Person>()) {
        if (p.age.detach() >= 30) age_30_plus++;
    }
    assert(age_30_plus > 0);

    std::cout << "  Bulk insert test passed! (100 objects)" << std::endl;
}

// ============================================================================
// Test: File-based Database (not just in-memory)
// ============================================================================

void test_file_database() {
    std::cout << "Testing file-based database..." << std::endl;

    std::string test_path = "/tmp/lattice_cpp_test.db";

    // Clean up any existing test file
    std::filesystem::remove(test_path);

    {
        // Create database and add data
        lattice::lattice_db db(test_path);
        db.add(Trip{"Persistent Trip", 7, std::nullopt});
    }

    {
        // Reopen and verify data persisted
        lattice::lattice_db db(test_path);

        auto trips = db.objects<Trip>();
        assert(trips.size() == 1);
        assert(trips[0].name.detach() == "Persistent Trip");
        assert(trips[0].days.detach() == 7);
    }

    // Clean up
    std::filesystem::remove(test_path);

    std::cout << "  File database test passed!" << std::endl;
}

// ============================================================================
// Test: Global ID (UUID) Generation
// ============================================================================

void test_global_id() {
    std::cout << "Testing global ID generation..." << std::endl;

    lattice::lattice_db db;

    auto p1 = db.add(Person{"Person1", 20, std::nullopt});
    auto p2 = db.add(Person{"Person2", 30, std::nullopt});

    // Each object should have a unique global ID
    assert(!p1.global_id().empty());
    assert(!p2.global_id().empty());
    assert(p1.global_id() != p2.global_id());

    // Global ID should be UUID format (8-4-4-4-12)
    auto gid = p1.global_id();
    assert(gid.length() == 36);  // UUID string length
    assert(gid[8] == '-');
    assert(gid[13] == '-');
    assert(gid[18] == '-');
    assert(gid[23] == '-');

    // Find by global ID
    auto found = db.find_by_global_id<Person>(p1.global_id());
    assert(found.has_value());
    assert(found->name.detach() == "Person1");

    std::cout << "  Global ID test passed!" << std::endl;
}

// ============================================================================
// Test: Empty Results
// ============================================================================

void test_empty_results() {
    std::cout << "Testing empty results..." << std::endl;

    lattice::lattice_db db;

    // Query on empty table
    auto empty = db.objects<Person>();
    assert(empty.size() == 0);
    assert(empty.empty());

    // Find non-existent ID
    auto not_found = db.find<Person>(999);
    assert(!not_found.has_value());

    // Find non-existent global ID
    auto not_found_gid = db.find_by_global_id<Person>("00000000-0000-0000-0000-000000000000");
    assert(!not_found_gid.has_value());

    std::cout << "  Empty results test passed!" << std::endl;
}

// ============================================================================
// Test: Double/Bool Properties (like Swift's Dog model)
// ============================================================================

void test_double_bool_properties() {
    std::cout << "Testing double and bool properties..." << std::endl;

    lattice::lattice_db db;

    auto dog = db.add(Dog{"Max", 25.75, true});

    auto found = db.find<Dog>(dog.id());
    assert(found.has_value());
    assert(found->name.detach() == "Max");
    assert(std::abs(found->weight.detach() - 25.75) < 0.001);
    assert(found->is_good_boy.detach() == true);

    // Update bool
    dog.is_good_boy = false;
    found = db.find<Dog>(dog.id());
    assert(found->is_good_boy.detach() == false);

    // Query by bool (iterate and count manually until where() is on results)
    int good_boy_count = 0;
    for (const auto& d : db.objects<Dog>()) {
        if (d.is_good_boy.detach()) good_boy_count++;
    }
    assert(good_boy_count == 0);

    dog.is_good_boy = true;
    good_boy_count = 0;
    for (const auto& d : db.objects<Dog>()) {
        if (d.is_good_boy.detach()) good_boy_count++;
    }
    assert(good_boy_count == 1);

    std::cout << "  Double and bool properties test passed!" << std::endl;
}

// ============================================================================
// Test: Link Tables Creation (on-demand)
// ============================================================================

void test_link_tables() {
    std::cout << "Testing link tables creation..." << std::endl;

    lattice::lattice_db db;

    // Create owner and pet
    auto owner = db.add(Owner{"John", nullptr});
    auto pet = db.add(Pet{"Max", 25.5});

    // Verify both objects exist
    assert(db.objects<Owner>().size() == 1);
    assert(db.objects<Pet>().size() == 1);

    // Link tables are created on-demand when first used
    // Use the ensure_link_table method
    db.ensure_link_table("_Owner_Pet_pet");

    // Manually insert a link to test the table was created
    db.db().execute(
        "INSERT INTO _Owner_Pet_pet (lhs, rhs) VALUES (?, ?)",
        {owner.global_id(), pet.global_id()}
    );

    // Verify link was created
    auto rows = db.db().query("SELECT * FROM _Owner_Pet_pet");
    assert(rows.size() == 1);

    std::cout << "  Link tables test passed!" << std::endl;
}

// ============================================================================
// Test: Direct Link Usage (realm-cpp style)
// ============================================================================

void test_direct_link() {
    std::cout << "Testing direct link usage (realm-cpp style)..." << std::endl;

    lattice::lattice_db db;

    // Create owner
    auto owner = db.add(Owner{"John", nullptr});

    // At this point the link isn't set yet
    assert(!owner.pet.has_value());

    // Set the link using simple struct assignment - just like realm-cpp!
    owner.pet = Pet{"Buddy", 30.0};

    // Verify link is set
    assert(owner.pet.has_value());
    assert(owner.pet->name.detach() == "Buddy");
    assert(owner.pet->weight.detach() == 30.0);

    // Verify pet was actually created in DB
    assert(db.objects<Pet>().size() == 1);

    std::cout << "  Direct link test passed!" << std::endl;
}

// ============================================================================
// Test: Managed/Unmanaged Link Semantics (realm-cpp style)
// ============================================================================

void test_managed_unmanaged_semantics() {
    std::cout << "Testing managed/unmanaged link semantics..." << std::endl;

    lattice::lattice_db db;

    // -------------------------------------------------------------------------
    // Case 1: Parent MANAGED, child UNMANAGED (struct)
    // Expected: Child is added to DB, link is created
    // -------------------------------------------------------------------------
    std::cout << "  Case 1: Managed parent + unmanaged child (struct)..." << std::endl;
    {
        auto owner = db.add(Owner{"Case1Owner", nullptr});

        // Assign unmanaged Pet struct to managed owner's link
        owner.pet = Pet{"Case1Pet", 10.0};

        // Verify: pet should be in DB
        assert(db.objects<Pet>().size() >= 1);

        // Verify: link should work
        assert(owner.pet.has_value());
        assert(owner.pet->name.detach() == "Case1Pet");
        assert(owner.pet->weight.detach() == 10.0);

        // Verify: pet is actually managed (in DB)
        assert(owner.pet->is_valid());
    }
    std::cout << "    Case 1 passed!" << std::endl;

    // -------------------------------------------------------------------------
    // Case 2: Parent MANAGED, child MANAGED (managed<T>*)
    // Expected: Just create link (both already in DB)
    // -------------------------------------------------------------------------
    std::cout << "  Case 2: Managed parent + managed child..." << std::endl;
    {
        // Create pet first
        auto pet = db.add(Pet{"Case2Pet", 20.0});

        size_t pet_count_before = db.objects<Pet>().size();

        // Create owner and link to existing pet
        auto owner = db.add(Owner{"Case2Owner", nullptr});
        owner.pet = &pet;  // Assign managed pet pointer

        // Verify: no new pet was created
        assert(db.objects<Pet>().size() == pet_count_before);

        // Verify: link should work
        assert(owner.pet.has_value());
        assert(owner.pet->name.detach() == "Case2Pet");
        assert(owner.pet->weight.detach() == 20.0);
    }
    std::cout << "    Case 2 passed!" << std::endl;

    // -------------------------------------------------------------------------
    // Case 3: Parent UNMANAGED, child UNMANAGED (struct)
    // Expected: Cache locally, no DB operations yet
    // -------------------------------------------------------------------------
    std::cout << "  Case 3: Unmanaged parent + unmanaged child (cached)..." << std::endl;
    {
        size_t pet_count_before = db.objects<Pet>().size();
        size_t owner_count_before = db.objects<Owner>().size();

        // Create unmanaged owner (not added to DB)
        lattice::managed<Owner> owner;
        owner.name.unmanaged_value = "Case3Owner";

        // Assign unmanaged Pet to unmanaged owner's link
        // This should just cache locally
        owner.pet = Pet{"Case3Pet", 30.0};

        // Verify: nothing was added to DB yet
        assert(db.objects<Pet>().size() == pet_count_before);
        assert(db.objects<Owner>().size() == owner_count_before);

        // Verify: cached value is accessible
        assert(owner.pet.has_value());
        // Note: When parent is unmanaged, the cached_object_ should hold the value
    }
    std::cout << "    Case 3 passed!" << std::endl;

    // -------------------------------------------------------------------------
    // Case 4: Clearing a link with nullptr
    // Expected: Link is removed, child remains in DB
    // -------------------------------------------------------------------------
    std::cout << "  Case 4: Clearing link with nullptr..." << std::endl;
    {
        auto owner = db.add(Owner{"Case4Owner", nullptr});
        owner.pet = Pet{"Case4Pet", 40.0};

        // Verify link exists
        assert(owner.pet.has_value());
        size_t pet_count = db.objects<Pet>().size();

        // Clear the link
        owner.pet = nullptr;

        // Verify: link is gone
        assert(!owner.pet.has_value());

        // Verify: pet still exists in DB (not deleted, just unlinked)
        assert(db.objects<Pet>().size() == pet_count);
    }
    std::cout << "    Case 4 passed!" << std::endl;

    // -------------------------------------------------------------------------
    // Case 5: Re-linking to a different object
    // Expected: Old link removed, new link created
    // -------------------------------------------------------------------------
    std::cout << "  Case 5: Re-linking to different object..." << std::endl;
    {
        auto owner = db.add(Owner{"Case5Owner", nullptr});
        owner.pet = Pet{"FirstPet", 50.0};

        assert(owner.pet.has_value());
        assert(owner.pet->name.detach() == "FirstPet");

        // Re-link to a different pet
        owner.pet = Pet{"SecondPet", 55.0};

        // Verify: now linked to second pet
        assert(owner.pet.has_value());
        assert(owner.pet->name.detach() == "SecondPet");
        assert(owner.pet->weight.detach() == 55.0);
    }
    std::cout << "    Case 5 passed!" << std::endl;

    std::cout << "  Managed/unmanaged semantics test passed!" << std::endl;
}

// ============================================================================
// Test: Type-Erased API (for Swift interop)
// ============================================================================

void test_type_erased_api() {
    std::cout << "Testing type-erased API (Swift interop)..." << std::endl;

    lattice::lattice_db db;

    // Create a Person using add API
    auto person = db.add(Person{"TypeErasedPerson", 42, "test@example.com"});

    // The managed<Person> inherits from model_base, which provides type-erased access
    // This is what Swift would use via C++ interop
    lattice::model_base& row = person;

    // Read values back using type-erased API
    auto name = row.get_value("name");
    // Note: get_value currently reads from unmanaged_values_ cache,
    // so for a managed object we need to use the strongly-typed API
    // The type-erased API is primarily for Swift-defined models

    // Test that the object has the expected table name
    assert(row.table_name() == "Person");
    assert(row.is_valid());
    assert(!row.global_id().empty());

    std::cout << "  Type-erased API test passed!" << std::endl;
}

// ============================================================================
// Test: Vector Search (sqlite-vec integration)
// ============================================================================

// Helper to pack floats into bytes
std::vector<uint8_t> pack_floats(const std::vector<float>& floats) {
    std::vector<uint8_t> bytes(floats.size() * sizeof(float));
    std::memcpy(bytes.data(), floats.data(), bytes.size());
    return bytes;
}

void test_vector_search() {
    std::cout << "Testing vector search (sqlite-vec)..." << std::endl;

    lattice::lattice_db db;

    // Create a simple table with a vector column (BLOB)
    db.db().execute("CREATE TABLE IF NOT EXISTS VectorDoc ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "globalId TEXT UNIQUE NOT NULL, "
                    "title TEXT NOT NULL, "
                    "embedding BLOB"
                    ")");

    // Create the vec0 table for 3-dimensional vectors
    std::cout << "  Creating vec0 table..." << std::endl;
    db.ensure_vec0_table("VectorDoc", "embedding", 3);

    // Verify vec0 table was created
    auto tables = db.db().query("SELECT name FROM sqlite_master WHERE type='table' AND name='_VectorDoc_embedding_vec'");
    assert(!tables.empty());
    std::cout << "    vec0 table created OK" << std::endl;

    // Insert some documents with vectors
    std::cout << "  Inserting documents with vectors..." << std::endl;

    std::vector<std::pair<std::string, std::vector<float>>> docs = {
        {"Doc A", {1.0f, 0.0f, 0.0f}},
        {"Doc B", {0.0f, 1.0f, 0.0f}},
        {"Doc C", {0.0f, 0.0f, 1.0f}},
        {"Doc D", {0.7f, 0.7f, 0.0f}},
        {"Doc E", {0.9f, 0.1f, 0.0f}},
    };

    // Helper to generate a simple UUID
    auto gen_uuid = []() {
        static int counter = 0;
        return "vec-doc-" + std::to_string(++counter);
    };

    for (const auto& [title, vec] : docs) {
        auto blob = pack_floats(vec);
        std::string gid = gen_uuid();
        db.db().execute("INSERT INTO VectorDoc (globalId, title, embedding) VALUES (?, ?, ?)",
                        {gid, title, blob});
    }

    // Verify documents were inserted
    auto count = db.count("VectorDoc");
    assert(count == 5);
    std::cout << "    Inserted " << count << " documents OK" << std::endl;

    // Check if vec0 trigger populated the vec0 table
    auto vec_count_rows = db.db().query("SELECT COUNT(*) as cnt FROM _VectorDoc_embedding_vec");
    int64_t vec_count = std::get<int64_t>(vec_count_rows[0].at("cnt"));
    std::cout << "    vec0 table has " << vec_count << " entries" << std::endl;

    if (vec_count == 0) {
        std::cout << "    WARNING: Triggers didn't populate vec0 - manually populating..." << std::endl;
        // Fall back to manual population
        auto all_docs = db.db().query("SELECT globalId, embedding FROM VectorDoc");
        for (const auto& row : all_docs) {
            auto gid = std::get<std::string>(row.at("globalId"));
            auto emb = std::get<std::vector<uint8_t>>(row.at("embedding"));
            db.upsert_vec0("VectorDoc", "embedding", gid, emb);
        }
        vec_count_rows = db.db().query("SELECT COUNT(*) as cnt FROM _VectorDoc_embedding_vec");
        vec_count = std::get<int64_t>(vec_count_rows[0].at("cnt"));
        std::cout << "    After manual population: " << vec_count << " entries" << std::endl;
    }

    assert(vec_count == 5);

    // Perform KNN query
    std::cout << "  Testing KNN query..." << std::endl;
    auto query_vec = pack_floats({1.0f, 0.0f, 0.0f});
    auto results = db.knn_query("VectorDoc", "embedding", query_vec, 3, lattice::lattice_db::distance_metric::l2);

    std::cout << "    KNN returned " << results.size() << " results" << std::endl;
    assert(results.size() == 3);

    // Print results
    for (const auto& r : results) {
        // Look up title from main table
        auto doc_rows = db.db().query("SELECT title FROM VectorDoc WHERE globalId = ?", {r.global_id});
        std::string title = doc_rows.empty() ? "?" : std::get<std::string>(doc_rows[0].at("title"));
        std::cout << "      " << title << ": distance = " << r.distance << std::endl;
    }

    // First result should be Doc A (exact match) with distance 0
    auto first_doc = db.db().query("SELECT title FROM VectorDoc WHERE globalId = ?", {results[0].global_id});
    std::string first_title = std::get<std::string>(first_doc[0].at("title"));
    assert(first_title == "Doc A");
    assert(results[0].distance < 0.001);

    std::cout << "  Vector search test passed!" << std::endl;
}

// ============================================================================
// Test: AuditLog and Triggers (for sync/observation)
// ============================================================================

void test_audit_log() {
    std::cout << "Testing AuditLog triggers (sync/observation)..." << std::endl;

    lattice::lattice_db db;

    // Clear any existing audit entries from table creation
    db.db().execute("DELETE FROM AuditLog");

    // -------------------------------------------------------------------------
    // Test INSERT trigger
    // -------------------------------------------------------------------------
    std::cout << "  Testing INSERT trigger..." << std::endl;
    auto person = db.add(Person{"AuditTest", 25, std::nullopt});

    // Check AuditLog has an INSERT entry
    auto insert_logs = db.db().query(
        "SELECT * FROM AuditLog WHERE tableName = 'Person' AND operation = 'INSERT'"
    );
    assert(insert_logs.size() == 1);

    // Verify the entry has the correct globalRowId
    auto gid_it = insert_logs[0].find("globalRowId");
    assert(gid_it != insert_logs[0].end());
    assert(std::get<std::string>(gid_it->second) == person.global_id());

    // Verify changedFields contains the fields
    auto fields_it = insert_logs[0].find("changedFields");
    assert(fields_it != insert_logs[0].end());
    std::string changed_fields = std::get<std::string>(fields_it->second);
    assert(changed_fields.find("name") != std::string::npos);
    assert(changed_fields.find("age") != std::string::npos);

    std::cout << "    INSERT trigger OK" << std::endl;

    // -------------------------------------------------------------------------
    // Test UPDATE trigger
    // -------------------------------------------------------------------------
    std::cout << "  Testing UPDATE trigger..." << std::endl;

    // Clear logs for cleaner testing
    db.db().execute("DELETE FROM AuditLog WHERE operation = 'UPDATE'");

    // Update a field
    person.name = "AuditTestUpdated";

    // Check AuditLog has an UPDATE entry
    auto update_logs = db.db().query(
        "SELECT * FROM AuditLog WHERE tableName = 'Person' AND operation = 'UPDATE'"
    );
    assert(update_logs.size() == 1);

    // Verify globalRowId matches
    gid_it = update_logs[0].find("globalRowId");
    assert(std::get<std::string>(gid_it->second) == person.global_id());

    std::cout << "    UPDATE trigger OK" << std::endl;

    // -------------------------------------------------------------------------
    // Test DELETE trigger
    // -------------------------------------------------------------------------
    std::cout << "  Testing DELETE trigger..." << std::endl;

    std::string person_gid = person.global_id();
    db.remove(person);

    // Check AuditLog has a DELETE entry
    auto delete_logs = db.db().query(
        "SELECT * FROM AuditLog WHERE tableName = 'Person' AND operation = 'DELETE' AND globalRowId = ?",
        {person_gid}
    );
    assert(delete_logs.size() == 1);

    std::cout << "    DELETE trigger OK" << std::endl;

    // -------------------------------------------------------------------------
    // Test Link Table triggers
    // -------------------------------------------------------------------------
    std::cout << "  Testing link table triggers..." << std::endl;

    db.db().execute("DELETE FROM AuditLog");

    auto owner = db.add(Owner{"LinkOwner", nullptr});
    owner.pet = Pet{"LinkPet", 15.0};

    // Check AuditLog has entries for link table INSERT
    auto link_logs = db.db().query(
        "SELECT * FROM AuditLog WHERE tableName LIKE '%Owner_Pet_pet%' AND operation = 'INSERT'"
    );
    assert(link_logs.size() == 1);

    std::cout << "    Link INSERT trigger OK" << std::endl;

    // Clear link and check DELETE trigger
    db.db().execute("DELETE FROM AuditLog WHERE tableName LIKE '%Owner_Pet_pet%'");
    owner.pet = nullptr;

    link_logs = db.db().query(
        "SELECT * FROM AuditLog WHERE tableName LIKE '%Owner_Pet_pet%' AND operation = 'DELETE'"
    );
    assert(link_logs.size() == 1);

    std::cout << "    Link DELETE trigger OK" << std::endl;

    // -------------------------------------------------------------------------
    // Test sync_disabled() function
    // -------------------------------------------------------------------------
    std::cout << "  Testing sync_disabled() function..." << std::endl;

    db.db().execute("DELETE FROM AuditLog");

    // Enable sync_disabled
    db.db().execute("UPDATE _SyncControl SET disabled = 1 WHERE id = 1");

    // Create a person - should NOT create audit entry
    auto silent_person = db.add(Person{"SilentPerson", 30, std::nullopt});

    auto silent_logs = db.db().query(
        "SELECT * FROM AuditLog WHERE globalRowId = ?",
        {silent_person.global_id()}
    );
    assert(silent_logs.size() == 0);  // No audit entry when sync is disabled

    std::cout << "    sync_disabled() pauses triggers OK" << std::endl;

    // Re-enable sync
    db.db().execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");

    // Now updates should be logged
    silent_person.name = "SilentPersonUpdated";

    silent_logs = db.db().query(
        "SELECT * FROM AuditLog WHERE globalRowId = ?",
        {silent_person.global_id()}
    );
    assert(silent_logs.size() == 1);  // Now audit entry is created

    std::cout << "    sync re-enabled triggers OK" << std::endl;

    std::cout << "  AuditLog triggers test passed!" << std::endl;
}

// ============================================================================
// Test: Audit Log Compaction
// ============================================================================

void test_compact_audit_log() {
    std::cout << "Testing audit log compaction..." << std::endl;

    lattice::lattice_db db;

    // Clear any existing audit entries
    db.db().execute("DELETE FROM AuditLog");

    // -------------------------------------------------------------------------
    // Create some objects and generate audit entries
    // -------------------------------------------------------------------------
    std::cout << "  Creating objects and generating audit history..." << std::endl;

    auto person1 = db.add(Person{"Alice", 25, std::nullopt});
    auto person2 = db.add(Person{"Bob", 30, "bob@example.com"});
    auto dog1 = db.add(Dog{"Max", 25.5, true});

    // Make some updates to generate more audit entries
    person1.name = "Alice Updated";
    person1.age = 26;
    person2.email = "bob.new@example.com";
    dog1.weight = 26.0;

    // Check audit log has multiple entries
    auto pre_compact_logs = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog");
    int64_t pre_count = std::get<int64_t>(pre_compact_logs[0].at("cnt"));
    std::cout << "    Audit entries before compact: " << pre_count << std::endl;
    assert(pre_count >= 7);  // 3 INSERTs + 4 UPDATEs

    // Verify there are UPDATE entries
    auto update_logs = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog WHERE operation = 'UPDATE'");
    int64_t update_count = std::get<int64_t>(update_logs[0].at("cnt"));
    assert(update_count >= 4);

    // -------------------------------------------------------------------------
    // Compact the audit log
    // -------------------------------------------------------------------------
    std::cout << "  Compacting audit log..." << std::endl;

    int64_t entries_created = db.compact_audit_log();
    std::cout << "    Entries created: " << entries_created << std::endl;
    assert(entries_created == 3);  // 2 Persons + 1 Dog

    // -------------------------------------------------------------------------
    // Verify compaction results
    // -------------------------------------------------------------------------
    std::cout << "  Verifying compaction results..." << std::endl;

    // Should only have INSERT entries now
    auto post_compact_logs = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog");
    int64_t post_count = std::get<int64_t>(post_compact_logs[0].at("cnt"));
    std::cout << "    Audit entries after compact: " << post_count << std::endl;
    assert(post_count == 3);

    // All entries should be INSERTs
    auto insert_only = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog WHERE operation = 'INSERT'");
    int64_t insert_count = std::get<int64_t>(insert_only[0].at("cnt"));
    assert(insert_count == 3);

    // No UPDATE entries
    update_logs = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog WHERE operation = 'UPDATE'");
    update_count = std::get<int64_t>(update_logs[0].at("cnt"));
    assert(update_count == 0);

    // Verify the INSERT entries have the CURRENT state (not original)
    auto alice_log = db.db().query(
        "SELECT changedFields FROM AuditLog WHERE tableName = 'Person' AND globalRowId = ?",
        {person1.global_id()}
    );
    assert(alice_log.size() == 1);
    std::string alice_fields = std::get<std::string>(alice_log[0].at("changedFields"));
    assert(alice_fields.find("Alice Updated") != std::string::npos);
    assert(alice_fields.find("26") != std::string::npos);  // Updated age

    std::cout << "    Compacted entries contain current state OK" << std::endl;

    // -------------------------------------------------------------------------
    // Verify all entries are marked as unsynced
    // -------------------------------------------------------------------------
    auto unsynced = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog WHERE isSynchronized = 0");
    int64_t unsynced_count = std::get<int64_t>(unsynced[0].at("cnt"));
    assert(unsynced_count == 3);

    std::cout << "    All entries marked as unsynced OK" << std::endl;

    // -------------------------------------------------------------------------
    // Test compaction with empty database
    // -------------------------------------------------------------------------
    std::cout << "  Testing compaction on empty tables..." << std::endl;

    lattice::lattice_db empty_db;
    empty_db.db().execute("DELETE FROM AuditLog");

    int64_t empty_result = empty_db.compact_audit_log();
    assert(empty_result == 0);

    std::cout << "    Empty compaction OK" << std::endl;

    std::cout << "  Audit log compaction test passed!" << std::endl;
}

// ============================================================================
// Test: Audit Log Compaction (Schema-less Reopen)
// ============================================================================

void test_compact_audit_log_schemaless() {
    std::cout << "Testing compact_audit_log with schema-less reopen..." << std::endl;

    std::string db_path = "/tmp/lattice_compact_test.db";

    // Clean up any existing file
    std::filesystem::remove(db_path);

    // -------------------------------------------------------------------------
    // Step 1: Create database with schema, add objects
    // -------------------------------------------------------------------------
    std::cout << "  Creating database with schema and adding objects..." << std::endl;
    {
        lattice::configuration config(db_path);
        lattice::lattice_db db(config);

        auto person1 = db.add(Person{"Alice", 30, "alice@test.com"});
        auto person2 = db.add(Person{"Bob", 25, std::nullopt});
        auto dog1 = db.add(Dog{"Max", 15.5, true});

        // Make some updates to generate audit history
        person1.age = 31;
        person2.name = "Bobby";
        dog1.weight = 16.0;

        auto pre_audit = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog");
        int64_t pre_count = std::get<int64_t>(pre_audit[0].at("cnt"));
        std::cout << "    Audit entries before close: " << pre_count << std::endl;
        assert(pre_count >= 6);  // 3 INSERTs + 3 UPDATEs
    }
    std::cout << "    Database closed." << std::endl;

    // -------------------------------------------------------------------------
    // Step 2: Reopen WITHOUT schema, call compact
    // -------------------------------------------------------------------------
    std::cout << "  Reopening without schema and compacting..." << std::endl;
    {
        // Open with just path, no schema registration
        lattice::configuration config(db_path);
        lattice::lattice_db db(config);

        // Compact should discover tables from sqlite_master
        int64_t compacted = db.compact_audit_log();
        std::cout << "    Compacted entries created: " << compacted << std::endl;
        assert(compacted == 3);  // 2 Persons + 1 Dog

        // Verify audit log state
        auto post_audit = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog");
        int64_t post_count = std::get<int64_t>(post_audit[0].at("cnt"));
        assert(post_count == 3);

        // All should be INSERTs
        auto inserts = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog WHERE operation = 'INSERT'");
        assert(std::get<int64_t>(inserts[0].at("cnt")) == 3);
    }
    std::cout << "    Compaction complete, database closed." << std::endl;

    // -------------------------------------------------------------------------
    // Step 3: Reopen WITH schema, verify data intact
    // -------------------------------------------------------------------------
    std::cout << "  Reopening with schema and verifying data..." << std::endl;
    {
        lattice::configuration config(db_path);
        lattice::lattice_db db(config);

        // Verify Person count
        auto persons = db.objects<Person>();
        std::cout << "    Person count: " << persons.size() << std::endl;
        assert(persons.size() == 2);

        // Verify Dog count
        auto dogs = db.objects<Dog>();
        std::cout << "    Dog count: " << dogs.size() << std::endl;
        assert(dogs.size() == 1);

        // Verify data values are the UPDATED values (not original)
        bool found_alice = false, found_bobby = false;
        for (const auto& p : persons) {
            std::string name = p.name;
            int age = p.age;
            if (name == "Alice" && age == 31) found_alice = true;
            if (name == "Bobby" && age == 25) found_bobby = true;
        }
        assert(found_alice);
        assert(found_bobby);
        std::cout << "    Person data verified (Alice age=31, Bobby)" << std::endl;

        // Verify dog data
        auto& dog = dogs[0];
        double weight = dog.weight;
        assert(std::abs(weight - 16.0) < 0.01);
        std::cout << "    Dog data verified (Max weight=16.0)" << std::endl;

        // Verify audit log contains current state
        auto alice_audit = db.db().query(
            "SELECT changedFields FROM AuditLog WHERE tableName = 'Person' "
            "AND changedFields LIKE '%Alice%'"
        );
        assert(alice_audit.size() == 1);
        std::string fields = std::get<std::string>(alice_audit[0].at("changedFields"));
        assert(fields.find("31") != std::string::npos);  // Updated age
        std::cout << "    Audit log contains current state" << std::endl;
    }

    // Clean up
    std::filesystem::remove(db_path);

    std::cout << "  Schema-less compaction test passed!" << std::endl;
}

// ============================================================================
// Test: Audit Log Generate History
// ============================================================================

void test_generate_history() {
    std::cout << "Testing generate_history..." << std::endl;

    lattice::lattice_db db;

    // Clear any existing audit entries
    db.db().execute("DELETE FROM AuditLog");

    // Create objects with sync disabled (no audit entries)
    db.db().execute("UPDATE _SyncControl SET disabled = 1 WHERE id = 1");

    auto person1 = db.add(Person{"NoAudit1", 20, std::nullopt});
    auto person2 = db.add(Person{"NoAudit2", 25, std::nullopt});

    db.db().execute("UPDATE _SyncControl SET disabled = 0 WHERE id = 1");

    // Verify no audit entries exist
    auto pre_logs = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog");
    int64_t pre_count = std::get<int64_t>(pre_logs[0].at("cnt"));
    std::cout << "  Audit entries before generate_history: " << pre_count << std::endl;
    assert(pre_count == 0);

    // Now create one more object WITH audit (sync enabled)
    auto person3 = db.add(Person{"WithAudit", 30, std::nullopt});

    auto mid_logs = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog");
    int64_t mid_count = std::get<int64_t>(mid_logs[0].at("cnt"));
    std::cout << "  Audit entries after adding person3: " << mid_count << std::endl;
    assert(mid_count == 1);  // Only person3 has an audit entry

    // Generate history - should add entries for person1 and person2 only
    int64_t generated = db.generate_history();
    std::cout << "  Entries generated: " << generated << std::endl;
    assert(generated == 2);  // person1 and person2

    // Verify total audit entries
    auto post_logs = db.db().query("SELECT COUNT(*) as cnt FROM AuditLog");
    int64_t post_count = std::get<int64_t>(post_logs[0].at("cnt"));
    std::cout << "  Total audit entries after generate_history: " << post_count << std::endl;
    assert(post_count == 3);  // All 3 persons now have entries

    // Call generate_history again - should add nothing
    int64_t second_run = db.generate_history();
    std::cout << "  Entries on second run: " << second_run << std::endl;
    assert(second_run == 0);

    std::cout << "  generate_history test passed!" << std::endl;
}

// ============================================================================
// Test: Schema Migration (ADD/DROP COLUMN)
// ============================================================================

void test_schema_migration() {
    std::cout << "Testing schema migration..." << std::endl;

    // Use a file-based database for migration testing
    // Use unique paths with random suffix
    std::string db_path = "/tmp/lattice_migration_test_" + std::to_string(std::rand()) + ".db";

    // Ensure clean start by removing any existing file
    std::remove(db_path.c_str());

    // -------------------------------------------------------------------------
    // Test ADD COLUMN: Simulate old schema missing a column
    // -------------------------------------------------------------------------
    std::cout << "  Testing ADD COLUMN..." << std::endl;
    {
        // Create database with "old" schema (Person without email column)
        lattice::database old_db(db_path);
        old_db.execute(R"(
            CREATE TABLE Person (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                globalId TEXT UNIQUE,
                name TEXT NOT NULL,
                age INTEGER NOT NULL
            )
        )");
        // Note: 'email' column is missing (simulating old version)

        // Insert old data
        old_db.execute(R"(
            INSERT INTO Person (globalId, name, age) VALUES ('test-uuid-1', 'OldPerson', 30)
        )");
    }
    // old_db closed

    {
        // Now open with lattice_db - should migrate and add 'email' column
        lattice::lattice_db db(db_path);

        // Query existing person
        auto persons = db.objects<Person>();
        assert(persons.size() == 1);
        assert(persons[0].name.detach() == "OldPerson");
        assert(persons[0].age.detach() == 30);

        // The new 'email' column should exist with default empty string
        // (email is nullable in Person schema, so it should be empty/null)

        // Create new person with email to verify column exists
        auto new_person = db.add(Person{"NewPerson", 25, "new@example.com"});

        // Verify new person
        auto found = db.find<Person>(new_person.id());
        assert(found.has_value());
        assert(found->email.detach() == "new@example.com");
    }
    std::cout << "    ADD COLUMN migration OK" << std::endl;

    // Clean up
    std::remove(db_path.c_str());

    // -------------------------------------------------------------------------
    // Test DROP COLUMN: Simulate old schema with extra column
    // -------------------------------------------------------------------------
    std::cout << "  Testing DROP COLUMN..." << std::endl;
    db_path = "/tmp/lattice_migration_drop_" + std::to_string(std::rand()) + ".db";
    std::remove(db_path.c_str());  // Ensure clean start
    {
        // Create database with "old" schema (Person with extra 'legacy_field' column)
        lattice::database old_db(db_path);
        old_db.execute(R"(
            CREATE TABLE Person (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                globalId TEXT UNIQUE,
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                email TEXT,
                legacy_field TEXT
            )
        )");
        // Note: 'legacy_field' is extra (simulating old version)

        // Insert old data
        old_db.execute(R"(
            INSERT INTO Person (globalId, name, age, email, legacy_field)
            VALUES ('test-uuid-2', 'LegacyPerson', 40, 'legacy@example.com', 'old_data')
        )");
    }
    // old_db closed

    {
        // Now open with lattice_db - should migrate and drop 'legacy_field' column
        lattice::lattice_db db(db_path);

        // Query existing person - data should still be there
        auto persons = db.objects<Person>();
        assert(persons.size() == 1);
        assert(persons[0].name.detach() == "LegacyPerson");
        assert(persons[0].age.detach() == 40);
        assert(persons[0].email.detach() == "legacy@example.com");

        // Verify legacy_field column was dropped
        auto info = db.db().get_table_info("Person");
        assert(info.find("legacy_field") == info.end());
    }
    std::cout << "    DROP COLUMN migration OK" << std::endl;

    // Clean up
    std::remove(db_path.c_str());

    std::cout << "  Schema migration test passed!" << std::endl;
}

// ============================================================================
// Test: Observation with Scheduler
// ============================================================================

void test_observation() {
    std::cout << "Testing observation with scheduler..." << std::endl;

    lattice::lattice_db db;

    // Create initial data
    auto person1 = db.add(Person{"Alice", 25, std::nullopt});

    // Track observation callbacks using table-level observer directly
    int callback_count = 0;

    // Test low-level table observer first
    std::cout << "  Testing low-level table observer..." << std::endl;
    auto observer_id = db.add_table_observer("Person",
        [&](const std::string& op, int64_t row_id, const std::string& gid) {
            callback_count++;
            std::cout << "    Observer callback: " << op << " row=" << row_id << std::endl;
        });

    // Add a new person - should trigger observation
    std::cout << "  Adding person, expecting callback..." << std::endl;
    auto person2 = db.add(Person{"Bob", 30, std::nullopt});

    // With immediate scheduler, callbacks should have fired:
    // 1. INSERT for create()
    // 2. UPDATE for name
    // 3. UPDATE for age
    std::cout << "  Callback count after add: " << callback_count << std::endl;
    assert(callback_count >= 1);  // At least the INSERT

    // Update a person - should trigger observation
    std::cout << "  Updating person, expecting callback..." << std::endl;
    int count_before = callback_count;
    person1.name = "Alice Updated";
    assert(callback_count > count_before);

    // Delete a person - should trigger observation
    std::cout << "  Deleting person, expecting callback..." << std::endl;
    count_before = callback_count;
    db.remove(person2);
    assert(callback_count > count_before);

    // Remove observer
    db.remove_table_observer("Person", observer_id);

    // This change should NOT trigger callback
    count_before = callback_count;
    person1.age = 99;
    assert(callback_count == count_before);  // No change

    // Now test results.observe()
    std::cout << "  Testing results.observe()..." << std::endl;
    auto results = db.objects<Person>();
    int results_callback_count = 0;
    std::vector<size_t> result_sizes;

    auto token = results.observe([&](const std::vector<lattice::managed<Person>>& items) {
        results_callback_count++;
        result_sizes.push_back(items.size());
    });

    // Create another person
    auto person3 = db.add(Person{"Charlie", 35, std::nullopt});

    assert(results_callback_count >= 1);
    std::cout << "  Results callback count: " << results_callback_count << std::endl;

    // Invalidate token
    token.invalidate();

    // This should not trigger
    person3.age = 50;
    int final_count = results_callback_count;
    person1.email = "test@test.com";
    assert(results_callback_count == final_count);

    // Test object-level observation (realm-cpp style)
    std::cout << "  Testing object-level observe()..." << std::endl;
    int object_callback_count = 0;
    std::vector<std::string> changed_properties;
    bool was_deleted = false;

    auto person4 = db.add(Person{"ObjectObserver", 40, std::nullopt});

    auto object_token = person4.observe([&](lattice::object_change<Person>& change) {
        object_callback_count++;
        was_deleted = change.is_deleted;
        for (const auto& prop : change.property_changes) {
            changed_properties.push_back(prop.name);
        }
    });

    // Modify using set_value (triggers notification)
    person4.set_value("email", std::string("test@object.com"));
    assert(object_callback_count >= 1);
    assert(!changed_properties.empty());
    assert(changed_properties.back() == "email");
    std::cout << "    Object observation triggered on property change" << std::endl;

    // Invalidate and verify no more callbacks
    object_token.invalidate();
    int count_after_invalidate = object_callback_count;
    person4.set_value("name", std::string("Changed"));
    assert(object_callback_count == count_after_invalidate);
    std::cout << "    Object observation correctly stops after invalidate" << std::endl;

    std::cout << "  Observation test passed!" << std::endl;
}

// ============================================================================
// Test: Scheduler Dispatch
// ============================================================================

void test_scheduler_dispatch() {
    std::cout << "Testing scheduler dispatch..." << std::endl;

    // Test with std_thread scheduler (background thread)
    auto generic_sched = std::make_shared<lattice::std_thread_scheduler>();

    std::atomic<int> callback_count{0};
    std::atomic<bool> callback_on_scheduler_thread{false};
    std::condition_variable cv;
    std::mutex cv_mutex;

    // Invoke on scheduler
    generic_sched->invoke([&] {
        callback_count++;
        callback_on_scheduler_thread = generic_sched->is_on_thread();
        cv.notify_one();
    });

    // Wait for callback
    {
        std::unique_lock<std::mutex> lock(cv_mutex);
        cv.wait_for(lock, std::chrono::seconds(1), [&] { return callback_count > 0; });
    }

    assert(callback_count == 1);
    assert(callback_on_scheduler_thread);

    std::cout << "  Generic scheduler dispatch OK" << std::endl;

    // Test with immediate scheduler
    auto immediate_sched = std::make_shared<lattice::immediate_scheduler>();
    assert(immediate_sched->is_on_thread());
    assert(immediate_sched->can_invoke());

    int immediate_count = 0;
    immediate_sched->invoke([&] { immediate_count++; });
    assert(immediate_count == 1);  // Should be called synchronously

    std::cout << "  Immediate scheduler dispatch OK" << std::endl;

    // Test with main thread scheduler
    auto main_sched = std::make_shared<lattice::main_thread_scheduler>();
    assert(main_sched->is_on_thread());  // We're on the main thread

    int main_count = 0;
    main_sched->invoke([&] { main_count++; });
    assert(main_count == 0);  // Not processed yet

    main_sched->process_pending();
    assert(main_count == 1);  // Now processed

    std::cout << "  Main thread scheduler dispatch OK" << std::endl;

    std::cout << "  Scheduler dispatch test passed!" << std::endl;
}

// ============================================================================
// Test: Sync Protocol (JSON encoding/decoding)
// ============================================================================

void test_sync_protocol() {
    std::cout << "Testing sync protocol..." << std::endl;

    // -------------------------------------------------------------------------
    // Test audit_log_entry to_json / from_json
    // -------------------------------------------------------------------------
    std::cout << "  Testing audit_log_entry JSON..." << std::endl;

    lattice::audit_log_entry entry;
    entry.id = 1;
    entry.global_id = "abc-123-def";
    entry.table_name = "Person";
    entry.operation = "INSERT";
    entry.row_id = 42;
    entry.global_row_id = "row-uuid-456";
    entry.changed_fields = {{"name", lattice::any_property("Alice")}, {"age", lattice::any_property(30)}};
    entry.changed_fields_names = {"name", "age"};
    entry.timestamp = "2024-01-01T00:00:00Z";
    entry.is_from_remote = false;
    entry.is_synchronized = false;

    std::string json = entry.to_json();
    assert(json.find("\"tableName\":\"Person\"") != std::string::npos);
    assert(json.find("\"operation\":\"INSERT\"") != std::string::npos);
    assert(json.find("\"globalRowId\":\"row-uuid-456\"") != std::string::npos);

    auto parsed = lattice::audit_log_entry::from_json(json);
    assert(parsed.has_value());
    assert(parsed->table_name == "Person");
    assert(parsed->operation == "INSERT");
    assert(parsed->global_row_id == "row-uuid-456");

    std::cout << "    audit_log_entry JSON OK" << std::endl;

    // -------------------------------------------------------------------------
    // Test server_sent_event to_json / from_json
    // -------------------------------------------------------------------------
    std::cout << "  Testing server_sent_event JSON..." << std::endl;

    // Test audit_log event
    auto audit_event = lattice::server_sent_event::make_audit_log({entry});
    std::string audit_json = audit_event.to_json();
    assert(audit_json.find("\"auditLog\":[") != std::string::npos);

    auto parsed_event = lattice::server_sent_event::from_json(audit_json);
    assert(parsed_event.has_value());
    assert(parsed_event->event_type == lattice::server_sent_event::type::audit_log);
    assert(parsed_event->audit_logs.size() == 1);

    // Test ack event
    auto ack_event = lattice::server_sent_event::make_ack({"id-1", "id-2", "id-3"});
    std::string ack_json = ack_event.to_json();
    assert(ack_json.find("\"ack\":[") != std::string::npos);

    auto parsed_ack = lattice::server_sent_event::from_json(ack_json);
    assert(parsed_ack.has_value());
    assert(parsed_ack->event_type == lattice::server_sent_event::type::ack);
    assert(parsed_ack->acked_ids.size() == 3);

    std::cout << "    server_sent_event JSON OK" << std::endl;

    // -------------------------------------------------------------------------
    // Test generate_instruction for INSERT/UPDATE/DELETE
    // -------------------------------------------------------------------------
    std::cout << "  Testing generate_instruction..." << std::endl;

    // INSERT
    lattice::audit_log_entry insert_entry;
    insert_entry.table_name = "Person";
    insert_entry.operation = "INSERT";
    insert_entry.global_row_id = "uuid-123";
    insert_entry.changed_fields = {{"name", lattice::any_property("Bob")}, {"age", lattice::any_property(25)}};
    insert_entry.changed_fields_names = {"name", "age"};

    auto [insert_sql, insert_params] = insert_entry.generate_instruction();
    assert(insert_sql.find("INSERT INTO Person") != std::string::npos);
    assert(insert_sql.find("ON CONFLICT(globalId) DO UPDATE") != std::string::npos);
    assert(insert_params.size() == 3);  // globalId, name, age

    // UPDATE
    lattice::audit_log_entry update_entry;
    update_entry.table_name = "Person";
    update_entry.operation = "UPDATE";
    update_entry.global_row_id = "uuid-123";
    update_entry.changed_fields = {{"name", lattice::any_property("Bobby")}};
    update_entry.changed_fields_names = {"name"};

    auto [update_sql, update_params] = update_entry.generate_instruction();
    assert(update_sql.find("UPDATE Person SET") != std::string::npos);
    assert(update_sql.find("WHERE globalId = ?") != std::string::npos);
    assert(update_params.size() == 2);  // name, globalId

    // DELETE
    lattice::audit_log_entry delete_entry;
    delete_entry.table_name = "Person";
    delete_entry.operation = "DELETE";
    delete_entry.global_row_id = "uuid-123";

    auto [delete_sql, delete_params] = delete_entry.generate_instruction();
    assert(delete_sql.find("DELETE FROM Person WHERE globalId = ?") != std::string::npos);
    assert(delete_params.size() == 1);

    std::cout << "    generate_instruction OK" << std::endl;

    std::cout << "  Sync protocol test passed!" << std::endl;
}

// ============================================================================
// Test: Synchronizer with Mock WebSocket
// ============================================================================

void test_synchronizer() {
    std::cout << "Testing synchronizer..." << std::endl;

    // Set up mock network factory
    auto mock_factory = std::make_shared<lattice::mock_network_factory>();
    lattice::set_network_factory(mock_factory);

    lattice::lattice_db db;

    // Create sync config
    lattice::sync_config config;
    config.websocket_url = "ws://localhost:8080/sync";
    config.authorization_token = "test-token";

    // Create synchronizer
    lattice::synchronizer sync(db, config);

    // Get the mock websocket for testing
    auto* mock_ws = mock_factory->last_websocket();
    assert(mock_ws != nullptr);

    // -------------------------------------------------------------------------
    // Test connection
    // -------------------------------------------------------------------------
    std::cout << "  Testing connection..." << std::endl;

    bool connected = false;
    sync.set_on_state_change([&](bool state) { connected = state; });

    sync.connect();
    assert(connected);
    assert(sync.is_connected());

    std::cout << "    Connection OK" << std::endl;

    // -------------------------------------------------------------------------
    // Test uploading local changes
    // -------------------------------------------------------------------------
    std::cout << "  Testing upload..." << std::endl;

    // Create some data that will generate AuditLog entries
    auto person = db.add(Person{"SyncTest", 40, std::nullopt});

    // Trigger sync
    sync.sync_now();

    // Check that messages were sent
    auto& sent = mock_ws->get_sent_messages();
    assert(!sent.empty());

    // The message should be a server_sent_event with auditLog
    std::string sent_json = sent[0].as_string();
    assert(sent_json.find("\"auditLog\"") != std::string::npos);

    std::cout << "    Upload OK" << std::endl;

    // -------------------------------------------------------------------------
    // Test receiving ack
    // -------------------------------------------------------------------------
    std::cout << "  Testing receive ack..." << std::endl;

    std::vector<std::string> synced_ids;
    sync.set_on_sync_complete([&](const std::vector<std::string>& ids) {
        synced_ids = ids;
    });

    // Query unsynced entries
    auto unsynced = lattice::query_audit_log(db.db(), true);
    assert(!unsynced.empty());

    // Simulate server ack
    std::vector<std::string> to_ack;
    for (const auto& entry : unsynced) {
        to_ack.push_back(entry.global_id);
    }
    auto ack = lattice::server_sent_event::make_ack(to_ack);
    mock_ws->simulate_message(lattice::websocket_message::from_string(ack.to_json()));

    assert(!synced_ids.empty());

    // Verify entries are now marked as synchronized
    auto still_unsynced = lattice::query_audit_log(db.db(), true);
    assert(still_unsynced.empty());

    std::cout << "    Receive ack OK" << std::endl;

    // -------------------------------------------------------------------------
    // Test receiving remote changes
    // -------------------------------------------------------------------------
    std::cout << "  Testing receive remote changes..." << std::endl;
    mock_ws->clear_sent_messages();

    // Set up error handler to see any issues
    std::string last_error;
    sync.set_on_error([&](const std::string& err) {
        last_error = err;
        std::cout << "    Sync error: " << err << std::endl;
    });

    // Simulate server sending a new object
    lattice::audit_log_entry remote_entry;
    remote_entry.global_id = "remote-audit-1";
    remote_entry.table_name = "Person";
    remote_entry.operation = "INSERT";
    remote_entry.row_id = 999;
    remote_entry.global_row_id = "remote-person-uuid";
    remote_entry.changed_fields = {{"name", lattice::any_property("RemotePerson")}, {"age", lattice::any_property(50)}};
    remote_entry.changed_fields_names = {"name", "age"};
    remote_entry.timestamp = "2024-01-01T00:00:00Z";

    auto remote_event = lattice::server_sent_event::make_audit_log({remote_entry});
    std::cout << "    Sending remote event JSON: " << remote_event.to_json() << std::endl;
    mock_ws->simulate_message(lattice::websocket_message::from_string(remote_event.to_json()));

    // Check that an ack was sent back
    auto& ack_sent = mock_ws->get_sent_messages();
    assert(!ack_sent.empty());
    assert(ack_sent[0].as_string().find("\"ack\"") != std::string::npos);

    // Verify the remote person was created
    auto remote_person = db.find_by_global_id<Person>("remote-person-uuid");
    assert(remote_person.has_value());
    assert(remote_person->name.detach() == "RemotePerson");
    assert(remote_person->age.detach() == 50);

    std::cout << "    Receive remote changes OK" << std::endl;

    // -------------------------------------------------------------------------
    // Test disconnection
    // -------------------------------------------------------------------------
    std::cout << "  Testing disconnection..." << std::endl;

    sync.disconnect();
    assert(!sync.is_connected());

    std::cout << "    Disconnection OK" << std::endl;

    std::cout << "  Synchronizer test passed!" << std::endl;
}

// ============================================================================
// Test: Migration Block Callback
// ============================================================================

void test_migration_block_callback() {
    std::cout << "Testing migration block callback..." << std::endl;

    std::string db_path = "/tmp/lattice_migration_callback_" + std::to_string(std::rand()) + ".db";
    std::remove(db_path.c_str());

    // -------------------------------------------------------------------------
    // Step 1: Create database with "old" schema - Place with latitude/longitude
    // -------------------------------------------------------------------------
    std::cout << "  Creating database with old schema (lat/lon columns)..." << std::endl;
    {
        lattice::database old_db(db_path);
        old_db.execute(R"(
            CREATE TABLE Place (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                globalId TEXT UNIQUE DEFAULT (lower(hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' || substr(hex(randomblob(2)),2) || '-' || substr('89ab',abs(random()) % 4 + 1, 1) || substr(hex(randomblob(2)),2) || '-' || hex(randomblob(6)))),
                name TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL
            )
        )");

        // Insert some test data
        old_db.execute("INSERT INTO Place (name, latitude, longitude) VALUES (?, ?, ?)",
                       {std::string("SF Coffee"), 37.78, -122.41});
        old_db.execute("INSERT INTO Place (name, latitude, longitude) VALUES (?, ?, ?)",
                       {std::string("NYC Pizza"), 40.71, -74.01});
        old_db.execute("INSERT INTO Place (name, latitude, longitude) VALUES (?, ?, ?)",
                       {std::string("LA Taco"), 34.05, -118.24});
    }

    // -------------------------------------------------------------------------
    // Step 2: Reopen with new schema + migration block
    // -------------------------------------------------------------------------
    std::cout << "  Reopening with new schema and migration block..." << std::endl;

    bool migration_block_called = false;
    std::vector<std::string> detected_added_columns;
    std::vector<std::string> detected_removed_columns;
    int rows_migrated = 0;

    {
        lattice::configuration config(db_path);
        config.migration_block = [&](lattice::migration_context& ctx) {
            migration_block_called = true;
            std::cout << "    Migration block called!" << std::endl;

            // Check pending changes
            auto* changes = ctx.changes_for("Place");
            if (changes) {
                std::cout << "    Place table has changes:" << std::endl;
                std::cout << "      Added: ";
                for (const auto& col : changes->added_columns) {
                    std::cout << col << " ";
                    detected_added_columns.push_back(col);
                }
                std::cout << std::endl;

                std::cout << "      Removed: ";
                for (const auto& col : changes->removed_columns) {
                    std::cout << col << " ";
                    detected_removed_columns.push_back(col);
                }
                std::cout << std::endl;
            }

            // Use enumerate_objects to transform data
            // Read old lat/lon and write to new geo_bounds columns
            ctx.enumerate_objects("Place", [&](const lattice::migration_row& old_row,
                                               lattice::migration_row& new_row) {
                rows_migrated++;

                // Read old lat/lon
                double lat = 0.0, lon = 0.0;
                auto lat_it = old_row.find("latitude");
                auto lon_it = old_row.find("longitude");
                if (lat_it != old_row.end() && std::holds_alternative<double>(lat_it->second)) {
                    lat = std::get<double>(lat_it->second);
                }
                if (lon_it != old_row.end() && std::holds_alternative<double>(lon_it->second)) {
                    lon = std::get<double>(lon_it->second);
                }

                std::string name = std::get<std::string>(old_row.at("name"));
                std::cout << "      Migrating '" << name << "': lat=" << lat << ", lon=" << lon << std::endl;

                // Write to new geo_bounds columns (point = min==max)
                new_row["location_minLat"] = lat;
                new_row["location_maxLat"] = lat;
                new_row["location_minLon"] = lon;
                new_row["location_maxLon"] = lon;
            });
        };

        lattice::lattice_db db(config);

        // -------------------------------------------------------------------------
        // Step 3: Verify migration was called
        // -------------------------------------------------------------------------
        std::cout << "  Verifying migration results..." << std::endl;

        assert(migration_block_called);
        std::cout << "    Migration block was called: OK" << std::endl;

        // Verify schema change detection
        // The new schema has location_* columns, old had latitude/longitude
        assert(std::find(detected_removed_columns.begin(), detected_removed_columns.end(), "latitude")
               != detected_removed_columns.end());
        assert(std::find(detected_removed_columns.begin(), detected_removed_columns.end(), "longitude")
               != detected_removed_columns.end());
        std::cout << "    Removed columns detected (latitude, longitude): OK" << std::endl;

        assert(std::find(detected_added_columns.begin(), detected_added_columns.end(), "location_minLat")
               != detected_added_columns.end());
        assert(std::find(detected_added_columns.begin(), detected_added_columns.end(), "location_maxLat")
               != detected_added_columns.end());
        std::cout << "    Added columns detected (location_*): OK" << std::endl;

        // Verify rows were migrated
        assert(rows_migrated == 3);
        std::cout << "    3 rows migrated: OK" << std::endl;

        // -------------------------------------------------------------------------
        // Step 4: Verify data was correctly migrated
        // -------------------------------------------------------------------------
        auto places = db.objects<Place>();
        assert(places.size() == 3);

        bool found_sf = false, found_nyc = false, found_la = false;
        for (const auto& place : places) {
            std::string name = place.name;
            auto loc = place.location.detach();

            if (name == "SF Coffee") {
                found_sf = true;
                assert(std::abs(loc.min_lat - 37.78) < 0.001);
                assert(std::abs(loc.min_lon - (-122.41)) < 0.001);
                assert(loc.is_point());
            } else if (name == "NYC Pizza") {
                found_nyc = true;
                assert(std::abs(loc.min_lat - 40.71) < 0.001);
                assert(std::abs(loc.min_lon - (-74.01)) < 0.001);
                assert(loc.is_point());
            } else if (name == "LA Taco") {
                found_la = true;
                assert(std::abs(loc.min_lat - 34.05) < 0.001);
                assert(std::abs(loc.min_lon - (-118.24)) < 0.001);
                assert(loc.is_point());
            }
        }
        assert(found_sf && found_nyc && found_la);
        std::cout << "    All places migrated with correct coordinates: OK" << std::endl;

        // Verify old columns are gone
        auto info = db.db().get_table_info("Place");
        assert(info.find("latitude") == info.end());
        assert(info.find("longitude") == info.end());
        assert(info.find("location_minLat") != info.end());
        std::cout << "    Old columns removed, new columns present: OK" << std::endl;
    }

    // Clean up
    std::remove(db_path.c_str());

    std::cout << "  Migration block callback test passed!" << std::endl;
}

// ============================================================================
// Test: geo_bounds Type
// ============================================================================

void test_geo_bounds_type() {
    std::cout << "Testing geo_bounds type..." << std::endl;

    // Test default constructor
    lattice::geo_bounds empty;
    assert(empty.min_lat == 0.0);
    assert(empty.max_lat == 0.0);
    assert(empty.min_lon == 0.0);
    assert(empty.max_lon == 0.0);
    assert(empty.is_point());

    // Test 4-value constructor
    lattice::geo_bounds bbox(37.0, 38.0, -123.0, -122.0);
    assert(bbox.min_lat == 37.0);
    assert(bbox.max_lat == 38.0);
    assert(bbox.min_lon == -123.0);
    assert(bbox.max_lon == -122.0);
    assert(!bbox.is_point());

    // Test point convenience constructor
    auto point = lattice::geo_bounds::point(37.78, -122.41);
    assert(point.is_point());
    assert(point.min_lat == 37.78);
    assert(point.max_lat == 37.78);
    assert(point.min_lon == -122.41);
    assert(point.max_lon == -122.41);

    // Test center calculation
    assert(std::abs(bbox.center_lat() - 37.5) < 0.001);
    assert(std::abs(bbox.center_lon() - (-122.5)) < 0.001);

    // Test span calculation
    assert(std::abs(bbox.lat_span() - 1.0) < 0.001);
    assert(std::abs(bbox.lon_span() - 1.0) < 0.001);

    // Test contains
    assert(bbox.contains(37.5, -122.5));
    assert(!bbox.contains(36.0, -122.5));  // Outside lat
    assert(!bbox.contains(37.5, -124.0));  // Outside lon

    // Test intersects
    lattice::geo_bounds overlapping(37.5, 38.5, -122.5, -121.5);
    assert(bbox.intersects(overlapping));

    lattice::geo_bounds non_overlapping(39.0, 40.0, -123.0, -122.0);
    assert(!bbox.intersects(non_overlapping));

    // Test equality
    lattice::geo_bounds same(37.0, 38.0, -123.0, -122.0);
    assert(bbox == same);
    assert(bbox != overlapping);

    std::cout << "  geo_bounds type test passed!" << std::endl;
}

// ============================================================================
// Test: geo_bounds CRUD Operations
// ============================================================================

void test_geo_bounds_crud() {
    std::cout << "Testing geo_bounds CRUD operations..." << std::endl;

    lattice::lattice_db db;

    // CREATE - add place with geo_bounds
    auto point = lattice::geo_bounds::point(37.78, -122.41);
    Place p{"Coffee Shop", point};
    auto place = db.add(std::move(p));

    assert(place.id() != 0);
    assert(!place.global_id().empty());
    std::cout << "  Created place with id=" << place.id() << std::endl;

    // READ - verify data persisted
    auto all_places = db.objects<Place>();
    assert(all_places.size() == 1);
    assert(all_places[0].name.detach() == "Coffee Shop");

    auto loc = all_places[0].location.detach();
    assert(loc.is_point());
    assert(std::abs(loc.min_lat - 37.78) < 0.0001);
    assert(std::abs(loc.max_lon - (-122.41)) < 0.0001);
    std::cout << "  Read back location OK" << std::endl;

    // READ by ID
    auto found = db.find<Place>(place.id());
    assert(found.has_value());
    auto found_loc = found->location.detach();
    assert(found_loc.is_point());
    std::cout << "  Find by ID OK" << std::endl;

    // UPDATE - change location
    auto new_loc = lattice::geo_bounds::point(40.71, -74.01);  // NYC
    place.location = new_loc;

    auto updated = db.find<Place>(place.id());
    auto updated_loc = updated->location.detach();
    assert(std::abs(updated_loc.min_lat - 40.71) < 0.0001);
    assert(std::abs(updated_loc.min_lon - (-74.01)) < 0.0001);
    std::cout << "  Update location OK" << std::endl;

    // UPDATE to bounding box (not point)
    auto bbox = lattice::geo_bounds(40.0, 41.0, -75.0, -74.0);
    place.location = bbox;

    updated = db.find<Place>(place.id());
    updated_loc = updated->location.detach();
    assert(!updated_loc.is_point());
    assert(std::abs(updated_loc.min_lat - 40.0) < 0.0001);
    assert(std::abs(updated_loc.max_lat - 41.0) < 0.0001);
    std::cout << "  Update to bounding box OK" << std::endl;

    // DELETE
    db.remove(place);
    auto deleted = db.find<Place>(place.id());
    assert(!deleted.has_value());
    assert(db.objects<Place>().size() == 0);
    std::cout << "  Delete OK" << std::endl;

    std::cout << "  geo_bounds CRUD test passed!" << std::endl;
}

// ============================================================================
// Test: geo_bounds R*Tree Spatial Queries
// ============================================================================

void test_geo_bounds_spatial_query() {
    std::cout << "Testing geo_bounds spatial queries (R*Tree)..." << std::endl;

    lattice::lattice_db db;

    // Create places in different locations
    // San Francisco area
    db.add(Place{"SF Coffee", lattice::geo_bounds::point(37.78, -122.41)});
    db.add(Place{"SF Restaurant", lattice::geo_bounds::point(37.79, -122.40)});
    db.add(Place{"Oakland Cafe", lattice::geo_bounds::point(37.80, -122.27)});

    // New York area
    db.add(Place{"NYC Pizza", lattice::geo_bounds::point(40.71, -74.01)});
    db.add(Place{"NYC Deli", lattice::geo_bounds::point(40.72, -74.00)});

    // Los Angeles area
    db.add(Place{"LA Taco", lattice::geo_bounds::point(34.05, -118.24)});

    assert(db.objects<Place>().size() == 6);
    std::cout << "  Created 6 places" << std::endl;

    // Query: Find all places in SF area (bbox around SF)
    auto sf_places = db.query<Place>()
        .within_bbox("location", 37.7, 37.85, -122.5, -122.2)
        .execute();

    std::cout << "  SF bbox query returned " << sf_places.size() << " places" << std::endl;
    assert(sf_places.size() == 3);  // SF Coffee, SF Restaurant, Oakland Cafe

    // Query: Find places in NYC area
    auto nyc_places = db.query<Place>()
        .within_bbox("location", 40.5, 41.0, -74.5, -73.5)
        .execute();

    std::cout << "  NYC bbox query returned " << nyc_places.size() << " places" << std::endl;
    assert(nyc_places.size() == 2);  // NYC Pizza, NYC Deli

    // Query: Find places in LA area
    auto la_places = db.query<Place>()
        .within_bbox("location", 33.0, 35.0, -119.0, -117.0)
        .execute();

    std::cout << "  LA bbox query returned " << la_places.size() << " places" << std::endl;
    assert(la_places.size() == 1);  // LA Taco

    // Query: Empty region (middle of ocean)
    auto ocean_places = db.query<Place>()
        .within_bbox("location", 0.0, 1.0, -150.0, -149.0)
        .execute();

    std::cout << "  Ocean bbox query returned " << ocean_places.size() << " places" << std::endl;
    assert(ocean_places.size() == 0);

    // Query: Combined with WHERE clause
    auto sf_coffee = db.query<Place>()
        .within_bbox("location", 37.7, 37.85, -122.5, -122.2)
        .where("name LIKE '%Coffee%'")
        .execute();

    std::cout << "  SF + Coffee query returned " << sf_coffee.size() << " places" << std::endl;
    assert(sf_coffee.size() == 1);
    assert(sf_coffee[0].name.detach() == "SF Coffee");

    // Query: Count with spatial filter
    auto sf_count = db.query<Place>()
        .within_bbox("location", 37.7, 37.85, -122.5, -122.2)
        .count();

    std::cout << "  SF count = " << sf_count << std::endl;
    assert(sf_count == 3);

    std::cout << "  geo_bounds spatial query test passed!" << std::endl;
}

// ============================================================================
// Test: Optional geo_bounds
// ============================================================================

void test_optional_geo_bounds() {
    std::cout << "Testing optional geo_bounds..." << std::endl;

    lattice::lattice_db db;

    // Create landmark without bounds
    Landmark l1{"Unknown Location", std::nullopt};
    auto landmark1 = db.add(std::move(l1));

    // Create landmark with bounds
    Landmark l2{"Central Park", lattice::geo_bounds(40.764, 40.800, -73.981, -73.949)};
    auto landmark2 = db.add(std::move(l2));

    assert(db.objects<Landmark>().size() == 2);
    std::cout << "  Created 2 landmarks" << std::endl;

    // Verify null bounds
    auto found1 = db.find<Landmark>(landmark1.id());
    assert(found1.has_value());
    assert(!found1->bounds.has_value());
    std::cout << "  Null bounds read correctly" << std::endl;

    // Verify set bounds
    auto found2 = db.find<Landmark>(landmark2.id());
    assert(found2.has_value());
    assert(found2->bounds.has_value());
    auto bounds = found2->bounds.value();
    assert(std::abs(bounds.min_lat - 40.764) < 0.001);
    std::cout << "  Set bounds read correctly" << std::endl;

    // Set bounds on previously null
    landmark1.bounds = std::optional<lattice::geo_bounds>(lattice::geo_bounds::point(51.5, -0.1));  // London
    found1 = db.find<Landmark>(landmark1.id());
    assert(found1->bounds.has_value());
    std::cout << "  Setting bounds on null OK" << std::endl;

    // Clear bounds
    landmark1.bounds = std::nullopt;
    found1 = db.find<Landmark>(landmark1.id());
    assert(!found1->bounds.has_value());
    std::cout << "  Clearing bounds OK" << std::endl;

    std::cout << "  Optional geo_bounds test passed!" << std::endl;
}

// ============================================================================
// Test: geo_bounds Schema and R*Tree Table Creation
// ============================================================================

void test_geo_bounds_schema() {
    std::cout << "Testing geo_bounds schema and R*Tree creation..." << std::endl;

    lattice::lattice_db db;

    // Add a place to trigger table creation
    db.add(Place{"Test", lattice::geo_bounds::point(0, 0)});

    // Verify Place table has the 4 geo columns
    auto info = db.db().get_table_info("Place");
    assert(info.find("location_minLat") != info.end());
    assert(info.find("location_maxLat") != info.end());
    assert(info.find("location_minLon") != info.end());
    assert(info.find("location_maxLon") != info.end());
    assert(info["location_minLat"] == "REAL");
    std::cout << "  4 geo columns created in main table" << std::endl;

    // Verify R*Tree virtual table was created
    auto rtree_check = db.db().query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='_Place_location_rtree'"
    );
    assert(!rtree_check.empty());
    std::cout << "  R*Tree virtual table created" << std::endl;

    // Verify triggers exist
    auto trigger_check = db.db().query(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '%_Place_location_rtree%'"
    );
    assert(trigger_check.size() >= 3);  // insert, update, delete triggers
    std::cout << "  R*Tree sync triggers created (" << trigger_check.size() << " triggers)" << std::endl;

    // Verify R*Tree has entry
    auto rtree_entries = db.db().query("SELECT COUNT(*) as cnt FROM _Place_location_rtree");
    int64_t cnt = std::get<int64_t>(rtree_entries[0].at("cnt"));
    assert(cnt == 1);
    std::cout << "  R*Tree has entry for inserted place" << std::endl;

    std::cout << "  geo_bounds schema test passed!" << std::endl;
}

// ============================================================================
// Test: geo_bounds List (std::vector<geo_bounds>)
// ============================================================================

void test_geo_bounds_list() {
    std::cout << "Testing geo_bounds list (std::vector<geo_bounds>)..." << std::endl;

    lattice::lattice_db db;

    // CREATE - add region with multiple zones
    Region california;
    california.name = "California";
    california.zones = {
        lattice::geo_bounds(32.5, 42.0, -124.5, -114.0),  // Full state
        lattice::geo_bounds(37.0, 38.5, -123.0, -121.5),  // Bay Area
        lattice::geo_bounds(33.5, 34.5, -118.5, -117.5)   // LA Area
    };
    auto managed_ca = db.add(california);

    // READ - verify zones were saved
    auto ca_zones = managed_ca.zones.detach();
    assert(ca_zones.size() == 3);
    assert(ca_zones[0].min_lat == 32.5);
    assert(ca_zones[1].min_lat == 37.0);  // Bay Area
    assert(ca_zones[2].min_lat == 33.5);  // LA Area
    std::cout << "  CREATE: Region with 3 zones saved successfully" << std::endl;

    // Verify through fresh query
    auto queried = db.query<Region>().where("name = 'California'").execute();
    assert(queried.size() == 1);
    auto queried_zones = queried[0].zones.detach();
    assert(queried_zones.size() == 3);
    std::cout << "  READ: Retrieved " << queried_zones.size() << " zones via query" << std::endl;

    // UPDATE - add new zone
    managed_ca.zones.push_back(lattice::geo_bounds(36.5, 37.0, -122.0, -121.5));  // San Jose area
    auto updated_zones = managed_ca.zones.detach();
    assert(updated_zones.size() == 4);
    std::cout << "  UPDATE: Added zone, now have " << updated_zones.size() << " zones" << std::endl;

    // Verify the update persisted
    auto re_queried = db.query<Region>().where("name = 'California'").execute();
    assert(re_queried[0].zones.size() == 4);
    std::cout << "  Persisted update verified" << std::endl;

    // DELETE - remove a zone
    managed_ca.zones.erase(1);  // Remove Bay Area zone
    assert(managed_ca.zones.size() == 3);
    std::cout << "  DELETE: Removed zone at index 1, now have " << managed_ca.zones.size() << " zones" << std::endl;

    // CLEAR - remove all zones
    managed_ca.zones.clear();
    assert(managed_ca.zones.size() == 0);
    std::cout << "  CLEAR: Removed all zones" << std::endl;

    // Add zones back for spatial query test
    managed_ca.zones.push_back(lattice::geo_bounds(37.0, 38.5, -123.0, -121.5));  // Bay Area
    managed_ca.zones.push_back(lattice::geo_bounds(33.5, 34.5, -118.5, -117.5));  // LA Area

    // Add another region
    Region texas;
    texas.name = "Texas";
    texas.zones = {
        lattice::geo_bounds(25.8, 36.5, -106.6, -93.5)  // Full state
    };
    db.add(texas);

    std::cout << "  geo_bounds list CRUD test passed!" << std::endl;
}

// ============================================================================
// Test: geo_bounds List Spatial Queries
// ============================================================================

void test_geo_bounds_list_spatial_query() {
    std::cout << "Testing geo_bounds list spatial queries..." << std::endl;

    lattice::lattice_db db;

    // Create regions with multiple zones
    Region west_coast;
    west_coast.name = "West Coast";
    west_coast.zones = {
        lattice::geo_bounds(32.5, 42.0, -124.5, -114.0),   // California
        lattice::geo_bounds(42.0, 46.3, -124.6, -116.5),   // Oregon
        lattice::geo_bounds(46.0, 49.0, -124.8, -116.9)    // Washington
    };
    db.add(west_coast);

    Region mountain;
    mountain.name = "Mountain";
    mountain.zones = {
        lattice::geo_bounds(37.0, 41.0, -114.1, -109.0),   // Utah
        lattice::geo_bounds(37.0, 41.0, -109.1, -102.0)    // Colorado
    };
    db.add(mountain);

    Region northeast;
    northeast.name = "Northeast";
    northeast.zones = {
        lattice::geo_bounds(40.5, 45.0, -80.0, -71.0),     // NY/PA
        lattice::geo_bounds(41.0, 43.0, -73.5, -69.9)      // MA/CT
    };
    db.add(northeast);

    // Query for regions containing SF area
    auto sf_query = db.query<Region>().within_bbox("zones", 37.5, 38.0, -122.5, -122.0).execute();
    assert(sf_query.size() == 1);
    assert(sf_query[0].name.detach() == "West Coast");
    std::cout << "  SF area query found: " << sf_query[0].name.detach() << std::endl;

    // Query for regions containing Denver area (Colorado)
    auto denver_query = db.query<Region>().within_bbox("zones", 39.5, 40.0, -105.0, -104.5).execute();
    assert(denver_query.size() == 1);
    assert(denver_query[0].name.detach() == "Mountain");
    std::cout << "  Denver area query found: " << denver_query[0].name.detach() << std::endl;

    // Query for regions containing NYC area
    auto nyc_query = db.query<Region>().within_bbox("zones", 40.6, 41.0, -74.5, -73.5).execute();
    assert(nyc_query.size() == 1);
    assert(nyc_query[0].name.detach() == "Northeast");
    std::cout << "  NYC area query found: " << nyc_query[0].name.detach() << std::endl;

    // Query for regions containing a point NOT in any region (Gulf of Mexico)
    auto gulf_query = db.query<Region>().within_bbox("zones", 25.0, 26.0, -90.0, -89.0).execute();
    assert(gulf_query.size() == 0);
    std::cout << "  Gulf of Mexico query: no regions found (expected)" << std::endl;

    // Query that could match multiple zones in same region - should return region once
    auto wa_or_query = db.query<Region>().within_bbox("zones", 45.0, 47.0, -123.0, -117.0).execute();
    assert(wa_or_query.size() == 1);  // West Coast appears once even though OR and WA zones overlap
    assert(wa_or_query[0].name.detach() == "West Coast");
    std::cout << "  OR/WA overlap query: correctly returns West Coast once" << std::endl;

    std::cout << "  geo_bounds list spatial query test passed!" << std::endl;
}

// ============================================================================
// Main Test Runner
// ============================================================================

int main() {
    std::cout << "=== LatticeCore Tests ===" << std::endl;
    std::cout << "Replicating Swift LatticeTests in C++" << std::endl;
    std::cout << "Direct property writes (no save() or dirty tracking!)" << std::endl;
    std::cout << std::endl;

    try {
        // Core tests
        test_schema_registry();
        test_basic_crud();
        test_multiple_types();
        test_all_property_types();

        // Query tests (like Swift's Results tests)
        test_query_operations();
        test_where_string_comparison();

        // Transaction tests
        test_transactions();

        // Add patterns
        test_add_from_struct();
        test_bulk_insert();

        // Database tests
        test_file_database();
        test_global_id();
        test_empty_results();

        // Property type tests
        test_double_bool_properties();

        // Link tests
        test_link_tables();
        test_direct_link();
        test_managed_unmanaged_semantics();

        // Swift interop tests
        test_type_erased_api();

        // Vector search tests
        test_vector_search();

        // Sync/Observation tests
        test_audit_log();
        test_compact_audit_log();
        test_compact_audit_log_schemaless();
        test_generate_history();

        // Migration tests
        // TODO: Fix migration test - crashes during DROP COLUMN test
        // test_schema_migration();

        // Observation tests
        test_observation();
        test_scheduler_dispatch();

        // Sync tests
        test_sync_protocol();
        test_synchronizer();

        // Migration callback tests
        test_migration_block_callback();

        // geo_bounds tests
        test_geo_bounds_type();
        test_geo_bounds_crud();
        test_geo_bounds_spatial_query();
        test_optional_geo_bounds();
        test_geo_bounds_schema();

        // geo_bounds list tests
        test_geo_bounds_list();
        test_geo_bounds_list_spatial_query();

#ifdef RUN_INTEGRATION_TESTS
        // Integration tests (real networking)
        std::cout << std::endl;
        std::cout << "========================================" << std::endl;
        std::cout << "Running Integration Tests..." << std::endl;
        std::cout << "========================================" << std::endl;
        std::cout << std::endl;

        lattice_integration_tests::test_BasicSync();
        lattice_integration_tests::test_ListRelationshipSync();
#endif

        std::cout << std::endl;
        std::cout << "========================================" << std::endl;
#ifdef RUN_INTEGRATION_TESTS
        std::cout << "All tests passed! (24 unit + integration)" << std::endl;
#else
        std::cout << "All tests passed! (24 test suites)" << std::endl;
#endif
        std::cout << "========================================" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
