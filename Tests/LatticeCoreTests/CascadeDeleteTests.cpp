#include "TestHelpers.hpp"

// ============================================================================
// Cascade Delete & Link Tests
// ============================================================================

TEST(Cascade, LinkTableCreation) {
    lattice::lattice_db db;

    auto owner = db.add(TestOwner{"John", nullptr});
    auto pet = db.add(TestPet{"Max", 25.5});

    EXPECT_EQ(db.objects<TestOwner>().size(), 1u);
    EXPECT_EQ(db.objects<TestPet>().size(), 1u);

    // Link tables are created on-demand
    db.ensure_link_table("_TestOwner_TestPet_pet");

    // Manually insert a link
    db.db().execute(
        "INSERT INTO _TestOwner_TestPet_pet (lhs, rhs) VALUES (?, ?)",
        {owner.global_id(), pet.global_id()});

    auto rows = db.db().query("SELECT * FROM _TestOwner_TestPet_pet");
    EXPECT_EQ(rows.size(), 1u);
}

TEST(Cascade, DirectLink) {
    lattice::lattice_db db;

    auto owner = db.add(TestOwner{"John", nullptr});
    EXPECT_FALSE(owner.pet.has_value());

    // Set link
    owner.pet = TestPet{"Buddy", 30.0};
    EXPECT_TRUE(owner.pet.has_value());
    EXPECT_EQ(std::string(owner.pet->name), "Buddy");
    EXPECT_NEAR(double(owner.pet->weight), 30.0, 0.001);

    // Pet should be in DB
    EXPECT_EQ(db.objects<TestPet>().size(), 1u);
}

TEST(Cascade, ManagedLink) {
    lattice::lattice_db db;

    auto pet = db.add(TestPet{"Case2Pet", 20.0});
    size_t pet_count = db.objects<TestPet>().size();

    auto owner = db.add(TestOwner{"Case2Owner", nullptr});
    owner.pet = &pet;

    // No new pet created
    EXPECT_EQ(db.objects<TestPet>().size(), pet_count);
    EXPECT_TRUE(owner.pet.has_value());
    EXPECT_EQ(std::string(owner.pet->name), "Case2Pet");
}

TEST(Cascade, ClearLink) {
    lattice::lattice_db db;

    auto owner = db.add(TestOwner{"Owner", nullptr});
    owner.pet = TestPet{"Pet", 40.0};
    ASSERT_TRUE(owner.pet.has_value());

    size_t pet_count = db.objects<TestPet>().size();
    owner.pet = nullptr;

    // Link gone, pet still exists
    EXPECT_FALSE(owner.pet.has_value());
    EXPECT_EQ(db.objects<TestPet>().size(), pet_count);
}

TEST(Cascade, Relink) {
    lattice::lattice_db db;

    auto owner = db.add(TestOwner{"Owner", nullptr});
    owner.pet = TestPet{"FirstPet", 50.0};
    EXPECT_EQ(std::string(owner.pet->name), "FirstPet");

    owner.pet = TestPet{"SecondPet", 55.0};
    EXPECT_EQ(std::string(owner.pet->name), "SecondPet");
}

TEST(Cascade, LinkTableCleanup) {
    lattice::lattice_db db;

    auto owner = db.add(TestOwner{"CleanupOwner", nullptr});
    owner.pet = TestPet{"CleanupPet", 15.0};

    // Verify link exists
    auto links = db.db().query(
        "SELECT COUNT(*) as cnt FROM _TestOwner_TestPet_pet WHERE lhs = ?",
        {owner.global_id()});
    EXPECT_EQ(std::get<int64_t>(links[0].at("cnt")), 1);

    // Delete owner — link table entries are NOT automatically cascaded
    // (this is current behavior; cascade cleanup is a planned feature)
    std::string gid = owner.global_id();
    db.remove(owner);

    // Owner row is gone
    EXPECT_EQ(db.objects<TestOwner>().size(), 0u);

    // Link entry still exists (orphaned) — documents current behavior
    links = db.db().query(
        "SELECT COUNT(*) as cnt FROM _TestOwner_TestPet_pet WHERE lhs = ?",
        {gid});
    EXPECT_EQ(std::get<int64_t>(links[0].at("cnt")), 1);
}

TEST(Cascade, PreservesTarget) {
    lattice::lattice_db db;

    auto owner = db.add(TestOwner{"Owner", nullptr});
    owner.pet = TestPet{"PreservedPet", 20.0};

    size_t pet_count = db.objects<TestPet>().size();
    db.remove(owner);

    // Pet should still exist (not cascaded)
    EXPECT_EQ(db.objects<TestPet>().size(), pet_count);
}

TEST(Cascade, UnmanagedParentChild) {
    lattice::lattice_db db;

    size_t pet_before = db.objects<TestPet>().size();
    size_t owner_before = db.objects<TestOwner>().size();

    // Unmanaged owner with unmanaged pet — cached locally, no DB ops
    lattice::managed<TestOwner> owner;
    owner.name.unmanaged_value = "Case3Owner";
    owner.pet = TestPet{"Case3Pet", 30.0};

    EXPECT_EQ(db.objects<TestPet>().size(), pet_before);
    EXPECT_EQ(db.objects<TestOwner>().size(), owner_before);
    EXPECT_TRUE(owner.pet.has_value());
}

TEST(Cascade, BulkDeleteCascade) {
    lattice::lattice_db db;

    // Create 100 owners with pets
    db.write([&]() {
        for (int i = 0; i < 100; i++) {
            auto owner = db.add(TestOwner{"Owner" + std::to_string(i), nullptr});
            owner.pet = TestPet{"Pet" + std::to_string(i), double(i)};
        }
    });

    EXPECT_EQ(db.objects<TestOwner>().size(), 100u);
    EXPECT_EQ(db.objects<TestPet>().size(), 100u);

    // Delete all owners
    for (auto& owner : db.objects<TestOwner>()) {
        db.remove(owner);
    }

    EXPECT_EQ(db.objects<TestOwner>().size(), 0u);

    // Pets should still exist (not cascade-deleted)
    EXPECT_EQ(db.objects<TestPet>().size(), 100u);
}
