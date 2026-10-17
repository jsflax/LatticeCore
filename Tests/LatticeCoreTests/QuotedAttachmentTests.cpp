#include "TestHelpers.hpp"
#include <set>

namespace {
std::set<std::string> attached_names(lattice::database& db) {
    std::set<std::string> names;
    for (const auto& row : db.query("PRAGMA database_list")) {
        names.insert(std::get<std::string>(row.at("name")));
    }
    return names;
}

std::set<std::string> visible_people(lattice::database& db) {
    std::set<std::string> names;
    for (const auto& row : db.query("SELECT name FROM TestPerson")) {
        names.insert(std::get<std::string>(row.at("name")));
    }
    return names;
}

std::set<std::string> live_people(lattice::lattice_db& db) {
    std::set<std::string> names;
    for (const auto& person : db.objects<TestPerson>()) {
        names.insert(person.name.detach());
    }
    return names;
}
} // namespace

TEST(QuotedAttachment, AttachReadDetachAndReattachPreserveOtherArms) {
    TempDB main_path("quoted_main"), other_path("quoted_other");
    TempDB quoted_path("quoted_\"arm's");
    const std::string alias = quoted_path.path.stem().string();
    lattice::lattice_db main{lattice::configuration(main_path.str())};
    lattice::lattice_db quoted{lattice::configuration(quoted_path.str())};
    lattice::lattice_db other{lattice::configuration(other_path.str())};
    main.add(TestPerson{"main", 1, std::nullopt});
    quoted.add(TestPerson{"quoted", 2, std::nullopt});
    other.add(TestPerson{"other", 3, std::nullopt});

    ASSERT_NO_THROW(main.attach(other));
    ASSERT_NO_THROW(main.attach(quoted));
    const std::set<std::string> all{"main", "quoted", "other"};
    EXPECT_EQ(visible_people(main.db()), all);
    EXPECT_EQ(visible_people(main.read_db()), all);
    EXPECT_EQ(live_people(main), all);  // _source must remain a valid lazy-read qualifier.
    EXPECT_EQ(attached_names(main.db()).count(alias), 1u);
    EXPECT_EQ(attached_names(main.read_db()).count(alias), 1u);

    // Test the public object route, then direct alias removal after reattach.
    ASSERT_NO_THROW(main.detach(quoted));
    const std::set<std::string> remaining{"main", "other"};
    EXPECT_EQ(visible_people(main.db()), remaining);
    EXPECT_EQ(visible_people(main.read_db()), remaining);
    EXPECT_EQ(live_people(main), remaining);
    EXPECT_EQ(attached_names(main.db()).count(alias), 0u);
    EXPECT_EQ(attached_names(main.read_db()).count(alias), 0u);
    ASSERT_NO_THROW(main.detach(quoted));  // Existing idempotent semantics.

    ASSERT_NO_THROW(main.attach(quoted));
    EXPECT_EQ(visible_people(main.db()), all);
    ASSERT_NO_THROW(main.detach_alias(alias));
    EXPECT_EQ(visible_people(main.db()), remaining);
    EXPECT_EQ(visible_people(main.read_db()), remaining);
    ASSERT_NO_THROW(main.detach(other));
    EXPECT_EQ(visible_people(main.db()), std::set<std::string>{"main"});
    EXPECT_EQ(visible_people(main.read_db()), std::set<std::string>{"main"});
}
