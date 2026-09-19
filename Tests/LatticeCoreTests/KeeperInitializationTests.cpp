#include "TestHelpers.hpp"
#include <optional>

using lattice::lattice_db;
using lattice::configuration;

// BEGIN KEEPER-CACHE-ONCE TESTS
#ifndef __EMSCRIPTEN__
namespace {
int64_t keeper_person_count(lattice_db& db, uint64_t generation) {
    auto rows = db.query_at_generation(generation, "SELECT COUNT(*) AS c FROM TestPerson");
    return (!rows || rows->empty()) ? -1 : std::get<int64_t>((*rows)[0].at("c"));
}

std::optional<int64_t> single_integer_setting(const std::vector<lattice::database::row_t>& rows) {
    if (rows.size() != 1 || rows.front().size() != 1) return std::nullopt;
    const auto* value = std::get_if<int64_t>(&rows.front().begin()->second);
    return value ? std::optional<int64_t>(*value) : std::nullopt;
}

std::optional<int64_t> keeper_setting(lattice_db& db, uint64_t generation, const char* sql) {
    auto rows = db.query_at_generation(generation, sql);
    return rows ? single_integer_setting(*rows) : std::nullopt;
}
} // namespace

TEST(ReadGeneration, OrdinaryReaderInitializationKeepsExistingProfile) {
    TempDB tmp("gen_ordinary_profile");
    lattice_db db{configuration(tmp.str())};
    db.add(TestPerson{"profile", 1, std::nullopt});

    const auto before = lattice::database::thread_statement_count();
    lattice::database ordinary(tmp.str(), lattice::database::open_mode::read_only);
    const auto initialization_statements = lattice::database::thread_statement_count() - before;
    EXPECT_EQ(initialization_statements, 4u);
    // Inspection is deliberately after the measured constructor interval.
    EXPECT_EQ(single_integer_setting(ordinary.query("PRAGMA cache_size")), std::optional<int64_t>(50000));
    EXPECT_EQ(single_integer_setting(ordinary.query("PRAGMA foreign_keys")), std::optional<int64_t>(1));
    EXPECT_EQ(single_integer_setting(ordinary.query("PRAGMA temp_store")), std::optional<int64_t>(2));
    const auto mmap = single_integer_setting(ordinary.query("PRAGMA mmap_size"));
    ASSERT_TRUE(mmap.has_value());
    EXPECT_GE(*mmap, 0);
}

TEST(ReadGeneration, FreshKeeperConfiguresFinalCacheOnce) {
    TempDB tmp("gen_keeper_profile");
    lattice_db db{configuration(tmp.str())};
    db.add(TestPerson{"profile", 1, std::nullopt});
    lattice::database ordinary(tmp.str(), lattice::database::open_mode::read_only);
    const auto ordinary_mmap = single_integer_setting(ordinary.query("PRAGMA mmap_size"));
    ASSERT_TRUE(ordinary_mmap.has_value());
    ASSERT_EQ(db.idle_read_pool_size(), 0u);

    const auto before = lattice::database::thread_statement_count();
    const auto generation = db.acquire_read_generation();
    const auto acquisition_statements = lattice::database::thread_statement_count() - before;
    ASSERT_NE(generation, 0u);
    EXPECT_EQ(acquisition_statements, 6u)
        << "four unchanged connection settings plus BEGIN and snapshot pin; no second cache pragma";
    // Inspect only after acquisition; these queries are not part of its count.
    EXPECT_EQ(keeper_setting(db, generation, "PRAGMA cache_size"), std::optional<int64_t>(2000));
    EXPECT_EQ(keeper_setting(db, generation, "PRAGMA foreign_keys"), std::optional<int64_t>(1));
    EXPECT_EQ(keeper_setting(db, generation, "PRAGMA temp_store"), std::optional<int64_t>(2));
    EXPECT_EQ(keeper_setting(db, generation, "PRAGMA mmap_size"), ordinary_mmap);
    EXPECT_EQ(keeper_person_count(db, generation), 1);
    db.release_read_generation(generation);
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
}

TEST(ReadGeneration, PooledKeeperRetainsFinalCacheWithoutReconfiguration) {
    TempDB tmp("gen_keeper_profile_reuse");
    lattice_db db{configuration(tmp.str())};
    db.add(TestPerson{"profile", 1, std::nullopt});
    const auto first = db.acquire_read_generation();
    ASSERT_NE(first, 0u);
    const auto first_mmap = keeper_setting(db, first, "PRAGMA mmap_size");
    ASSERT_TRUE(first_mmap.has_value());
    EXPECT_EQ(keeper_setting(db, first, "PRAGMA cache_size"), std::optional<int64_t>(2000));
    db.release_read_generation(first);
    ASSERT_EQ(db.idle_read_pool_size(), 1u);

    const auto before = lattice::database::thread_statement_count();
    const auto next = db.acquire_read_generation();
    const auto acquisition_statements = lattice::database::thread_statement_count() - before;
    ASSERT_NE(next, 0u);
    EXPECT_NE(next, first);
    EXPECT_EQ(acquisition_statements, 2u) << "pooled acquisition remains BEGIN plus snapshot pin";
    EXPECT_EQ(db.idle_read_pool_size(), 0u);
    EXPECT_EQ(keeper_setting(db, next, "PRAGMA cache_size"), std::optional<int64_t>(2000));
    EXPECT_EQ(keeper_setting(db, next, "PRAGMA foreign_keys"), std::optional<int64_t>(1));
    EXPECT_EQ(keeper_setting(db, next, "PRAGMA temp_store"), std::optional<int64_t>(2));
    EXPECT_EQ(keeper_setting(db, next, "PRAGMA mmap_size"), first_mmap);
    EXPECT_EQ(keeper_person_count(db, next), 1);
    db.release_read_generation(next);
    EXPECT_EQ(db.local_read_generations_outstanding(), 0u);
    EXPECT_EQ(db.idle_read_pool_size(), 1u);
}
#endif
// END KEEPER-CACHE-ONCE TESTS
