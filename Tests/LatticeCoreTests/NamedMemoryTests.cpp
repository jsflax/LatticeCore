#include "TestHelpers.hpp"

// ============================================================================
// Named in-memory identity (1.0 item E1, core side). A named memory URI
// ("file:<name>?mode=memory&cache=shared") opens ONE same-process database
// per name: same-name handles share storage and cross-notify via the
// URI-keyed instance registry; distinct names are fully isolated; no
// cross-process notifier or read-only side connections are created.
// ============================================================================

namespace {
std::string mem_uri(const std::string& name) {
    return "file:" + name + "?mode=memory&cache=shared";
}
} // namespace

TEST(NamedMemory, PathPolarityTable) {
    using cfg = lattice::configuration;
    // memory-like forms
    EXPECT_TRUE(cfg::path_is_memory(":memory:"));
    EXPECT_TRUE(cfg::path_is_memory(""));
    EXPECT_TRUE(cfg::path_is_memory("file::memory:?cache=shared"));
    EXPECT_TRUE(cfg::path_is_memory(mem_uri("polarity")));
    EXPECT_TRUE(cfg::path_is_memory("file:polarity?mode=memory"));
    // file forms
    EXPECT_FALSE(cfg::path_is_memory("/tmp/foo.sqlite"));
    EXPECT_FALSE(cfg::path_is_memory("file:/tmp/foo.sqlite?cache=shared"));
    EXPECT_TRUE(cfg(mem_uri("polarity")).is_in_memory());
}

TEST(NamedMemory, SameNameSharesOneDatabase) {
    const auto uri = mem_uri("shared_pos");
    lattice::lattice_db a{lattice::configuration(uri)};
    lattice::lattice_db b{lattice::configuration(uri)};  // second handle, same name

    a.add(TestPerson{"from-a", 1, std::nullopt});
    auto rows = b.db().query("SELECT COUNT(*) AS c FROM TestPerson");
    ASSERT_FALSE(rows.empty());
    EXPECT_EQ(std::get<int64_t>(rows[0].at("c")), 1) << "same-name handle must see the write";
}

TEST(NamedMemory, DistinctNamesAreIsolated) {
    lattice::lattice_db a{lattice::configuration(mem_uri("iso_a"))};
    lattice::lattice_db b{lattice::configuration(mem_uri("iso_b"))};

    a.add(TestPerson{"only-in-a", 1, std::nullopt});
    auto rows = b.db().query("SELECT COUNT(*) AS c FROM TestPerson");
    ASSERT_FALSE(rows.empty());
    EXPECT_EQ(std::get<int64_t>(rows[0].at("c")), 0) << "distinct names must not share storage";
}

// The production symptom the user directive pinned: cross-instance
// observation on in-memory lattices. Named memory delivers via the
// URI-keyed instance registry — and exactly once per write.
TEST(NamedMemory, CrossInstanceObservationExactlyOnce) {
    const auto uri = mem_uri("observe_x");
    lattice::lattice_db writer{lattice::configuration(uri)};
    lattice::lattice_db observerdb{lattice::configuration(uri)};

    std::atomic<int> fired{0};
    observerdb.add_table_observer("TestPerson",
        [&fired](const std::vector<lattice::lattice_db::change_event>& events) {
            fired.fetch_add(static_cast<int>(events.size()), std::memory_order_relaxed);
        });

    writer.add(TestPerson{"observed", 1, std::nullopt});

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (fired.load(std::memory_order_acquire) == 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(fired.load(), 1) << "observer must fire exactly once for one write";

    // A second write must deliver exactly once more (no double-delivery).
    writer.add(TestPerson{"observed-2", 2, std::nullopt});
    const auto deadline2 = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (fired.load(std::memory_order_acquire) < 2 &&
           std::chrono::steady_clock::now() < deadline2) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // catch late duplicates
    EXPECT_EQ(fired.load(), 2);
}
