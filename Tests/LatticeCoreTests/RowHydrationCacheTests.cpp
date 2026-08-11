#include "TestHelpers.hpp"

// ============================================================================
// Row-hydration cache tests — C0b (crash forensics: Engram SIGBUS 2026-08-10)
//
// managed<T>::detach() historically ran one `SELECT <col> … WHERE id = ?` per
// PROPERTY READ on a live object. The first live property read on a
// (table, row) now hydrates the FULL row in one statement and later reads in
// the same data generation are served from lattice_db's row cache with zero
// statements — while staying exactly as fresh as the per-column read was:
// any settled commit on any same-path connection moves the generation and
// forces a re-hydration.
//
// All asserts use database::thread_statement_count(): pacer/maintenance
// threads issue SQL on their own schedule, so global-counter deltas flake.
// ============================================================================

TEST(RowHydrationCache, FullRowHydrationServesAllPropertyReads) {
    TempDB tmp{"rowhydration"};
    lattice::lattice_db db(lattice::configuration(tmp.str()));

    // TestAllTypes has 7 columns — well past the >=4 the contract cares about.
    auto obj = db.add(TestAllTypes{7, 42, 2.5, true, "hello",
                                   9, std::string("opt")});

    // First property read hydrates the whole row: ONE statement, not one per
    // property (the pre-C0b pathology — N_fields prepares per observed row).
    const auto base = lattice::database::thread_statement_count();
    EXPECT_EQ(int(obj.int_val), 7);
    EXPECT_EQ(int64_t(obj.int64_val), 42);
    EXPECT_EQ(double(obj.double_val), 2.5);
    EXPECT_TRUE(bool(obj.bool_val));
    EXPECT_EQ(std::string(obj.string_val), "hello");
    std::optional<int> oi = obj.optional_int;
    ASSERT_TRUE(oi.has_value());
    EXPECT_EQ(*oi, 9);
    EXPECT_EQ(lattice::database::thread_statement_count() - base, 1u)
        << "expected ONE full-row hydration statement, not one per property";

    // Re-reads inside the same data generation are statement-free.
    const auto again = lattice::database::thread_statement_count();
    EXPECT_EQ(std::string(obj.string_val), "hello");
    EXPECT_EQ(int(obj.int_val), 7);
    EXPECT_TRUE(obj.optional_string.has_value());
    EXPECT_EQ(lattice::database::thread_statement_count() - again, 0u)
        << "cached reads must issue zero statements";
}

TEST(RowHydrationCache, CrossConnectionUpdateAdvancesGenerationAndRefreshes) {
    TempDB tmp{"rowhydration_fresh"};
    lattice::lattice_db db_a(lattice::configuration(tmp.str()));
    auto obj = db_a.add(TestAllTypes{1, 1, 1.0, false, "before",
                                     std::nullopt, std::nullopt});

    // Prime db_a's cache for this row and prove it is serving.
    EXPECT_EQ(std::string(obj.string_val), "before");
    const auto primed = lattice::database::thread_statement_count();
    EXPECT_EQ(std::string(obj.string_val), "before");
    EXPECT_EQ(lattice::database::thread_statement_count() - primed, 0u)
        << "cache was not primed";

    // ANOTHER connection updates the row. The settled-commit invalidation
    // fan-out (§2.3) bumps db_a's data generation inline, on the writer's
    // thread, before the write returns — same mechanism existing
    // multi-connection tests rely on for cross-handle read-your-writes.
    {
        lattice::lattice_db db_b(lattice::configuration(tmp.str()));
        auto rows = db_b.objects<TestAllTypes>();
        ASSERT_EQ(rows.size(), 1u);
        rows[0].string_val = std::string("after");
        rows[0].int_val = 99;
    }

    // The generation moved: the next read re-hydrates ONCE and serves the
    // NEW values; the second property read is cached again.
    const auto fresh = lattice::database::thread_statement_count();
    EXPECT_EQ(std::string(obj.string_val), "after")
        << "stale value served after a cross-connection update";
    EXPECT_EQ(int(obj.int_val), 99)
        << "stale value served after a cross-connection update";
    EXPECT_EQ(lattice::database::thread_statement_count() - fresh, 1u)
        << "generation move should cost exactly one re-hydration";
}

TEST(RowHydrationCache, ReadYourWritesInsideOpenTransaction) {
    TempDB tmp{"rowhydration_txn"};
    lattice::lattice_db db(lattice::configuration(tmp.str()));
    auto obj = db.add(TestAllTypes{1, 1, 1.0, false, "x",
                                   std::nullopt, std::nullopt});
    EXPECT_EQ(std::string(obj.string_val), "x");  // prime the cache

    // Inside an open transaction no commit signal has fired yet — the
    // connection's total_changes token must invalidate the cache so the
    // writer sees its own uncommitted values (the per-column read always
    // did: it queried the write connection).
    db.write([&] {
        obj.string_val = std::string("inside");
        EXPECT_EQ(std::string(obj.string_val), "inside")
            << "same-connection in-transaction write served stale cache";
    });
    EXPECT_EQ(std::string(obj.string_val), "inside");
}

TEST(RowHydrationCache, DeletedRowIsCachedAsMissing) {
    TempDB tmp{"rowhydration_del"};
    lattice::lattice_db db(lattice::configuration(tmp.str()));
    auto obj = db.add(TestAllTypes{5, 5, 5.0, true, "gone",
                                   std::nullopt, std::nullopt});
    EXPECT_EQ(std::string(obj.string_val), "gone");  // prime

    db.remove(obj);

    // Parity with the per-column read: a missing row falls back to the
    // construction-time unmanaged value. The miss itself is hydrated once
    // and then cached — repeated reads of a deleted row cost zero statements
    // (the pre-C0b path paid one statement per read here too).
    const auto base = lattice::database::thread_statement_count();
    EXPECT_EQ(std::string(obj.string_val), "gone");
    EXPECT_EQ(int(obj.int_val), 5);
    EXPECT_EQ(lattice::database::thread_statement_count() - base, 1u)
        << "expected one hydration to discover the missing row";
    const auto again = lattice::database::thread_statement_count();
    EXPECT_EQ(std::string(obj.string_val), "gone");
    EXPECT_EQ(lattice::database::thread_statement_count() - again, 0u);
}
