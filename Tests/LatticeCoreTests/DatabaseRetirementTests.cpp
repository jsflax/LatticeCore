#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/database_retirement.hpp"
#include <chrono>
#include <memory>
#include <stdexcept>

namespace {
using lattice::database;
using lattice::detail::database_retirement_access;
using retirement = lattice::detail::database_retirement_state;
using statement_owner = std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;

class retirement_probe_scope {
    const lattice::detail::database_retirement_test_hooks::probe* previous_;
public:
    explicit retirement_probe_scope(const lattice::detail::database_retirement_test_hooks::probe& probe)
        : previous_(lattice::detail::database_retirement_test_hooks::current) {
        lattice::detail::database_retirement_test_hooks::current = &probe;
    }
    ~retirement_probe_scope() { lattice::detail::database_retirement_test_hooks::current = previous_; }
    retirement_probe_scope(const retirement_probe_scope&) = delete;
    retirement_probe_scope& operator=(const retirement_probe_scope&) = delete;
};

void expect_checked_closed(const std::shared_ptr<const retirement>& state) {
    ASSERT_NE(state, nullptr);
    EXPECT_EQ(state->state(), retirement::phase::closed);
    EXPECT_EQ(state->checked_close_result(), SQLITE_OK);
    EXPECT_EQ(state->fallback_close_result(), retirement::not_attempted);
}

void expect_busy_unproved(const std::shared_ptr<const retirement>& state) {
    ASSERT_NE(state, nullptr);
    EXPECT_EQ(state->state(), retirement::phase::close_unproved);
    EXPECT_EQ(state->checked_close_result(), SQLITE_BUSY);
    EXPECT_EQ(state->fallback_close_result(), SQLITE_OK);
}
} // namespace

TEST(DatabaseRetirement, LogicalCloseDoesNotProvePhysicalClose) {
    auto owner = std::make_unique<database>(":memory:");
    owner->execute("CREATE TABLE RetirementValue(value INTEGER)");
    const auto state = database_retirement_access::retain(*owner);
    ASSERT_NE(state, nullptr);
    EXPECT_EQ(state->state(), retirement::phase::live);
    owner->close();
    EXPECT_TRUE(owner->is_closed());
    EXPECT_TRUE(owner->query("SELECT 1 AS value").empty());
    EXPECT_EQ(state->state(), retirement::phase::live);
    EXPECT_EQ(state->checked_close_result(), retirement::not_attempted);
    EXPECT_FALSE(state->raw_handle_escaped());
    owner.reset();
    expect_checked_closed(state);
    EXPECT_FALSE(state->raw_handle_escaped());
}

TEST(DatabaseRetirement, MoveConstructionPreservesClosedConnectionAndEvidence) {
    auto source = std::make_unique<database>(":memory:");
    source->close();
    const auto state = database_retirement_access::retain(*source);
    {
        database destination(std::move(*source));
        EXPECT_EQ(database_retirement_access::retain(destination), state);
        EXPECT_EQ(database_retirement_access::retain(*source), nullptr);
        EXPECT_TRUE(source->is_closed());
        EXPECT_TRUE(destination.is_closed());
        EXPECT_TRUE(destination.query("SELECT 41 AS value").empty());
        source.reset();
        EXPECT_EQ(state->state(), retirement::phase::live);
    }
    expect_checked_closed(state);
}

TEST(DatabaseRetirement, MoveAssignmentClosesDisplacedConnectionAndPreservesLogicalClose) {
    auto source = std::make_unique<database>(":memory:");
    source->close();
    const auto incoming = database_retirement_access::retain(*source);
    std::shared_ptr<const retirement> displaced;
    {
        database destination(":memory:");
        displaced = database_retirement_access::retain(destination);
        ASSERT_NE(incoming, displaced);
        destination = std::move(*source);
        expect_checked_closed(displaced);
        EXPECT_EQ(database_retirement_access::retain(destination), incoming);
        EXPECT_EQ(database_retirement_access::retain(*source), nullptr);
        EXPECT_TRUE(source->is_closed());
        EXPECT_TRUE(destination.is_closed());
        EXPECT_TRUE(destination.query("SELECT 43 AS value").empty());
        source.reset();
        EXPECT_EQ(incoming->state(), retirement::phase::live);
    }
    expect_checked_closed(incoming);
}

TEST(DatabaseRetirement, OpenConnectionCanReplaceClosedWrapperWithoutInheritingItsFlag) {
    database source(":memory:");
    database destination(":memory:");
    destination.close();
    const auto incoming = database_retirement_access::retain(source);
    const auto displaced = database_retirement_access::retain(destination);
    destination = std::move(source);
    EXPECT_FALSE(destination.is_closed());
    EXPECT_TRUE(source.is_closed());
    EXPECT_EQ(database_retirement_access::retain(destination), incoming);
    expect_checked_closed(displaced);
    const auto rows = destination.query("SELECT 47 AS value");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows.front().at("value")), 47);
    // Self move neither retires nor replaces the physical connection.
    auto& same = destination;
    destination = std::move(same);
    EXPECT_EQ(database_retirement_access::retain(destination), incoming);
    EXPECT_EQ(incoming->state(), retirement::phase::live);
}

TEST(DatabaseRetirement, RawEscapeSurvivesMovesLogicalCloseAndSuccessfulPhysicalClose) {
    std::shared_ptr<const retirement> escaped;
    {
        database source(":memory:");
        escaped = database_retirement_access::retain(source);
        ASSERT_NE(source.handle(), nullptr);
        ASSERT_TRUE(escaped->raw_handle_escaped());
        database moved(std::move(source));
        database destination(":memory:");
        destination = std::move(moved);
        destination.close();
        EXPECT_EQ(database_retirement_access::retain(destination), escaped);
        EXPECT_EQ(escaped->state(), retirement::phase::live);
        EXPECT_TRUE(escaped->raw_handle_escaped());
    }
    expect_checked_closed(escaped);
    // Physical closure does not clear the independent store-generation taint.
    EXPECT_TRUE(escaped->raw_handle_escaped());
}

TEST(DatabaseRetirement, BusyEscapedStatementRemainsUnprovedAfterWrapperAndFinalization) {
    statement_owner statement(nullptr, &sqlite3_finalize);
    auto owner = std::make_unique<database>(":memory:");
    const auto state = database_retirement_access::retain(*owner);
    sqlite3_stmt* raw = nullptr;
    const int prepared = sqlite3_prepare_v2(owner->handle(), "SELECT 53", -1, &raw, nullptr);
    statement.reset(raw);
    ASSERT_EQ(prepared, SQLITE_OK);
    ASSERT_EQ(sqlite3_step(statement.get()), SQLITE_ROW);
    owner->close();
    EXPECT_EQ(state->state(), retirement::phase::live);
    owner.reset();
    expect_busy_unproved(state);
    EXPECT_TRUE(state->raw_handle_escaped());
    // Finalization is permitted cleanup; never step a statement after retiring
    // its wrapper. No UDF destructor or late close_v2 pointer supplies proof.
    EXPECT_EQ(sqlite3_finalize(statement.release()), SQLITE_OK);
    expect_busy_unproved(state);
    EXPECT_TRUE(state->raw_handle_escaped());
}

TEST(DatabaseRetirement, BusyDisplacedConnectionDoesNotBorrowReplacementCloseEvidence) {
    statement_owner statement(nullptr, &sqlite3_finalize);
    auto destination = std::make_unique<database>(":memory:");
    const auto displaced = database_retirement_access::retain(*destination);
    sqlite3_stmt* raw = nullptr;
    const int prepared = sqlite3_prepare_v2(destination->handle(), "SELECT 59", -1, &raw, nullptr);
    statement.reset(raw);
    ASSERT_EQ(prepared, SQLITE_OK);
    ASSERT_EQ(sqlite3_step(statement.get()), SQLITE_ROW);
    database source(":memory:");
    const auto incoming = database_retirement_access::retain(source);
    *destination = std::move(source);
    expect_busy_unproved(displaced);
    EXPECT_TRUE(displaced->raw_handle_escaped());
    EXPECT_EQ(database_retirement_access::retain(*destination), incoming);
    EXPECT_FALSE(incoming->raw_handle_escaped());
    EXPECT_EQ(sqlite3_finalize(statement.release()), SQLITE_OK);
    destination.reset();
    expect_checked_closed(incoming);
    expect_busy_unproved(displaced);
    EXPECT_TRUE(displaced->raw_handle_escaped());
}

#ifndef __EMSCRIPTEN__
TEST(DatabaseRetirement, ActualBorrowedEngineReaderOutlivesOwnerPublicationAndWrapper) {
    TempDB file("retirement_borrowed_reader");
    auto owner = std::make_unique<lattice::lattice_db>(lattice::configuration(file.str()));
    owner->add(TestPerson{"retirement-reader", 61, std::nullopt});
    auto reader = owner->borrow_read_connection();
    ASSERT_NE(reader.get(), &owner->db());
    const auto state = database_retirement_access::retain(*reader);
    owner->close_read_db();
    EXPECT_EQ(state->state(), retirement::phase::live);
    owner.reset();
    EXPECT_EQ(state->state(), retirement::phase::live);
    const auto rows = reader->query("SELECT age AS value FROM TestPerson");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows.front().at("value")), 61);
    EXPECT_FALSE(state->raw_handle_escaped());
    reader.reset();
    expect_checked_closed(state);
}
#endif

TEST(DatabaseRetirement, CancelledConstructorClosesAndUnpublishesReadControl) {
    TempDB file("retirement_cancelled_constructor");
    { database seed(file.str()); seed.execute("CREATE TABLE RetirementSeed(value INTEGER)"); }
    auto control = std::make_shared<lattice::database_read_control>();
    control->deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    control->stop(1);
    std::shared_ptr<const retirement> state;
    lattice::detail::database_retirement_test_hooks::probe probe;
    probe.after_open = [&](database& opening, sqlite3*) { state = database_retirement_access::retain(opening); };
    retirement_probe_scope scope(probe);
    EXPECT_THROW((void)database(file.str(), database::open_mode::read_only, 20, control), lattice::db_error);
    EXPECT_EQ(control->target, nullptr);
    expect_checked_closed(state);
    ASSERT_NE(state, nullptr);
    EXPECT_FALSE(state->raw_handle_escaped());
}

TEST(DatabaseRetirement, ConstructorFaultPreservesPrimaryErrorAndBusyCleanupEvidence) {
    statement_owner statement(nullptr, &sqlite3_finalize);
    std::shared_ptr<const retirement> state;
    lattice::detail::database_retirement_test_hooks::probe probe;
    probe.after_open = [&](database& opening, sqlite3* handle) {
        state = database_retirement_access::retain(opening);
        sqlite3_stmt* raw = nullptr;
        const int prepared = sqlite3_prepare_v2(handle, "SELECT 67", -1, &raw, nullptr);
        statement.reset(raw);
        if (prepared != SQLITE_OK) throw std::runtime_error("retirement fixture prepare failed");
        throw std::runtime_error("retirement constructor sentinel");
    };
    retirement_probe_scope scope(probe);
    try {
        database opening(":memory:");
        FAIL() << "constructor fault must escape";
    } catch (const std::runtime_error& error) {
        EXPECT_EQ(std::string(error.what()), "retirement constructor sentinel");
    }
    ASSERT_NE(statement, nullptr);
    expect_busy_unproved(state);
    ASSERT_NE(state, nullptr);
    // A private fault retained a real statement without public handle escape;
    // BUSY itself is sufficient to keep closure unproved.
    EXPECT_FALSE(state->raw_handle_escaped());
    EXPECT_EQ(sqlite3_finalize(statement.release()), SQLITE_OK);
    expect_busy_unproved(state);
}

TEST(DatabaseRetirement, CheckedCloseDetachesWrapperRollbackCallbackBeforeCleanup) {
    int rollback_calls = 0;
    auto capture = std::make_shared<int>(71);
    std::weak_ptr<int> weak_capture = capture;
    std::shared_ptr<const retirement> state;
    {
        database owner(":memory:");
        state = database_retirement_access::retain(owner);
        owner.execute("CREATE TABLE RetirementTransaction(value INTEGER)");
        owner.set_txn_hooks({}, [capture, &rollback_calls] { ++rollback_calls; });
        capture.reset();
        owner.execute("BEGIN");
        owner.execute("INSERT INTO RetirementTransaction VALUES(71)");
        EXPECT_FALSE(weak_capture.expired());
    }
    expect_checked_closed(state);
    EXPECT_EQ(rollback_calls, 0);
    EXPECT_TRUE(weak_capture.expired());
}

TEST(DatabaseRetirement, ReadOnlyBusyCleanupRetiresReadControlAndRollbackCaptures) {
    TempDB file("retirement_readonly_busy");
    { database seed(file.str()); seed.execute("CREATE TABLE RetirementReadOnly(value INTEGER)"); }
    statement_owner statement(nullptr, &sqlite3_finalize);
    auto control = std::make_shared<lattice::database_read_control>();
    control->deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    std::weak_ptr<lattice::database_read_control> weak_control = control;
    auto capture = std::make_shared<int>(73);
    std::weak_ptr<int> weak_capture = capture;
    int rollback_calls = 0;
    auto owner = std::make_unique<database>(file.str(), database::open_mode::read_only, 20, control);
    const auto state = database_retirement_access::retain(*owner);
    owner->set_txn_hooks({}, [capture, &rollback_calls] { ++rollback_calls; });
    capture.reset();
    owner->execute("BEGIN");
    sqlite3_stmt* raw = nullptr;
    const int prepared = sqlite3_prepare_v2(owner->handle(), "SELECT 73", -1, &raw, nullptr);
    statement.reset(raw);
    ASSERT_EQ(prepared, SQLITE_OK);
    ASSERT_EQ(sqlite3_step(statement.get()), SQLITE_ROW);
    owner.reset();
    expect_busy_unproved(state);
    EXPECT_EQ(control->target, nullptr);
    EXPECT_TRUE(weak_capture.expired());
    control.reset();
    EXPECT_TRUE(weak_control.expired());
    EXPECT_EQ(sqlite3_finalize(statement.release()), SQLITE_OK);
    EXPECT_EQ(rollback_calls, 0);
    expect_busy_unproved(state);
    EXPECT_TRUE(state->raw_handle_escaped());
}
