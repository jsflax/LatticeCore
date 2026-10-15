#include "TestHelpers.hpp"
#include "../../Sources/LatticeSwiftCppBridge/include/lattice.hpp"
#include "../../Sources/LatticeCore/src/ordinary_export.hpp"
#include <array>
#include <stdexcept>

namespace {
template<class F> std::exception_ptr diagnostic_exception(F&& body) {
    try { body(); }
    catch (...) { return std::current_exception(); }
    throw std::logic_error("diagnostic fixture did not throw its real exception");
}
std::string diagnostic_text(const std::exception& error) {
    std::string text;
    lattice::assign_ordinary_bridge_exception(text, error);
    return text;
}
void contains(const std::string& text, const std::string& value) {
    EXPECT_NE(text.find(value), std::string::npos) << text;
}
}

TEST(OrdinaryErrorDiagnosticBridge, UnrelatedShortWhatAndSealedSuccessStayUnchanged) {
    const lattice::db_error error("unchanged unrelated native error");
    EXPECT_EQ(diagnostic_text(error), error.what());
    lattice::last_bridge_error() = "prior diagnostic";
    EXPECT_FALSE(lattice::record_ordinary_bridge_exception(error));
    EXPECT_EQ(lattice::last_bridge_error(), "prior diagnostic");
    EXPECT_FALSE(lattice::sealed([&]() -> bool { throw error; }));
    EXPECT_EQ(lattice::last_bridge_error(), error.what());
    EXPECT_TRUE(lattice::sealed([] { return true; }));
    EXPECT_TRUE(lattice::last_bridge_error().empty());
}

TEST(OrdinaryErrorDiagnosticBridge, EmptyUnrelatedWhatStaysEmptyAndNestedCauseStaysExplicit) {
    using namespace lattice::detail;
    const std::runtime_error empty("");
    EXPECT_TRUE(diagnostic_text(empty).empty());
    lattice::last_bridge_error() = "prior diagnostic";
    EXPECT_FALSE(lattice::sealed([&]() -> bool { throw empty; }));
    EXPECT_TRUE(lattice::last_bridge_error().empty());
    recovery_install_result value; // Real error-value only; no transaction issued.
    value.primary_error = diagnostic_exception([&] { throw empty; });
    const ordinary_order_write_error owned(value);
    const auto text = diagnostic_text(owned);
    contains(text, "phase=refused");
    contains(text, "primary:[empty-exception-message]");
    EXPECT_EQ(owned.settlement.primary_error, value.primary_error);
    EXPECT_EQ(owned.settlement.state, recovery_install_state::refused);
    EXPECT_FALSE(owned.settlement.unexpected_commit_observed);
}

#ifndef __EMSCRIPTEN__
TEST(OrdinaryErrorDiagnosticBridge, NestedCheckedErrorsKeepActualOwnedRollbackAndRetirement) {
    using namespace lattice::detail;
    lattice::configuration config(":memory:"); config.audit_retention_seconds = 0;
    auto owner = std::make_shared<lattice::lattice_db>(config);
    const auto retirement = database_retirement_access::retain(owner->db());
    ASSERT_TRUE(retirement);
    const auto settled = recovery_writer_access::install(owner, [](lattice::database&) {
        throw lattice::db_error("actual first owned-body failure");
    });
    ASSERT_EQ(settled.state, recovery_install_state::rolled_back);
    ASSERT_TRUE(settled.primary_error);
    ASSERT_FALSE(settled.cleanup_error);
    const auto primary = settled.primary_error;
    const auto closed = owner->close_checked();
    ASSERT_FALSE(closed.error); ASSERT_FALSE(closed.cleanup_error);
    owner.reset();
    ASSERT_EQ(retirement->state(), database_retirement_state::phase::closed);
    ASSERT_EQ(retirement->checked_close_result(), SQLITE_OK);
    const auto owned = diagnostic_exception([&] { throw ordinary_order_write_error(settled); });
    const auto inner = diagnostic_exception([&] { throw ordinary_checked_open_error(owned, {}, {}, retirement); });
    const auto outer = diagnostic_exception([&] { throw ordinary_checked_open_error(inner, {}, {}, retirement); });
    EXPECT_FALSE(lattice::sealed([&]() -> bool { std::rethrow_exception(outer); }));
    const auto text = lattice::last_bridge_error();
    contains(text, "ordinary-checked-open-error{retirement=retained");
    contains(text, "retirement_phase_observed=closed,checked_close=0");
    contains(text, "ordinary-owned-error{phase=rolled_back,deferred=none,unexpected_commit=false");
    contains(text, "primary.primary.primary:actual first owned-body failure");
    EXPECT_LT(text.find("phase=rolled_back"), text.find("actual first owned-body failure"));
    EXPECT_EQ(settled.primary_error, primary);
    EXPECT_FALSE(settled.cleanup_error); EXPECT_FALSE(settled.postcommit_error);
    EXPECT_FALSE(settled.notification_error); EXPECT_FALSE(settled.unexpected_commit_observed);
    EXPECT_EQ(retirement->state(), database_retirement_state::phase::closed);
    EXPECT_LE(text.size(), 2048u);
}
#endif

TEST(OrdinaryErrorDiagnosticBridge, ActualRetainedErrorValuesKeepEveryStageAndNonStdUnknown) {
    using namespace lattice::detail;
    // Error-value formatting only: default refused state issues no transaction.
    recovery_install_result value;
    value.primary_error = diagnostic_exception([] { throw lattice::db_error("first-primary-marker"); });
    value.cleanup_error = diagnostic_exception([] { throw 17; });
    value.postcommit_error = diagnostic_exception([] { throw std::logic_error("postcommit-marker"); });
    value.notification_error = diagnostic_exception([] { throw std::runtime_error("notification-marker"); });
    const ordinary_order_write_error error(value);
    const auto text = diagnostic_text(error);
    contains(text, "phase=refused,deferred=none,unexpected_commit=false");
    contains(text, "primary=present,cleanup=present,postcommit=present,notification=present");
    contains(text, "primary:first-primary-marker");
    contains(text, "cleanup:[non-std-exception;message-unknown]");
    contains(text, "postcommit:postcommit-marker");
    contains(text, "notification:notification-marker");
    EXPECT_EQ(error.settlement.primary_error, value.primary_error);
    EXPECT_EQ(error.settlement.cleanup_error, value.cleanup_error);
    EXPECT_EQ(error.settlement.postcommit_error, value.postcommit_error);
    EXPECT_EQ(error.settlement.notification_error, value.notification_error);
    EXPECT_EQ(error.settlement.state, recovery_install_state::refused);
}

TEST(OrdinaryErrorDiagnosticBridge, OversizedCauseKeepsEarlierFactsAndExplicitAggregateTruncation) {
    using namespace lattice::detail;
    recovery_install_result value;
    value.primary_error = diagnostic_exception([] { throw std::runtime_error(std::string(12000, 'x')); });
    value.cleanup_error = diagnostic_exception([] { throw lattice::db_error("retained cleanup error"); });
    const ordinary_order_write_error error(value);
    const auto text = diagnostic_text(error);
    EXPECT_EQ(text.size(), 2048u);
    EXPECT_TRUE(text.ends_with("[truncated]"));
    contains(text, "primary=present,cleanup=present");
    EXPECT_LT(text.find("cleanup=present"), text.find(std::string(16, 'x')));
    EXPECT_EQ(error.settlement.primary_error, value.primary_error);
    EXPECT_EQ(error.settlement.cleanup_error, value.cleanup_error);
    EXPECT_FALSE(error.settlement.unexpected_commit_observed);
}

TEST(OrdinaryErrorDiagnosticBridge, ExactFitCauseMarksOmittedRetainedSiblingAsTruncated) {
    using namespace lattice::detail;
    recovery_install_result value; // Default refused error DATA, no transaction issued.
    value.primary_error = diagnostic_exception([] { throw std::runtime_error("exact-fit-seed"); });
    value.cleanup_error = diagnostic_exception([] { throw lattice::db_error("later retained cleanup"); });
    const auto sample = diagnostic_text(ordinary_order_write_error(value));
    const auto at = sample.find("exact-fit-seed");
    ASSERT_NE(at, std::string::npos); ASSERT_LT(at, 2048u);
    value.primary_error = diagnostic_exception([&] { throw std::runtime_error(std::string(2048u - at, 'x')); });
    const ordinary_order_write_error error(value);
    const auto text = diagnostic_text(error);
    EXPECT_EQ(text.size(), 2048u);
    EXPECT_TRUE(text.ends_with("[truncated]"));
    contains(text, "primary=present,cleanup=present");
    EXPECT_EQ(error.settlement.primary_error, value.primary_error);
    EXPECT_EQ(error.settlement.cleanup_error, value.cleanup_error);
    EXPECT_EQ(text.find("later retained cleanup"), std::string::npos);
}

TEST(OrdinaryErrorDiagnosticBridge, DeepRetainedExceptionChainStopsWithExplicitUnknown) {
    using namespace lattice::detail;
    auto first = diagnostic_exception([] { throw lattice::db_error("deep actual error"); });
    for (unsigned at = 0; at != 8; ++at)
        first = diagnostic_exception([&] { throw ordinary_checked_open_error(first, {}, {}, {}); });
    std::string text;
    try { std::rethrow_exception(first); }
    catch (const std::exception& error) { lattice::assign_ordinary_bridge_exception(text, error); }
    contains(text, "[depth-limit;further-errors-unknown]");
    EXPECT_LE(text.size(), 2048u);
    // Re-reading the same real retained original produces the same finite text.
    std::string observed_again;
    try { std::rethrow_exception(first); }
    catch (const std::exception& error) { lattice::assign_ordinary_bridge_exception(observed_again, error); }
    EXPECT_EQ(text, observed_again);
}

TEST(OrdinaryErrorDiagnosticBridge, SharedActualErrorTreeStopsAtFiniteNodeBudget) {
    using namespace lattice::detail;
    const auto leaf = diagnostic_exception([] { throw lattice::db_error("shared first leaf"); });
    recovery_install_result child;
    child.primary_error = child.cleanup_error = child.postcommit_error = child.notification_error = leaf;
    const auto retained = diagnostic_exception([&] { throw ordinary_order_write_error(child); });
    recovery_install_result root;
    root.primary_error = root.cleanup_error = root.postcommit_error = root.notification_error = retained;
    const ordinary_order_write_error error(root);
    const auto text = diagnostic_text(error);
    contains(text, "[node-limit;further-errors-unknown]");
    EXPECT_EQ(text.find("[cycle;"), std::string::npos); // Shared siblings are not an ancestor cycle.
    EXPECT_LE(text.size(), 2048u);
    EXPECT_EQ(error.settlement.primary_error, retained);
    EXPECT_EQ(error.settlement.notification_error, retained);
}

TEST(OrdinaryErrorDiagnosticBridge, ActualErrorValueCycleIsUnknownAndNeverRewrittenByFormatter) {
    using namespace lattice::detail;
    // Adversarial error-value graph only; no transaction/ownership is issued.
    try { throw ordinary_order_write_error(recovery_install_result{}); }
    catch (ordinary_order_write_error& error) {
        const auto original = std::current_exception();
        error.settlement.primary_error = original;
        const auto text = diagnostic_text(error);
        const bool preserved = error.settlement.primary_error == original;
        error.settlement.primary_error = {}; // Break this fixture's intentional self-reference before any assertion.
        EXPECT_TRUE(preserved);
        // exception_ptr runtimes may copy on rethrow; either bound must remain explicit.
        EXPECT_TRUE(text.find("[cycle;further-errors-unknown]") != std::string::npos ||
                    text.find("[depth-limit;further-errors-unknown]") != std::string::npos) << text;
        EXPECT_LE(text.size(), 2048u);
    }
}

TEST(OrdinaryErrorDiagnosticBridge, SealedFailuresKeepInvalidAndCapacityIndependentFromText) {
    using namespace lattice::detail;
    const auto primary = diagnostic_exception([] { throw lattice::db_error("checked failure marker"); });
    const auto failed = lattice::sealed([&]() -> lattice::ordinary_history_export_ref {
        throw ordinary_checked_open_error(primary, {}, {}, {});
    });
    EXPECT_FALSE(failed.valid()); EXPECT_FALSE(failed.capacity_exhausted());
    contains(lattice::last_bridge_error(), "primary:checked failure marker");
    const auto unknown = lattice::sealed([]() -> lattice::ordinary_sync_export_ref { throw 23; });
    EXPECT_FALSE(unknown.valid()); EXPECT_FALSE(unknown.capacity_exhausted());
    EXPECT_EQ(lattice::last_bridge_error(), "Unknown C++ bridge exception");
    EXPECT_FALSE(lattice::ordinary_mounted_history_source_ref{}.valid());
    EXPECT_FALSE(lattice::ordinary_mounted_history_source_ref{}.capacity_exhausted());
}

#ifndef __EMSCRIPTEN__
TEST(OrdinaryErrorDiagnosticBridge, GenuineFourCutCapacityRefusalAndReleaseKeepOriginalFlags) {
    TempDB path("ordinary-error-diagnostic-capacity");
    lattice::swift_configuration config(path.str(), std::make_shared<lattice::immediate_scheduler>());
    config.audit_retention_seconds = 0; config.busy_timeout_ms = 100;
    lattice::swift_schema_entry entry; entry.table_name = "DiagnosticErrorRow";
    lattice::property_descriptor property;
    property.name = "value"; property.type = lattice::column_type::text;
    property.kind = lattice::property_kind::primitive;
    entry.properties["value"] = property;
    lattice::SchemaVector schemas{entry};
#if LATTICE_HAS_FRT
    auto owner = std::unique_ptr<lattice::swift_lattice_ref>(lattice::swift_lattice_ref::create(config, schemas));
#else
    auto owner = std::make_unique<lattice::swift_lattice_ref>(lattice::swift_lattice_ref::create(config, schemas));
#endif
    ASSERT_TRUE(owner); ASSERT_TRUE(owner->valid());
    {
        std::array<lattice::ordinary_sync_export_ref, 4> held;
        for (auto& cut : held) {
            cut = owner->capture_ordinary_export({"DiagnosticErrorRow"});
            ASSERT_TRUE(cut.valid()) << cut.failure();
            EXPECT_FALSE(cut.capacity_exhausted());
        }
        const auto refused = owner->capture_ordinary_history_export({"DiagnosticErrorRow"});
        EXPECT_FALSE(refused.valid()); EXPECT_TRUE(refused.capacity_exhausted());
        const lattice::ordinary_export_capacity_exhausted capacity;
        EXPECT_EQ(refused.failure(), capacity.what());
        held[0] = {}; // Actual last native cut/reference release returns one slot.
        const auto successor = owner->capture_ordinary_history_export({"DiagnosticErrorRow"});
        EXPECT_TRUE(successor.valid()) << successor.failure();
        EXPECT_FALSE(successor.capacity_exhausted());
        EXPECT_TRUE(successor.failure().empty());
    }
    const auto closed = owner->close_checked();
    EXPECT_FALSE(closed.failed()); EXPECT_FALSE(closed.cleanup_failed());
    EXPECT_TRUE(closed.cleanup_complete());
}
#endif
