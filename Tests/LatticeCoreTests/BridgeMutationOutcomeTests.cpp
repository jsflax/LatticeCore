#include "TestHelpers.hpp"
#include <error.hpp>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace {
struct EmptyBridgeWhat final : std::exception {
    const char* what() const noexcept override { return ""; }
};
}

TEST(BridgeMutationOutcome, EmptyWhatIsAuthoritativeFailureWithValidEmptyDiagnostic) {
    lattice::sealed([] { throw EmptyBridgeWhat{}; });
    const auto actual = lattice::borrow_bridge_outcome();
    EXPECT_TRUE(actual.failed);
    EXPECT_TRUE(actual.diagnostic_available);
    ASSERT_NE(actual.message_data, nullptr);
    EXPECT_EQ(actual.message_size, 0u);
    EXPECT_TRUE(lattice::last_bridge_error().empty());
}

TEST(BridgeMutationOutcome, DiagnosticCopyRefusalCannotEraseFailureOrExposeOldText) {
    lattice::clear_bridge_outcome();
    lattice::record_bridge_error("previous diagnostic");
    bool entered = false;
    // Fault at the actual diagnostic writer boundary, not simulated SQL or a
    // resource-pressure claim. The thrown C++ copy failure must be contained.
    lattice::detail::record_bridge_failure([&](std::string&) -> bool {
        entered = true;
        EXPECT_TRUE(lattice::last_bridge_call_failed());
        EXPECT_FALSE(lattice::borrow_bridge_outcome().diagnostic_available);
        throw std::bad_alloc{};
    });
    EXPECT_TRUE(entered);
    const auto actual = lattice::borrow_bridge_outcome();
    EXPECT_TRUE(actual.failed);
    EXPECT_FALSE(actual.diagnostic_available);
    EXPECT_EQ(actual.message_data, nullptr);
    EXPECT_EQ(actual.message_size, 0u);
    EXPECT_EQ(lattice::last_bridge_error(), "previous diagnostic")
        << "the compatibility string may survive; it is not failure authority";
}

TEST(BridgeMutationOutcome, NullDiagnosticIsFailedUnavailableAndBorrowIsNoexcept) {
    static_assert(noexcept(lattice::borrow_bridge_outcome()));
    static_assert(noexcept(lattice::last_bridge_call_failed()));
    static_assert(noexcept(lattice::clear_bridge_outcome()));
    lattice::clear_bridge_outcome();
    lattice::record_bridge_error(nullptr);
    const auto actual = lattice::borrow_bridge_outcome();
    EXPECT_TRUE(actual.failed);
    EXPECT_FALSE(actual.diagnostic_available);
    EXPECT_EQ(actual.message_data, nullptr);
}

TEST(BridgeMutationOutcome, BorrowDoesNotClearOrCopyAndNextSuccessfulCallClears) {
    lattice::record_bridge_error("retained diagnostic");
    const auto first = lattice::borrow_bridge_outcome();
    const auto second = lattice::borrow_bridge_outcome();
    EXPECT_TRUE(first.failed && second.failed);
    EXPECT_EQ(first.message_data, second.message_data);
    EXPECT_EQ(first.message_size, second.message_size);
    const std::string captured(first.message_data, first.message_size);
    EXPECT_EQ(lattice::sealed([] { return 17; }), 17);
    EXPECT_FALSE(lattice::borrow_bridge_outcome().failed);
    EXPECT_TRUE(lattice::last_bridge_error().empty());
    EXPECT_EQ(captured, "retained diagnostic");
}

TEST(BridgeMutationOutcome, ForeignExceptionIsContainedWithFailureStatus) {
    const auto result = lattice::sealed([]() -> int { throw 73; });
    EXPECT_EQ(result, 0);
    const auto actual = lattice::borrow_bridge_outcome();
    EXPECT_TRUE(actual.failed && actual.diagnostic_available);
    ASSERT_NE(actual.message_data, nullptr);
    EXPECT_EQ(std::string(actual.message_data, actual.message_size), "Unknown C++ bridge exception");
}

TEST(BridgeMutationOutcome, TypedCheckedResultRejectsEmptyWhatAndNestedSealedFailure) {
    lattice::cxx_error error(std::runtime_error("old"));
    EXPECT_FALSE(lattice::sealed_with_error(error, [] { throw EmptyBridgeWhat{}; }));
    EXPECT_TRUE(lattice::borrow_bridge_outcome().failed);
    EXPECT_TRUE(error.msg.empty());
    EXPECT_FALSE(lattice::sealed_with_error(error, [] {
        lattice::sealed([] { throw EmptyBridgeWhat{}; });
    }));
    EXPECT_TRUE(lattice::borrow_bridge_outcome().failed);
    EXPECT_TRUE(lattice::sealed_with_error(error, [] {}));
    EXPECT_FALSE(lattice::borrow_bridge_outcome().failed);
    EXPECT_TRUE(error.msg.empty());
}

#ifndef __linux__
#include <lattice.hpp>
// This test needs the real checked-vector helpers. Isolate only the three
// unrelated header-defined exports already owned by the Bridge archive.
#define try_catch lattice_mutation_fixture_unused_try_catch
#define last_uncaught_error lattice_mutation_fixture_unused_last_uncaught_error
#define test_throws lattice_mutation_fixture_unused_test_throws
#include <LatticeBridge.hpp>
#undef try_catch
#undef last_uncaught_error
#undef test_throws
#include <dynamic_object.hpp>
#include <list.hpp>
#include <memory>

namespace {
lattice::SchemaVector outcome_schema() {
    lattice::swift_schema_entry row;
    row.table_name = "OutcomeItem";
    lattice::property_descriptor value;
    value.name = "value";
    value.type = lattice::column_type::integer;
    row.properties["value"] = value;
    return {row};
}
std::unique_ptr<lattice::swift_lattice_ref> outcome_owner(const lattice::swift_configuration& config, const lattice::SchemaVector& schemas) {
#if LATTICE_HAS_FRT
    return std::unique_ptr<lattice::swift_lattice_ref>(lattice::swift_lattice_ref::create(config, schemas));
#else
    return std::make_unique<lattice::swift_lattice_ref>(lattice::swift_lattice_ref::create(config, schemas));
#endif
}
std::unique_ptr<lattice::dynamic_object_ref> outcome_object(const lattice::SchemaVector& schema, int64_t value, size_t index = 0) {
    lattice::swift_dynamic_object raw;
    raw.table_name = schema[index].table_name;
    raw.properties = schema[index].properties;
    raw.values["value"] = value;
#if LATTICE_HAS_FRT
    return std::unique_ptr<lattice::dynamic_object_ref>(lattice::dynamic_object_ref::wrap(lattice::dynamic_object(raw)));
#else
    return std::make_unique<lattice::dynamic_object_ref>(lattice::dynamic_object_ref::wrap(lattice::dynamic_object(raw)));
#endif
}
lattice::SchemaVector outcome_relationship_schema() {
    auto schema = outcome_schema();
    auto parent = schema[0]; parent.table_name = "OutcomeParent";
    lattice::property_descriptor links;
    links.name = "items"; links.kind = lattice::property_kind::list;
    links.target_table = "OutcomeItem"; links.type = lattice::column_type::text;
    parent.properties["items"] = links;
    links.name = "child"; links.kind = lattice::property_kind::link;
    parent.properties["child"] = links;
    schema.push_back(parent);
    return schema;
}
}

TEST(BridgeMutationOutcome, CheckedAddAndPreservingAddContainActualSQLiteFailure) {
    TempDB path{"checked_add_outcome"};
    const auto schema = outcome_schema();
    auto owner = outcome_owner(lattice::swift_configuration(path.str()), schema);
    owner->get()->db().execute("CREATE TRIGGER outcome_insert BEFORE INSERT ON OutcomeItem "
                              "BEGIN SELECT RAISE(ABORT, 'outcome-real-insert'); END");
    auto first = outcome_object(schema, 1);
    auto second = outcome_object(schema, 2);
    lattice::cxx_error error(std::runtime_error("old"));
    EXPECT_FALSE(owner->add_checked(*first, error));
    EXPECT_FALSE(owner->add_preserving_global_id_checked(*second, "preserved-outcome", error));
    const auto failure = lattice::borrow_bridge_outcome();
    ASSERT_TRUE(failure.failed && failure.diagnostic_available);
    EXPECT_NE(std::string(failure.message_data, failure.message_size).find("outcome-real-insert"), std::string::npos);
    EXPECT_EQ(owner->count("OutcomeItem", std::nullopt, std::nullopt, std::nullopt), 0u);
    EXPECT_FALSE(lattice::borrow_bridge_outcome().failed);
}

TEST(BridgeMutationOutcome, CheckedBulkInputAppendRejectsInvalidRefWithoutAcceptedPrefix) {
    lattice::DynamicObjectRefPtrVector offered;
#if LATTICE_HAS_FRT
    EXPECT_FALSE(lattice::push_dynamic_object_ref_checked(offered, nullptr));
#else
    const auto invalid = lattice::dynamic_object_ref::wrap(std::shared_ptr<lattice::dynamic_object>{});
    EXPECT_FALSE(lattice::push_dynamic_object_ref_checked(offered, invalid));
#endif
    EXPECT_TRUE(lattice::borrow_bridge_outcome().failed);
    EXPECT_TRUE(offered.empty());
    auto valid = outcome_object(outcome_schema(), 3);
#if LATTICE_HAS_FRT
    EXPECT_TRUE(lattice::push_dynamic_object_ref_checked(offered, valid.get()));
#else
    EXPECT_TRUE(lattice::push_dynamic_object_ref_checked(offered, *valid));
#endif
    EXPECT_FALSE(lattice::borrow_bridge_outcome().failed);
    EXPECT_EQ(offered.size(), 1u);
}

TEST(BridgeMutationOutcome, CheckedBulkRejectsRealSecondInsertAndClearsAtNextQuery) {
    TempDB path{"checked_bulk_outcome"};
    const auto schema = outcome_schema();
    auto owner = outcome_owner(lattice::swift_configuration(path.str()), schema);
    owner->get()->db().execute("CREATE TRIGGER outcome_bulk BEFORE INSERT ON OutcomeItem "
        "WHEN NEW.value = 2 AND EXISTS(SELECT 1 FROM OutcomeItem WHERE value = 1) "
        "BEGIN SELECT RAISE(ABORT, 'outcome-real-bulk-prefix'); END");
    auto first = outcome_object(schema, 1);
    auto second = outcome_object(schema, 2);
    lattice::cxx_error error(std::runtime_error("old"));
#if LATTICE_HAS_FRT
    std::vector<lattice::dynamic_object_ref*> offered{first.get(), second.get()};
#else
    std::vector<lattice::dynamic_object_ref> offered{*first, *second};
#endif
    EXPECT_FALSE(owner->add_bulk_checked(offered, error));
    const auto failure = lattice::borrow_bridge_outcome();
    ASSERT_TRUE(failure.failed && failure.diagnostic_available);
    const std::string captured(failure.message_data, failure.message_size);
    EXPECT_NE(captured.find("outcome-real-bulk-prefix"), std::string::npos);
    EXPECT_EQ(owner->count("OutcomeItem", std::nullopt, std::nullopt, std::nullopt), 0u);
    EXPECT_FALSE(lattice::borrow_bridge_outcome().failed);
}

TEST(BridgeMutationOutcome, ClosedOwnerCannotReturnAcceptedCheckedAddOrBulk) {
    TempDB path{"checked_closed_outcome"};
    const auto schema = outcome_schema();
    auto owner = outcome_owner(lattice::swift_configuration(path.str()), schema);
    auto first = outcome_object(schema, 1);
    owner->close();
    lattice::cxx_error error(std::runtime_error("old"));
    EXPECT_FALSE(owner->add_checked(*first, error));
#if LATTICE_HAS_FRT
    std::vector<lattice::dynamic_object_ref*> offered{first.get()};
#else
    std::vector<lattice::dynamic_object_ref> offered{*first};
#endif
    EXPECT_TRUE(lattice::borrow_bridge_outcome().failed);
    EXPECT_FALSE(owner->add_bulk_checked(offered, error));
    EXPECT_TRUE(lattice::borrow_bridge_outcome().failed);
}

TEST(BridgeMutationOutcome, ListFailedPredecessorAndInvalidProxyAssignmentAreContained) {
    TempDB path{"checked_list_predecessor"};
    const auto schema = outcome_relationship_schema();
    auto owner = outcome_owner(lattice::swift_configuration(path.str()), schema);
    auto parent = outcome_object(schema, 0, 1);
    lattice::cxx_error error(std::runtime_error("old"));
    ASSERT_TRUE(owner->add_checked(*parent, error));
#if LATTICE_HAS_FRT
    std::unique_ptr<lattice::link_list_ref> list(parent->get_link_list("items"));
#else
    auto list = std::make_unique<lattice::link_list_ref>(parent->get_link_list("items"));
#endif
    ASSERT_NE(list, nullptr);
    auto child = outcome_object(schema, 1);
    lattice::link_list::element_proxy invalid;
    invalid.assign(*child);
    EXPECT_TRUE(lattice::borrow_bridge_outcome().failed);
    list->assign_at(99, *child); // real managed-list lookup, not vector UB
    EXPECT_TRUE(lattice::borrow_bridge_outcome().failed);
    EXPECT_EQ(list->size(), 0u);
    EXPECT_FALSE(lattice::borrow_bridge_outcome().failed);
}

TEST(BridgeMutationOutcome, ManagedObjectLateLinkFailureIsSealedWithoutAtomicityClaim) {
    TempDB path{"checked_object_mutation"};
    const auto schema = outcome_relationship_schema();
    auto owner = outcome_owner(lattice::swift_configuration(path.str()), schema);
    auto parent = outcome_object(schema, 0, 1);
    auto child = outcome_object(schema, 51);
    lattice::cxx_error error(std::runtime_error("old"));
    ASSERT_TRUE(owner->add_checked(*parent, error));
    owner->get()->db().execute("CREATE TRIGGER outcome_link BEFORE INSERT ON _OutcomeParent_OutcomeItem_child "
        "WHEN EXISTS(SELECT 1 FROM OutcomeItem WHERE value = 51) "
        "BEGIN SELECT RAISE(ABORT, 'outcome-real-object-link'); END");
    EXPECT_NO_THROW(parent->set_object("child", *child));
    const auto failure = lattice::borrow_bridge_outcome();
    ASSERT_TRUE(failure.failed && failure.diagnostic_available);
    EXPECT_NE(std::string(failure.message_data, failure.message_size).find("outcome-real-object-link"), std::string::npos);
    // Whole graph SQL/binding rollback is a separate, still-unimplemented
    // prerequisite. This control checks only actual failure containment.
}

TEST(BridgeMutationOutcome, ManagedUnionInsertFailureIsSealed) {
    TempDB path{"checked_union_mutation"};
    auto schema = outcome_schema();
    lattice::property_descriptor choice;
    choice.name = "choice"; choice.type = lattice::column_type::text;
    choice.kind = lattice::property_kind::union_type; choice.is_union = true; choice.nullable = true;
    choice.union_desc.union_table_name = "_OutcomeItem_choice";
    lattice::union_case one; one.case_name = "text";
    lattice::union_case_value value; value.type = lattice::column_type::text; one.values.push_back(value);
    choice.union_desc.cases = {one}; schema[0].properties["choice"] = choice;
    auto owner = outcome_owner(lattice::swift_configuration(path.str()), schema);
    auto item = outcome_object(schema, 0);
    lattice::cxx_error error(std::runtime_error("old"));
    ASSERT_TRUE(owner->add_checked(*item, error));
    owner->get()->db().execute("CREATE TRIGGER outcome_union BEFORE INSERT ON _OutcomeItem_choice "
                              "BEGIN SELECT RAISE(ABORT, 'outcome-real-union'); END");
    lattice::union_value offered; offered.case_name = "text"; offered.set_string("_0", "value");
    EXPECT_NO_THROW(item->set_union("choice", offered));
    const auto failure = lattice::borrow_bridge_outcome();
    ASSERT_TRUE(failure.failed && failure.diagnostic_available);
    EXPECT_NE(std::string(failure.message_data, failure.message_size).find("outcome-real-union"), std::string::npos);
}
#endif


#ifndef __linux__
TEST(BridgeOwnerIdentity, SameNativeFacadesMatchAndDifferentOwnersDoNot) {
    TempDB first_path{"owner_identity_first"};
    TempDB second_path{"owner_identity_second"};
    const auto schema = outcome_schema();
    auto first = outcome_owner(lattice::swift_configuration(first_path.str()), schema);
    auto second = outcome_owner(lattice::swift_configuration(second_path.str()), schema);
    auto alias = outcome_owner(lattice::swift_configuration(first_path.str()), schema);
    static_assert(noexcept(first->owner_identity()));
    const auto actual = first->owner_identity();
    EXPECT_TRUE(actual.valid());
    EXPECT_TRUE(actual.matches(alias->owner_identity()));
    EXPECT_FALSE(actual.matches(second->owner_identity()));
    EXPECT_FALSE(actual.matches(lattice::bridge_owner_identity{}));
    first->close(); second->close();
}

TEST(BridgeOwnerIdentity, SamePathAfterCloseHasDifferentIncarnation) {
    TempDB path{"owner_identity_reopen"};
    const auto schema = outcome_schema();
    auto first = outcome_owner(lattice::swift_configuration(path.str()), schema);
    const auto prior = first->owner_identity();
    first->close();
    auto successor = outcome_owner(lattice::swift_configuration(path.str()), schema);
    EXPECT_TRUE(prior.valid()); // the original facade still retains its closed owner
    EXPECT_FALSE(prior.matches(successor->owner_identity()));
    successor->close();
}

TEST(BridgeOwnerIdentity, TokenDoesNotRetainNativeOwnerAfterClose) {
    TempDB path{"owner_identity_weak"};
    const auto schema = outcome_schema();
    auto owner = outcome_owner(lattice::swift_configuration(path.str()), schema);
    std::weak_ptr<lattice::swift_lattice> actual_owner =
        lattice::swift_lattice_ref::shared_for_lattice(owner->get());
    const auto identity = owner->owner_identity();
    ASSERT_TRUE(identity.valid());
    owner->close(); owner.reset();
    EXPECT_TRUE(actual_owner.expired());
    EXPECT_FALSE(identity.valid());
    EXPECT_FALSE(identity.matches(identity));
}

TEST(BridgeOwnerIdentity, ManagedObjectAndListKeepActualParentIdentityWithoutClearingFailure) {
    TempDB path{"owner_identity_managed"};
    const auto schema = outcome_relationship_schema();
    auto owner = outcome_owner(lattice::swift_configuration(path.str()), schema);
    auto parent = outcome_object(schema, 0, 1);
    EXPECT_FALSE(parent->owner_identity().valid());
    lattice::cxx_error error(std::runtime_error("old"));
    ASSERT_TRUE(owner->add_checked(*parent, error));
#if LATTICE_HAS_FRT
    std::unique_ptr<lattice::link_list_ref> list(parent->get_link_list("items"));
#else
    auto list = std::make_unique<lattice::link_list_ref>(parent->get_link_list("items"));
#endif
    ASSERT_NE(list, nullptr);
    list->assign_at(99, *parent);
    const auto failed = lattice::borrow_bridge_outcome();
    ASSERT_TRUE(failed.failed && failed.diagnostic_available);
    const std::string captured(failed.message_data, failed.message_size);
    const auto identity = owner->owner_identity();
    EXPECT_TRUE(identity.matches(parent->owner_identity()));
    EXPECT_TRUE(identity.matches(list->owner_identity()));
    lattice::link_list copied(*list->get());
    lattice::link_list moved(std::move(copied));
    EXPECT_TRUE(identity.matches(moved.owner_identity_));
    const auto after = lattice::borrow_bridge_outcome();
    ASSERT_TRUE(after.failed && after.diagnostic_available);
    EXPECT_EQ(std::string(after.message_data, after.message_size), captured);
    owner->close();
}
#endif

#if defined(__APPLE__)
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

TEST(BridgeOwnerIdentity, ForkedChildCannotCopyCompareOrMintInheritedIdentity) {
    TempDB path{"owner_identity_fork"};
    const auto schema = outcome_schema();
    auto owner = outcome_owner(lattice::swift_configuration(path.str()), schema);
    const auto original = owner->owner_identity();
    ASSERT_TRUE(original.valid());
    // Allocate the diagnostic namespace before any child exists.
    const std::string retained_store = path.str();
    const auto child = ::fork();
    if (child == 0) {
        // This child performs only the creator-fenced identity operations and
        // async-signal-safe exit; it never runs Native/SQLite cleanup or SQL.
        bool refused = !original.valid() && !original.matches(original);
        {
            const auto copied = original;
            lattice::bridge_owner_identity assigned;
            assigned = original;
            const auto getter = owner->owner_identity();
            refused = refused && !copied.valid() && !assigned.valid() && !getter.valid();
        }
        ::_exit(refused ? 0 : 91);
    }
    ASSERT_GT(child, 0); // parent only: no GoogleTest operation in the child
    int status = 0;
    int wait_error = 0;
    int signal_result = 0;
    int signal_error = 0;
    bool signal_attempted = false;
    bool reaped = false;
    bool still_owned = true;
    auto poll = [&] {
        const auto result = ::waitpid(child, &status, WNOHANG);
        const auto actual_error = result < 0 ? errno : 0;
        if (result == child) reaped = true;
        else if (result < 0 && actual_error != EINTR) {
            wait_error = actual_error;
            still_owned = false;
        }
    };
    const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!reaped && still_owned && std::chrono::steady_clock::now() < end) {
        poll();
        if (!reaped) std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    if (!reaped && still_owned) {
        // A finite ending reserve for this exact unreaped child; never signal
        // after waitpid reports that custody was lost or already consumed.
        signal_attempted = true;
        signal_result = ::kill(child, SIGKILL);
        signal_error = signal_result < 0 ? errno : 0;
        const auto ending = end + std::chrono::seconds(1);
        while (!reaped && still_owned && std::chrono::steady_clock::now() < ending) {
            poll();
            if (!reaped) std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    }
    if (!reaped) {
        // The exact child inherits this test process group and never changes
        // it. Leave custody/debt and files to the finite outer qualifier's
        // retained group ending. Unknown ending must never run close/TempDB.
        std::fprintf(stderr,
            "bridge-owner-identity child-ending-debt child=%ld group=%ld wait_errno=%d "
            "signal_attempted=%d signal_result=%d signal_errno=%d retained_store=%s\n",
            static_cast<long>(child), static_cast<long>(::getpgrp()), wait_error,
            signal_attempted ? 1 : 0, signal_result, signal_error, retained_store.c_str());
        std::fflush(stderr);
        ::_exit(92);
    }
    EXPECT_FALSE(signal_attempted) << "identity child exceeded its original work deadline; "
        << "kill result=" << signal_result << " errno=" << signal_error;
    EXPECT_EQ(wait_error, 0);
    EXPECT_TRUE(WIFEXITED(status));
    if (WIFEXITED(status)) EXPECT_EQ(WEXITSTATUS(status), 0);
    EXPECT_TRUE(original.valid());
    owner->close();
}
#endif
