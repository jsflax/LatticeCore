#include "TestHelpers.hpp"
#include <lattice.hpp>
#include "../../Sources/LatticeCore/src/recovery_refresh.hpp"
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"

namespace {
std::unique_ptr<lattice::swift_lattice_ref> recovery_ref(const std::string& path) {
    lattice::swift_configuration config(path, std::make_shared<lattice::immediate_scheduler>());
    config.audit_retention_seconds = 0;
    lattice::SchemaVector schemas;
#if LATTICE_HAS_FRT
    auto ref = std::unique_ptr<lattice::swift_lattice_ref>(lattice::swift_lattice_ref::create(config, schemas));
#else
    auto ref = std::make_unique<lattice::swift_lattice_ref>(lattice::swift_lattice_ref::create(config, schemas));
#endif
    lattice::detail::recovery_refresh_test_access::use_manual_preparation(*ref->get());
    if (auto* notifier = lattice::instance_registry::instance().get_or_create_notifier(path)) notifier->stop_listening();
    return ref;
}
struct refresh_receipt {
    int calls = 0, destroyed = 0;
};
void refresh_call(void* pointer) { ++static_cast<refresh_receipt*>(pointer)->calls; }
void refresh_destroy(void* pointer) { ++static_cast<refresh_receipt*>(pointer)->destroyed; }
}

TEST(RecoveryRefreshBridge, RefForwardingDeliversWithoutAuditPayloadAndCoalescesWitness) {
    TempDB path("refresh-bridge-forwarding"); auto ref = recovery_ref(path.str());
    auto owner = lattice::swift_lattice_ref::shared_for_lattice(ref->get());
    ASSERT_TRUE(owner);
    const auto installed = lattice::detail::recovery_writer_access::install(owner, [&](auto&) {
        lattice::detail::bump_recovery_witness(*owner);
    });
    ASSERT_EQ(installed.state, lattice::detail::recovery_install_state::committed);
    int audit_events = 0;
    const auto audit = owner->add_table_observer("AuditLog", [&](const auto&) { ++audit_events; });
    refresh_receipt receipt;
    const auto token = ref->add_recovery_refresh_observer(&receipt, refresh_call, refresh_destroy);
    ASSERT_NE(token, 0u); EXPECT_EQ(receipt.calls, 1); EXPECT_EQ(receipt.destroyed, 0);
    owner->request_recovery_refresh(); EXPECT_EQ(receipt.calls, 1);
    EXPECT_EQ(audit_events, 0); EXPECT_TRUE(owner->db().query("SELECT * FROM AuditLog").empty());
    ref->remove_recovery_refresh_observer(token);
    ref->remove_recovery_refresh_observer(token);
    EXPECT_EQ(receipt.destroyed, 1);
    owner->request_recovery_refresh(); EXPECT_EQ(receipt.calls, 1);
    owner->remove_table_observer("AuditLog", audit);
}

TEST(RecoveryRefreshBridge, RejectedRegistrationConsumesContextExactlyOnce) {
    TempDB path("refresh-bridge-refusal"); auto ref = recovery_ref(path.str());
    refresh_receipt missing, closed;
    EXPECT_EQ(ref->add_recovery_refresh_observer(&missing, nullptr, refresh_destroy), 0u);
    EXPECT_EQ(missing.destroyed, 1); EXPECT_EQ(missing.calls, 0);
    ref->close();
    EXPECT_EQ(ref->add_recovery_refresh_observer(&closed, refresh_call, refresh_destroy), 0u);
    EXPECT_EQ(closed.destroyed, 1); EXPECT_EQ(closed.calls, 0);
}

TEST(RecoveryRefreshBridge, LogicalCloseStillAllowsContextRemoval) {
    TempDB path("refresh-bridge-closed-removal"); auto ref = recovery_ref(path.str());
    refresh_receipt receipt;
    const auto token = ref->add_recovery_refresh_observer(&receipt, refresh_call, refresh_destroy);
    ASSERT_NE(token, 0u); EXPECT_EQ(receipt.destroyed, 0);
    ref->close();
    ref->remove_recovery_refresh_observer(token);
    EXPECT_EQ(receipt.destroyed, 1); EXPECT_EQ(receipt.calls, 0);
}

TEST(RecoveryRefreshBridge, ContextDestructionMayReenterRemovalOffTheLeafLock) {
    TempDB path("refresh-bridge-reentrant-release"); auto ref = recovery_ref(path.str());
    struct receipt_type { lattice::swift_lattice_ref* ref; int destroyed = 0; } receipt{ref.get()};
    const auto token = ref->add_recovery_refresh_observer(&receipt, [](void*) {}, [](void* pointer) {
        auto& receipt = *static_cast<receipt_type*>(pointer);
        receipt.ref->remove_recovery_refresh_observer(0); ++receipt.destroyed;
    });
    ASSERT_NE(token, 0u);
    ref->remove_recovery_refresh_observer(token);
    EXPECT_EQ(receipt.destroyed, 1);
}
