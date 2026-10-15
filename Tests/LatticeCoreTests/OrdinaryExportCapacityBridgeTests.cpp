#include "TestHelpers.hpp"
#include "../../Sources/LatticeSwiftCppBridge/include/ordinary_sync_export.hpp"
#include "../../Sources/LatticeCore/src/ordinary_capacity_notification.hpp"
#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>

#ifndef __EMSCRIPTEN__
namespace {
using bridge = lattice::ordinary_sync_export_capacity_ref;
struct bridge_state {
    std::mutex mutex;
    std::optional<bridge> first, second;
    std::atomic<size_t> notifications{0}, releases{0}, nested_releases{0};
    std::atomic<bool> released_inside_callback{false}, reenter{false}, reentered{false};
};
struct bridge_context { std::shared_ptr<bridge_state> state; bool nested=false; };
void cancel_bridge_copies(const std::shared_ptr<bridge_state>& state) {
    std::optional<bridge> first, second;
    { std::lock_guard lock(state->mutex); first.swap(state->first); second.swap(state->second); }
    // The fixture lock cannot contain native callback/context destruction.
    if(first)first->cancel();
    if(second)second->cancel();
}
void ignored_hint(void*) {}
void release_context(void* raw) {
    std::unique_ptr<bridge_context> context(static_cast<bridge_context*>(raw));
    const auto state=context->state;
    if(context->nested){state->nested_releases.fetch_add(1);return;}
    state->releases.fetch_add(1);
    if(state->reenter.load()) {
        // An actual registration here would deadlock if final context release
        // occurred while the genuine registry mutex was held.
        auto nested=bridge::observe(new bridge_context{state,true},ignored_hint,release_context);
        state->reentered.store(nested.valid());
        nested.cancel();
    }
}
void cancel_from_hint(void* raw) {
    const auto state=static_cast<bridge_context*>(raw)->state;
    state->notifications.fetch_add(1);
    cancel_bridge_copies(state);
    if(state->releases.load()!=0)state->released_inside_callback.store(true);
}
auto idle_deadline(){return std::chrono::steady_clock::now()+std::chrono::seconds(5);}
}

TEST(OrdinaryExportCapacityBridge, InvalidInputAndActualRegistryRefusalConsumeContextExactlyOnce) {
    EXPECT_FALSE(bridge{}.valid());
    auto invalid_state=std::make_shared<bridge_state>();
    auto invalid=bridge::observe(new bridge_context{invalid_state},nullptr,release_context);
    EXPECT_FALSE(invalid.valid());
    EXPECT_EQ(invalid_state->releases.load(),1u);
    invalid.cancel();invalid.cancel();
    EXPECT_EQ(invalid_state->releases.load(),1u);
    ASSERT_TRUE(lattice::detail::await_ordinary_export_capacity_notification_idle(idle_deadline()));
    std::array<lattice::ordinary_export_capacity_observer,64> held;
    for(auto& owner:held){owner=lattice::observe_ordinary_export_capacity([]{});ASSERT_TRUE(owner.valid());}
    auto refused_state=std::make_shared<bridge_state>();
    auto refused=bridge::observe(new bridge_context{refused_state},cancel_from_hint,release_context);
    EXPECT_FALSE(refused.valid());
    EXPECT_EQ(refused_state->releases.load(),1u);
    EXPECT_EQ(refused_state->notifications.load(),0u);
    refused.cancel();
    EXPECT_EQ(refused_state->releases.load(),1u);
    for(auto& owner:held)owner.cancel();
    EXPECT_TRUE(lattice::detail::await_ordinary_export_capacity_notification_idle(idle_deadline()));
}

TEST(OrdinaryExportCapacityBridge, RealLastCutReleaseRetainsContextThroughCopyCancellationAndReentry) {
    ASSERT_TRUE(lattice::detail::await_ordinary_export_capacity_notification_idle(idle_deadline()));
    TempDB path{"export_capacity_bridge_context"};
    lattice::configuration config(path.str());config.audit_retention_seconds=0;config.busy_timeout_ms=100;
    lattice::lattice_db source{config};source.add(TestPerson{"hint",1,std::nullopt});
    std::optional<lattice::ordinary_sync_export> cut=lattice::capture_ordinary_sync_export(source,{"TestPerson"});
    auto state=std::make_shared<bridge_state>();state->reenter.store(true);
    state->first=bridge::observe(new bridge_context{state},cancel_from_hint,release_context);
    ASSERT_TRUE(state->first->valid());state->second=state->first;
    EXPECT_EQ(state->notifications.load(),0u); // registration is never a positive capacity result
    cut.reset(); // actual final native export reservation destruction supplies the hint
    const bool settled=lattice::detail::await_ordinary_export_capacity_notification_idle(idle_deadline());
    // Cleanup also owns timeout/failure paths without borrowing callback stack state.
    cancel_bridge_copies(state);
    EXPECT_TRUE(settled);
    EXPECT_TRUE(lattice::detail::await_ordinary_export_capacity_notification_idle(idle_deadline()));
    EXPECT_EQ(state->notifications.load(),1u);
    EXPECT_FALSE(state->released_inside_callback.load());
    EXPECT_EQ(state->releases.load(),1u);
    EXPECT_TRUE(state->reentered.load());
    EXPECT_EQ(state->nested_releases.load(),1u);
}
#endif
