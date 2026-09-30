#include "TestHelpers.hpp"
#include <lattice.hpp>
#include <condition_variable>

namespace {
std::unique_ptr<lattice::swift_lattice_ref> coarse_ref(const std::string& path) {
    lattice::swift_configuration config(path);
    lattice::SchemaVector schemas;
#if LATTICE_HAS_FRT
    return std::unique_ptr<lattice::swift_lattice_ref>(
        lattice::swift_lattice_ref::create(config, schemas));
#else
    return std::make_unique<lattice::swift_lattice_ref>(
        lattice::swift_lattice_ref::create(config, schemas));
#endif
}

struct CoarseReceipt {
    std::atomic<int> calls{0};
    std::atomic<int> destroyed{0};
    std::atomic<int> reason{-1};
};

void coarse_signal(void* pointer, int reason) {
    auto& receipt = *static_cast<CoarseReceipt*>(pointer);
    receipt.reason.store(reason);
    receipt.calls.fetch_add(1);
}
void coarse_destroy(void* pointer) {
    static_cast<CoarseReceipt*>(pointer)->destroyed.fetch_add(1);
}
} // namespace

TEST(CoarseInvalidationBridge, PayloadFreeReasonsAndIdempotentRemoval) {
    TempDB path("coarse_reasons");
    auto ref = coarse_ref(path.str());
    CoarseReceipt receipt;
    const auto token = ref->add_coarse_invalidation_hook(&receipt, coarse_signal, coarse_destroy);
    ASSERT_NE(token, 0u);
    EXPECT_TRUE(lattice::last_bridge_error().empty());
    std::vector<std::string> tables(512, "SomeTable");
    using Reason = lattice::lattice_db::invalidation_reason;
    for (const auto reason : {Reason::commit, Reason::rollback, Reason::advance}) {
        ref->get()->fire_invalidation_hooks(tables, reason);
        EXPECT_EQ(receipt.reason.load(), static_cast<int>(reason));
    }
    EXPECT_EQ(receipt.calls.load(), 3);
    EXPECT_EQ(receipt.destroyed.load(), 0);
    EXPECT_TRUE(ref->remove_coarse_invalidation_hook(token));
    EXPECT_TRUE(ref->remove_coarse_invalidation_hook(token));
    EXPECT_EQ(receipt.destroyed.load(), 1);
    ref->get()->fire_invalidation_hooks(tables, Reason::commit);
    EXPECT_EQ(receipt.calls.load(), 3);
}

TEST(CoarseInvalidationBridge, RejectedRegistrationConsumesContext) {
    TempDB path("coarse_failure");
    auto ref = coarse_ref(path.str());
    CoarseReceipt missing;
    EXPECT_EQ(ref->add_coarse_invalidation_hook(&missing, nullptr, coarse_destroy), 0u);
    EXPECT_FALSE(lattice::last_bridge_error().empty());
    EXPECT_EQ(missing.destroyed.load(), 1);
    EXPECT_EQ(missing.calls.load(), 0);
    ref->close();
    CoarseReceipt closed;
    EXPECT_EQ(ref->add_coarse_invalidation_hook(&closed, coarse_signal, coarse_destroy), 0u);
    EXPECT_EQ(closed.destroyed.load(), 1);
    EXPECT_EQ(closed.calls.load(), 0);
}

TEST(CoarseInvalidationBridge, CopiedHookOwnsContextUntilCallbackReturns) {
    TempDB path("coarse_inflight");
    auto ref = coarse_ref(path.str());
    struct Receipt {
        std::mutex mutex;
        std::condition_variable condition;
        bool entered = false;
        bool release = false;
        std::atomic<int> destroyed{0};
    } receipt;
    const auto token = ref->add_coarse_invalidation_hook(&receipt,
        [](void* pointer, int) {
            // Test-only blocking callback to expose the copied-hook lifetime.
            // Production callbacks must obey the nonblocking hook contract.
            auto& receipt = *static_cast<Receipt*>(pointer);
            std::unique_lock<std::mutex> lock(receipt.mutex);
            receipt.entered = true;
            receipt.condition.notify_all();
            receipt.condition.wait_for(lock, std::chrono::seconds(5), [&] { return receipt.release; });
        },
        [](void* pointer) { static_cast<Receipt*>(pointer)->destroyed.fetch_add(1); });
    ASSERT_NE(token, 0u);
    std::thread caller([&] {
        ref->get()->fire_invalidation_hooks({}, lattice::lattice_db::invalidation_reason::advance);
    });
    {
        std::unique_lock<std::mutex> lock(receipt.mutex);
        const bool entered = receipt.condition.wait_for(
            lock, std::chrono::seconds(3), [&] { return receipt.entered; });
        EXPECT_TRUE(entered) << "hook never entered; release and join still run below";
    }
    EXPECT_TRUE(ref->remove_coarse_invalidation_hook(token));
    EXPECT_EQ(receipt.destroyed.load(), 0);
    {
        std::lock_guard<std::mutex> lock(receipt.mutex);
        receipt.release = true;
        receipt.condition.notify_all();
    }
    caller.join();
    EXPECT_EQ(receipt.destroyed.load(), 1);
}

TEST(CoarseInvalidationBridge, CallbackExceptionCannotUnwindHookFrame) {
    TempDB path("coarse_exception");
    auto ref = coarse_ref(path.str());
    CoarseReceipt receipt;
    const auto token = ref->add_coarse_invalidation_hook(&receipt,
        [](void*, int) { throw 7; }, coarse_destroy);
    ASSERT_NE(token, 0u);
    EXPECT_NO_THROW(ref->get()->fire_invalidation_hooks(
        {}, lattice::lattice_db::invalidation_reason::advance));
    EXPECT_TRUE(ref->remove_coarse_invalidation_hook(token));
    EXPECT_EQ(receipt.destroyed.load(), 1);
}

TEST(CoarseInvalidationBridge, ContextDestroyMayReenterHookBookkeeping) {
    TempDB path("coarse_reentrant_destroy");
    auto ref = coarse_ref(path.str());
    struct Receipt {
        lattice::swift_lattice_ref* ref;
        int destroyed = 0;
        bool removed = false;
    } receipt{ref.get()};
    const auto token = ref->add_coarse_invalidation_hook(&receipt,
        [](void*, int) {},
        [](void* pointer) {
            auto& receipt = *static_cast<Receipt*>(pointer);
            // This takes the same hook mutex. Final context destruction must
            // happen after remove_invalidation_hook has released that lock.
            receipt.removed = receipt.ref->remove_coarse_invalidation_hook(0);
            ++receipt.destroyed;
        });
    ASSERT_NE(token, 0u);
    EXPECT_TRUE(ref->remove_coarse_invalidation_hook(token));
    EXPECT_EQ(receipt.destroyed, 1);
    EXPECT_TRUE(receipt.removed);
}
