#include <gtest/gtest.h>
#include <lattice/scheduler.hpp>
#include <memory>

namespace {
struct Counts { int invoked = 0; int destroyed = 0; };
struct Context { Counts* counts; };

lattice::generic_scheduler scheduler_for(Counts& counts) {
    return lattice::generic_scheduler(new Context{&counts},
        [](void* work, void* raw) {
            ++static_cast<Context*>(raw)->counts->invoked;
            lattice::generic_scheduler::execute_work(work);
        },
        [](void*) { return true; },
        [](const lattice::scheduler*, void*) { return false; },
        [](void*) { return true; },
        [](void* raw) {
            auto* context = static_cast<Context*>(raw);
            ++context->counts->destroyed;
            delete context;
        });
}
}

TEST(GenericSchedulerLifetime, TemporaryAndCopiesShareExactlyOneContextOwner) {
    Counts counts;
    std::shared_ptr<lattice::scheduler> first, second;
    {
        auto original = scheduler_for(counts);
        {
            auto copy = original;
            first = copy.make_shared();
        }
        second = original.make_shared();
        EXPECT_EQ(counts.destroyed, 0);
    }
    EXPECT_EQ(counts.destroyed, 0);
    int work = 0;
    first->invoke([&] { ++work; });
    first.reset();
    EXPECT_EQ(counts.destroyed, 0);
    second->invoke([&] { ++work; });
    EXPECT_EQ(work, 2);
    EXPECT_EQ(counts.invoked, 2);
    second.reset();
    EXPECT_EQ(counts.destroyed, 1);
}

TEST(GenericSchedulerLifetime, DirectTemporaryMakeSharedRetainsItsContext) {
    Counts counts;
    auto shared = scheduler_for(counts).make_shared();
    EXPECT_EQ(counts.destroyed, 0);
    int work = 0;
    shared->invoke([&] { ++work; });
    EXPECT_EQ(work, 1);
    shared.reset();
    EXPECT_EQ(counts.destroyed, 1);
}
