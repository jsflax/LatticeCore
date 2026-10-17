#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_domain.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_domain_test_access.hpp"
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <future>
#include <limits>
#include <mutex>
#include <new>
#include <stdexcept>
#include <thread>
#if defined(__APPLE__) || defined(__linux__)
#include <cerrno>
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {
using Domain=lattice::detail::observer_delivery_domain;
using Status=Domain::status;
using Units=Domain::units;
using Clock=Domain::clock;

bool queued(const Domain& domain,size_t count) {
    const auto end=Clock::now()+std::chrono::seconds(2);
    do {
        if(domain.inspect().waiters==count)return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }while(Clock::now()<end);
    return domain.inspect().waiters==count;
}

// Real production acquire calls on owned threads, not a replacement scheduler.
// Failure cleanup cancels and waits for the actual worker tail under its fixed
// original deadline+ending reserve before join. Unknown ending retains debt.
struct AdmissionWorker {
    Domain::cancellation stop;
    Domain::client client;
    const Units wanted;
    const Clock::time_point deadline,ending;
    std::promise<Domain::admission> promise;
    std::future<Domain::admission> future=promise.get_future();
    std::mutex mutex;
    std::condition_variable changed;
    bool ended=false;
    std::thread thread;
    AdmissionWorker(const Domain& domain,Domain::client value,Units request)
        :stop(domain),client(std::move(value)),wanted(request),
         deadline(Clock::now()+std::chrono::seconds(4)),ending(deadline+std::chrono::seconds(1)),
         thread([this]{
             try {auto result=client.acquire(wanted,deadline,&stop);promise.set_value(std::move(result));}
             catch(...){promise.set_exception(std::current_exception());}
             {std::lock_guard lock(mutex);ended=true;}changed.notify_all();
         }){}
    void join() noexcept {
        if(!thread.joinable())return;
        {std::unique_lock lock(mutex);if(!changed.wait_until(lock,ending,[&]{return ended;})){
            std::fprintf(stderr,"observer-domain worker ending unknown; retained in-memory domain, original deadline elapsed\n");
            std::fflush(stderr);std::_Exit(94);
        }}
        try {thread.join();}
        catch(...){
            std::fprintf(stderr,"observer-domain worker join failed; retained in-memory domain and joinable worker\n");
            std::fflush(stderr);std::_Exit(94);
        }
    }
    Domain::admission take(){join();return future.get();}
    ~AdmissionWorker(){stop.request_stop();join();}
};

TEST(ObserverDeliveryDomain, RejectsInvalidOverflowAndOversizedRequestsBeforeAdmission) {
    EXPECT_THROW(Domain::create({0,1,{1,1},{}}),std::invalid_argument);
    EXPECT_THROW(Domain::create({65,1,{1,1},{}}),std::invalid_argument);
    EXPECT_THROW(Domain::create({1,257,{1,1},{}}),std::invalid_argument);
    EXPECT_THROW(Domain::create({2,1,{std::numeric_limits<uint64_t>::max(),1},{}}),std::invalid_argument);
    auto domain=Domain::create({1,1,{2,20},{1,10}});
    auto registered=domain.register_client();ASSERT_EQ(registered.code,Status::admitted);
    EXPECT_EQ(domain.register_client().code,Status::client_capacity);
    EXPECT_EQ(registered.value.try_acquire({0,1}).code,Status::invalid);
    EXPECT_EQ(registered.value.try_acquire({1,0}).code,Status::invalid);
    EXPECT_EQ(registered.value.try_acquire({4,1}).code,Status::oversized);
    EXPECT_EQ(registered.value.try_acquire({1,31}).code,Status::oversized);
    EXPECT_EQ(domain.inspect().used,(Units{}));
    auto other=Domain::create({1,1,{1,1},{}});Domain::cancellation foreign(other);
    EXPECT_EQ(registered.value.acquire({1,1},Clock::now()+std::chrono::seconds(1),&foreign).code,Status::invalid);
    EXPECT_EQ(domain.inspect().waiters,0u);
}

TEST(ObserverDeliveryDomain, BothAxesProtectQuietGuaranteedShareFromHotBurst) {
    auto domain=Domain::create({2,4,{2,20},{2,20}});
    auto hot=domain.register_client().value,quiet=domain.register_client().value;
    auto busy=hot.try_acquire({4,40});ASSERT_EQ(busy.code,Status::admitted);
    auto small=quiet.try_acquire({2,20});ASSERT_EQ(small.code,Status::admitted);
    EXPECT_EQ(domain.inspect().used,(Units{6,60}));
    EXPECT_EQ(domain.inspect().burst_used,(Units{2,20}));
    EXPECT_EQ(hot.try_acquire({1,1}).code,Status::unavailable);
    busy.held.reset();small.held.reset();
    auto bytes=hot.try_acquire({1,40});ASSERT_EQ(bytes.code,Status::admitted);
    EXPECT_EQ(hot.try_acquire({1,1}).code,Status::unavailable) << "free count is not byte credit";
    auto quiet_again=quiet.try_acquire({2,20});ASSERT_EQ(quiet_again.code,Status::admitted);
    EXPECT_EQ(domain.inspect().burst_used,(Units{0,20}));
}

TEST(ObserverDeliveryDomain, ActualAllocationFailureRetiresReservedClientAndProspectiveLeaseCell) {
    auto domain=Domain::create({1,1,{1,10},{}});
    using Access=lattice::detail::observer_delivery_domain_test_access;
    Access::fail_next_client_allocation();EXPECT_THROW(domain.register_client(),std::bad_alloc);
    EXPECT_EQ(domain.inspect().clients,0u);EXPECT_EQ(domain.inspect().used,(Units{}));
    auto registered=domain.register_client();ASSERT_EQ(registered.code,Status::admitted);
    Access::fail_next_lease_allocation();EXPECT_THROW(registered.value.try_acquire({1,10}),std::bad_alloc);
    EXPECT_EQ(domain.inspect().waiters,0u);EXPECT_EQ(domain.inspect().used,(Units{}));
    auto actual=registered.value.try_acquire({1,10});ASSERT_EQ(actual.code,Status::admitted);
    EXPECT_EQ(domain.inspect().used,(Units{1,10}));actual.held.reset();
    EXPECT_EQ(domain.inspect().used,(Units{}));
}

TEST(ObserverDeliveryDomain, ActualLastCaptureRetirementReturnsOneChargeDespiteThrowAndCopies) {
    auto domain=Domain::create({1,2,{1,10},{}});auto client=domain.register_client().value;
    auto admitted=client.try_acquire({1,10});ASSERT_EQ(admitted.code,Status::admitted);
    std::function<void()> work=[held=admitted.held]{
        Domain::delivery_scope actual_callback(held);
        throw std::runtime_error("actual delivery callback failure");
    };
    auto copied=work;admitted.held.reset();
    EXPECT_EQ(domain.inspect().leases,1u);
    EXPECT_THROW(work(),std::runtime_error);
    work={};EXPECT_EQ(domain.inspect().used,(Units{1,10}));
    copied={};EXPECT_EQ(domain.inspect().used,(Units{}));
    EXPECT_EQ(domain.inspect().leases,0u);
    EXPECT_EQ(client.try_acquire({1,10}).code,Status::admitted);
}

TEST(ObserverDeliveryDomain, BurstWaitersAreFifoWhileQuietGuaranteedClientProgresses) {
    auto domain=Domain::create({4,4,{1,10},{1,10}});
    auto hot=domain.register_client().value,b=domain.register_client().value,
         c=domain.register_client().value,quiet=domain.register_client().value;
    auto hot_held=hot.try_acquire({2,20});auto b_held=b.try_acquire({1,10});auto c_held=c.try_acquire({1,10});
    ASSERT_EQ(hot_held.code,Status::admitted);ASSERT_EQ(b_held.code,Status::admitted);ASSERT_EQ(c_held.code,Status::admitted);
    AdmissionWorker first(domain,b,{1,10});ASSERT_TRUE(queued(domain,1));
    AdmissionWorker second(domain,c,{1,10});ASSERT_TRUE(queued(domain,2));
    auto quiet_held=quiet.try_acquire({1,10});ASSERT_EQ(quiet_held.code,Status::admitted);
    EXPECT_EQ(domain.inspect().waiters,2u);
    hot_held.held.reset();
    auto first_grant=first.take();ASSERT_EQ(first_grant.code,Status::admitted);
    EXPECT_EQ(second.future.wait_for(std::chrono::seconds(0)),std::future_status::timeout)
        << "the first actual lease still owns shared burst";
    EXPECT_EQ(domain.inspect().waiters,1u);
    first_grant.held.reset();
    auto second_grant=second.take();ASSERT_EQ(second_grant.code,Status::admitted);
    EXPECT_EQ(domain.inspect().waiters,0u);
}

TEST(ObserverDeliveryDomain, NewSameClientRequestCannotOvertakeItsQueuedOriginal) {
    auto domain=Domain::create({1,3,{1,10},{1,10}});auto client=domain.register_client().value;
    auto held=client.try_acquire({1,10});ASSERT_EQ(held.code,Status::admitted);
    AdmissionWorker first(domain,client,{2,20});ASSERT_TRUE(queued(domain,1));
    EXPECT_EQ(client.try_acquire({1,10}).code,Status::unavailable);
    held.held.reset();auto granted=first.take();ASSERT_EQ(granted.code,Status::admitted);
    EXPECT_EQ(client.try_acquire({1,10}).code,Status::unavailable);
    granted.held.reset();EXPECT_EQ(client.try_acquire({1,10}).code,Status::admitted);
}

TEST(ObserverDeliveryDomain, BoundedWaiterCellCancellationAndAbsoluteDeadlineLeaveNoQueueDebt) {
    auto domain=Domain::create({1,1,{1,10},{}});auto client=domain.register_client().value;
    auto held=client.try_acquire({1,10});ASSERT_EQ(held.code,Status::admitted);
    AdmissionWorker pending(domain,client,{1,10});ASSERT_TRUE(queued(domain,1));
    EXPECT_EQ(client.acquire({1,10},Clock::now()+std::chrono::seconds(4)).code,Status::waiter_capacity);
    pending.stop.request_stop();auto cancelled=pending.take();EXPECT_EQ(cancelled.code,Status::cancelled);
    EXPECT_FALSE(cancelled.held.valid());EXPECT_EQ(domain.inspect().waiters,0u);
    EXPECT_EQ(domain.inspect().used,(Units{1,10}));
    const auto end=Clock::now()+std::chrono::milliseconds(20);
    EXPECT_EQ(client.acquire({1,10},end).code,Status::deadline);
    EXPECT_EQ(domain.inspect().waiters,0u);EXPECT_EQ(domain.inspect().used,(Units{1,10}));
    held.held.reset();
    EXPECT_EQ(client.acquire({1,10},Clock::now()-std::chrono::seconds(1)).code,Status::deadline)
        << "free capacity does not renew an expired original deadline";
    Domain::cancellation already_stopped(domain);already_stopped.request_stop();
    EXPECT_EQ(client.acquire({1,10},Clock::now()+std::chrono::seconds(1),&already_stopped).code,Status::cancelled);
    EXPECT_EQ(domain.inspect().used,(Units{}));
}

TEST(ObserverDeliveryDomain, CloseRefusesWaitersWithoutRefundingLiveLeaseOrReusingItsClientSlot) {
    auto domain=Domain::create({1,2,{1,10},{}});auto client=domain.register_client().value;
    auto held=client.try_acquire({1,10});ASSERT_EQ(held.code,Status::admitted);
    {
        AdmissionWorker pending(domain,client,{1,10});ASSERT_TRUE(queued(domain,1));
        client.close();EXPECT_EQ(pending.take().code,Status::closed);
    }
    client.reset();EXPECT_EQ(domain.register_client().code,Status::client_capacity);
    EXPECT_EQ(domain.inspect().used,(Units{1,10}));
    held.held.reset();auto successor=domain.register_client();ASSERT_EQ(successor.code,Status::admitted);
    auto next=successor.value.try_acquire({1,10});ASSERT_EQ(next.code,Status::admitted);
    domain.close();EXPECT_EQ(domain.register_client().code,Status::closed);
    EXPECT_EQ(successor.value.try_acquire({1,10}).code,Status::closed);
    EXPECT_EQ(domain.inspect().used,(Units{1,10}));
    next.held.reset();EXPECT_EQ(domain.inspect().used,(Units{}));
}

TEST(ObserverDeliveryDomain, ReentrantSameDomainNeverWaitsAndScopeRetainsItsActualTail) {
    auto domain=Domain::create({1,2,{2,20},{}});auto client=domain.register_client().value;
    auto held=client.try_acquire({1,10});ASSERT_EQ(held.code,Status::admitted);
    {
        Domain::delivery_scope callback(held.held);held.held.reset();
        auto remaining=client.acquire({1,10},Clock::now()+std::chrono::seconds(4));ASSERT_EQ(remaining.code,Status::admitted);
        EXPECT_EQ(client.acquire({1,10},Clock::now()+std::chrono::seconds(4)).code,Status::reentrant_would_block);
        EXPECT_EQ(domain.inspect().waiters,0u);EXPECT_EQ(domain.inspect().used,(Units{2,20}));
        remaining.held.reset();EXPECT_EQ(domain.inspect().used,(Units{1,10}));
        auto unrelated=Domain::create({1,1,{1,1},{}});auto other=unrelated.register_client().value;
        EXPECT_EQ(other.acquire({1,1},Clock::now()+std::chrono::seconds(1)).code,Status::admitted);
    }
    EXPECT_EQ(domain.inspect().used,(Units{}));
}

#if defined(__APPLE__) || defined(__linux__)
TEST(ObserverDeliveryDomain, ChildDenialAndActualLeaseResetNeverTouchInheritedHeldMetadataLeaf) {
    auto domain=Domain::create({1,1,{1,10},{}});auto client=domain.register_client().value;
    auto held=client.try_acquire({1,10});ASSERT_EQ(held.code,Status::admitted);Domain::cancellation stop(domain);
    const auto work_end=Clock::now()+std::chrono::seconds(2),ending_end=work_end+std::chrono::seconds(1);
    using Access=lattice::detail::observer_delivery_domain_test_access;
    Access::lock_metadata(domain);
    errno=0;const pid_t child=::fork();const int fork_error=child<0?errno:0;
    if(child==0){
        const bool refused=domain.inspect().code==Status::wrong_process &&
            client.try_acquire({1,10}).code==Status::wrong_process && !held.held.valid();
        held.held.reset();stop.request_stop();client.close();domain.close();
        ::_exit(refused?0:3); // no inherited fixture/Native API reuse claim
    }
    int status=0,wait_error=0,kill_result=0,kill_error=0;bool reaped=false,timed_out=false;
    if(child>0){
        const auto reap=[&]{const auto rc=::waitpid(child,&status,WNOHANG);
            if(rc==child)reaped=true;else if(rc<0&&errno!=EINTR)wait_error=errno;};
        while(!reaped&&!wait_error&&Clock::now()<work_end){reap();if(!reaped)std::this_thread::sleep_for(std::chrono::milliseconds(1));}
        timed_out=!reaped;
        if(!reaped&&!wait_error){errno=0;kill_result=::kill(child,SIGKILL);kill_error=kill_result?errno:0;
            while(!reaped&&!wait_error&&Clock::now()<ending_end){reap();if(!reaped)std::this_thread::sleep_for(std::chrono::milliseconds(1));}}
    }
    Access::unlock_metadata(domain);
    if(child>0&&!reaped){
        std::fprintf(stderr,"observer-domain fork ending unknown child=%ld parent=%ld parentGroup=%ld waitErr=%d killRc=%d killErr=%d; retained in-memory domain\n",
            static_cast<long>(child),static_cast<long>(::getpid()),static_cast<long>(::getpgrp()),wait_error,kill_result,kill_error);
        std::fflush(stderr);::_exit(95);
    }
    ASSERT_GT(child,0)<<"fork errno="<<fork_error;EXPECT_FALSE(timed_out);EXPECT_EQ(wait_error,0);EXPECT_EQ(kill_result,0);EXPECT_EQ(kill_error,0);
    ASSERT_TRUE(WIFEXITED(status));EXPECT_EQ(WEXITSTATUS(status),0);
    EXPECT_EQ(domain.inspect().used,(Units{1,10}));held.held.reset();EXPECT_EQ(domain.inspect().used,(Units{}));
}
#endif
}
