#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/sync_callback_lifetime.hpp"
#include <lattice/sync.hpp>
#include <deque>
#include <future>

#ifndef __EMSCRIPTEN__
namespace lattice::detail {
struct sync_retirement_test_access {
    static std::shared_ptr<sync_retirement_lane> create(size_t capacity=64,
        sync_retirement_lane::launch_function launch=sync_retirement_lane::launch) {
        return std::shared_ptr<sync_retirement_lane>(new sync_retirement_lane(capacity,launch));
    }
    static bool await_state(sync_retirement_lane& lane,size_t count,int state) {
        std::unique_lock<std::mutex> lock(lane.mutex_);
        return lane.settled_.wait_for(lock,std::chrono::seconds(5),[&] {
            size_t found=0;for(size_t i=0;i<lane.capacity_;++i)
                if(static_cast<int>(lane.slots_[i].state)==state)++found;
            return found==count;
        });
    }
};
}
namespace {
using namespace lattice;
using cell=detail::sync_callback_lifetime;
using lane_access=detail::sync_retirement_test_access;
struct manual_scheduler final:scheduler {
    std::deque<std::function<void()>> work;
    void invoke(std::function<void()>&& fn)override{work.push_back(std::move(fn));}
    bool is_on_thread()const noexcept override{return true;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
    void shutdown()override{work.clear();}
    void run(){auto pending=std::move(work);work.clear();for(auto& fn:pending)fn();}
};
struct transport final:sync_transport {
    std::function<void()> stop,destroy;
    ~transport(){if(destroy)destroy();}
    void connect(const std::string&,const std::map<std::string,std::string>&)override{}
    void disconnect()override{if(stop)stop();}
    transport_state state()const override{return transport_state::closed;}
    void send(const transport_message&)override{}
    void set_on_open(on_open_handler)override{}
    void set_on_message(on_message_handler)override{}
    void set_on_error(on_error_handler)override{}
    void set_on_close(on_close_handler)override{}
};
struct test_owner final:synchronizer {
    using synchronizer::synchronizer;
    std::shared_ptr<lattice_db> retained_owner(){return owned_db_;}
};
struct context {
    std::shared_ptr<manual_scheduler> queue=std::make_shared<manual_scheduler>();
    std::unique_ptr<test_owner> owner;
    std::shared_ptr<cell> lifetime;
    context(){
        configuration cfg(":memory:");cfg.sched=queue;cfg.audit_retention_seconds=0;
        sync_config sync;sync.sync_id="callback-cell-qualification";
        sync.upload_coalesce_ms=0;sync.checkpoint_passive_interval_ms=0;
        owner=std::make_unique<test_owner>(std::make_unique<lattice_db>(cfg),sync,std::make_unique<transport>());
        // A separate mechanical cell borrows the real, still-live owner. These
        // cases do not claim to exercise the synchronizer's full send path.
        lifetime=std::make_shared<cell>(owner.get(),owner->retained_owner());
    }
};
}

TEST(SyncCallbackLifetime, QueuedWorkCannotBorrowReplacementGeneration) {
    context c;auto sched=detail::make_sync_lifetime_scheduler(c.queue,c.lifetime);int calls=0;
    c.lifetime->queued(1,[&]{
        c.lifetime->publish_generation(3);
        EXPECT_EQ(c.lifetime->dispatch_generation(),1u);
        sched->invoke([&]{++calls;});
    });
    c.queue->run();EXPECT_EQ(calls,0);
    sched->invoke([&]{++calls;});c.queue->run();EXPECT_EQ(calls,1);
}

TEST(SyncCallbackLifetime, EndedProtectedAttemptAllowsOnlyExplicitTerminalNotification) {
    context c;auto sched=detail::make_sync_lifetime_scheduler(c.queue,c.lifetime);int calls=0;
    c.lifetime->begin_connect(1,true);
    sched->invoke([&]{calls+=100;});c.lifetime->end_protected_attempt();
    c.lifetime->transport([&]{calls+=1000;});
    detail::schedule_sync_terminal_notification(sched,c.lifetime,1,[&]{++calls;});
    c.queue->run();EXPECT_EQ(calls,1);
    detail::schedule_sync_terminal_notification(sched,c.lifetime,1,[&]{++calls;});
    c.lifetime->publish_generation(3);c.queue->run();EXPECT_EQ(calls,1);
}

TEST(SyncCallbackLifetime, SelfRetirementReturnsAndRefusesAllLaterWork) {
    context c;int calls=0;
    c.lifetime->queued(1,[&]{c.lifetime->retire_and_wait();++calls;});
    c.lifetime->queued(1,[&]{++calls;});
    c.lifetime->terminal_notification(1,[&]{++calls;});
    c.lifetime->transport([&]{++calls;});EXPECT_EQ(calls,1);
}

TEST(SyncCallbackLifetime, ThrowingExecutionReleasesItsAdmission) {
    context c;
    EXPECT_THROW(c.lifetime->queued(1,[]{throw std::runtime_error("callback fault");}),std::runtime_error);
    EXPECT_NO_THROW(c.lifetime->retire_and_wait());
    EXPECT_FALSE(c.lifetime->current(1));
}

TEST(SyncCallbackLifetime, AUsedPhysicalEndpointCannotAcquireProtectedAttempt) {
    context c;c.lifetime->begin_connect(1,false);c.lifetime->publish_generation(3);
    EXPECT_FALSE(c.lifetime->can_begin_protected(3));
    EXPECT_THROW(c.lifetime->begin_connect(3,true),db_error);
}

TEST(SyncRetirementLane, All64ReservedSlotsCountBeforePublicationAndCancelReturnsCapacity) {
    auto lane=lane_access::create();std::vector<detail::sync_retirement_lane::reservation> reservations;
    for(int i=0;i<64;++i)reservations.push_back(lane->reserve(std::make_shared<transport>()));
    EXPECT_THROW(lane->reserve(std::make_shared<transport>()),db_error);
    reservations.clear();EXPECT_TRUE(lane_access::await_state(*lane,64,0));
    EXPECT_NO_THROW(lane->reserve(std::make_shared<transport>()));
}

TEST(SyncRetirementLane, LaunchFailureCannotPublishReservation) {
    const auto fail=[](std::function<void()>)->std::thread{throw std::runtime_error("launch refused");};
    EXPECT_THROW(lane_access::create(64,fail),std::runtime_error);
    const auto empty=[](std::function<void()>)->std::thread{return {};};
    EXPECT_THROW(lane_access::create(64,empty),db_error);
}

TEST(SyncRetirementLane, ActualPacerJoinAndTransportDestructionPrecedeSlotReuse) {
    auto lane=lane_access::create(1);std::promise<void> release,disconnected;auto ready=release.get_future();
    auto stopped=disconnected.get_future();std::atomic<int> destroyed{0};const auto caller=std::this_thread::get_id();
    auto wire=std::make_shared<transport>();wire->stop=[&]{EXPECT_NE(std::this_thread::get_id(),caller);disconnected.set_value();};
    wire->destroy=[&]{++destroyed;};
    auto reservation=lane->reserve(wire);wire.reset();reservation.publish();
    std::thread pacer([&]{ready.wait();});reservation.retire(std::move(pacer));
    const bool saw_disconnect=stopped.wait_for(std::chrono::seconds(5))==std::future_status::ready;
    EXPECT_TRUE(saw_disconnect);EXPECT_EQ(destroyed.load(),0);
    EXPECT_THROW(lane->reserve(std::make_shared<transport>()),db_error);
    release.set_value();EXPECT_TRUE(lane_access::await_state(*lane,1,0));EXPECT_EQ(destroyed.load(),1);
}

TEST(SyncRetirementLane, FailedDisconnectQuarantinesItsReservation) {
    auto lane=lane_access::create(1);auto wire=std::make_shared<transport>();
    wire->stop=[]{throw std::runtime_error("disconnect refused");};std::weak_ptr<transport> retained=wire;
    auto reservation=lane->reserve(wire);wire.reset();reservation.publish();reservation.retire();
    EXPECT_TRUE(lane_access::await_state(*lane,1,4));EXPECT_FALSE(retained.expired());
    EXPECT_THROW(lane->reserve(std::make_shared<transport>()),db_error);
    // The private test lane owns a threadless fake. Production lane lifetime
    // is process-wide, retaining quarantined real endpoints through shutdown.
}
#endif
