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


#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <sys/wait.h>
#include <unistd.h>
namespace lattice::detail {
struct sync_callback_lifetime_test_access {
    // Denial-only control: hold the actual existing leaf in a foreign thread.
    // This neither admits a callback nor changes generation/owner permission.
    static bool hold(sync_callback_lifetime& cell,std::atomic<bool>& acquired,
        const std::atomic<bool>& release,std::atomic<bool>& expired)noexcept {
        if(cell.creator_!=database_native_process())return false;
        const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(2);
        while(!cell.mutex_.try_lock()){
            if(std::chrono::steady_clock::now()>=end)return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        std::unique_lock<std::mutex> leaf(cell.mutex_,std::adopt_lock);
        acquired.store(true,std::memory_order_release);
        while(!release.load(std::memory_order_acquire)){
            if(std::chrono::steady_clock::now()>=end){expired.store(true,std::memory_order_release);break;}
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return true;
    }
};
}
namespace {
struct callback_fork_hold {
    std::atomic<bool> acquired{false},release{false},expired{false},held{false};
    std::thread worker;
    void end()noexcept{release.store(true,std::memory_order_release);if(worker.joinable())worker.join();}
    ~callback_fork_hold(){end();}
};
TEST(SyncCallbackLifetime, ForkReturningFromAdmittedCallbackAbandonsBeforeInheritedOwnerLeaf){
    context c;callback_fork_hold hold;const auto parent=::getpid();pid_t child=-1;
    bool setup=false;int fork_error=0;std::exception_ptr callback_error;
    const std::exception_ptr actual_error=std::make_exception_ptr(std::runtime_error("callback-fork-control"));
    const std::function<void(const std::string&)> public_error=[&](const std::string&){
        hold.worker=std::thread([&]{hold.held.store(lattice::detail::sync_callback_lifetime_test_access::hold(
            *c.lifetime,hold.acquired,hold.release,hold.expired),std::memory_order_release);});
        const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(1);
        while(!hold.acquired.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<end)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        setup=hold.acquired.load(std::memory_order_acquire)&&!hold.expired.load(std::memory_order_acquire);
        if(setup){errno=0;child=::fork();fork_error=child<0?errno:0;
            if(child==0)return; // return through the actual admitted execution
        }
        hold.release.store(true,std::memory_order_release);
    };
    try{c.lifetime->queued(1,[&]{lattice::detail::report_sync_background_error_dispatched(public_error,actual_error,"callback-fork-control");});}
    catch(...){if(::getpid()!=parent)::_exit(2);callback_error=std::current_exception();}
    if(::getpid()!=parent)::_exit(0); // before ALL inherited Native/thread cleanup
    hold.end(); // release/join precedes every fatal assertion and child wait
    int status=0,wait_error=0,kill_result=0,kill_error=0;bool reaped=child<=0,unknown=false,timed_out=false;
    const auto work_end=std::chrono::steady_clock::now()+std::chrono::seconds(2),ending_end=work_end+std::chrono::seconds(1);
    while(child>0&&std::chrono::steady_clock::now()<work_end){errno=0;const auto rc=::waitpid(child,&status,WNOHANG);
        if(rc==child){reaped=true;break;}if(rc<0&&errno!=EINTR){wait_error=errno;unknown=true;break;}
        std::this_thread::sleep_for(std::chrono::milliseconds(1));}
    if(!reaped&&!unknown){timed_out=true;errno=0;kill_result=::kill(child,SIGKILL);kill_error=kill_result<0?errno:0;
        while(std::chrono::steady_clock::now()<ending_end){errno=0;const auto rc=::waitpid(child,&status,WNOHANG);
            if(rc==child){reaped=true;break;}if(rc<0&&errno!=EINTR){wait_error=errno;unknown=true;break;}
            std::this_thread::sleep_for(std::chrono::milliseconds(1));}}
    if(!reaped||unknown){std::fprintf(stderr,"callback-lifetime child-ending-debt child=%ld group=%ld waitError=%d killResult=%d killError=%d retainedNativeOwner=1\n",
        long(child),long(::getpgrp()),wait_error,kill_result,kill_error);std::fflush(stderr);::_exit(95);}
    ASSERT_FALSE(callback_error);ASSERT_TRUE(setup);EXPECT_TRUE(hold.held.load());EXPECT_FALSE(hold.expired.load());
    ASSERT_GT(child,0)<<"forkError="<<fork_error;
    EXPECT_FALSE(timed_out);EXPECT_EQ(wait_error,0);EXPECT_EQ(kill_result,0);EXPECT_EQ(kill_error,0);
    ASSERT_TRUE(WIFEXITED(status));EXPECT_EQ(WEXITSTATUS(status),0);EXPECT_EQ(c.lifetime->active_callbacks(),0u);
}
}
#endif

#ifndef __EMSCRIPTEN__
#include <array>
#include <cstdio>
#include <cstdlib>
#include <lattice/native_retirement_storage.hpp>
namespace {
// A real supported scheduler with explicit manual execution. Pacer enqueue and
// caller-side pumping may overlap; callback execution/destruction is off leaf.
struct publication_scheduler final:lattice::scheduler {
    std::mutex mutex;std::deque<std::function<void()>> pending;
    void invoke(std::function<void()>&& work)override {
        std::lock_guard<std::mutex> leaf(mutex);pending.push_back(std::move(work));
    }
    bool is_on_thread()const noexcept override{return true;}
    bool is_same_as(const lattice::scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
    void shutdown()override {
        std::deque<std::function<void()>> discarded;
        {std::lock_guard<std::mutex> leaf(mutex);discarded.swap(pending);}
    }
    void run() {
        std::deque<std::function<void()>> work;
        {std::lock_guard<std::mutex> leaf(mutex);work.swap(pending);}
        for(auto& callback:work)callback();
    }
};
struct publication_owner final:lattice::synchronizer {
    using synchronizer::synchronizer;
    on_error_handler selected_error(){return copy_error_handler();}
    on_sync_complete_handler selected_completion(){return copy_completion_handler();}
    void connected_for_replay(){is_connected_.store(true);}
    std::shared_ptr<lattice::detail::sync_callback_lifetime> actual_lifetime(){return callback_lifetime_;}
    void dispatched_error() {
        const auto creator=lattice::detail::database_native_process();
        lattice::detail::native_retirement_storage<std::shared_ptr<lattice::detail::sync_callback_lifetime>,
            std::exception_ptr,std::function<void()>> held;
        if(!held.in_creator_process()||creator!=lattice::detail::database_native_process())return;
        held.get<0>()=callback_lifetime_;held.get<1>()=std::make_exception_ptr(std::runtime_error("publication-control"));
        const auto generation=held.get<0>()->dispatch_generation();
        held.get<2>()=[this,error=held.get<1>()]{report_dispatched_error(error,"publication-control");};
        if(!held.in_creator_process()||creator!=lattice::detail::database_native_process())return;
        held.get<0>()->queued(generation,held.get<2>());
    }
};
struct publication_context {
    std::shared_ptr<publication_scheduler> queue=std::make_shared<publication_scheduler>();
    std::unique_ptr<publication_owner> owner;
    publication_context() {
        lattice::configuration cfg(":memory:");cfg.sched=queue;cfg.audit_retention_seconds=0;
        lattice::sync_config sync;sync.sync_id="handler-publication-controls";
        sync.upload_coalesce_ms=0;sync.checkpoint_passive_interval_ms=0;
        owner=std::make_unique<publication_owner>(std::make_unique<lattice::lattice_db>(cfg),sync,std::make_unique<transport>());
    }
    ~publication_context(){owner.reset();queue->shutdown();}
};
struct publication_threads {
    std::array<std::thread,2> workers;
    std::array<std::atomic<bool>,2> done{};
    std::atomic<bool> release{false};
    std::array<std::exception_ptr,2> failures;
    template<class Work> void start(size_t index,Work work) {
        workers[index]=std::thread([this,index,work=std::move(work)] {
            try{work();}catch(...){failures[index]=std::current_exception();}
            done[index].store(true,std::memory_order_release);
        });
    }
    bool await(const std::atomic<bool>& value,std::chrono::milliseconds budget=std::chrono::milliseconds(500))const {
        const auto end=std::chrono::steady_clock::now()+budget;
        while(!value.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<end)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return value.load(std::memory_order_acquire);
    }
    void end()noexcept {
        release.store(true,std::memory_order_release);
        const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(3);
        for(size_t i=0;i<workers.size();++i)if(workers[i].joinable()) {
            while(!done[i].load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<end)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            if(!done[i].load(std::memory_order_acquire)) {
                std::fprintf(stderr,"handler-publication worker-ending-debt worker=%zu retainedNativeOwner=1\n",i);
                std::fflush(stderr);std::_Exit(95);
            }
            try{workers[i].join();}catch(...) {
                std::fprintf(stderr,"handler-publication join-ending-debt worker=%zu retainedNativeOwner=1\n",i);
                std::fflush(stderr);std::_Exit(95);
            }
        }
    }
    ~publication_threads(){end();}
};
struct blocked_publication_copy {
    struct state {
        std::atomic<bool> armed{false},entered{false},expired{false};
        std::atomic<int> calls{0};publication_threads* threads=nullptr;
    };
    std::shared_ptr<state> value;std::shared_ptr<int> marker;
    blocked_publication_copy(std::shared_ptr<state> v,std::shared_ptr<int> m):value(std::move(v)),marker(std::move(m)){}
    blocked_publication_copy(const blocked_publication_copy& other):value(other.value),marker(other.marker) {
        if(value->armed.load(std::memory_order_acquire)) {
            value->entered.store(true,std::memory_order_release);
            if(!value->threads->await(value->threads->release,std::chrono::seconds(2)))value->expired.store(true);
        }
    }
    void operator()(const std::string&)const{value->calls.fetch_add(1);}
};
struct counted_publication_handler {
    struct state {std::atomic<int> copies{0},calls{0};std::function<void()> action;};
    std::shared_ptr<state> value;
    explicit counted_publication_handler(std::shared_ptr<state> v):value(std::move(v)){}
    counted_publication_handler(const counted_publication_handler& other):value(other.value){value->copies.fetch_add(1);}
    void operator()(const std::string&)const{value->calls.fetch_add(1);if(value->action)value->action();}
};
}

TEST(SyncHandlerPublication, ReplacementDoesNotWaitForOrRetireTheSelectedOpaqueCopy) {
    auto state=std::make_shared<blocked_publication_copy::state>();
    std::atomic<int> replacement_calls{0};std::weak_ptr<int> old_capture;
    publication_context c;publication_threads threads;state->threads=&threads;
    {auto marker=std::make_shared<int>(7);old_capture=marker;
        c.owner->set_on_error(blocked_publication_copy(state,std::move(marker)));}
    state->armed.store(true,std::memory_order_release);
    threads.start(0,[&]{const auto error=c.owner->selected_error();if(error)error("selected");});
    const bool actual_copy_entered=threads.await(state->entered);
    threads.start(1,[&]{c.owner->set_on_error([&](const std::string&){++replacement_calls;});});
    const bool replacement_finished_while_copy_held=threads.await(threads.done[1]);
    const bool selected_capture_retained=!old_capture.expired();
    threads.end();
    ASSERT_FALSE(threads.failures[0]);ASSERT_FALSE(threads.failures[1]);
    EXPECT_TRUE(actual_copy_entered);EXPECT_TRUE(replacement_finished_while_copy_held);
    EXPECT_TRUE(selected_capture_retained);EXPECT_FALSE(state->expired.load());
    EXPECT_EQ(state->calls.load(),1);EXPECT_TRUE(old_capture.expired());
    const auto current=c.owner->selected_error();ASSERT_TRUE(current);current("replacement");
    EXPECT_EQ(replacement_calls.load(),1);
}

TEST(SyncHandlerPublication, DifferentHandlerSettersRetainBothFinalPublications) {
    std::atomic<int> error_value{0},completion_value{0};
    publication_context c;publication_threads threads;
    threads.start(0,[&]{for(int i=1;i<=32;++i)c.owner->set_on_error([&,i](const std::string&){error_value.store(i);});});
    threads.start(1,[&]{for(int i=1;i<=32;++i)c.owner->set_on_sync_complete([&,i](const std::vector<std::string>&){completion_value.store(i);});});
    threads.end();ASSERT_FALSE(threads.failures[0]);ASSERT_FALSE(threads.failures[1]);
    const auto error=c.owner->selected_error();const auto completed=c.owner->selected_completion();
    ASSERT_TRUE(error);ASSERT_TRUE(completed);error("final");completed({"final"});
    EXPECT_EQ(error_value.load(),32);EXPECT_EQ(completion_value.load(),32);
}

TEST(SyncHandlerPublication, ConnectedStateReplaySelectsAtExecutionAndResetIsSilent) {
    std::atomic<int> first{0},second{0};publication_context c;c.owner->connected_for_replay();
    c.owner->set_on_state_change([&](bool value){if(value)++first;});
    c.owner->set_on_state_change({});EXPECT_NO_THROW(c.queue->run());EXPECT_EQ(first.load(),0);
    c.owner->set_on_state_change([&](bool value){if(value)++first;});
    c.owner->set_on_state_change([&](bool value){if(value)++second;});
    EXPECT_NO_THROW(c.queue->run());EXPECT_EQ(first.load(),0);EXPECT_EQ(second.load(),2);
}

TEST(SyncHandlerPublication, DirectErrorKeepsOneOpaqueCopyThroughReplacementAndOwnerDeletion) {
    auto state=std::make_shared<counted_publication_handler::state>();publication_context c;
    state->action=[&]{c.owner->set_on_error([](const std::string&){});c.owner.reset();};
    c.owner->set_on_error(counted_publication_handler(state));state->copies.store(0);
    EXPECT_NO_THROW(c.owner->dispatched_error());
    EXPECT_EQ(state->copies.load(),1);EXPECT_EQ(state->calls.load(),1);EXPECT_FALSE(c.owner);
    EXPECT_NO_THROW(c.queue->run());
}

TEST(SyncHandlerPublication, DisplacedCaptureCanReenterItsSetterBeforeTheCallerReturns) {
    struct action {std::function<void()> run;~action(){if(run)run();}};
    std::atomic<int> nested{0},outer{0};publication_context c;publication_threads threads;
    {auto held=std::make_shared<action>();held->run=[&]{c.owner->set_on_error([&](const std::string&){++nested;});};
        c.owner->set_on_error([held](const std::string&){});}
    threads.start(0,[&]{c.owner->set_on_error([&](const std::string&){++outer;});});
    const bool returned=threads.await(threads.done[0]);threads.end();ASSERT_FALSE(threads.failures[0]);
    EXPECT_TRUE(returned);const auto current=c.owner->selected_error();ASSERT_TRUE(current);current("reentered");
    EXPECT_EQ(nested.load(),1);EXPECT_EQ(outer.load(),0);
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(SyncHandlerPublication, PublishedErrorForkReturnAbandonsBeforeInheritedLifetimeLeaf) {
    publication_context c;callback_fork_hold hold;const auto parent=::getpid();pid_t child=-1;
    const auto life=c.owner->actual_lifetime();bool setup=false;int fork_error=0;
    std::exception_ptr failure;std::chrono::steady_clock::time_point issued;
    c.owner->set_on_error([&](const std::string&){
        hold.worker=std::thread([&]{hold.held.store(lattice::detail::sync_callback_lifetime_test_access::hold(
            *life,hold.acquired,hold.release,hold.expired),std::memory_order_release);});
        const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(1);
        while(!hold.acquired.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<end)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        setup=hold.acquired.load(std::memory_order_acquire)&&!hold.expired.load(std::memory_order_acquire);
        if(setup){issued=std::chrono::steady_clock::now();errno=0;child=::fork();fork_error=child<0?errno:0;
            if(child==0)return;}
        hold.release.store(true,std::memory_order_release);
    });
    try{c.owner->dispatched_error();}catch(...){if(::getpid()!=parent)::_exit(2);failure=std::current_exception();}
    if(::getpid()!=parent)::_exit(0); // Before every inherited Native/thread owner.
    hold.end();
    int status=0,wait_error=0,kill_result=0,kill_error=0;bool reaped=child<=0,unknown=false,timed_out=false;
    const auto work_end=issued+std::chrono::seconds(2),ending_end=work_end+std::chrono::seconds(1);
    while(child>0&&std::chrono::steady_clock::now()<work_end){errno=0;const auto rc=::waitpid(child,&status,WNOHANG);
        timed_out=timed_out||std::chrono::steady_clock::now()>=work_end;
        if(rc==child){reaped=true;break;}if(rc<0&&errno!=EINTR){wait_error=errno;unknown=true;break;}
        std::this_thread::sleep_for(std::chrono::milliseconds(1));}
    if(child>0&&!reaped&&!unknown){timed_out=true;errno=0;kill_result=::kill(child,SIGKILL);kill_error=kill_result<0?errno:0;
        while(std::chrono::steady_clock::now()<ending_end){errno=0;const auto rc=::waitpid(child,&status,WNOHANG);
            if(rc==child){reaped=true;break;}if(rc<0&&errno!=EINTR){wait_error=errno;unknown=true;break;}
            std::this_thread::sleep_for(std::chrono::milliseconds(1));}}
    if(!reaped||unknown){std::fprintf(stderr,"handler-publication child-ending-debt child=%ld group=%ld waitError=%d killResult=%d killError=%d retainedNativeOwner=1\n",
        long(child),long(::getpgrp()),wait_error,kill_result,kill_error);std::fflush(stderr);::_exit(95);}
    ASSERT_FALSE(failure);ASSERT_TRUE(setup);EXPECT_TRUE(hold.held.load());EXPECT_FALSE(hold.expired.load());
    ASSERT_GT(child,0)<<"forkError="<<fork_error;EXPECT_FALSE(timed_out);EXPECT_EQ(wait_error,0);
    EXPECT_EQ(kill_result,0);EXPECT_EQ(kill_error,0);ASSERT_TRUE(WIFEXITED(status));EXPECT_EQ(WEXITSTATUS(status),0);
    EXPECT_EQ(life->active_callbacks(),0u);
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
struct second_what_fork_state {
    const pid_t parent=::getpid();
    std::atomic<unsigned> calls{0};
    pid_t child=-1;int fork_error=0;
    std::chrono::steady_clock::time_point issued;
};
struct second_what_fork final:std::exception {
    second_what_fork_state* state;
    explicit second_what_fork(second_what_fork_state& value):state(&value){}
    const char* what()const noexcept override {
        const auto ordinal=state->calls.fetch_add(1)+1;
        if(ordinal==2&&::getpid()==state->parent) {
            state->issued=std::chrono::steady_clock::now();errno=0;
            state->child=::fork();state->fork_error=state->child<0?errno:0;
        }
        return "publication second what control";
    }
};
struct reject_replay_scheduler final:lattice::scheduler {
    const std::thread::id caller=std::this_thread::get_id();
    second_what_fork_state* const failure;
    std::atomic<bool> armed{false};
    std::shared_ptr<publication_scheduler> queue=std::make_shared<publication_scheduler>();
    explicit reject_replay_scheduler(second_what_fork_state& value):failure(&value){}
    void invoke(std::function<void()>&& work)override {
        if(std::this_thread::get_id()==caller&&armed.exchange(false))throw second_what_fork(*failure);
        queue->invoke(std::move(work));
    }
    bool is_on_thread()const noexcept override{return true;}
    bool is_same_as(const lattice::scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
    void shutdown()override{queue->shutdown();}
};
struct replay_publication_owner final:lattice::synchronizer {
    using synchronizer::synchronizer;
    void actual_replay_message() {
        on_transport_message(lattice::transport_message::from_string(
            lattice::server_sent_event::make_replay_request().to_json()));
    }
};
struct parent_only_handler_copy {
    const pid_t creator;
    std::atomic<unsigned>* const calls;
    parent_only_handler_copy(pid_t process,std::atomic<unsigned>& value):creator(process),calls(&value){}
    parent_only_handler_copy(const parent_only_handler_copy& prior):creator(prior.creator),calls(prior.calls) {
        if(::getpid()!=creator)::_exit(73); // Actual opaque copy in the child is the failure witness.
    }
    void operator()(const std::string&)const{calls->fetch_add(1);}
};
}
TEST(SyncHandlerPublication, SecondTransportExceptionWhatCannotCopyTheInheritedSelectedHandler) {
    second_what_fork_state state;std::atomic<unsigned> callbacks{0};
    const auto old_log=lattice::get_log_level();
    struct restore_log {lattice::log_level prior;~restore_log(){lattice::set_log_level(prior);}} restore{old_log};
    lattice::set_log_level(lattice::log_level::error); // The actual first LOG_ERROR what() must run.
    const auto queue=std::make_shared<reject_replay_scheduler>(state);
    lattice::configuration cfg(":memory:");cfg.sched=queue;cfg.audit_retention_seconds=0;
    lattice::sync_config sync;sync.sync_id="handler-second-what-control";
    sync.upload_coalesce_ms=0;sync.checkpoint_passive_interval_ms=0;
    auto owner=std::make_unique<replay_publication_owner>(std::make_unique<lattice::lattice_db>(cfg),sync,std::make_unique<transport>());
    owner->set_on_error(parent_only_handler_copy(state.parent,callbacks));
    queue->armed.store(true);std::exception_ptr failure;
    try{owner->actual_replay_message();}catch(...){if(::getpid()!=state.parent)::_exit(2);failure=std::current_exception();}
    if(::getpid()!=state.parent)::_exit(0); // Before all inherited Native/thread/configuration owners.
    int status=0,wait_error=0,kill_result=0,kill_error=0;
    bool reaped=state.child<=0,unknown=false,timed_out=false;
    const auto work_end=state.issued+std::chrono::seconds(2),ending_end=work_end+std::chrono::seconds(1);
    while(state.child>0&&std::chrono::steady_clock::now()<work_end){errno=0;const auto rc=::waitpid(state.child,&status,WNOHANG);
        timed_out=timed_out||std::chrono::steady_clock::now()>=work_end;
        if(rc==state.child){reaped=true;break;}if(rc<0&&errno!=EINTR){wait_error=errno;unknown=true;break;}
        std::this_thread::sleep_for(std::chrono::milliseconds(1));}
    if(state.child>0&&!reaped&&!unknown){timed_out=true;errno=0;kill_result=::kill(state.child,SIGKILL);kill_error=kill_result<0?errno:0;
        while(std::chrono::steady_clock::now()<ending_end){errno=0;const auto rc=::waitpid(state.child,&status,WNOHANG);
            if(rc==state.child){reaped=true;break;}if(rc<0&&errno!=EINTR){wait_error=errno;unknown=true;break;}
            std::this_thread::sleep_for(std::chrono::milliseconds(1));}}
    if(!reaped||unknown){std::fprintf(stderr,"handler-second-what child-ending-debt child=%ld parent=%ld group=%ld store=:memory: waitError=%d killResult=%d killError=%d\n",
        long(state.child),long(state.parent),long(::getpgrp()),wait_error,kill_result,kill_error);std::fflush(stderr);::_exit(95);}
    ASSERT_FALSE(failure);ASSERT_EQ(state.calls.load(),2u);ASSERT_GT(state.child,0)<<"forkError="<<state.fork_error;
    EXPECT_FALSE(timed_out);EXPECT_EQ(wait_error,0);EXPECT_EQ(kill_result,0);EXPECT_EQ(kill_error,0);
    ASSERT_TRUE(WIFEXITED(status));EXPECT_EQ(WEXITSTATUS(status),0);
    EXPECT_NO_THROW(queue->queue->run());EXPECT_EQ(callbacks.load(),1u);
    owner.reset();queue->shutdown();
}
#endif
