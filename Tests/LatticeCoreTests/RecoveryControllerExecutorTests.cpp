#include "TestHelpers.hpp"
#include "RecoveryControllerExecutorTestAccess.hpp"
#include "../../Sources/LatticeCore/src/controller_store_ending.hpp"
#include <atomic>
#include <chrono>
#include <cstdio>
#include <future>
#include <system_error>
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <cerrno>
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
namespace lattice::detail {
struct controller_store_ending_test_access {
    static void bind(controller_worker_original& original,recovery_controller_executor& executor){original.bind(executor.retain_ending());}
    static void failure(controller_worker_original& original,std::exception_ptr error){original.failed_constructor(std::move(error));}
};
}
namespace {
using Access=lattice::detail::recovery_controller_executor_test_access;
using Error=lattice::detail::controller_executor_error;
using Executor=lattice::detail::recovery_controller_executor;
using namespace std::chrono_literals;
[[noreturn]] void executor_debt(const char* stage)noexcept{
    std::fprintf(stderr,"controller-lane ending-debt stage=%s process=%ld group=%ld\n",stage,long(::getpid()),long(::getpgrp()));std::fflush(stderr);::_exit(95);
}
struct Gate {
    std::mutex mutex;std::condition_variable changed;bool entered=false,released=false,timed_out=false;
    void wait(){std::unique_lock lock(mutex);entered=true;changed.notify_all();if(!changed.wait_for(lock,2s,[&]{return released;}))timed_out=true;}
    bool ready(){std::unique_lock lock(mutex);return changed.wait_for(lock,1s,[&]{return entered;});}
    void release(){std::lock_guard lock(mutex);released=true;changed.notify_all();}
    bool expired(){std::lock_guard lock(mutex);return timed_out;}
};
struct Harness {
    std::shared_ptr<lattice::detail::sync_retirement_lane> lane;
    std::shared_ptr<Executor> executor;
    Access::ticket observed;
    explicit Harness(size_t capacity=64):lane(Access::lane(capacity)),executor(Access::create(lane)),observed(Access::observe(*executor)){}
    std::vector<std::shared_ptr<Gate>> gates;
    std::shared_ptr<Gate> gate(){auto value=std::make_shared<Gate>();gates.push_back(value);return value;}
    ~Harness(){for(auto& gate:gates)gate->release();if(executor)executor->retire();
        if(!Access::joined(observed,3s))executor_debt("actual-worker-join");
        if(!Access::free(lane,1s))executor_debt("actual-reservation-settlement");
        executor.reset();observed.reset();lane.reset();}
};
std::atomic<unsigned> launches{0};
std::thread counting_launch(std::function<void()> work){++launches;return std::thread(std::move(work));}
std::thread failing_launch(std::function<void()>){++launches;throw std::system_error(std::make_error_code(std::errc::resource_unavailable_try_again));}
TEST(RecoveryControllerExecutor, ActualSixtyFourSlotCeilingRefusesBeforeWorkerLaunch){
    auto lane=Access::lane();std::vector<lattice::detail::sync_retirement_lane::reservation> held;held.reserve(64);
    for(unsigned i=0;i<64;++i)held.push_back(lane->reserve_empty());ASSERT_EQ(Access::used(lane),64u);launches=0;
    try{auto refused=Access::create(lane,counting_launch);ADD_FAILURE()<<"excess controller was admitted";refused->retire();}
    catch(const Error& error){EXPECT_EQ(error.kind,Error::reason::retirement_admission);EXPECT_TRUE(error.first);}
    EXPECT_EQ(launches.load(),0u);held.clear();ASSERT_TRUE(Access::free(lane,1s));
}
TEST(RecoveryControllerExecutor, InjectedPreLaunchFailureRefundsActualUnstartedReservation){
    auto lane=Access::lane(1);launches=0;
    try{auto refused=Access::create(lane,failing_launch);ADD_FAILURE()<<"injected launch failure was not retained";refused->retire();}
    catch(const Error& error){EXPECT_EQ(error.kind,Error::reason::launch);ASSERT_TRUE(error.first);
        try{std::rethrow_exception(error.first);}catch(const std::system_error& actual){EXPECT_EQ(actual.code(),std::make_error_code(std::errc::resource_unavailable_try_again));}}
    EXPECT_EQ(launches.load(),1u);ASSERT_TRUE(Access::free(lane,1s));
    auto accepted=Access::create(lane);const auto ending=Access::observe(*accepted);accepted->retire();
    if(!Access::joined(ending,3s)||!Access::free(lane,1s))executor_debt("post-launch-failure-ending");
}
TEST(RecoveryControllerExecutor, ActivePendingAndTimedCellsAreBoundedAndActualCloseKeepsSlotUntilJoin){
    Harness h(1);const auto gate=h.gate();h.executor->invoke([gate]{gate->wait();});ASSERT_TRUE(gate->ready());
    struct Counts{std::atomic<unsigned> pending{0},timer{0},displaced{0};};const auto counts=std::make_shared<Counts>();
    struct Capture{std::shared_ptr<Counts> counts;explicit Capture(std::shared_ptr<Counts> actual):counts(std::move(actual)){}~Capture(){++counts->displaced;}};
    auto capture=std::make_shared<Capture>(counts);
    h.executor->invoke([capture,counts]{++counts->pending;});capture.reset();
    EXPECT_THROW(h.executor->invoke([]{}),Error);
    h.executor->retry_at(std::chrono::steady_clock::now()+1s,[counts]{++counts->timer;});
    const auto before=Access::snapshot(h.observed);EXPECT_TRUE(before.active&&before.pending&&before.timed);
    h.executor->retire();const auto transferred=Access::snapshot(h.observed);
    EXPECT_TRUE(transferred.stopping&&transferred.transferred);EXPECT_FALSE(transferred.joined);EXPECT_EQ(Access::used(h.lane),1u);
    EXPECT_EQ(counts->displaced.load(),1u);EXPECT_EQ(counts->pending.load(),0u);EXPECT_EQ(counts->timer.load(),0u);
    EXPECT_THROW(Access::create(h.lane),Error);
    EXPECT_THROW(h.executor->invoke([]{}),Error);gate->release();ASSERT_TRUE(Access::joined(h.observed,3s));EXPECT_FALSE(gate->expired());
}
TEST(RecoveryControllerExecutor, OwnWorkerFinalReleaseTransfersRealThreadAndJoinsWithoutSelfDetach){
    Harness h;const auto gate=h.gate();auto captured=h.executor;
    h.executor->invoke([captured,gate]{gate->wait();});ASSERT_TRUE(gate->ready());captured.reset();h.executor.reset();
    EXPECT_FALSE(Access::snapshot(h.observed).transferred);gate->release();
    ASSERT_TRUE(Access::joined(h.observed,3s));const auto ended=Access::snapshot(h.observed);
    EXPECT_TRUE(ended.transferred&&ended.loop_ended&&ended.joined);EXPECT_EQ(ended.retirement_error,0);EXPECT_FALSE(gate->expired());
}
TEST(RecoveryControllerExecutor, ActualWorkerFailureIsRetainedAndLaterTurnStillEnds){
    Harness h;const auto second=std::make_shared<std::promise<void>>();auto done=second->get_future();const auto gate=h.gate();
    h.executor->invoke([gate]{gate->wait();throw std::runtime_error("actual-controller-lane-failure");});ASSERT_TRUE(gate->ready());
    h.executor->invoke([second]{second->set_value();});gate->release();ASSERT_EQ(done.wait_for(2s),std::future_status::ready);
    const auto actual=Access::snapshot(h.observed);ASSERT_TRUE(actual.worker_error);
    try{std::rethrow_exception(actual.worker_error);}catch(const std::runtime_error& error){EXPECT_STREQ(error.what(),"actual-controller-lane-failure");}
    EXPECT_EQ(actual.turns,2u);EXPECT_FALSE(gate->expired());
}
TEST(RecoveryControllerExecutor, AbsoluteTimedWakeReplacesOneCellAndCancellationDoesNotPublishIt){
    Harness h;const auto gate=h.gate();h.executor->invoke([gate]{gate->wait();});ASSERT_TRUE(gate->ready());
    struct Counts{std::atomic<unsigned> old{0},actual{0};std::promise<void> fired;};const auto counts=std::make_shared<Counts>();auto done=counts->fired.get_future();
    const auto fixed_due=std::chrono::steady_clock::now()+50ms;
    h.executor->retry_at(fixed_due,[counts]{++counts->old;});h.executor->retry_at(fixed_due,[counts]{++counts->actual;counts->fired.set_value();});gate->release();
    ASSERT_EQ(done.wait_for(2s),std::future_status::ready);EXPECT_EQ(counts->old.load(),0u);EXPECT_EQ(counts->actual.load(),1u);
    h.executor->retry_at(std::chrono::steady_clock::now()+1s,[counts]{++counts->old;});h.executor->cancel_retry();
    const auto state=Access::snapshot(h.observed);EXPECT_FALSE(state.timed);EXPECT_EQ(state.timed_wakes,1u);EXPECT_FALSE(gate->expired());
}
TEST(RecoveryControllerExecutor, ForkChildAbandonsInheritedCapsuleAndRefusesSingletonBeforeAnyReaperLeaf){
    Harness h;const auto gate=h.gate();h.executor->invoke([gate]{gate->wait();});ASSERT_TRUE(gate->ready());
    const auto child=::fork();
    if(child==0){
        bool denied=false,singleton_denied=false;
        try{h.executor->invoke([]{});}catch(const Error& error){denied=error.kind==Error::reason::foreign_process;}catch(...){}
        h.executor.reset(); // creator fence abandons inherited resources before any leaf/capture destruction
        try{auto fresh=std::make_unique<Executor>();}catch(const Error& error){singleton_denied=error.kind==Error::reason::retirement_admission;}catch(...){}
        ::_exit(denied&&singleton_denied?0:1);
    }
    ASSERT_GT(child,0);int status=0;bool reaped=false,unknown=false,timed_out=false;int wait_error=0,kill_result=0,kill_error=0;
    const auto work_end=std::chrono::steady_clock::now()+2s,ending_end=work_end+1s;
    while(std::chrono::steady_clock::now()<work_end){const auto rc=::waitpid(child,&status,WNOHANG);if(rc==child){reaped=true;break;}if(rc<0&&errno!=EINTR){wait_error=errno;unknown=true;break;}std::this_thread::sleep_for(1ms);}
    if(!reaped&&!unknown){timed_out=true;errno=0;kill_result=::kill(child,SIGKILL);kill_error=kill_result<0?errno:0;
        while(std::chrono::steady_clock::now()<ending_end){const auto rc=::waitpid(child,&status,WNOHANG);if(rc==child){reaped=true;break;}if(rc<0&&errno!=EINTR){wait_error=errno;unknown=true;break;}std::this_thread::sleep_for(1ms);}}
    if(!reaped||unknown){std::fprintf(stderr,"controller-lane child-ending-debt child=%ld group=%ld waitError=%d killResult=%d killError=%d retainedParentLane=1\n",long(child),long(::getpgrp()),wait_error,kill_result,kill_error);std::fflush(stderr);::_exit(95);}
    gate->release();EXPECT_FALSE(timed_out);EXPECT_EQ(wait_error,0);EXPECT_EQ(kill_result,0);EXPECT_EQ(kill_error,0);ASSERT_TRUE(WIFEXITED(status));EXPECT_EQ(WEXITSTATUS(status),0);EXPECT_FALSE(gate->expired());
}

// Production ending DATA controls. Original seven tests/oracles above remain
// unchanged; these do not claim Core deletion, Native physical close, callback
// quiescence, or filesystem permission from a worker join alone.
TEST(ControllerStoreEnding, ActualIdleWorkerIsNotJoinedUntilRealReaperReturn){
    Harness h;auto ending=h.executor->retain_ending();
    using State=Executor::ending::disposition;
    EXPECT_EQ(ending.wait_until(std::chrono::steady_clock::now()),State::pending);
    h.executor->retire();
    EXPECT_EQ(ending.wait_until(std::chrono::steady_clock::now()+3s),State::joined);
    const auto actual=Access::snapshot(h.observed);
    EXPECT_TRUE(actual.transferred&&actual.loop_ended&&actual.joined);
    EXPECT_FALSE(actual.active||actual.pending||actual.timed);
}
TEST(ControllerStoreEnding, TransferDoesNotEndActualExecutingCapture){
    Harness h;auto ending=h.executor->retain_ending();const auto gate=h.gate();
    struct Counts{std::atomic<unsigned> ended{0};};auto counts=std::make_shared<Counts>();
    struct Capture{std::shared_ptr<Counts> counts;~Capture(){++counts->ended;}};
    auto capture=std::make_shared<Capture>();capture->counts=counts;
    h.executor->invoke([gate,capture]{gate->wait();});capture.reset();ASSERT_TRUE(gate->ready());
    h.executor->retire();
    EXPECT_EQ(ending.wait_until(std::chrono::steady_clock::now()),Executor::ending::disposition::pending);
    EXPECT_EQ(counts->ended.load(),0u);
    gate->release();
    ASSERT_EQ(ending.wait_until(std::chrono::steady_clock::now()+3s),Executor::ending::disposition::joined);
    EXPECT_EQ(counts->ended.load(),1u);EXPECT_FALSE(gate->expired());
}
TEST(ControllerStoreEnding, ActualCallingWorkerRefusesSelfWait){
    Harness h;auto ending=std::make_shared<Executor::ending>(h.executor->retain_ending());
    auto result=std::make_shared<std::promise<Executor::ending::disposition>>();auto done=result->get_future();
    h.executor->invoke([ending,result]{result->set_value(ending->wait_until(std::chrono::steady_clock::now()+3s));});
    ASSERT_EQ(done.wait_for(2s),std::future_status::ready);
    EXPECT_EQ(done.get(),Executor::ending::disposition::reentrant);
    h.executor->retire();
    EXPECT_EQ(ending->wait_until(std::chrono::steady_clock::now()+3s),Executor::ending::disposition::joined);
}
TEST(ControllerStoreEnding, ReservedButUnboundCandidateCannotProduceJoinedEvidence){
    lattice::detail::controller_worker_original original;
    auto pending=original.wait_until(std::chrono::steady_clock::now());
    EXPECT_EQ(pending.actual,Executor::ending::disposition::pending);EXPECT_FALSE(pending.constructor_failed);
    std::exception_ptr first;try{throw std::runtime_error("actual-constructor-failure");}catch(...){first=std::current_exception();}
    lattice::detail::controller_store_ending_test_access::failure(original,first);
    auto failed=original.wait_until(std::chrono::steady_clock::now());
    EXPECT_EQ(failed.actual,Executor::ending::disposition::unavailable);EXPECT_TRUE(failed.constructor_failed);
}
TEST(ControllerStoreEnding, BoundCandidateKeepsConstructorFailureDistinctFromActualJoin){
    Harness h;lattice::detail::controller_worker_original original;
    lattice::detail::controller_store_ending_test_access::bind(original,*h.executor);
    std::exception_ptr first;try{throw std::runtime_error("post-bind-constructor-failure");}catch(...){first=std::current_exception();}
    lattice::detail::controller_store_ending_test_access::failure(original,first);
    auto live=original.wait_until(std::chrono::steady_clock::now());
    EXPECT_EQ(live.actual,Executor::ending::disposition::pending);EXPECT_TRUE(live.constructor_failed);
    h.executor->retire();auto joined=original.wait_until(std::chrono::steady_clock::now()+3s);
    EXPECT_EQ(joined.actual,Executor::ending::disposition::joined);EXPECT_TRUE(joined.constructor_failed);
}
TEST(ControllerStoreEnding, ActualWorkerErrorCannotBeRelabeledCleanEnding){
    Harness h;auto ending=h.executor->retain_ending();auto gate=h.gate();
    h.executor->invoke([gate]{gate->wait();throw std::runtime_error("actual-ending-first-worker-failure");});
    ASSERT_TRUE(gate->ready());h.executor->retire();gate->release();
    ASSERT_TRUE(Access::joined(h.observed,3s));
    EXPECT_EQ(ending.wait_until(std::chrono::steady_clock::now()),Executor::ending::disposition::failed);
    const auto actual=Access::snapshot(h.observed);ASSERT_TRUE(actual.worker_error);
    try{std::rethrow_exception(actual.worker_error);}catch(const std::runtime_error& error){EXPECT_STREQ(error.what(),"actual-ending-first-worker-failure");}
}
TEST(ControllerStoreEnding, ForkChildCannotWaitOrReleaseParentEndingPayload){
    Harness h;auto gate=h.gate();h.executor->invoke([gate]{gate->wait();});ASSERT_TRUE(gate->ready());
    auto ending=std::make_unique<Executor::ending>(h.executor->retain_ending());
    auto original=std::make_unique<lattice::detail::controller_worker_original>();
    lattice::detail::controller_store_ending_test_access::bind(*original,*h.executor);
    const auto child=::fork();
    if(child==0){
        ::signal(SIGALRM,SIG_DFL);sigset_t unblocked;sigemptyset(&unblocked);sigaddset(&unblocked,SIGALRM);::sigprocmask(SIG_UNBLOCK,&unblocked,nullptr);::alarm(2);
        const bool denied=ending->wait_until(std::chrono::steady_clock::now())==Executor::ending::disposition::foreign;
        const bool original_denied=original->wait_until(std::chrono::steady_clock::now()).actual==Executor::ending::disposition::foreign;
        ending.reset();original.reset();::_exit(denied&&original_denied?0:1);
    }
    if(child<0)executor_debt("new-ending-fork-refused");
    int status=0;bool reaped=false,unknown=false;int wait_error=0;
    const auto end=std::chrono::steady_clock::now()+3s;
    while(std::chrono::steady_clock::now()<end){const auto rc=::waitpid(child,&status,WNOHANG);if(rc==child){reaped=true;break;}if(rc<0&&errno!=EINTR){unknown=true;wait_error=errno;break;}std::this_thread::sleep_for(1ms);}
    if(!reaped||unknown){std::fprintf(stderr,"controller-ending child-debt child=%ld waitError=%d retainedParentLane=1\n",long(child),wait_error);std::fflush(stderr);::_exit(95);}
    gate->release();ASSERT_TRUE(WIFEXITED(status));EXPECT_EQ(WEXITSTATUS(status),0);
    h.executor->retire();EXPECT_EQ(ending->wait_until(std::chrono::steady_clock::now()+3s),Executor::ending::disposition::joined);
    EXPECT_FALSE(gate->expired());
}
TEST(ControllerStoreEnding, RealJoinCannotOvertakeCancelledCaptureDestructionTail){
    Harness h;auto ending=h.executor->retain_ending();auto active=h.gate(),destroying=h.gate();
    h.executor->invoke([active]{active->wait();});ASSERT_TRUE(active->ready());
    struct Capture{std::shared_ptr<Gate> gate;~Capture(){gate->wait();}};
    auto capture=std::make_shared<Capture>();capture->gate=destroying;
    h.executor->invoke([capture]{});capture.reset();
    std::promise<void> completed; // outlives cleanup guard and its actual thread
    // Guard exists before thread publication; every assertion unwind releases
    // BOTH pauses before bounded actual retirement-thread ending/join.
    struct Retiring {
        std::shared_ptr<Gate> active,destroying;
        std::thread thread;std::future<void> returned;
        ~Retiring()noexcept{
            active->release();destroying->release();
            if(thread.joinable()){
                if(returned.wait_for(3s)!=std::future_status::ready)executor_debt("cancel-tail-retire-return");
                try{thread.join();}catch(...){executor_debt("cancel-tail-retire-join");}
            }
        }
    } retiring{active,destroying,{},{}};
    retiring.returned=completed.get_future();
    retiring.thread=std::thread([executor=h.executor,&completed]{executor->retire();completed.set_value();});
    ASSERT_TRUE(destroying->ready());active->release();
    ASSERT_TRUE(Access::joined(h.observed,3s)); // real raw reaper result, not a mock flag
    EXPECT_EQ(ending.wait_until(std::chrono::steady_clock::now()),Executor::ending::disposition::pending);
    EXPECT_EQ(retiring.returned.wait_for(0ms),std::future_status::timeout);
    destroying->release();
    if(retiring.returned.wait_for(3s)!=std::future_status::ready)executor_debt("cancel-tail-release-return");
    try{retiring.thread.join();}catch(...){executor_debt("cancel-tail-release-join");}
    EXPECT_EQ(ending.wait_until(std::chrono::steady_clock::now()+3s),Executor::ending::disposition::joined);
    EXPECT_FALSE(active->expired());EXPECT_FALSE(destroying->expired());
}
TEST(ControllerStoreEnding, JoinedUnreportedConstructorKeepsItsOriginalUntilOutcomePublication){
    Harness h;lattice::detail::controller_worker_original original;
    lattice::detail::controller_store_ending_test_access::bind(original,*h.executor);
    h.executor->retire();ASSERT_TRUE(Access::joined(h.observed,3s));
    const auto unreported=original.wait_until(std::chrono::steady_clock::now());
    EXPECT_EQ(unreported.actual,Executor::ending::disposition::joined);
    EXPECT_FALSE(unreported.constructor_complete);EXPECT_FALSE(unreported.constructor_failed);
    std::exception_ptr first;try{throw std::runtime_error("actual-unreported-constructor-failure");}catch(...){first=std::current_exception();}
    lattice::detail::controller_store_ending_test_access::failure(original,first);
    const auto reported=original.wait_until(std::chrono::steady_clock::now());
    EXPECT_EQ(reported.actual,Executor::ending::disposition::joined);
    EXPECT_TRUE(reported.constructor_complete);EXPECT_TRUE(reported.constructor_failed);
}
}
#endif
