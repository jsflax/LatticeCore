#include "TestHelpers.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/sync_discovery_deferral.hpp"
#include "../../Sources/LatticeCore/src/sync_callback_lifetime.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include <deque>
#include <future>

namespace {
using namespace lattice;
using namespace std::chrono_literals;
using discovery_queue=detail::sync_discovery_deferral;
std::shared_ptr<discovery_queue::operation> admission_unit(uint64_t generation=1) {
    auto unit=std::make_shared<discovery_queue::operation>();
    unit->type=discovery_queue::kind::intake;unit->label="admission fixture";
    unit->generation=generation;unit->charge=1024;unit->step=[](auto&){return true;};
    return unit;
}
}

TEST(SyncDiscoveryAdmission, RejectedUnbegunTicketRetainsPayloadAndFencesOnce) {
    discovery_queue queue;auto work=admission_unit();queue.push(work);
    const auto ticket=queue.dispatch(discovery_queue::clock::now());ASSERT_TRUE(ticket);
    EXPECT_TRUE(queue.reject_unbegun(ticket));EXPECT_FALSE(queue.reject_unbegun(ticket));
    EXPECT_TRUE(queue.failed(1));EXPECT_TRUE(queue.pending(1));EXPECT_EQ(work.use_count(),2);
    EXPECT_LE(queue.wake_at(),discovery_queue::clock::now());
    EXPECT_TRUE(queue.take_failure(1));EXPECT_FALSE(queue.take_failure(1));
    EXPECT_EQ(queue.wake_at(),discovery_queue::clock::time_point::max());
    EXPECT_EQ(queue.begin(ticket,discovery_queue::clock::now()),nullptr);
    EXPECT_FALSE(queue.dispatch(discovery_queue::clock::now()+10s));
    queue.cancel(3);EXPECT_EQ(work.use_count(),1);EXPECT_FALSE(queue.failed(3));
}
TEST(SyncDiscoveryAdmission, BegunAndCompletedTicketsCannotRejectAnotherReservation) {
    discovery_queue queue;auto first=admission_unit(),second=admission_unit();
    queue.push(first);queue.push(second);const auto now=discovery_queue::clock::now();
    const auto old=queue.dispatch(now);ASSERT_EQ(queue.begin(old,now),first);
    EXPECT_FALSE(queue.reject_unbegun(old));queue.finish(old,first,true,now);
    const auto next=queue.dispatch(now);ASSERT_TRUE(next);EXPECT_NE(next.serial,old.serial);
    EXPECT_FALSE(queue.reject_unbegun(old));EXPECT_FALSE(queue.failed(1));
    ASSERT_EQ(queue.begin(next,now),second);queue.finish(next,second,true,now);
    EXPECT_FALSE(queue.pending(1));
}
TEST(SyncDiscoveryAdmission, CancelledTicketCannotRejectReplacementGeneration) {
    discovery_queue queue;queue.push(admission_unit());const auto old=queue.dispatch(discovery_queue::clock::now());
    queue.cancel(3);auto replacement=admission_unit(3);queue.push(replacement);
    const auto now=discovery_queue::clock::now();const auto next=queue.dispatch(now);
    EXPECT_FALSE(queue.reject_unbegun(old));EXPECT_FALSE(queue.failed(3));
    ASSERT_EQ(queue.begin(next,now),replacement);queue.finish(next,replacement,true,now);
    EXPECT_FALSE(queue.pending(3));
}


TEST(SyncDiscoveryAdmission, PublishedIdleHeadAlreadyBelongsToItsSubmittingTurn) {
    discovery_queue queue;auto first=admission_unit();const auto now=discovery_queue::clock::now();
    const auto admitted=queue.push_and_dispatch(first,now);
    ASSERT_EQ(admitted.state,discovery_queue::admission::accepted);ASSERT_TRUE(admitted.reserved);
    // Force the rival timer to probe after publication but before any scheduler
    // callback begins. No timing assumption or sleep decides this interleaving.
    EXPECT_FALSE(queue.dispatch(now+1s));EXPECT_TRUE(queue.pending(1));
    ASSERT_EQ(queue.begin(admitted.reserved,now),first);
    EXPECT_FALSE(queue.finish_and_continue(admitted.reserved,first,true,now,true));
    EXPECT_FALSE(queue.pending(1));EXPECT_FALSE(queue.failed(1));
}
TEST(SyncDiscoveryAdmission, SameDispatchOwnsFourFifoTurnsThenYieldsWithoutLoss) {
    discovery_queue queue;std::vector<std::shared_ptr<discovery_queue::operation>> work;
    const auto now=discovery_queue::clock::now();work.push_back(admission_unit());
    auto ticket=queue.push_and_dispatch(work[0],now).reserved;ASSERT_TRUE(ticket);
    for(unsigned i=1;i<discovery_queue::turn_limit+1;++i) {
        work.push_back(admission_unit());const auto added=queue.push_and_dispatch(work.back(),now);
        EXPECT_EQ(added.state,discovery_queue::admission::accepted);EXPECT_FALSE(added.reserved);
    }
    const auto original=ticket;
    for(unsigned turn=0;turn<discovery_queue::turn_limit;++turn) {
        ASSERT_EQ(queue.begin(ticket,now),work[turn]);
        ticket=queue.finish_and_continue(ticket,work[turn],true,now,turn+1<discovery_queue::turn_limit);
        if(turn+1<discovery_queue::turn_limit) {
            ASSERT_TRUE(ticket);EXPECT_EQ(ticket.serial,original.serial);
            EXPECT_FALSE(queue.dispatch(now+1s)); // Rival cannot steal continuation.
        } else EXPECT_FALSE(ticket);
    }
    EXPECT_TRUE(queue.pending(1));const auto next=queue.dispatch(now);ASSERT_TRUE(next);
    EXPECT_NE(next.serial,original.serial);EXPECT_FALSE(queue.reject_unbegun(original));
    ASSERT_EQ(queue.begin(next,now),work.back());queue.finish(next,work.back(),true,now);
    EXPECT_FALSE(queue.pending(1));
}
TEST(SyncDiscoveryAdmission, BusyReleasesSubmittingTicketToTheBoundedTimerRetry) {
    discovery_queue queue;auto work=admission_unit();const auto now=discovery_queue::clock::now();
    const auto ticket=queue.push_and_dispatch(work,now).reserved;ASSERT_TRUE(ticket);
    ASSERT_EQ(queue.begin(ticket,now),work);
    EXPECT_FALSE(queue.finish_and_continue(ticket,work,false,now,true));
    EXPECT_EQ(work->attempts,1u);EXPECT_EQ(work->deadline,now+5s);
    EXPECT_FALSE(queue.dispatch(now+4ms));const auto retry=queue.dispatch(now+5ms);ASSERT_TRUE(retry);
    EXPECT_NE(retry.serial,ticket.serial);EXPECT_FALSE(queue.reject_unbegun(ticket));
    ASSERT_EQ(queue.begin(retry,now+5ms),work);queue.finish(retry,work,true,now+5ms);
    EXPECT_FALSE(queue.pending(1));
}
TEST(SyncDiscoveryAdmission, ReservedContinuationRejectionAndCancellationKeepExactPayloads) {
    discovery_queue queue;auto first=admission_unit(),later=admission_unit();const auto now=discovery_queue::clock::now();
    const auto ticket=queue.push_and_dispatch(first,now).reserved;queue.push(later);
    ASSERT_EQ(queue.begin(ticket,now),first);
    const auto next=queue.finish_and_continue(ticket,first,true,now,true);ASSERT_TRUE(next);
    EXPECT_TRUE(queue.reject_unbegun(next));EXPECT_EQ(first.use_count(),1);EXPECT_EQ(later.use_count(),2);
    EXPECT_TRUE(queue.failed(1));EXPECT_TRUE(queue.take_failure(1));EXPECT_FALSE(queue.take_failure(1));
    queue.cancel(3);EXPECT_EQ(later.use_count(),1);EXPECT_FALSE(queue.begin(next,now));
    const auto replacement=queue.push_and_dispatch(admission_unit(3),now);ASSERT_TRUE(replacement.reserved);
    EXPECT_FALSE(queue.reject_unbegun(next));EXPECT_FALSE(queue.failed(3));
}

#ifndef __EMSCRIPTEN__
namespace lattice {
struct sync_discovery_admission_test_access {
    static auto owner(synchronizer_base& sync){return sync.owned_db_;}
    static auto queue(synchronizer_base& sync){return sync.discovery_deferral_;}
    static uint64_t generation(synchronizer_base& sync){return sync.reconnect_lifecycle_.load();}
    static uint64_t policy(synchronizer_base& sync){return sync.upload_policy_revision_.load();}
    static void enqueue(synchronizer_base& sync,std::function<bool(detail::sync_discovery_operation&)> step) {
        sync.enqueue_discovery(detail::sync_discovery_kind::intake,"admission fixture",1024,std::move(step));
    }
    static void background(synchronizer_base& sync){sync.background_upload();}
    static void pump(synchronizer_base& sync){sync.pump_discovery();}
    static void stop_pacer(synchronizer_base& sync){sync.stop_pacer();}
    static void connected(synchronizer_base& sync) {
        sync.is_connected_=true;sync.recovery_export_route_->publish(generation(sync),true);
    }
};
}
namespace {
using access=sync_discovery_admission_test_access;
class admission_scheduler final:public scheduler {
public:
    enum class action {queue,throw_before,retain_then_throw,inline_then_throw};
private:
    std::mutex mutex_;std::condition_variable ready_;
    std::deque<std::function<void()>> pending_;
    action next_=action::queue;bool stopped_=false;
    static thread_local bool dropping_;
public:
    std::atomic<int> recursive_drop_invocations{0};
    void set_next(action next){std::lock_guard<std::mutex> lock(mutex_);next_=next;}
    void invoke(std::function<void()>&& fn)override {
        // An unsafe destructor callback fails promptly instead of deadlocking
        // this fixture on a recursive scheduler lock acquisition.
        if(dropping_){++recursive_drop_invocations;throw std::runtime_error("scheduler reentered during capture destruction");}
        action selected;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if(stopped_)return;
            selected=next_;next_=action::queue;
            if(selected==action::queue||selected==action::retain_then_throw)pending_.push_back(std::move(fn));
        }
        ready_.notify_all();
        if(selected==action::inline_then_throw)fn();
        if(selected!=action::queue)throw std::runtime_error("fixture scheduler admission exception");
    }
    bool is_on_thread()const noexcept override{return false;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
    void shutdown()override {
        std::deque<std::function<void()>> released;
        {std::lock_guard<std::mutex> lock(mutex_);stopped_=true;released.swap(pending_);}
        ready_.notify_all();
    }
    bool run_one(std::chrono::milliseconds wait=0ms) {
        std::function<void()> work;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if(pending_.empty()&&wait.count()>0)ready_.wait_for(lock,wait,[&]{return !pending_.empty()||stopped_;});
            if(pending_.empty())return false;
            work=std::move(pending_.front());pending_.pop_front();
        }
        work();return true;
    }
    bool await_queued() {
        std::unique_lock<std::mutex> lock(mutex_);
        return ready_.wait_for(lock,5s,[&]{return !pending_.empty();});
    }
    void discard_under_lock() {
        std::lock_guard<std::mutex> lock(mutex_);
        dropping_=true;
        struct reset {bool& flag;~reset(){flag=false;}} guard{dropping_};
        pending_.clear();
    }
};
thread_local bool admission_scheduler::dropping_=false;
struct admission_wire final:sync_transport {
    std::vector<std::string> frames;
    void connect(const std::string&,const std::map<std::string,std::string>&)override{}
    void disconnect()override{}
    transport_state state()const override{return transport_state::open;}
    bool supports_reconnect()const override{return false;}
    void send(const transport_message& frame)override{frames.push_back(frame.as_string());}
    void set_on_open(on_open_handler)override{}
    void set_on_message(on_message_handler)override{}
    void set_on_error(on_error_handler)override{}
    void set_on_close(on_close_handler)override{}
};
struct admission_held_writer {
    std::promise<void> entered,released;std::future<void> ready=entered.get_future();
    std::shared_future<void> proceed=released.get_future().share();std::thread worker;bool allowed=false;
    explicit admission_held_writer(lattice_db& owner) {
        auto* handle=detail::canonical_writer_custody_test_access::fault_handle(owner.db());
        worker=std::thread([this,handle] {
            auto* mutex=sqlite3_db_mutex(handle);if(!mutex)std::abort();
            sqlite3_mutex_enter(mutex);entered.set_value();
            if(proceed.wait_for(10s)!=std::future_status::ready)std::abort();
            sqlite3_mutex_leave(mutex);
        });
        if(ready.wait_for(5s)!=std::future_status::ready)std::abort();
    }
    void allow(){if(!allowed){allowed=true;released.set_value();}if(worker.joinable())worker.join();}
    ~admission_held_writer(){allow();}
};
struct admission_late_hook {
    std::function<void()> prior=std::move(detail::sync_background_test_hooks::before_late_discovery);
    explicit admission_late_hook(std::function<void()> hook){detail::sync_background_test_hooks::before_late_discovery=std::move(hook);}
    ~admission_late_hook(){detail::sync_background_test_hooks::before_late_discovery=std::move(prior);}
};
class SyncDiscoveryAdmissionRuntime:public ::testing::Test {
protected:
    std::shared_ptr<admission_scheduler> scheduled=std::make_shared<admission_scheduler>();
    std::unique_ptr<synchronizer> sync;std::shared_ptr<lattice_db> owner;
    admission_wire* wire=nullptr;std::vector<std::string> errors;
    void SetUp()override {
        configuration cfg(":memory:");cfg.sched=scheduled;cfg.audit_retention_seconds=0;cfg.busy_timeout_ms=100;
        sync_config config;config.sync_id="wss:admission-fix";config.upload_coalesce_ms=0;config.checkpoint_passive_interval_ms=0;config.chunk_size=10;
        auto transport=std::make_unique<admission_wire>();wire=transport.get();
        sync=std::make_unique<synchronizer>(std::make_unique<lattice_db>(cfg),config,std::move(transport));
        owner=access::owner(*sync);sync->set_on_error([this](const std::string& error){errors.push_back(error);});
        flush();
    }
    void TearDown()override{sync.reset();owner.reset();}
    bool failed(){return access::queue(*sync)->failed(access::generation(*sync));}
    bool pending(){return access::queue(*sync)->pending(access::generation(*sync));}
    void flush(){
        for(unsigned i=0;i<128;++i)if(!scheduled->run_one())return;
        FAIL()<<"manual scheduler did not settle within 128 callbacks";
    }
    bool await(const std::function<bool()>& done) {
        const auto deadline=std::chrono::steady_clock::now()+5s;
        while(!done()&&std::chrono::steady_clock::now()<deadline)scheduled->run_one(5ms);
        return done();
    }
    bool settle_with_manual_pump() {
        const auto deadline=std::chrono::steady_clock::now()+5s;
        while(pending()&&!failed()&&std::chrono::steady_clock::now()<deadline) {
            access::pump(*sync);scheduled->run_one(1ms);
        }
        flush();return !pending()&&!failed();
    }
    void prepare_local_row() {
        // Pacer scheduling itself is covered by the retry rejection test. A
        // stopped timer lets these policy cases choose callback order exactly.
        access::stop_pacer(*sync);
        owner->add(TestPerson{"retained-policy-row",1,std::nullopt});flush();
        register_replication_slot(owner->db(),"wss:admission-fix");flush();access::connected(*sync);
    }
    void park_real_vector(std::unique_ptr<admission_held_writer>& held,int& stages) {
        admission_late_hook hook([&]{++stages;held=std::make_unique<admission_held_writer>(*owner);});
        access::background(*sync);ASSERT_TRUE(scheduled->run_one());
        ASSERT_TRUE(held);ASSERT_TRUE(pending());EXPECT_TRUE(wire->frames.empty());
    }
};

TEST_F(SyncDiscoveryAdmissionRuntime, ThrowBeforeFirstAdmissionFencesWithoutExecutingOrEvicting) {
    int effects=0;scheduled->set_next(admission_scheduler::action::throw_before);
    access::enqueue(*sync,[&](auto&){++effects;return true;});
    ASSERT_TRUE(await([&]{return !errors.empty();}));EXPECT_TRUE(failed());EXPECT_TRUE(pending());EXPECT_EQ(effects,0);
    access::enqueue(*sync,[&](auto&){++effects;return true;});flush();EXPECT_EQ(effects,0);EXPECT_EQ(errors.size(),1u);
    sync->disconnect();EXPECT_FALSE(pending());EXPECT_FALSE(failed());
}
TEST_F(SyncDiscoveryAdmissionRuntime, RetainedThenRejectedCallbackCannotExecuteLater) {
    int effects=0;scheduled->set_next(admission_scheduler::action::retain_then_throw);
    access::enqueue(*sync,[&](auto&){++effects;return true;});
    ASSERT_TRUE(await([&]{return !errors.empty();}));flush();EXPECT_TRUE(failed());EXPECT_TRUE(pending());EXPECT_EQ(effects,0);
    EXPECT_EQ(errors.size(),1u);
}
TEST_F(SyncDiscoveryAdmissionRuntime, DroppedQueuedCallbackFencesOutsideSchedulerCaptureDestruction) {
    int effects=0;access::enqueue(*sync,[&](auto&){++effects;return true;});
    ASSERT_TRUE(scheduled->await_queued());
    scheduled->discard_under_lock();
    ASSERT_TRUE(await([&]{return !errors.empty();}));EXPECT_TRUE(failed());EXPECT_TRUE(pending());EXPECT_EQ(effects,0);
    EXPECT_EQ(scheduled->recursive_drop_invocations.load(),0);EXPECT_EQ(errors.size(),1u);
}
TEST_F(SyncDiscoveryAdmissionRuntime, InlineCompletionThenThrowDoesNotRepeatOrFenceNewWork) {
    int effects=0;scheduled->set_next(admission_scheduler::action::inline_then_throw);
    access::enqueue(*sync,[&](auto&){++effects;return true;});
    ASSERT_TRUE(await([&]{return !errors.empty();}));EXPECT_FALSE(failed());EXPECT_FALSE(pending());EXPECT_EQ(effects,1);
    access::enqueue(*sync,[&](auto&){++effects;return true;});
    ASSERT_TRUE(await([&]{return effects==2;}));EXPECT_FALSE(failed());EXPECT_FALSE(pending());EXPECT_EQ(errors.size(),1u);
}
TEST_F(SyncDiscoveryAdmissionRuntime, RetryRejectionIsTerminalWithoutReplayingTheBusyHead) {
    int probes=0;discovery_queue::operation* retained=nullptr;
    access::enqueue(*sync,[&](auto& work){
        ++probes;retained=&work;
        EXPECT_EQ(work.attempts,0u);scheduled->set_next(admission_scheduler::action::throw_before);return false;
    });
    ASSERT_TRUE(scheduled->run_one(5s));
    ASSERT_NE(retained,nullptr);EXPECT_EQ(retained->attempts,1u);
    EXPECT_NE(retained->deadline,discovery_queue::clock::time_point::max());
    ASSERT_TRUE(await([&]{return !errors.empty();}));EXPECT_TRUE(failed());EXPECT_TRUE(pending());EXPECT_EQ(probes,1);
    EXPECT_EQ(errors.size(),1u);
}
TEST_F(SyncDiscoveryAdmissionRuntime, ReservedFirstCallbackCannotBeStolenWhileSchedulerRetainsIt) {
    int effects=0;access::enqueue(*sync,[&](auto&){++effects;return true;});
    ASSERT_TRUE(scheduled->await_queued());
    const auto queue=access::queue(*sync);const auto generation=access::generation(*sync);
    EXPECT_FALSE(queue->dispatch(discovery_queue::clock::now()+1s));
    EXPECT_TRUE(queue->pending(generation));EXPECT_EQ(effects,0);
    ASSERT_TRUE(scheduled->run_one(5s));EXPECT_EQ(effects,1);
    EXPECT_FALSE(pending());EXPECT_FALSE(failed());EXPECT_TRUE(errors.empty());
}
TEST_F(SyncDiscoveryAdmissionRuntime, ActualWriterBusyStillRetriesThroughTheLivePacer) {
    owner->add(TestPerson{"first-busy-live-pacer",1,std::nullopt});flush();
    register_replication_slot(owner->db(),"wss:admission-fix");flush();access::connected(*sync);
    admission_held_writer held(*owner);access::background(*sync);
    const bool first=scheduled->run_one(5s);EXPECT_TRUE(first);
    EXPECT_TRUE(pending());EXPECT_TRUE(wire->frames.empty());EXPECT_TRUE(errors.empty());
    held.allow(); // Release on every exit before any assertion can return.
    // No manual pump: only the production pacer can admit the due retry.
    ASSERT_TRUE(await([&]{return !wire->frames.empty()||!errors.empty();}));
    ASSERT_EQ(wire->frames.size(),1u);EXPECT_TRUE(errors.empty());EXPECT_FALSE(pending());EXPECT_FALSE(failed());
    const auto frame=server_sent_event::from_json(wire->frames[0]);ASSERT_TRUE(frame);
    ASSERT_EQ(frame->audit_logs.size(),1u);EXPECT_EQ(frame->audit_logs[0].table_name,"TestPerson");
    EXPECT_EQ(frame->audit_logs[0].operation,"INSERT");
    EXPECT_NE(frame->audit_logs[0].changed_fields_to_json().find("first-busy-live-pacer"),std::string::npos);
}
TEST_F(SyncDiscoveryAdmissionRuntime, ExcludingFilterInvalidatesParkedUnfilteredVector) {
    prepare_local_row();std::unique_ptr<admission_held_writer> held;int stages=0;park_real_vector(held,stages);
    ASSERT_TRUE(held);held->allow();const auto before=access::policy(*sync);
    sync->update_sync_filter({});ASSERT_TRUE(scheduled->run_one());EXPECT_GT(access::policy(*sync),before);
    EXPECT_FALSE(query_audit_log_for_sync(owner->db(),"wss:admission-fix",std::nullopt).empty());
    EXPECT_EQ(read_upload_floor(owner->db(),"wss:admission-fix"),0);
    EXPECT_TRUE(settle_with_manual_pump());EXPECT_TRUE(wire->frames.empty());EXPECT_TRUE(errors.empty());
}
TEST_F(SyncDiscoveryAdmissionRuntime, ClearInvalidatesParkedVectorAndCreatesSeparateFreshDemand) {
    prepare_local_row();std::unique_ptr<admission_held_writer> held;int stages=0;park_real_vector(held,stages);
    ASSERT_TRUE(held);held->allow();const auto before=access::policy(*sync);
    sync->clear_sync_filter();ASSERT_TRUE(scheduled->run_one());EXPECT_GT(access::policy(*sync),before);
    EXPECT_FALSE(query_audit_log_for_sync(owner->db(),"wss:admission-fix",std::nullopt).empty());
    EXPECT_EQ(read_upload_floor(owner->db(),"wss:admission-fix"),0);
    {admission_late_hook fresh([&]{++stages;});EXPECT_TRUE(settle_with_manual_pump());}
    EXPECT_EQ(stages,2);ASSERT_EQ(wire->frames.size(),1u);EXPECT_TRUE(errors.empty());
}
TEST_F(SyncDiscoveryAdmissionRuntime, UpdateThenClearCannotReuseAnOldMatchingPolicyShape) {
    prepare_local_row();std::unique_ptr<admission_held_writer> held;int stages=0;park_real_vector(held,stages);
    ASSERT_TRUE(held);held->allow();const auto before=access::policy(*sync);
    sync->update_sync_filter({});sync->clear_sync_filter();
    ASSERT_TRUE(scheduled->run_one());ASSERT_TRUE(scheduled->run_one());EXPECT_EQ(access::policy(*sync),before+2);
    {admission_late_hook fresh([&]{++stages;});EXPECT_TRUE(settle_with_manual_pump());}
    EXPECT_EQ(stages,2);ASSERT_EQ(wire->frames.size(),1u);EXPECT_TRUE(errors.empty());
}
} // namespace
#endif
