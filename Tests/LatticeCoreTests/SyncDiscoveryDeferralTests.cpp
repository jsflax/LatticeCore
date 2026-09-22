#include "TestHelpers.hpp"
#include <lattice.hpp>
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/sync_discovery_deferral.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include "../../Sources/LatticeCore/src/receive_delivery_guard.hpp"
#include <future>
#include <deque>

namespace {
using namespace lattice;
using namespace lattice::detail;
using namespace std::chrono_literals;
using queue=sync_discovery_deferral;
std::shared_ptr<queue::operation> unit(queue::kind kind=queue::kind::intake,size_t charge=1024,uint64_t generation=1) {
    auto work=std::make_shared<queue::operation>();
    work->type=kind;work->label="fixture";work->generation=generation;work->charge=charge;
    work->step=[](auto&){return true;};return work;
}
}

TEST(SyncDiscoveryDeferral, BusyHeadPreventsLaterIntakeAndUsesBoundedWake) {
    queue q;auto first=unit(),later=unit();
    ASSERT_EQ(q.push(first),queue::admission::accepted);ASSERT_EQ(q.push(later),queue::admission::accepted);
    const auto now=queue::clock::now();const auto ticket=q.dispatch(now);
    ASSERT_EQ(q.begin(ticket,now),first);EXPECT_FALSE(q.dispatch(now));
    EXPECT_FALSE(q.finish(ticket,first,false,now));EXPECT_FALSE(q.dispatch(now+4ms));
    const auto retry=q.dispatch(now+5ms);ASSERT_EQ(q.begin(retry,now+5ms),first);
    EXPECT_FALSE(q.finish(retry,first,true,now+5ms));
    const auto next=q.dispatch(now+5ms);ASSERT_EQ(q.begin(next,now+5ms),later);
    q.finish(next,later,true,now+5ms);EXPECT_FALSE(q.pending(1));
}
TEST(SyncDiscoveryDeferral, CapacityFailureRetainsHeadAndReportsOnceUntilExplicitCancel) {
    queue q;auto first=unit();ASSERT_EQ(q.push(first),queue::admission::accepted);
    for(size_t i=1;i<queue::capacity;++i)ASSERT_EQ(q.push(unit()),queue::admission::accepted);
    EXPECT_EQ(q.push(unit()),queue::admission::exhausted);EXPECT_TRUE(q.pending(1));
    EXPECT_TRUE(q.failed(1));EXPECT_TRUE(q.take_failure(1));EXPECT_FALSE(q.take_failure(1));
    EXPECT_FALSE(q.dispatch(queue::clock::now()+1s));EXPECT_EQ(first.use_count(),2);
    q.cancel(3);EXPECT_EQ(first.use_count(),1);EXPECT_FALSE(q.pending(3));EXPECT_FALSE(q.failed(3));
    EXPECT_EQ(q.push(unit(queue::kind::intake,1024,1)),queue::admission::obsolete);
    EXPECT_EQ(q.push(unit(queue::kind::intake,1024,3)),queue::admission::accepted);
}
TEST(SyncDiscoveryDeferral, PayloadAndLateStageResizeShareOneBudget) {
    queue q;auto first=unit(queue::kind::upload,queue::byte_limit-1024),second=unit();
    ASSERT_EQ(q.push(first),queue::admission::accepted);ASSERT_EQ(q.push(second),queue::admission::accepted);
    const auto now=queue::clock::now();const auto ticket=q.dispatch(now);ASSERT_EQ(q.begin(ticket,now),first);
    EXPECT_FALSE(q.resize(first.get(),queue::byte_limit));EXPECT_EQ(first->charge,queue::byte_limit-1024);
    EXPECT_TRUE(q.resize(first.get(),1024));q.finish(ticket,first,true,now);
    EXPECT_EQ(q.push(unit(queue::kind::intake,queue::byte_limit)),queue::admission::exhausted);
}
TEST(SyncDiscoveryDeferral, AttemptsAndElapsedDeadlineBothFenceWithoutDroppingPayload) {
    for(bool elapsed:{false,true}) {
        queue q;auto work=unit();ASSERT_EQ(q.push(work),queue::admission::accepted);
        auto now=queue::clock::now();work->deadline=now+5s;
        if(elapsed) {work->attempts=1;EXPECT_TRUE(q.dispatch(now+5s));EXPECT_EQ(q.begin({1,1},now+5s),nullptr);}
        else for(unsigned i=0;i<queue::attempt_limit;++i) {
            const auto ticket=q.dispatch(now);ASSERT_TRUE(ticket);ASSERT_EQ(q.begin(ticket,now),work);
            q.finish(ticket,work,false,now);if(i+1<queue::attempt_limit)now=q.wake_at();
        }
        EXPECT_TRUE(q.failed(1));EXPECT_TRUE(q.pending(1));EXPECT_EQ(work.use_count(),2);
    }
}
TEST(SyncDiscoveryDeferral, SlowOrdinaryQueueWaitDoesNotInventAFirstBusyDeadline) {
    queue q;auto first=unit(),later=unit();q.push(first);q.push(later);
    const auto now=queue::clock::now();auto ticket=q.dispatch(now);ASSERT_EQ(q.begin(ticket,now),first);
    q.finish(ticket,first,true,now+20s);ticket=q.dispatch(now+20s);
    EXPECT_EQ(q.begin(ticket,now+20s),later);EXPECT_FALSE(q.failed(1));
}
TEST(SyncDiscoveryDeferral, OldTicketsAndCloseCannotAdoptReplacementGeneration) {
    queue q;auto old=unit();q.push(old);const auto ticket=q.dispatch(queue::clock::now());
    q.cancel(3);q.push(unit(queue::kind::ack,1024,3));EXPECT_EQ(q.begin(ticket,queue::clock::now()),nullptr);
    EXPECT_TRUE(q.pending(3));q.cancel(3,true);EXPECT_FALSE(q.pending(3));
    EXPECT_EQ(q.push(unit(queue::kind::intake,1024,3)),queue::admission::obsolete);
}
TEST(SyncDiscoveryDeferral, CaptureDestructorsRunOutsideLeafLock) {
    queue q;std::atomic<int> destroyed{0};
    struct capture {queue* q;std::atomic<int>* calls;~capture(){(void)q->pending(1);++*calls;}};
    auto work=unit();auto retained=std::make_shared<capture>();retained->q=&q;retained->calls=&destroyed;
    work->step=[retained](auto&){return true;};retained.reset();q.push(work);work.reset();
    q.cancel(3);EXPECT_EQ(destroyed.load(),1);
}
TEST(SyncDiscoveryDeferral, ActiveOrStagedUploadPreservesOneLaterEnumerationDemand) {
    queue q;auto first=unit(queue::kind::upload);q.push(first);
    const auto now=queue::clock::now();auto ticket=q.dispatch(now);ASSERT_EQ(q.begin(ticket,now),first);
    auto later=unit(queue::kind::upload);EXPECT_EQ(q.push(later),queue::admission::accepted);
    EXPECT_EQ(q.push(unit(queue::kind::upload)),queue::admission::coalesced);
    first->coalescible=false;q.finish(ticket,first,false,now);
    // The parked exact send vector cannot absorb an unrelated new query. The
    // already retained later query can absorb it without losing that demand.
    EXPECT_EQ(q.push(unit(queue::kind::upload)),queue::admission::coalesced);
    ticket=q.dispatch(now+5ms);ASSERT_EQ(q.begin(ticket,now+5ms),first);q.finish(ticket,first,true,now+5ms);
    ticket=q.dispatch(now+5ms);EXPECT_EQ(q.begin(ticket,now+5ms),later);
}

#ifndef __EMSCRIPTEN__
namespace lattice::detail {
struct sync_discovery_test_access {
    static synchronizer_base* configured(lattice_db& owner){return owner.synchronizer_.get();}
    static void flush_scheduler(synchronizer_base& sync){
        const auto done=std::make_shared<std::promise<void>>();auto result=done->get_future();
        sync.scheduler_->invoke([done]{done->set_value();});
        if(result.wait_for(std::chrono::seconds(5))!=std::future_status::ready)throw std::runtime_error("actual scheduler did not settle");
    }
    static void schedule(synchronizer_base& sync,std::function<void()> work){sync.scheduler_->invoke(std::move(work));}

    static std::shared_ptr<lattice_db> owner(synchronizer_base& sync){return sync.owned_db_;}
    static auto queue(synchronizer_base& sync){return sync.discovery_deferral_;}
    static uint64_t generation(synchronizer_base& sync){return sync.reconnect_lifecycle_.load();}
    static void background(synchronizer_base& sync){sync.background_upload();}
    static void input(synchronizer_base& sync,const server_sent_event& event){const auto json=event.to_json();sync.on_transport_message(transport_message::from_binary({json.begin(),json.end()}));}
    static void connected(synchronizer_base& sync){sync.is_connected_=true;sync.recovery_export_route_->publish(generation(sync),true);}
    static void disconnected(synchronizer_base& sync){sync.is_connected_=false;}
    static void filter(synchronizer_base& sync){sync.config_.sync_filter=std::vector<sync_filter_entry>{{"TestPerson",std::nullopt}};}
    static size_t in_flight(synchronizer_base& sync){std::lock_guard<std::mutex> lock(sync.in_flight_mutex_);return sync.in_flight_ids_.size();}
};
}
namespace {
struct sync_wire_state {
    std::mutex mutex;std::condition_variable ready;
    std::vector<std::string> frames,errors;std::vector<std::vector<std::string>> completions;
    sync_transport::on_open_handler opened;
    void sent(const transport_message& frame){std::lock_guard<std::mutex> lock(mutex);frames.push_back(frame.as_string());ready.notify_all();}
    void error(const std::string& error){std::lock_guard<std::mutex> lock(mutex);errors.push_back(error);ready.notify_all();}
    void complete(const std::vector<std::string>& ids){std::lock_guard<std::mutex> lock(mutex);completions.push_back(ids);ready.notify_all();}
    bool await(size_t nframes,size_t nerrors=0,size_t ncomplete=0){std::unique_lock<std::mutex> lock(mutex);return ready.wait_for(lock,5s,[&]{return frames.size()>=nframes&&errors.size()>=nerrors&&completions.size()>=ncomplete;});}
    size_t count(){std::lock_guard<std::mutex> lock(mutex);return frames.size();}
    std::vector<std::string> copy(){std::lock_guard<std::mutex> lock(mutex);return frames;}
};
struct sync_wire final:sync_transport {
    std::shared_ptr<sync_wire_state> shared;
    explicit sync_wire(std::shared_ptr<sync_wire_state> value):shared(std::move(value)){}
    void connect(const std::string&,const std::map<std::string,std::string>&)override{}
    void disconnect()override{}
    transport_state state()const override{return transport_state::open;}
    bool supports_reconnect()const override{return false;}
    void send(const transport_message& frame)override{shared->sent(frame);}
    void set_on_open(on_open_handler f)override{shared->opened=std::move(f);}
    void set_on_message(on_message_handler)override{}
    void set_on_error(on_error_handler)override{}
    void set_on_close(on_close_handler)override{}
};
struct held_writer_mutex {
    std::promise<void> entered,release;
    std::shared_future<void> ready=entered.get_future().share(),proceed=release.get_future().share();
    std::atomic<bool> released{false};std::thread worker;
    explicit held_writer_mutex(lattice_db& owner,std::function<void()> before_leave={}) {
        auto* h=canonical_writer_custody_test_access::fault_handle(owner.db());
        worker=std::thread([this,h,before_leave=std::move(before_leave)] {
            auto* mutex=sqlite3_db_mutex(h);if(!mutex)std::abort();
            sqlite3_mutex_enter(mutex);entered.set_value();
            if(proceed.wait_for(10s)!=std::future_status::ready)std::abort();
            if(before_leave)try{before_leave();}catch(...){std::abort();}
            sqlite3_mutex_leave(mutex);
        });
        if(ready.wait_for(5s)!=std::future_status::ready)std::abort();
    }
    void allow(){if(!released.exchange(true))release.set_value();if(worker.joinable())worker.join();}
    ~held_writer_mutex(){allow();}
};
struct late_probe_hook {
    std::function<void()> prior=std::move(sync_background_test_hooks::before_late_discovery);
    explicit late_probe_hook(std::function<void()> work){sync_background_test_hooks::before_late_discovery=std::move(work);}
    ~late_probe_hook(){sync_background_test_hooks::before_late_discovery=std::move(prior);}
};
// Explicit test-thread execution keeps the scoped late-stage fault installed
// until the actual continuation reaches it, regardless of which thread admits
// the ticket. The production pacer still owns all due retry admissions.
class discovery_stage_scheduler final:public scheduler {
    mutable std::mutex mutex_;std::condition_variable ready_;
    std::deque<std::function<void()>> pending_;bool closed_=false;
public:
    void invoke(std::function<void()>&& work)override {
        {std::lock_guard<std::mutex> lock(mutex_);if(closed_)return;pending_.push_back(std::move(work));}
        ready_.notify_all();
    }
    bool is_on_thread()const noexcept override{return false;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{std::lock_guard<std::mutex> lock(mutex_);return !closed_;}
    void shutdown()override {
        std::deque<std::function<void()>> released;
        {std::lock_guard<std::mutex> lock(mutex_);closed_=true;released.swap(pending_);}
        ready_.notify_all(); // Capture destructors execute outside this leaf.
    }
    bool run_until(const std::function<bool()>& settled) {
        const auto deadline=std::chrono::steady_clock::now()+5s;
        for(unsigned turn=0;turn<128&&!settled();++turn) {
            std::function<void()> work;
            {std::unique_lock<std::mutex> lock(mutex_);
             if(!ready_.wait_until(lock,deadline,[&]{return closed_||!pending_.empty();}))return false;
             if(closed_)return false;
             work=std::move(pending_.front());pending_.pop_front();}
            work();if(std::chrono::steady_clock::now()>=deadline)return settled();
        }
        return settled();
    }
    bool drain_current() {
        for(unsigned turn=0;turn<128;++turn) {
            std::function<void()> work;
            {std::lock_guard<std::mutex> lock(mutex_);if(pending_.empty())return true;work=std::move(pending_.front());pending_.pop_front();}
            work();
        }
        return false;
    }
};
class SyncDiscoveryContention : public ::testing::Test {
protected:
    std::shared_ptr<sync_wire_state> wire=std::make_shared<sync_wire_state>();
    std::unique_ptr<synchronizer> sync;
    std::shared_ptr<lattice_db> owner;
    void open(const std::string& path=":memory:",std::shared_ptr<scheduler> scheduled={}) {
        configuration cfg(path);cfg.audit_retention_seconds=0;cfg.busy_timeout_ms=100;cfg.sched=scheduled?std::move(scheduled):std::make_shared<immediate_scheduler>();
        sync_config config;config.sync_id="wss:discovery-test";config.upload_coalesce_ms=0;config.checkpoint_passive_interval_ms=0;config.chunk_size=10;
        sync=std::make_unique<synchronizer>(std::make_unique<lattice_db>(cfg),config,std::make_unique<sync_wire>(wire));
        owner=sync_discovery_test_access::owner(*sync);
        if(path!=":memory:"){auto* notifier=instance_registry::instance().get_or_create_notifier(path);if(notifier)notifier->stop_listening();}
        sync->set_on_error([state=wire](const std::string& error){state->error(error);});
        sync->set_on_sync_complete([state=wire](const auto& ids){state->complete(ids);});
        register_replication_slot(owner->db(),"wss:discovery-test");
    }
    void SetUp()override{open();}
    void TearDown()override{sync.reset();owner.reset();}
    audit_log_entry change(const std::string& id,const std::string& initial) {
        owner->add(TestPerson{initial,1,std::nullopt});audit_log_entry e;
        e.global_id=id;e.table_name="TestPerson";e.operation="UPDATE";
        e.global_row_id=std::get<std::string>(owner->db().query("SELECT globalId FROM TestPerson WHERE name=?",{initial}).at(0).at("globalId"));
        e.changed_fields_names={"name"};e.changed_fields={{"name",any_property(initial+"-changed")}};e.timestamp="1789819200.0";return e;
    }
    std::string name(const audit_log_entry& e){return std::get<std::string>(owner->db().query("SELECT name FROM TestPerson WHERE globalId=?",{e.global_row_id}).at(0).at("name"));}
    bool pending(){return sync_discovery_test_access::queue(*sync)->pending(sync_discovery_test_access::generation(*sync));}
    void receive(const audit_log_entry& e){sync_discovery_test_access::input(*sync,server_sent_event::make_audit_log({e}));}
};
TEST_F(SyncDiscoveryContention, TypedOutcomeOnlyForInitialMutexProbeAndNoFalseAbsence) {
    held_writer_mutex held(*owner);
    EXPECT_THROW(recovery_export_adapter::protected_store(owner),export_discovery_busy);
    EXPECT_FALSE(recovery_export_adapter::try_protected_store(owner).has_value());
    EXPECT_FALSE(recovery_export_adapter::try_prepare_pending(owner,"wss:discovery-test",1,10,{},false).has_value());
    held.allow();ASSERT_TRUE(recovery_export_adapter::try_protected_store(owner).has_value());
    EXPECT_FALSE(*recovery_export_adapter::try_protected_store(owner));
    owner->db().execute("BEGIN DEFERRED");owner->db().query("SELECT name FROM sqlite_schema LIMIT 1");
    EXPECT_THROW(recovery_export_adapter::try_protected_store(owner),db_error);
    owner->db().execute("ROLLBACK");
    owner->db().execute("CREATE TABLE _lattice_obligation_producer_profile(channel BLOB)");
    EXPECT_THROW(recovery_export_adapter::try_protected_store(owner),db_error);
}
TEST_F(SyncDiscoveryContention, IntakeRetainsExactFramesAndCheckpointOrderAcrossBusy) {
    const auto first=change("busy-first","first"),second=change("busy-second","second");
    held_writer_mutex held(*owner);receive(first);receive(second);EXPECT_TRUE(pending());EXPECT_EQ(wire->count(),0u);
    held.allow();ASSERT_TRUE(wire->await(2));
    EXPECT_EQ(name(first),"first-changed");EXPECT_EQ(name(second),"second-changed");
    const auto frames=wire->copy();ASSERT_EQ(frames.size(),2u);
    EXPECT_EQ(server_sent_event::from_json(frames[0])->acked_ids,(std::vector<std::string>{first.global_id}));
    EXPECT_EQ(server_sent_event::from_json(frames[1])->acked_ids,(std::vector<std::string>{second.global_id}));
    const auto received=receive_delivery_guard_access::read(*owner,"wss:discovery-test");
    EXPECT_EQ(received.checkpoint,std::optional<std::string>(second.global_id));EXPECT_EQ(received.generation,2);
}
TEST_F(SyncDiscoveryContention, PolicyRemovalAckRemainsBehindTheOriginalBusyIntake) {
    auto first=change("normal-busy","normal"),removal=first;removal.global_id="policy-removal";removal.operation="DELETE";removal.changed_fields_names={"__lattice_filter_removal"};
    held_writer_mutex held(*owner);
    sync_discovery_test_access::input(*sync,server_sent_event::make_audit_log({first,removal}));
    EXPECT_EQ(wire->count(),0u);held.allow();ASSERT_TRUE(wire->await(1));
    const auto ack=server_sent_event::from_json(wire->copy()[0]);ASSERT_TRUE(ack);
    EXPECT_EQ(ack->acked_ids,(std::vector<std::string>{first.global_id,removal.global_id}));EXPECT_EQ(name(first),"normal-changed");
}
TEST_F(SyncDiscoveryContention, AckBookkeepingAndCompletionExecuteOnceAfterBusyRelease) {
    const auto changed=change("unused-remote","local");
    const auto id=std::get<std::string>(owner->db().query("SELECT globalId FROM AuditLog ORDER BY id LIMIT 1").at(0).at("globalId"));
    sync_discovery_test_access::connected(*sync);
    held_writer_mutex held(*owner);sync_discovery_test_access::input(*sync,server_sent_event::make_ack({id}));
    EXPECT_TRUE(pending());{std::lock_guard<std::mutex> lock(wire->mutex);EXPECT_TRUE(wire->completions.empty());}
    held.allow();ASSERT_TRUE(wire->await(0,0,1));
    {std::lock_guard<std::mutex> lock(wire->mutex);ASSERT_EQ(wire->completions.size(),1u);EXPECT_EQ(wire->completions[0],(std::vector<std::string>{id}));}
    EXPECT_EQ(std::get<int64_t>(owner->db().query("SELECT isSynchronized AS n FROM AuditLog WHERE globalId=?",{id}).at(0).at("n")),1);
    (void)changed;
}
TEST_F(SyncDiscoveryContention, InitialUploadFirstProbeDefersBeforeSlotAndOpenSetup) {
    change("unused-remote","initial");held_writer_mutex held(*owner);
    wire->opened();EXPECT_TRUE(pending());EXPECT_EQ(wire->count(),0u);
    held.allow();ASSERT_TRUE(wire->await(1));
    const auto frame=server_sent_event::from_json(wire->copy()[0]);ASSERT_TRUE(frame);EXPECT_EQ(frame->event_type,server_sent_event::type::audit_log);
}
TEST_F(SyncDiscoveryContention, ForegroundAndDrainCannotReportDeferredUploadAsComplete) {
    change("unused-remote","upload");sync_discovery_test_access::connected(*sync);
    held_writer_mutex held(*owner);sync_discovery_test_access::background(*sync);EXPECT_TRUE(pending());
    EXPECT_THROW(sync->sync_now(),db_error);
    EXPECT_THROW(sync->drain(std::chrono::steady_clock::now()+100ms),db_error);
    EXPECT_EQ(wire->count(),0u);held.allow();ASSERT_TRUE(wire->await(1));
}
TEST_F(SyncDiscoveryContention, LateBusyRetainsExactRealVectorAndDoesNotReenumerate) {
    change("unused-remote","late");sync_discovery_test_access::connected(*sync);
    const auto expected=query_audit_log_for_sync(owner->db(),"wss:discovery-test",std::nullopt);
    std::unique_ptr<held_writer_mutex> held;int stages=0;
    {late_probe_hook hook([&]{++stages;held=std::make_unique<held_writer_mutex>(*owner);});sync_discovery_test_access::background(*sync);}
    EXPECT_EQ(stages,1);EXPECT_TRUE(pending());EXPECT_EQ(wire->count(),0u);ASSERT_TRUE(held);held->allow();
    ASSERT_TRUE(wire->await(1));EXPECT_EQ(stages,1);
    const auto frame=server_sent_event::from_json(wire->copy()[0]);ASSERT_TRUE(frame);ASSERT_EQ(frame->audit_logs.size(),expected.size());
    EXPECT_EQ(frame->audit_logs[0].global_id,expected[0].global_id);EXPECT_EQ(frame->audit_logs[0].changed_fields_names,expected[0].changed_fields_names);
    EXPECT_EQ(wire->copy()[0],server_sent_event::make_audit_log(expected).to_json());
}
TEST_F(SyncDiscoveryContention, LateBusyCancelLeavesRealAuditRowsUnsynchronizedAndFloorPinned) {
    change("unused-remote","cancel-late");sync_discovery_test_access::connected(*sync);
    std::unique_ptr<held_writer_mutex> held;
    {late_probe_hook hook([&]{held=std::make_unique<held_writer_mutex>(*owner);});sync_discovery_test_access::background(*sync);}
    ASSERT_TRUE(held);EXPECT_TRUE(pending());sync->disconnect();held->allow();
    EXPECT_FALSE(pending());EXPECT_EQ(wire->count(),0u);
    EXPECT_FALSE(query_audit_log_for_sync(owner->db(),"wss:discovery-test",std::nullopt).empty());
    EXPECT_EQ(read_upload_floor(owner->db(),"wss:discovery-test"),0);
}
TEST_F(SyncDiscoveryContention, FilteredLateBusyIsExplicitUnsupportedRefusal) {
    sync.reset();owner.reset();const auto scheduled=std::make_shared<discovery_stage_scheduler>();open(":memory:",scheduled);
    change("unused-remote","filtered");ASSERT_TRUE(scheduled->drain_current());ASSERT_FALSE(pending());
    sync_discovery_test_access::filter(*sync);sync_discovery_test_access::connected(*sync);
    std::unique_ptr<held_writer_mutex> held;
    {late_probe_hook hook([&]{held=std::make_unique<held_writer_mutex>(*owner);});
     sync_discovery_test_access::background(*sync);
     EXPECT_TRUE(scheduled->run_until([&]{return bool(held);}));}
    ASSERT_TRUE(held);held->allow();
    EXPECT_TRUE(scheduled->run_until([&]{std::lock_guard<std::mutex> lock(wire->mutex);return !wire->errors.empty();}));
    ASSERT_TRUE(wire->await(0,1));EXPECT_FALSE(pending());EXPECT_EQ(wire->count(),0u);
    {std::lock_guard<std::mutex> lock(wire->mutex);EXPECT_NE(wire->errors[0].find("not supported"),std::string::npos);}
    sync.reset();scheduled->shutdown(); // No queued capture outlives the fault locals.
}
TEST_F(SyncDiscoveryContention, DisconnectAndCloseAccountForBusyPayloadWithoutAckOrEffects) {
    const auto e=change("old-generation","before");const auto queue=sync_discovery_test_access::queue(*sync);
    held_writer_mutex held(*owner);receive(e);EXPECT_TRUE(pending());
    sync->disconnect();EXPECT_FALSE(pending());held.allow();EXPECT_EQ(name(e),"before");EXPECT_EQ(wire->count(),0u);
    held_writer_mutex again(*owner);receive(e);EXPECT_TRUE(pending());sync.reset();again.allow();
    EXPECT_EQ(name(e),"before");EXPECT_EQ(wire->count(),0u);
}
TEST_F(SyncDiscoveryContention, LaterMalformedClassifierRefusalDoesNotBecomeRetryOrLegacyAbsence) {
    const auto e=change("refused-after-busy","before");
    held_writer_mutex held(*owner,[db=owner]{db->db().execute("CREATE TABLE _lattice_obligation_producer_profile(channel BLOB)");});
    receive(e);EXPECT_TRUE(pending());held.allow();ASSERT_TRUE(wire->await(0,1));
    EXPECT_FALSE(pending());EXPECT_EQ(name(e),"before");EXPECT_EQ(wire->count(),0u);
}
TEST_F(SyncDiscoveryContention, ErrorCallbackCanRetireOwnerOnTheExistingRetryPacer) {
    const auto e=change("close-from-retry","before");
    const auto retiring=std::make_shared<std::unique_ptr<synchronizer>>(std::move(sync));
    const std::weak_ptr<std::unique_ptr<synchronizer>> weak=retiring;
    const auto retired=std::make_shared<std::promise<void>>();auto done=retired->get_future();
    (*retiring)->set_on_error([weak,retired](const std::string&){if(const auto owner=weak.lock())owner->reset();retired->set_value();});
    held_writer_mutex held(*owner,[db=owner]{db->db().execute("CREATE TABLE _lattice_obligation_producer_profile(channel BLOB)");});
    sync_discovery_test_access::input(**retiring,server_sent_event::make_audit_log({e}));
    held.allow();ASSERT_EQ(done.wait_for(5s),std::future_status::ready);
    EXPECT_FALSE(*retiring);EXPECT_EQ(name(e),"before");EXPECT_EQ(wire->count(),0u);
}
TEST_F(SyncDiscoveryContention, PreopenedSiblingEnrollmentWhileBusyCannotBecomeLegacyIntake) {
    sync.reset();owner.reset();TempDB file{"discovery_deferral_sibling"};open(file.str());
    const auto e=change("protected-after-busy","before");
    configuration cfg(file.str());cfg.audit_retention_seconds=0;cfg.busy_timeout_ms=100;
    auto current=std::make_shared<lattice_db>(cfg);
    if(auto* notifier=instance_registry::instance().get_or_create_notifier(file.str()))notifier->stop_listening();
    held_writer_mutex held(*owner);receive(e);EXPECT_TRUE(pending());
    const recovery_obligation_producer_discovery_limits caps{{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
    auto committed=[](const recovery_install_result& r) {
        if(r.primary_error)std::rethrow_exception(r.primary_error);
        if(r.cleanup_error)std::rethrow_exception(r.cleanup_error);
        if(r.postcommit_error)std::rethrow_exception(r.postcommit_error);
        if(r.state!=recovery_install_state::committed)throw std::runtime_error("fixture enrollment did not commit");
    };
    recovery_obligation_address address;
    committed(recovery_writer_access::install(current,[&](database&) {
        receive_install_store receiver(current,caps.installations);receiver.initialize();
        receive_install_binding binding{"discovery-deferral","authority","source","epoch","scope","schema"};receiver.bind(binding);
        recovery_obligation_store journal(current,caps.obligations,caps.installations);journal.initialize();
        address=journal.bind({binding,"grant","receipts"}).address;
    }));
    committed(recovery_local_producer_adapter::enroll_for_qualification(current,{address,{"TestPerson"},{'g'}},caps));
    held.allow();ASSERT_TRUE(wire->await(0,1));EXPECT_FALSE(pending());
    EXPECT_TRUE(recovery_export_adapter::protected_store(current));EXPECT_EQ(name(e),"before");EXPECT_EQ(wire->count(),0u);
    sync.reset();owner.reset();current.reset();
}
TEST_F(SyncDiscoveryContention, PostIntakeCommitRefusalIsNeverRetriedAsInitialBusy) {
    const auto e=change("committed-intake","before");int calls=0;
    const auto prior=std::move(receive_guard_test_hooks::after_intake_commit);
    struct restore {std::function<void()> prior;~restore(){receive_guard_test_hooks::after_intake_commit=std::move(prior);}} reset{prior};
    receive_guard_test_hooks::after_intake_commit=[&]{++calls;throw std::runtime_error("fixture postcommit refusal");};
    receive(e);EXPECT_EQ(calls,1);EXPECT_FALSE(pending());EXPECT_EQ(wire->count(),0u);EXPECT_EQ(name(e),"before");
    const auto guard=receive_delivery_guard_access::read(*owner,"wss:discovery-test");EXPECT_EQ(guard.state,receive_guard_state::in_progress);
}

// Completion is independent of owner lifetime and cannot lose a running
// turn's original error when close cancels the same finite FIFO cell.
TEST(SyncDiscoveryCompletion, CancelAndRetirementPreserveTheRunningError) {
    queue q;auto work=unit(queue::kind::drain_upload);work->completion=std::make_shared<sync_discovery_completion>();
    const auto completion=work->completion;const auto now=queue::clock::now();
    const auto admitted=q.push_and_dispatch(work,now);ASSERT_TRUE(admitted.reserved);
    ASSERT_EQ(q.begin(admitted.reserved,now),work);q.cancel(3,true);
    const auto error=std::make_exception_ptr(std::runtime_error("original running failure"));
    completion->finish(true,error);const auto result=completion->read();
    EXPECT_EQ(result.state,sync_discovery_completion::outcome::cancelled);
    ASSERT_TRUE(result.error);try{std::rethrow_exception(result.error);}catch(const std::exception& e){EXPECT_STREQ(e.what(),"original running failure");}
    EXPECT_FALSE(q.pending(3));
}
TEST(SyncDiscoveryCompletion, CancelledQueuedDrainDoesNotExecuteOrBlockLaterWork) {
    queue q;auto work=unit(queue::kind::drain_upload);work->completion=std::make_shared<sync_discovery_completion>();
    auto later=unit();const auto now=queue::clock::now();q.push(work);q.push(later);
    work->completion->cancel(sync_discovery_completion::outcome::expired);
    EXPECT_EQ(q.begin(q.dispatch(now),now),nullptr);
    const auto next=q.dispatch(now);ASSERT_EQ(q.begin(next,now),later);q.finish(next,later,true,now);
    EXPECT_FALSE(q.pending(1));EXPECT_FALSE(q.failed(1));
}
TEST_F(SyncDiscoveryContention, CheckedDrainWaitsForEarlierBusyIntakeThenReportsPendingAckHonestly) {
    const auto e=change("drain-earlier-intake","before");sync_discovery_test_access::connected(*sync);
    held_writer_mutex held(*owner);receive(e);ASSERT_TRUE(pending());
    auto result=std::async(std::launch::async,[&]{return sync->drain_checked(std::chrono::steady_clock::now()+2s);});
    EXPECT_EQ(result.wait_for(30ms),std::future_status::timeout);held.allow();
    ASSERT_EQ(result.wait_for(5s),std::future_status::ready);const auto outcome=result.get();
    EXPECT_FALSE(outcome.error);EXPECT_EQ(outcome.state,sync_drain_state::deadline_pending);
    EXPECT_EQ(name(e),"before-changed");EXPECT_GT(wire->count(),0u);
    // No server ACK was fabricated: the real local original stays pending.
    EXPECT_GT(sync_discovery_test_access::in_flight(*sync),0u);
}
TEST_F(SyncDiscoveryContention, RacingAdmissionBetweenDrainChecksIsOrderedBeforeItsUpload) {
    const auto e=change("drain-racing-intake","race");sync_discovery_test_access::connected(*sync);
    held_writer_mutex held(*owner);std::promise<void> inserted;auto seen=inserted.get_future();
    auto result=std::async(std::launch::async,[&]{
        struct reset {~reset(){sync_background_test_hooks::before_drain_admission={};}} reset_hook;
        sync_background_test_hooks::before_drain_admission=[&]{receive(e);inserted.set_value();};
        return sync->drain_checked(std::chrono::steady_clock::now()+2s);
    });
    ASSERT_EQ(seen.wait_for(5s),std::future_status::ready);EXPECT_TRUE(pending());
    EXPECT_EQ(result.wait_for(30ms),std::future_status::timeout);held.allow();
    ASSERT_EQ(result.wait_for(5s),std::future_status::ready);const auto outcome=result.get();
    EXPECT_FALSE(outcome.error);EXPECT_EQ(outcome.state,sync_drain_state::deadline_pending);
    EXPECT_EQ(name(e),"race-changed");const auto frames=wire->copy();ASSERT_FALSE(frames.empty());
    const auto first=server_sent_event::from_json(frames.front());ASSERT_TRUE(first);
    EXPECT_EQ(first->acked_ids,(std::vector<std::string>{e.global_id}));
}
TEST_F(SyncDiscoveryContention, RetiringOwnerSettlesQueuedDrainWithoutWaitingForDeadline) {
    const auto e=change("drain-retired","before");sync_discovery_test_access::connected(*sync);
    held_writer_mutex held(*owner);receive(e);
    auto* raw=sync.get();auto result=std::async(std::launch::async,[raw]{return raw->drain_checked(std::chrono::steady_clock::now()+2s);});
    EXPECT_EQ(result.wait_for(30ms),std::future_status::timeout);sync->disconnect();
    ASSERT_EQ(result.wait_for(1s),std::future_status::ready);const auto outcome=result.get();
    EXPECT_FALSE(outcome.error);EXPECT_TRUE(outcome.state==sync_drain_state::retired||outcome.state==sync_drain_state::disconnected);
    held.allow();EXPECT_EQ(name(e),"before");EXPECT_EQ(wire->count(),0u);
}
TEST_F(SyncDiscoveryContention, DrainOnItsOwnSchedulerReturnsExplicitPendingWithoutWaiting) {
    sync_discovery_test_access::connected(*sync);sync_drain_result outcome;
    sync_discovery_test_access::schedule(*sync,[&]{outcome=sync->drain_checked(std::chrono::steady_clock::now()+2s);});
    EXPECT_EQ(outcome.state,sync_drain_state::reentrant_pending);EXPECT_FALSE(outcome.error);EXPECT_FALSE(pending());
}
struct close_wire_state {
    std::atomic<int> sends{0},disconnects{0},destroyed{0};
    std::atomic<bool> fail_send{false},fail_disconnect{false};
};
class close_wire final:public sync_transport {
    std::shared_ptr<close_wire_state> state_;
public:
    explicit close_wire(std::shared_ptr<close_wire_state> state):state_(std::move(state)){}
    ~close_wire(){++state_->destroyed;}
    void connect(const std::string&,const std::map<std::string,std::string>&)override{}
    void disconnect()override{++state_->disconnects;if(state_->fail_disconnect)throw std::runtime_error("cleanup disconnect failure");}
    transport_state state()const override{return transport_state::open;}
    bool supports_reconnect()const override{return false;}
    void send(const transport_message&)override{++state_->sends;if(state_->fail_send)throw std::runtime_error("first actual upload failure");}
    void set_on_open(on_open_handler)override{}
    void set_on_message(on_message_handler)override{}
    void set_on_error(on_error_handler)override{}
    void set_on_close(on_close_handler)override{}
};
class close_network final:public network_factory {
public:
    const std::shared_ptr<close_wire_state> state=std::make_shared<close_wire_state>();
    std::unique_ptr<http_client> create_http_client()override{return std::make_unique<null_http_client>();}
    std::unique_ptr<sync_transport> create_sync_transport()override{return std::make_unique<close_wire>(state);}
};
class SyncCheckedClose:public ::testing::Test {
protected:
    TempDB file{"checked_close"};std::shared_ptr<network_factory> prior;
    std::shared_ptr<close_network> factory=std::make_shared<close_network>();
    configuration config(){configuration out(file.str());out.audit_retention_seconds=0;out.busy_timeout_ms=100;
        out.websocket_url="wss://checked-close.invalid/sync";out.authorization_token="fixture-token";return out;}
    void SetUp()override{prior=get_network_factory();set_network_factory(factory);}
    void TearDown()override{set_network_factory(prior);}
    void stop_notifier(){if(auto* n=instance_registry::instance().get_or_create_notifier(file.str()))n->stop_listening();}
    void connect(lattice_db& owner){auto* sync=sync_discovery_test_access::configured(owner);if(!sync)throw std::runtime_error("actual configured sync missing");
        sync_discovery_test_access::flush_scheduler(*sync);
        auto actual=sync_discovery_test_access::owner(*sync);register_replication_slot(actual->db(),"wss:"+config().websocket_url);
        sync_discovery_test_access::connected(*sync);}
    void held_close(bool racing) {
        auto parent=std::make_unique<lattice_db>(config());stop_notifier();parent->add(TestPerson{"before",1,std::nullopt});connect(*parent);
        auto* sync=sync_discovery_test_access::configured(*parent);auto child=sync_discovery_test_access::owner(*sync);
        std::weak_ptr<lattice_db> weak_child=child;
        audit_log_entry entry;entry.global_id="actual-close-intake";entry.table_name="TestPerson";entry.operation="UPDATE";
        entry.global_row_id=std::get<std::string>(child->db().query("SELECT globalId FROM TestPerson").at(0).at("globalId"));
        entry.changed_fields_names={"name"};entry.changed_fields={{"name",any_property("after")}};entry.timestamp="1789819200.0";
        held_writer_mutex held(*child);child.reset();std::promise<void> admitted;auto observed=admitted.get_future();
        if(!racing)sync_discovery_test_access::input(*sync,server_sent_event::make_audit_log({entry}));
        auto closed=std::async(std::launch::async,[&]{
            struct reset {~reset(){sync_background_test_hooks::before_drain_admission={};}} reset_hook;
            sync_background_test_hooks::before_drain_admission=[&]{
                if(racing)sync_discovery_test_access::input(*sync,server_sent_event::make_audit_log({entry}));
                admitted.set_value();
            };
            return parent->close_checked();
        });
        const auto admission=observed.wait_for(5s);const auto blocked=closed.wait_for(30ms);
        held.allow(); // Always release the actual writer before failure unwind.
        ASSERT_EQ(admission,std::future_status::ready);EXPECT_EQ(blocked,std::future_status::timeout);
        ASSERT_EQ(closed.wait_for(5s),std::future_status::ready);const auto result=closed.get();
        EXPECT_EQ(result.sync,sync_drain_state::deadline_pending);EXPECT_FALSE(result.error);EXPECT_TRUE(result.cleanup_complete);
        EXPECT_TRUE(parent->is_closed());EXPECT_EQ(sync_discovery_test_access::configured(*parent),nullptr);
        EXPECT_TRUE(weak_child.expired());EXPECT_EQ(factory->state->destroyed.load(),1);parent.reset();
        configuration read_config(file.str());read_config.audit_retention_seconds=0;lattice_db reopened(read_config);stop_notifier();
        EXPECT_EQ(std::get<std::string>(reopened.db().query("SELECT name FROM TestPerson").at(0).at("name")),"after");
        EXPECT_FALSE(reopened.db().query("SELECT id FROM AuditLog WHERE isSynchronized=0").empty());reopened.close();
    }
    static std::string message(const std::exception_ptr& error){if(!error)return {};try{std::rethrow_exception(error);}catch(const std::exception& e){return e.what();}catch(...){return "unknown";}}
};
TEST_F(SyncCheckedClose, ActualConfiguredCloseJoinsEarlierBusyDiscovery) {held_close(false);}
TEST_F(SyncCheckedClose, ActualConfiguredCloseJoinsDiscoveryRacingItsAdmission) {held_close(true);}
TEST_F(SyncCheckedClose, ActualConfiguredClosePreservesUploadErrorAndCompletesOtherCleanup) {
    auto owner=std::make_unique<lattice_db>(config());stop_notifier();owner->add(TestPerson{"pending",1,std::nullopt});connect(*owner);
    factory->state->fail_send=true;factory->state->fail_disconnect=true;
    const auto result=owner->close_checked();EXPECT_EQ(result.sync,sync_drain_state::failed);
    EXPECT_EQ(message(result.error),"first actual upload failure");EXPECT_EQ(message(result.cleanup_error),"cleanup disconnect failure");
    EXPECT_FALSE(result.cleanup_complete);EXPECT_TRUE(owner->is_closed());
    EXPECT_EQ(sync_discovery_test_access::configured(*owner),nullptr);EXPECT_GE(factory->state->disconnects.load(),1);
    EXPECT_EQ(factory->state->destroyed.load(),1);owner.reset();
    // Real reopen must reacquire the registry/flock after failed cleanup. It
    // does not infer any ACK: the original is still present and unsynchronized.
    factory->state->fail_send=false;factory->state->fail_disconnect=false;
    auto reopened=std::make_unique<lattice_db>(config());stop_notifier();ASSERT_NE(sync_discovery_test_access::configured(*reopened),nullptr);
    EXPECT_FALSE(reopened->db().query("SELECT id FROM AuditLog WHERE isSynchronized=0").empty());reopened->close();
}
TEST_F(SyncCheckedClose, LegacyNativeCloseRethrowsOnlyAfterActualCleanup) {
    auto owner=std::make_unique<lattice_db>(config());stop_notifier();owner->add(TestPerson{"pending",1,std::nullopt});connect(*owner);
    factory->state->fail_send=true;
    try{owner->close();FAIL()<<"expected original upload error";}catch(const std::exception& e){EXPECT_STREQ(e.what(),"first actual upload failure");}
    EXPECT_TRUE(owner->is_closed());EXPECT_EQ(sync_discovery_test_access::configured(*owner),nullptr);EXPECT_EQ(factory->state->destroyed.load(),1);
}
TEST_F(SyncCheckedClose, DestructorContainsActualTransportFailureAndReleasesConfiguredOwnership) {
    {auto owner=std::make_unique<lattice_db>(config());stop_notifier();factory->state->fail_disconnect=true;}
    EXPECT_GE(factory->state->disconnects.load(),1);EXPECT_EQ(factory->state->destroyed.load(),1);
    factory->state->fail_disconnect=false;auto reopened=std::make_unique<lattice_db>(config());stop_notifier();
    EXPECT_NE(sync_discovery_test_access::configured(*reopened),nullptr);reopened->close();
}
TEST_F(SyncCheckedClose, OwnedSwiftBridgeResultSurvivesRealConfiguredOwnerDestruction) {
    SchemaVector schemas;swift_schema_entry schema;schema.table_name="CloseBridgeRow";
    property_descriptor property{};property.name="value";property.type=column_type::text;property.kind=property_kind::primitive;
    schema.properties[property.name]=property;schemas.push_back(schema);
#if LATTICE_HAS_FRT
    auto ref=std::unique_ptr<swift_lattice_ref>(swift_lattice_ref::create(config(),schemas));
#else
    auto ref=std::make_unique<swift_lattice_ref>(swift_lattice_ref::create(config(),schemas));
#endif
    ASSERT_TRUE(ref&&ref->valid());stop_notifier();
    swift_dynamic_object raw;raw.table_name=schema.table_name;raw.properties=schema.properties;raw.values["value"]=std::string("pending");
    {dynamic_object object(raw);ref->get()->add(object);}connect(*ref->get());factory->state->fail_send=true;factory->state->fail_disconnect=true;
    static_assert(noexcept(ref->close_checked()));static_assert(noexcept(ref->close()));
    auto result=ref->close_checked();EXPECT_TRUE(ref->get()->is_closed());ref.reset();
    EXPECT_TRUE(result.failed());EXPECT_TRUE(result.cleanup_failed());EXPECT_FALSE(result.cleanup_complete());
    EXPECT_EQ(result.sync_state(),static_cast<int32_t>(sync_drain_state::failed));
    EXPECT_EQ(result.take_message(),"first actual upload failure");EXPECT_FALSE(result.message_unavailable());
    EXPECT_EQ(factory->state->destroyed.load(),1);
}

class drop_drain_scheduler final:public scheduler {
public:
    void invoke(std::function<void()>&&)override{}
    bool is_on_thread()const noexcept override{return false;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
};
TEST_F(SyncDiscoveryContention, DroppedScheduledDrainSettlesAsFailureWithoutDeadlineWait) {
    sync.reset();owner.reset();open(":memory:",std::make_shared<drop_drain_scheduler>());
    sync_discovery_test_access::connected(*sync);
    const auto result=sync->drain_checked(std::chrono::steady_clock::now()+2s);
    EXPECT_EQ(result.state,sync_drain_state::failed);EXPECT_TRUE(result.error);
    EXPECT_TRUE(sync_discovery_test_access::queue(*sync)->failed(sync_discovery_test_access::generation(*sync)));
}
TEST_F(SyncDiscoveryContention, FiniteQueueRejectsDrainInsteadOfReportingFlushedSuccess) {
    sync_discovery_test_access::connected(*sync);const auto q=sync_discovery_test_access::queue(*sync);
    const auto generation=sync_discovery_test_access::generation(*sync);
    auto first=unit(queue::kind::intake,1024,generation);ASSERT_EQ(q->push(first),queue::admission::accepted);
    const auto now=queue::clock::now();ASSERT_EQ(q->begin(q->dispatch(now),now),first);
    for(size_t i=1;i<queue::capacity;++i)ASSERT_EQ(q->push(unit(queue::kind::intake,1024,generation)),queue::admission::accepted);
    const auto result=sync->drain_checked(std::chrono::steady_clock::now()+2s);
    EXPECT_EQ(result.state,sync_drain_state::failed);ASSERT_TRUE(result.error);
    EXPECT_TRUE(q->pending(generation));EXPECT_TRUE(q->failed(generation));EXPECT_EQ(wire->count(),0u);
    sync->disconnect();EXPECT_FALSE(q->pending(sync_discovery_test_access::generation(*sync)));
}

TEST_F(SyncDiscoveryContention, CheckedDrainReportsDrainedOnlyAfterTheActualAckSettles) {
    change("unused-remote","actual-ack");sync_discovery_test_access::connected(*sync);
    auto result=std::async(std::launch::async,[&]{return sync->drain_checked(std::chrono::steady_clock::now()+2s);});
    ASSERT_TRUE(wire->await(1));EXPECT_EQ(result.wait_for(30ms),std::future_status::timeout);
    const auto frame=server_sent_event::from_json(wire->copy().front());ASSERT_TRUE(frame);
    ASSERT_EQ(frame->event_type,server_sent_event::type::audit_log);std::vector<std::string> ids;
    for(const auto& entry:frame->audit_logs)ids.push_back(entry.global_id);
    sync_discovery_test_access::input(*sync,server_sent_event::make_ack(ids));
    ASSERT_EQ(result.wait_for(5s),std::future_status::ready);const auto outcome=result.get();
    EXPECT_EQ(outcome.state,sync_drain_state::drained);EXPECT_FALSE(outcome.error);EXPECT_FALSE(pending());
    EXPECT_EQ(sync_discovery_test_access::in_flight(*sync),0u);
    EXPECT_TRUE(owner->db().query("SELECT id FROM AuditLog WHERE isSynchronized=0").empty());
}

TEST_F(SyncCheckedClose, LegacySwiftBridgeCloseContainsRealCleanupFailure) {
#if LATTICE_HAS_FRT
    auto ref=std::unique_ptr<swift_lattice_ref>(swift_lattice_ref::create(config(),{}));
#else
    auto ref=std::make_unique<swift_lattice_ref>(swift_lattice_ref::create(config(),{}));
#endif
    ASSERT_TRUE(ref&&ref->valid());stop_notifier();factory->state->fail_disconnect=true;
    EXPECT_NO_THROW(ref->close());EXPECT_NE(last_bridge_error().find("cleanup disconnect failure"),std::string::npos);
    EXPECT_TRUE(ref->get()->is_closed());EXPECT_EQ(factory->state->destroyed.load(),1);ref.reset();
}
} // namespace
#endif
