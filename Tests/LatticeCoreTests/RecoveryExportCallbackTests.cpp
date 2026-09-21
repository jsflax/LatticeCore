#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include <condition_variable>
#include <deque>
#include <future>

#ifndef __EMSCRIPTEN__
struct ExportCallbackRow {std::string value;};
LATTICE_SCHEMA(ExportCallbackRow,value);

namespace {
using namespace lattice;
using namespace lattice::detail;
using namespace std::chrono_literals;

// Deliberately generic: queued jobs receive no scheduler-side exception catch.
// Taking a job establishes that a message handler returned before its work ran.
struct callback_scheduler final:scheduler {
    std::mutex mutex;bool closed=false;std::deque<std::function<void()>> jobs;
    static thread_local const callback_scheduler* current;
    void invoke(std::function<void()>&& job)override {
        std::lock_guard<std::mutex> lock(mutex);if(!closed)jobs.push_back(std::move(job));
    }
    bool is_on_thread()const noexcept override{return current==this;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
    void shutdown()override {
        std::deque<std::function<void()>> old;
        {std::lock_guard<std::mutex> lock(mutex);closed=true;old.swap(jobs);}
    }
    std::function<void()> take() {
        std::lock_guard<std::mutex> lock(mutex);if(jobs.empty())return {};
        auto job=std::move(jobs.front());jobs.pop_front();return job;
    }
    void run(std::function<void()> job) {
        struct restore {const callback_scheduler* prior;~restore(){current=prior;}} turn{current};
        current=this;job();
    }
    void drain() {
        for(size_t i=0;i<128;++i){auto job=take();if(!job)return;run(std::move(job));}
        throw std::runtime_error("callback test exceeded finite scheduler turn budget");
    }
};
thread_local const callback_scheduler* callback_scheduler::current=nullptr;

struct callback_wire_state {
    std::mutex mutex;
    sync_transport::on_open_handler opened;
    sync_transport::on_message_handler message;
    sync_transport::on_error_handler error;
    sync_transport::on_close_handler closed;
    std::function<void()> sending;
    std::vector<std::string> frames;
    std::atomic<transport_state> status{transport_state::closed};
    std::atomic<int> connects{0};
    std::promise<void> destroyed;
    std::shared_future<void> destruction=destroyed.get_future().share();
    void open() {
        sync_transport::on_open_handler callback;
        {std::lock_guard<std::mutex> lock(mutex);callback=opened;}
        if(!callback)throw std::runtime_error("missing actual open callback");
        status=transport_state::open;callback();
    }
    void receive(const server_sent_event& event) {
        sync_transport::on_message_handler callback;
        {std::lock_guard<std::mutex> lock(mutex);callback=message;}
        if(!callback)throw std::runtime_error("missing actual message callback");
        const auto json=event.to_json();const auto wire=transport_message::from_binary({json.begin(),json.end()});
        callback(wire);
    }
    void fail(const std::string& reason) {
        sync_transport::on_error_handler callback;
        {std::lock_guard<std::mutex> lock(mutex);callback=error;}
        if(!callback)throw std::runtime_error("missing actual error callback");callback(reason);
    }
    void throw_on_send() {
        std::lock_guard<std::mutex> lock(mutex);
        sending=[]{throw std::runtime_error("actual initial protected send failure");};
    }
    std::vector<std::string> sent(){std::lock_guard<std::mutex> lock(mutex);return frames;}
};
struct callback_wire final:sync_transport {
    std::shared_ptr<callback_wire_state> shared;
    explicit callback_wire(std::shared_ptr<callback_wire_state> s):shared(std::move(s)){}
    ~callback_wire(){shared->destroyed.set_value();}
    void connect(const std::string&,const std::map<std::string,std::string>&)override {
        ++shared->connects;shared->status=transport_state::connecting;
    }
    void disconnect()override{shared->status=transport_state::closed;}
    transport_state state()const override{return shared->status.load();}
    bool supports_reconnect()const override{return false;}
    void send(const transport_message& frame)override {
        const auto state=shared;std::function<void()> callback;
        {std::lock_guard<std::mutex> lock(state->mutex);state->frames.push_back(frame.as_string());callback=state->sending;}
        if(callback)callback();
    }
    void set_on_open(on_open_handler fn)override{std::lock_guard<std::mutex> lock(shared->mutex);shared->opened=std::move(fn);}
    void set_on_message(on_message_handler fn)override{std::lock_guard<std::mutex> lock(shared->mutex);shared->message=std::move(fn);}
    void set_on_error(on_error_handler fn)override{std::lock_guard<std::mutex> lock(shared->mutex);shared->error=std::move(fn);}
    void set_on_close(on_close_handler fn)override{std::lock_guard<std::mutex> lock(shared->mutex);shared->closed=std::move(fn);}
};
struct callback_sync final:synchronizer {
    using synchronizer::synchronizer;
    std::shared_ptr<lattice_db> retained_owner(){return owned_db_;}
};
struct callback_sync_holder {std::unique_ptr<callback_sync> sync;};
struct callback_errors {
    std::mutex mutex;std::vector<std::string> values;
    void add(const std::string& message){std::lock_guard<std::mutex> lock(mutex);values.push_back(message);}
    std::vector<std::string> copy(){std::lock_guard<std::mutex> lock(mutex);return values;}
};
struct callback_ack_hold {
    std::mutex mutex;std::condition_variable ready;bool released=false;
    std::atomic<bool> timed_out{false};
    std::promise<void> completed;std::shared_future<void> completion=completed.get_future().share();
    void wait() {
        std::unique_lock<std::mutex> lock(mutex);
        if(!ready.wait_for(lock,5s,[&]{return released;})) {
            timed_out=true;throw std::runtime_error("callback test ACK custody gate timed out");
        }
    }
    void release(){std::lock_guard<std::mutex> lock(mutex);released=true;ready.notify_all();}
};
void callback_commit(const recovery_install_result& result) {
    if(result.state!=recovery_install_state::committed) {
        if(result.primary_error)std::rethrow_exception(result.primary_error);
        throw std::runtime_error("callback fixture owned transaction did not commit");
    }
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
}
struct callback_claim {recovery_obligation_entry entry;recovery_obligation_scope scope;bool pinned=false;};
class RecoveryExportCallback:public ::testing::Test {
protected:
    std::shared_ptr<callback_scheduler> queue=std::make_shared<callback_scheduler>();
    std::shared_ptr<callback_wire_state> transport=std::make_shared<callback_wire_state>();
    std::shared_ptr<callback_sync_holder> holder=std::make_shared<callback_sync_holder>();
    std::shared_ptr<callback_errors> errors=std::make_shared<callback_errors>();
    std::shared_ptr<callback_ack_hold> ack_hold=std::make_shared<callback_ack_hold>();
    std::shared_ptr<const sync_background_test_hooks::ack_schedule> previous_ack=sync_background_test_hooks::ack;
    std::shared_ptr<lattice_db> owner;
    recovery_obligation_producer_discovery_limits limits{{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
    recovery_obligation_address address;std::string original;
    void SetUp()override {
        configuration cfg(":memory:");cfg.audit_retention_seconds=0;cfg.sched=queue;
        sync_config config;config.sync_id="protected-callback-route";config.checkpoint_passive_interval_ms=0;
        config.upload_coalesce_ms=0;config.ack_timeout_base_ms=10000;
        holder->sync=std::make_unique<callback_sync>(std::make_unique<lattice_db>(cfg),config,std::make_unique<callback_wire>(transport));
        owner=holder->sync->retained_owner();
        callback_commit(recovery_writer_access::install(owner,[&](database&) {
            receive_install_store receiver(owner,limits.installations);receiver.initialize();
            receive_install_binding binding{"callback-contribution","authority","source","epoch","scope","schema"};receiver.bind(binding);
            recovery_obligation_store journal(owner,limits.obligations,limits.installations);journal.initialize();
            address=journal.bind({binding,"grant","receipts"}).address;
        }));
        callback_commit(recovery_local_producer_adapter::enroll_for_qualification(owner,{address,{"ExportCallbackRow"},{'g'}},limits));
        install_error_handler(false,false);queue->drain();
        const auto hold=ack_hold;auto schedule=std::make_shared<sync_background_test_hooks::ack_schedule>();
        schedule->before_expiry=[hold]{hold->wait();};schedule->completed=[hold]{hold->completed.set_value();};
        sync_background_test_hooks::ack=std::move(schedule);
    }
    void TearDown()override {
        holder->sync.reset();ack_hold->release();
        if(!transport->sent().empty())EXPECT_EQ(ack_hold->completion.wait_for(5s),std::future_status::ready);
        EXPECT_FALSE(ack_hold->timed_out.load());sync_background_test_hooks::ack=std::move(previous_ack);
        queue->shutdown();
    }
    void install_error_handler(bool retire,bool throws) {
        const auto observed=errors;std::weak_ptr<callback_sync_holder> weak=holder;
        holder->sync->set_on_error([observed,weak,retire,throws](const std::string& message) {
            observed->add(message);
            if(retire){if(const auto current=weak.lock())current->sync.reset();}
            if(throws)throw std::runtime_error("error callback failed after recording its receipt");
        });
    }
    void add_original() {
        const auto row=owner->add(ExportCallbackRow{"actual generated original"});
        const auto rows=owner->db().query("SELECT globalId FROM AuditLog WHERE tableName='ExportCallbackRow' AND globalRowId=? ORDER BY id DESC LIMIT 1",{row.global_id()});
        if(rows.size()!=1)throw std::runtime_error("missing actual generated callback original");
        original=std::get<std::string>(rows[0].at("globalId"));queue->drain();
    }
    void open_empty() {
        holder->sync->connect();transport->open();queue->drain();
        if(transport->connects!=1||!transport->sent().empty()||!errors->copy().empty())
            throw std::runtime_error("empty protected route did not open cleanly");
    }
    void freeze() {
        recovery_obligation_address next;
        callback_commit(recovery_writer_access::install(owner,[&](database&) {
            recovery_obligation_store journal(owner,limits.obligations,limits.installations);next=journal.freeze(address,1).address;
        }));address=std::move(next);
    }
    callback_claim claim() {
        callback_claim result;
        callback_commit(recovery_writer_access::install(owner,[&](database&) {
            recovery_obligation_store journal(owner,limits.obligations,limits.installations);
            const auto found=journal.find(address,original);const auto scope=journal.read(address.channel);
            if(!found||!scope)throw std::runtime_error("actual callback claim disappeared");
            result.entry=*found;result.scope=*scope;
            result.pinned=journal.pins_audit(found->record.audit_id,found->record.original_id);
        }));return result;
    }
    void expect_error(const std::string& stage,const std::string& cause) {
        const auto reported=errors->copy();ASSERT_EQ(reported.size(),1u);
        EXPECT_NE(reported[0].find(stage),std::string::npos);
        EXPECT_NE(reported[0].find(cause),std::string::npos);
    }
    void expect_original_frame() {
        const auto frames=transport->sent();ASSERT_EQ(frames.size(),1u);
        const auto event=server_sent_event::from_json(frames[0]);ASSERT_TRUE(event);ASSERT_EQ(event->audit_logs.size(),1u);
        EXPECT_EQ(event->audit_logs[0].global_id,original);
    }
};
}

TEST_F(RecoveryExportCallback, DelayedOpenAfterFreezeContainsInitialUploadRefusal) {
    add_original();holder->sync->connect();ASSERT_EQ(transport->connects.load(),1);freeze();
    EXPECT_NO_THROW(transport->open());EXPECT_TRUE(errors->copy().empty());
    auto initial=queue->take();ASSERT_TRUE(initial);
    EXPECT_NO_THROW(queue->run(std::move(initial)));
    EXPECT_NO_THROW(queue->drain());
    expect_error("initial upload","frozen or installed");EXPECT_TRUE(transport->sent().empty());
    const auto state=claim();EXPECT_FALSE(state.entry.first_export_claim);EXPECT_TRUE(state.pinned);
    EXPECT_EQ(state.scope.mode,recovery_obligation_mode::frozen);
}

TEST_F(RecoveryExportCallback, DeferredProtectedIntakeRefusalCannotEscapeGenericScheduler) {
    open_empty();
    audit_log_entry incoming;incoming.id=1;incoming.global_id="aaaaaaaa-0000-4000-8000-000000000901";
    incoming.table_name="ExportCallbackRow";incoming.operation="INSERT";incoming.row_id=1;
    incoming.global_row_id="aaaaaaaa-0000-4000-8000-000000000902";incoming.timestamp="1";
    incoming.changed_fields["value"]=any_property("must not apply");incoming.changed_fields_names={"value"};
    const auto model=owner->db().query("SELECT * FROM ExportCallbackRow");
    const auto audit=owner->db().query("SELECT * FROM AuditLog");
    EXPECT_NO_THROW(transport->receive(server_sent_event::make_audit_log({incoming})));
    EXPECT_TRUE(errors->copy().empty());auto apply=queue->take();ASSERT_TRUE(apply);
    EXPECT_NO_THROW(queue->run(std::move(apply)));
    EXPECT_NO_THROW(queue->drain());
    expect_error("remote intake","unadapted remote intake");EXPECT_TRUE(transport->sent().empty());
    EXPECT_EQ(owner->db().query("SELECT * FROM ExportCallbackRow"),model);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),audit);
}

TEST_F(RecoveryExportCallback, DeferredActualAckAfterFreezeCannotEscapeOrSettleClaim) {
    add_original();holder->sync->connect();transport->open();queue->drain();expect_original_frame();
    const auto sent=claim();ASSERT_TRUE(sent.entry.first_export_claim);freeze();const auto frozen=claim();
    const auto legacy=owner->db().query("SELECT * FROM _lattice_sync_state");
    EXPECT_NO_THROW(transport->receive(server_sent_event::make_ack({original})));
    EXPECT_TRUE(errors->copy().empty());auto ack=queue->take();ASSERT_TRUE(ack);
    EXPECT_NO_THROW(queue->run(std::move(ack)));
    EXPECT_NO_THROW(queue->drain());
    expect_error("transport ACK","frozen generation");EXPECT_EQ(transport->sent().size(),1u);
    const auto refused=claim();EXPECT_EQ(refused.entry,frozen.entry);EXPECT_EQ(refused.scope,frozen.scope);EXPECT_TRUE(refused.pinned);
    EXPECT_EQ(refused.entry.first_export_claim,sent.entry.first_export_claim);
    EXPECT_EQ(refused.entry.stage,recovery_obligation_stage::open);EXPECT_FALSE(refused.entry.acknowledged);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_sync_state"),legacy);
}

TEST_F(RecoveryExportCallback, DeferredProtectedReplayRefusalCannotEscapeGenericScheduler) {
    open_empty();const auto slots=owner->db().query("SELECT * FROM _lattice_replication_slots");
    const auto sync_state=owner->db().query("SELECT * FROM _lattice_sync_state");
    const auto membership=owner->db().query("SELECT * FROM _lattice_sync_set");
    EXPECT_NO_THROW(transport->receive(server_sent_event::make_replay_request()));
    EXPECT_TRUE(errors->copy().empty());auto replay=queue->take();ASSERT_TRUE(replay);
    EXPECT_NO_THROW(queue->run(std::move(replay)));
    EXPECT_NO_THROW(queue->drain());
    expect_error("transport replay","legacy replay reset");EXPECT_TRUE(transport->sent().empty());
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_replication_slots"),slots);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_sync_state"),sync_state);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_sync_set"),membership);
}

TEST_F(RecoveryExportCallback, ThrowingInitialSendAndErrorCallbackKeepCommittedClaim) {
    add_original();transport->throw_on_send();install_error_handler(false,true);holder->sync->connect();
    EXPECT_NO_THROW(transport->open());auto initial=queue->take();ASSERT_TRUE(initial);
    EXPECT_NO_THROW(queue->run(std::move(initial)));
    EXPECT_TRUE(errors->copy().empty());
    EXPECT_NO_THROW(queue->drain());
    expect_error("initial upload","actual initial protected send failure");expect_original_frame();
    const auto state=claim();ASSERT_TRUE(state.entry.first_export_claim);EXPECT_TRUE(state.pinned);
    EXPECT_EQ(state.entry.stage,recovery_obligation_stage::open);EXPECT_FALSE(state.entry.acknowledged);
    ASSERT_TRUE(holder->sync);holder->sync.reset();
    EXPECT_EQ(transport->destruction.wait_for(5s),std::future_status::ready);
    EXPECT_EQ(claim().entry,state.entry);
}

TEST_F(RecoveryExportCallback, InitialSendErrorCallbackCanRetireOwnerThenThrow) {
    add_original();transport->throw_on_send();install_error_handler(true,true);holder->sync->connect();
    EXPECT_NO_THROW(transport->open());auto initial=queue->take();ASSERT_TRUE(initial);
    EXPECT_NO_THROW(queue->run(std::move(initial)));
    EXPECT_TRUE(errors->copy().empty());ASSERT_TRUE(holder->sync);
    const auto committed=claim();ASSERT_TRUE(committed.entry.first_export_claim);
    EXPECT_NO_THROW(queue->drain());
    EXPECT_FALSE(holder->sync);expect_error("initial upload","actual initial protected send failure");expect_original_frame();
    EXPECT_EQ(transport->destruction.wait_for(5s),std::future_status::ready);
    const auto retired=claim();EXPECT_EQ(retired.entry,committed.entry);EXPECT_TRUE(retired.pinned);
    // The retained physical callback cannot revive the retired owner.
    EXPECT_NO_THROW(transport->open());
    EXPECT_NO_THROW(queue->drain());
    EXPECT_EQ(errors->copy().size(),1u);EXPECT_EQ(transport->sent().size(),1u);
}

TEST_F(RecoveryExportCallback, ProtectedTerminalErrorCallbackCanRetireOwnerThenThrow) {
    open_empty();install_error_handler(true,true);
    EXPECT_NO_THROW(transport->fail("actual protected terminal failure"));
    EXPECT_TRUE(errors->copy().empty());ASSERT_TRUE(holder->sync);
    auto terminal=queue->take();ASSERT_TRUE(terminal);
    EXPECT_NO_THROW(queue->run(std::move(terminal)));
    EXPECT_NO_THROW(queue->drain());
    EXPECT_FALSE(holder->sync);
    const auto reported=errors->copy();ASSERT_EQ(reported.size(),1u);EXPECT_EQ(reported[0],"actual protected terminal failure");
    EXPECT_TRUE(transport->sent().empty());EXPECT_EQ(transport->destruction.wait_for(5s),std::future_status::ready);
    EXPECT_NO_THROW(transport->fail("stale protected failure"));
    EXPECT_NO_THROW(queue->drain());
    EXPECT_EQ(errors->copy().size(),1u);
}
#endif
