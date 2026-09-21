#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include "../../Sources/LatticeCore/src/sync_discovery_deferral.hpp"
#include <cstring>
#include <deque>
#include <future>

#ifndef __EMSCRIPTEN__
struct ExportFailureRow {std::string value;};
LATTICE_SCHEMA(ExportFailureRow,value);

namespace lattice::detail {
struct recovery_export_test_access {
    static std::shared_ptr<lattice_db> owner(synchronizer_base& sync){return sync.owned_db_;}
    static void start_and_wake_pacer(synchronizer_base& sync){
        sync.config_.upload_coalesce_ms=1;sync.start_pacer();
        const auto state=sync.pacer_state_;
        {std::lock_guard<std::mutex> lock(state->mutex);
         state->next_allowed_tick=std::chrono::steady_clock::now();state->requested=true;}
        state->ready.notify_one();
    }
    static bool discovery_settled(synchronizer_base& sync){
        const auto generation=sync.reconnect_lifecycle_.load();
        return !sync.discovery_deferral_->pending(generation)&&!sync.discovery_deferral_->failed(generation);
    }
    static void next_ack_timeout(synchronizer_base& sync,int milliseconds){sync.config_.ack_timeout_base_ms=milliseconds;}
};
}
namespace {
using namespace lattice;
using namespace lattice::detail;
using namespace std::chrono_literals;
struct controlled_inline_scheduler final:scheduler {
    std::mutex mutex;bool paused=false,closed=false;std::deque<std::function<void()>> pending;
    static thread_local const controlled_inline_scheduler* current;
    void invoke(std::function<void()>&& work)override{
        {std::lock_guard<std::mutex> lock(mutex);if(closed)return;if(paused){pending.push_back(std::move(work));return;}}
        struct turn {const controlled_inline_scheduler* prior;~turn(){current=prior;}} restore{current};
        current=this;work();
    }
    void pause(bool value){std::lock_guard<std::mutex> lock(mutex);paused=value;}
    size_t pending_count(){std::lock_guard<std::mutex> lock(mutex);return pending.size();}
    bool is_on_thread()const noexcept override{return current==this;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
    void shutdown()override{std::deque<std::function<void()>> old;{std::lock_guard<std::mutex> lock(mutex);closed=true;old.swap(pending);}}
};
thread_local const controlled_inline_scheduler* controlled_inline_scheduler::current=nullptr;
struct wire_state {
    std::mutex mutex;
    sync_transport::on_open_handler opened;
    sync_transport::on_message_handler message;
    sync_transport::on_error_handler error;
    sync_transport::on_close_handler closed;
    std::function<void(const transport_message&)> sending;
    std::vector<std::string> frames;
    std::promise<void> destroyed;
    std::shared_future<void> destruction=destroyed.get_future().share();
    std::atomic<transport_state> state{transport_state::closed};
    void open(){sync_transport::on_open_handler callback;{std::lock_guard<std::mutex> lock(mutex);callback=opened;}state=transport_state::open;callback();}
    void late_close(){sync_transport::on_close_handler callback;{std::lock_guard<std::mutex> lock(mutex);callback=closed;}callback(1000,"late retained callback");}
    void on_send(std::function<void(const transport_message&)> callback){std::lock_guard<std::mutex> lock(mutex);sending=std::move(callback);}
    size_t count(){std::lock_guard<std::mutex> lock(mutex);return frames.size();}
};
struct wire final:sync_transport {
    std::shared_ptr<wire_state> shared;
    explicit wire(std::shared_ptr<wire_state> s):shared(std::move(s)){}
    ~wire(){shared->destroyed.set_value();}
    void connect(const std::string&,const std::map<std::string,std::string>&)override{shared->state=transport_state::connecting;}
    void disconnect()override{shared->state=transport_state::closed;}
    transport_state state()const override{return shared->state.load();}
    bool supports_reconnect()const override{return false;}
    void send(const transport_message& frame)override{
        const auto state=shared;std::function<void(const transport_message&)> callback;
        {std::lock_guard<std::mutex> lock(state->mutex);state->frames.push_back(frame.as_string());callback=state->sending;}
        if(callback)callback(frame);
    }
    void set_on_open(on_open_handler fn)override{std::lock_guard<std::mutex> lock(shared->mutex);shared->opened=std::move(fn);}
    void set_on_message(on_message_handler fn)override{std::lock_guard<std::mutex> lock(shared->mutex);shared->message=std::move(fn);}
    void set_on_error(on_error_handler fn)override{std::lock_guard<std::mutex> lock(shared->mutex);shared->error=std::move(fn);}
    void set_on_close(on_close_handler fn)override{std::lock_guard<std::mutex> lock(shared->mutex);shared->closed=std::move(fn);}
};
void committed(const recovery_install_result& result){
    if(result.state!=recovery_install_state::committed){if(result.primary_error)std::rethrow_exception(result.primary_error);throw std::runtime_error("fixture owned operation did not commit");}
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
}
struct ack_gate {
    std::promise<void> entered,release,complete;
    std::shared_future<void> arrival=entered.get_future().share(),proceed=release.get_future().share(),finished=complete.get_future().share();
    std::atomic<bool> released{false};
    void allow(){if(!released.exchange(true))release.set_value();}
    ~ack_gate(){allow();}
};
struct release_ack_gate {std::shared_ptr<ack_gate> gate;~release_ack_gate(){gate->allow();}};
struct installed_ack_hook {
    std::shared_ptr<const sync_background_test_hooks::ack_schedule> prior=sync_background_test_hooks::ack;
    explicit installed_ack_hook(const std::shared_ptr<ack_gate>& gate){
        auto schedule=std::make_shared<sync_background_test_hooks::ack_schedule>();
        schedule->before_expiry=[gate]{gate->entered.set_value();if(gate->proceed.wait_for(5s)!=std::future_status::ready)throw std::runtime_error("test ACK gate timed out");};
        schedule->completed=[gate]{gate->complete.set_value();};sync_background_test_hooks::ack=schedule;
    }
    ~installed_ack_hook(){sync_background_test_hooks::ack=std::move(prior);}
};
class RecoveryExportAdmission : public ::testing::Test {
protected:
    std::shared_ptr<controlled_inline_scheduler> scheduler=std::make_shared<controlled_inline_scheduler>();
    std::shared_ptr<wire_state> transport=std::make_shared<wire_state>();
    std::unique_ptr<synchronizer> sync;std::mutex sync_mutex;
    std::shared_ptr<lattice_db> owner;
    recovery_obligation_producer_discovery_limits limits{{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
    recovery_obligation_address address;
    std::string original;
    void SetUp()override{
        configuration cfg(":memory:");cfg.audit_retention_seconds=0;cfg.sched=scheduler;
        sync_config config;config.sync_id="protected-test-route";config.checkpoint_passive_interval_ms=0;config.upload_coalesce_ms=0;
        sync=std::make_unique<synchronizer>(std::make_unique<lattice_db>(cfg),config,std::make_unique<wire>(transport));
        owner=recovery_export_test_access::owner(*sync);
        committed(recovery_writer_access::install(owner,[&](database&){
            receive_install_store receiver(owner,limits.installations);receiver.initialize();
            receive_install_binding binding{"contribution","authority","source","epoch","scope","schema"};receiver.bind(binding);
            recovery_obligation_store journal(owner,limits.obligations,limits.installations);journal.initialize();address=journal.bind({binding,"grant","receipts"}).address;
        }));
        committed(recovery_local_producer_adapter::enroll_for_qualification(owner,{address,{"ExportFailureRow"},{'g'}},limits));
    }
    void retire(){std::unique_ptr<synchronizer> retiring;{std::lock_guard<std::mutex> lock(sync_mutex);retiring=std::move(sync);}retiring.reset();}
    bool has_sync(){std::lock_guard<std::mutex> lock(sync_mutex);return bool(sync);}
    void TearDown()override{retire();scheduler->shutdown();}
    void add_while_dispatch_paused(){
        scheduler->pause(true);const auto row=owner->add(ExportFailureRow{"real generated original"});
        const auto rows=owner->db().query("SELECT globalId FROM AuditLog WHERE tableName='ExportFailureRow' AND globalRowId=? ORDER BY id DESC LIMIT 1",{row.global_id()});
        original=std::get<std::string>(rows.at(0).at("globalId"));scheduler->pause(false);
    }
    void open_empty(){sync->connect();transport->open();EXPECT_EQ(transport->count(),0u);}
    std::optional<int64_t> first_claim(){
        std::optional<int64_t> claim;
        committed(recovery_writer_access::install(owner,[&](database&){
            recovery_obligation_store journal(owner,limits.obligations,limits.installations);
            const auto entry=journal.find(address,original);if(!entry)throw std::runtime_error("missing generated obligation");claim=entry->first_export_claim;
        }));return claim;
    }
    void freeze(){
        recovery_obligation_address next;
        committed(recovery_writer_access::install(owner,[&](database&){
            recovery_obligation_store journal(owner,limits.obligations,limits.installations);
            next=journal.freeze(address,1).address;
        }));
        // A rolled-back freeze never publishes its provisional generation.
        address=std::move(next);
    }
};
}

TEST_F(RecoveryExportAdmission, DrainCanLoseOwnerInsideActualProtectedSend) {
    open_empty();add_while_dispatch_paused();bool durable_before_handoff=false;
    transport->on_send([&](const transport_message& frame){
        const auto parsed=server_sent_event::from_json(frame.as_string());ASSERT_TRUE(parsed);ASSERT_EQ(parsed->audit_logs.size(),1u);
        EXPECT_EQ(parsed->audit_logs[0].global_id,original);durable_before_handoff=first_claim().has_value();retire();
    });
    auto* caller=sync.get();EXPECT_NO_THROW(caller->drain(std::chrono::steady_clock::now()+2s));
    EXPECT_FALSE(has_sync());EXPECT_TRUE(durable_before_handoff);EXPECT_EQ(transport->count(),1u);
    transport->late_close();EXPECT_TRUE(first_claim());
    EXPECT_EQ(transport->destruction.wait_for(5s),std::future_status::ready);
}

TEST_F(RecoveryExportAdmission, DrainPreservesThrowAfterSendDestroysOwner) {
    open_empty();add_while_dispatch_paused();
    transport->on_send([&](const transport_message&){retire();throw std::runtime_error("send failed after retirement");});
    auto* caller=sync.get();
    try{caller->drain(std::chrono::steady_clock::now()+2s);FAIL()<<"missing original send failure";}
    catch(const std::runtime_error& e){EXPECT_EQ(std::string(e.what()),"send failed after retirement");}
    EXPECT_FALSE(has_sync());EXPECT_EQ(transport->count(),1u);EXPECT_TRUE(first_claim());
    EXPECT_EQ(transport->destruction.wait_for(5s),std::future_status::ready);
}

TEST_F(RecoveryExportAdmission, PacerFreezeRefusalIsReportedWithoutBytesOrProcessFailure) {
    open_empty();
    // The actual initial discovery must finish before observer suppression.
    // Atomic first-turn admission makes this caller's inline completion a
    // deterministic boundary; zero sent bytes alone was not that proof.
    ASSERT_TRUE(recovery_export_test_access::discovery_settled(*sync));
    ASSERT_EQ(scheduler->pending_count(),0u);
    add_while_dispatch_paused();
    ASSERT_TRUE(recovery_export_test_access::discovery_settled(*sync));
    freeze();std::promise<std::string> error;auto reported=error.get_future();
    sync->set_on_error([&](const std::string& message){error.set_value(message);throw std::runtime_error("error callback itself failed");});
    recovery_export_test_access::start_and_wake_pacer(*sync);
    const bool ready=reported.wait_for(5s)==std::future_status::ready;EXPECT_TRUE(ready);
    if(ready)EXPECT_NE(reported.get().find("frozen or installed"),std::string::npos);
    retire();EXPECT_EQ(transport->count(),0u);EXPECT_FALSE(first_claim());
    EXPECT_NO_THROW(owner->add(ExportFailureRow{"local successor remains admitted"}));
    EXPECT_EQ(transport->destruction.wait_for(5s),std::future_status::ready);
    // Only intentionally suppressed observer callbacks remain. Retire first,
    // then settle their capture ownership without executing a stale upload.
    scheduler->shutdown();EXPECT_EQ(scheduler->pending_count(),0u);
}

TEST_F(RecoveryExportAdmission, PacerSendExceptionReportsFailureAndKeepsCommittedClaim) {
    open_empty();add_while_dispatch_paused();std::promise<std::string> error;auto reported=error.get_future();
    sync->set_on_error([&](const std::string& message){error.set_value(message);});
    transport->on_send([](const transport_message&){throw std::runtime_error("injected physical send failure");});
    recovery_export_test_access::start_and_wake_pacer(*sync);
    const bool ready=reported.wait_for(5s)==std::future_status::ready;EXPECT_TRUE(ready);
    if(ready)EXPECT_NE(reported.get().find("injected physical send failure"),std::string::npos);
    retire();EXPECT_EQ(transport->count(),1u);EXPECT_TRUE(first_claim());
    EXPECT_EQ(transport->destruction.wait_for(5s),std::future_status::ready);
}

TEST_F(RecoveryExportAdmission, PacerActualSendCanRetireItsOwnSynchronizerAndThread) {
    open_empty();add_while_dispatch_paused();std::promise<void> retired;auto completion=retired.get_future();
    transport->on_send([&](const transport_message&){retire();retired.set_value();});
    recovery_export_test_access::start_and_wake_pacer(*sync);
    const bool ready=completion.wait_for(5s)==std::future_status::ready;EXPECT_TRUE(ready);
    if(!ready)retire();
    EXPECT_FALSE(has_sync());EXPECT_EQ(transport->count(),1u);EXPECT_TRUE(first_claim());
    EXPECT_EQ(transport->destruction.wait_for(5s),std::future_status::ready);
}

TEST_F(RecoveryExportAdmission, ActualAckRetryRefusesFrozenScopeAndPreservesCommittedClaim) {
    add_while_dispatch_paused();recovery_export_test_access::next_ack_timeout(*sync,0);
    auto gate=std::make_shared<ack_gate>();release_ack_gate cleanup{gate};std::promise<std::string> error;auto reported=error.get_future();
    sync->set_on_error([&](const std::string& message){error.set_value(message);});
    {installed_ack_hook hook(gate);sync->connect();transport->open();}
    EXPECT_EQ(gate->arrival.wait_for(5s),std::future_status::ready);ASSERT_EQ(transport->count(),1u);
    const auto before=first_claim();ASSERT_TRUE(before);freeze();gate->allow();
    const bool finished=gate->finished.wait_for(5s)==std::future_status::ready;EXPECT_TRUE(finished);
    const bool ready=reported.wait_for(5s)==std::future_status::ready;EXPECT_TRUE(ready);
    if(ready)EXPECT_NE(reported.get().find("frozen or installed"),std::string::npos);
    retire();EXPECT_EQ(transport->count(),1u);EXPECT_EQ(first_claim(),before);
    EXPECT_EQ(transport->destruction.wait_for(5s),std::future_status::ready);
}

TEST_F(RecoveryExportAdmission, ActualAckRetryCanRetireOwnerWithoutHoldingAckLeaf) {
    add_while_dispatch_paused();recovery_export_test_access::next_ack_timeout(*sync,0);auto gate=std::make_shared<ack_gate>();release_ack_gate cleanup{gate};
    {installed_ack_hook hook(gate);sync->connect();transport->open();}
    EXPECT_EQ(gate->arrival.wait_for(5s),std::future_status::ready);ASSERT_EQ(transport->count(),1u);
    const auto before=first_claim();ASSERT_TRUE(before);
    // The first worker already captured zero. The retry's own watchdog must
    // not race another immediate expiration into this two-frame schedule.
    recovery_export_test_access::next_ack_timeout(*sync,10000);
    transport->on_send([&](const transport_message&){retire();});gate->allow();
    const bool finished=gate->finished.wait_for(5s)==std::future_status::ready;EXPECT_TRUE(finished);
    if(!finished)retire();
    EXPECT_FALSE(has_sync());EXPECT_EQ(transport->count(),2u);EXPECT_EQ(first_claim(),before);
    EXPECT_EQ(transport->destruction.wait_for(5s),std::future_status::ready);
}
namespace {
struct claim_fault_scope {
    enum class kind { deny_commit, ignore_first_export };
    kind mode;int hits=0;
    recovery_local_producer_test_hooks::authorizer_fault fault;
    const recovery_local_producer_test_hooks::authorizer_fault* previous_fault;
    claim_fault_scope* previous_scope;
    static thread_local claim_fault_scope* current;
    static int restrict_action(int action,const char* one,const char* two,const char*) noexcept {
        auto& self=*current;
        if(self.mode==kind::deny_commit && action==SQLITE_TRANSACTION && one && std::strcmp(one,"COMMIT")==0) {
            ++self.hits;return SQLITE_DENY;
        }
        if(self.mode==kind::ignore_first_export && action==SQLITE_UPDATE && one && two &&
           std::strcmp(one,"_lattice_obligation_entry")==0 && std::strcmp(two,"first_export")==0) {
            ++self.hits;return SQLITE_IGNORE;
        }
        return SQLITE_OK;
    }
    claim_fault_scope(const lattice_db* owner,kind value):mode(value),fault{owner,&restrict_action},
        previous_fault(recovery_local_producer_test_hooks::fault),previous_scope(current) {
        current=this;recovery_local_producer_test_hooks::fault=&fault;
    }
    ~claim_fault_scope(){recovery_local_producer_test_hooks::fault=previous_fault;current=previous_scope;}
};
thread_local claim_fault_scope* claim_fault_scope::current=nullptr;

struct claim_commit_hooks {
    std::function<void()> before,after;
    void (*previous_before)()=recovery_export_test_hooks::before_claim_commit;
    void (*previous_after)()=recovery_export_test_hooks::after_claim_commit;
    claim_commit_hooks* previous;
    static thread_local claim_commit_hooks* current;
    claim_commit_hooks(std::function<void()> b,std::function<void()> a):before(std::move(b)),after(std::move(a)),previous(current) {
        current=this;
        recovery_export_test_hooks::before_claim_commit=[] {if(current->before)current->before();};
        recovery_export_test_hooks::after_claim_commit=[] {if(current->after)current->after();};
    }
    ~claim_commit_hooks(){
        recovery_export_test_hooks::before_claim_commit=previous_before;
        recovery_export_test_hooks::after_claim_commit=previous_after;current=previous;
    }
};
thread_local claim_commit_hooks* claim_commit_hooks::current=nullptr;

struct durable_claim_state {
    recovery_obligation_entry entry;
    recovery_obligation_scope scope;
    recovery_obligation_usage usage;
    bool pinned=false;
};
durable_claim_state read_claim_state(const std::shared_ptr<lattice_db>& owner,
    const recovery_obligation_producer_discovery_limits& limits,const recovery_obligation_address& address,
    const std::string& original) {
    durable_claim_state result;
    committed(recovery_writer_access::install(owner,[&](database&) {
        recovery_obligation_store journal(owner,limits.obligations,limits.installations);
        const auto scope=journal.read(address.channel);const auto entry=journal.find(address,original);
        if(!scope||!entry)throw std::runtime_error("missing actual generated claim state");
        result.scope=*scope;result.entry=*entry;result.usage=journal.usage();
        result.pinned=journal.pins_audit(entry->record.audit_id,entry->record.original_id);
    }));
    return result;
}
std::vector<std::string> sent_frames(const std::shared_ptr<wire_state>& transport) {
    std::lock_guard<std::mutex> lock(transport->mutex);return transport->frames;
}
}

TEST_F(RecoveryExportAdmission, ActualClaimCommitDenialSendsNothingAndRetryKeepsOriginalClaim) {
    open_empty();add_while_dispatch_paused();
    const auto before=read_claim_state(owner,limits,address,original);ASSERT_FALSE(before.entry.first_export_claim);
    const auto counters=owner->db().query("SELECT * FROM _lattice_obligation_store");
    const auto audit=owner->db().query("SELECT * FROM AuditLog WHERE globalId=?",{original});
    int prepared=0,after_commit=0,denied=0;std::optional<int64_t> provisional;
    {
        claim_commit_hooks hooks([&] {
            ++prepared;recovery_obligation_store journal(owner,limits.obligations,limits.installations);
            const auto entry=journal.find(address,original);
            if(!entry||!entry->first_export_claim)throw std::runtime_error("claim not written before actual COMMIT denial");
            provisional=entry->first_export_claim;
        },[&]{++after_commit;});
        claim_fault_scope fault(owner.get(),claim_fault_scope::kind::deny_commit);
        EXPECT_THROW(sync->drain(std::chrono::steady_clock::now()),db_error);
        denied=fault.hits;
    }
    EXPECT_EQ(prepared,1);EXPECT_EQ(denied,1);EXPECT_EQ(after_commit,0);ASSERT_TRUE(provisional);
    EXPECT_EQ(transport->count(),0u);EXPECT_FALSE(owner->db().is_in_transaction());
    const auto refused=read_claim_state(owner,limits,address,original);
    EXPECT_EQ(refused.entry,before.entry);EXPECT_EQ(refused.scope,before.scope);EXPECT_EQ(refused.usage,before.usage);
    EXPECT_TRUE(refused.pinned);EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_obligation_store"),counters);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog WHERE globalId=?",{original}),audit);
    EXPECT_NO_THROW(sync->drain(std::chrono::steady_clock::now()));
    const auto frames=sent_frames(transport);ASSERT_EQ(frames.size(),1u);
    const auto sent=server_sent_event::from_json(frames[0]);ASSERT_TRUE(sent);ASSERT_EQ(sent->audit_logs.size(),1u);
    EXPECT_EQ(sent->audit_logs[0].global_id,original);
    const auto retried=read_claim_state(owner,limits,address,original);ASSERT_TRUE(retried.entry.first_export_claim);
    EXPECT_EQ(retried.entry.first_export_claim,provisional);EXPECT_EQ(retried.entry.record,before.entry.record);
    EXPECT_EQ(retried.entry.stage,recovery_obligation_stage::open);EXPECT_FALSE(retried.entry.acknowledged);EXPECT_TRUE(retried.pinned);
    retire();EXPECT_EQ(first_claim(),retried.entry.first_export_claim);
}

TEST_F(RecoveryExportAdmission, IgnoredActualFirstClaimRefusesPostimageAndRetrySendsOriginal) {
    open_empty();add_while_dispatch_paused();
    const auto before=read_claim_state(owner,limits,address,original);ASSERT_FALSE(before.entry.first_export_claim);
    const auto counters=owner->db().query("SELECT * FROM _lattice_obligation_store");
    const auto audit=owner->db().query("SELECT * FROM AuditLog WHERE globalId=?",{original});
    int ignored=0,prepared=0,after_commit=0;std::string failure;
    {
        claim_commit_hooks hooks([&]{++prepared;},[&]{++after_commit;});
        claim_fault_scope fault(owner.get(),claim_fault_scope::kind::ignore_first_export);
        try{sync->drain(std::chrono::steady_clock::now());ADD_FAILURE()<<"ignored claim unexpectedly sent";}
        catch(const recovery_obligation_error& error){
            EXPECT_EQ(error.code,recovery_obligation_error_code::corrupt_state);failure=error.what();
        }
        ignored=fault.hits;
    }
    EXPECT_GT(ignored,0);EXPECT_NE(failure.find("postimage"),std::string::npos);
    EXPECT_EQ(prepared,0);EXPECT_EQ(after_commit,0);EXPECT_EQ(transport->count(),0u);
    EXPECT_FALSE(owner->db().is_in_transaction());
    const auto refused=read_claim_state(owner,limits,address,original);
    EXPECT_EQ(refused.entry,before.entry);EXPECT_EQ(refused.scope,before.scope);EXPECT_EQ(refused.usage,before.usage);
    EXPECT_TRUE(refused.pinned);EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_obligation_store"),counters);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog WHERE globalId=?",{original}),audit);
    EXPECT_NO_THROW(sync->drain(std::chrono::steady_clock::now()));
    const auto frames=sent_frames(transport);ASSERT_EQ(frames.size(),1u);
    const auto sent=server_sent_event::from_json(frames[0]);ASSERT_TRUE(sent);ASSERT_EQ(sent->audit_logs.size(),1u);
    EXPECT_EQ(sent->audit_logs[0].global_id,original);
    const auto retried=read_claim_state(owner,limits,address,original);ASSERT_TRUE(retried.entry.first_export_claim);
    EXPECT_EQ(retried.entry.record,before.entry.record);EXPECT_EQ(retried.entry.stage,recovery_obligation_stage::open);
    EXPECT_FALSE(retried.entry.acknowledged);EXPECT_TRUE(retried.pinned);
    retire();EXPECT_EQ(first_claim(),retried.entry.first_export_claim);
}

TEST_F(RecoveryExportAdmission, FreezeAfterActualClaimCommitRefusesHandoffAndKeepsPinnedClaim) {
    open_empty();add_while_dispatch_paused();const auto prior_address=address;
    int after_commit=0;std::optional<int64_t> known_claim;std::optional<durable_claim_state> claimed;
    {
        claim_commit_hooks hooks({},[&] {
            ++after_commit;
            if(owner->db().is_in_transaction())throw std::runtime_error("claim seam ran before transaction settlement");
            claimed=read_claim_state(owner,limits,address,original);known_claim=claimed->entry.first_export_claim;
            if(!known_claim)throw std::runtime_error("after-COMMIT seam has no committed claim");
            freeze();
        });
        EXPECT_THROW(sync->drain(std::chrono::steady_clock::now()),db_error);
    }
    EXPECT_EQ(after_commit,1);ASSERT_TRUE(known_claim);ASSERT_TRUE(claimed);
    EXPECT_EQ(transport->count(),0u);EXPECT_FALSE(owner->db().is_in_transaction());
    EXPECT_EQ(address.incarnation,prior_address.incarnation);EXPECT_EQ(address.generation,prior_address.generation+1);
    const auto frozen=read_claim_state(owner,limits,address,original);
    EXPECT_EQ(frozen.entry,claimed->entry);EXPECT_EQ(frozen.usage,claimed->usage);EXPECT_TRUE(frozen.pinned);
    EXPECT_EQ(frozen.scope.mode,recovery_obligation_mode::frozen);
    EXPECT_GE(frozen.scope.freeze_export_high_water,*known_claim);
    EXPECT_EQ(frozen.entry.stage,recovery_obligation_stage::open);EXPECT_FALSE(frozen.entry.acknowledged);
    retire();EXPECT_EQ(first_claim(),known_claim);
}

TEST_F(RecoveryExportAdmission, ActualBareAckOnlyUpdatesLegacyBookkeepingAndCannotSettleObligation) {
    open_empty();add_while_dispatch_paused();
    struct callback_state {std::mutex mutex;std::vector<std::string> errors,completed_ids;};
    const auto callbacks=std::make_shared<callback_state>();
    sync->set_on_error([callbacks](const std::string& error){
        std::lock_guard<std::mutex> lock(callbacks->mutex);callbacks->errors.push_back(error);
    });
    sync->set_on_sync_complete([callbacks](const std::vector<std::string>& ids){
        std::lock_guard<std::mutex> lock(callbacks->mutex);callbacks->completed_ids=ids;
    });
    EXPECT_NO_THROW(sync->drain(std::chrono::steady_clock::now()));
    const auto frames=sent_frames(transport);ASSERT_EQ(frames.size(),1u);
    const auto sent=server_sent_event::from_json(frames[0]);ASSERT_TRUE(sent);ASSERT_EQ(sent->audit_logs.size(),1u);
    EXPECT_EQ(sent->audit_logs[0].global_id,original);
    const auto before=read_claim_state(owner,limits,address,original);ASSERT_TRUE(before.entry.first_export_claim);
    const auto counters=owner->db().query("SELECT * FROM _lattice_obligation_store");
    const auto audit=owner->db().query("SELECT * FROM AuditLog WHERE globalId=?",{original});
    ASSERT_TRUE(owner->db().query("SELECT 1 FROM _lattice_sync_state WHERE audit_entry_id=? AND sync_id=? AND is_synchronized=1",
        {before.entry.record.audit_id,std::string("protected-test-route")}).empty());
    sync_transport::on_message_handler receive;
    {std::lock_guard<std::mutex> lock(transport->mutex);receive=transport->message;}
    ASSERT_TRUE(receive);
    const auto encoded=server_sent_event::make_ack({original}).to_json();
    const auto ack=transport_message::from_binary({encoded.begin(),encoded.end()});
    EXPECT_NO_THROW(receive(ack));
    {std::lock_guard<std::mutex> lock(callbacks->mutex);
     EXPECT_TRUE(callbacks->errors.empty());EXPECT_EQ(callbacks->completed_ids,std::vector<std::string>{original});}
    const auto legacy=owner->db().query("SELECT is_synchronized FROM _lattice_sync_state WHERE audit_entry_id=? AND sync_id=?",
        {before.entry.record.audit_id,std::string("protected-test-route")});
    ASSERT_EQ(legacy.size(),1u);EXPECT_EQ(std::get<int64_t>(legacy[0].at("is_synchronized")),1);
    const auto acknowledged=read_claim_state(owner,limits,address,original);
    EXPECT_EQ(acknowledged.entry,before.entry);EXPECT_EQ(acknowledged.scope,before.scope);EXPECT_EQ(acknowledged.usage,before.usage);
    EXPECT_EQ(acknowledged.entry.stage,recovery_obligation_stage::open);EXPECT_FALSE(acknowledged.entry.acknowledged);
    EXPECT_EQ(acknowledged.entry.settled_install_sequence,0);EXPECT_TRUE(acknowledged.pinned);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_obligation_store"),counters);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog WHERE globalId=?",{original}),audit);
    EXPECT_NO_THROW(sync->drain(std::chrono::steady_clock::now()));EXPECT_EQ(transport->count(),1u);
    retire();EXPECT_EQ(first_claim(),before.entry.first_export_claim);
}
#endif
