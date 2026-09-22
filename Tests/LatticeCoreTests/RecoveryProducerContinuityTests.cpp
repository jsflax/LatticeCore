#include "TestHelpers.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/recovery_producer_continuity.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include <deque>
#include <condition_variable>
#include <future>
#include <fstream>
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <spawn.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif
extern char** environ;
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
struct ContinuousSharedRow {std::string value;};
LATTICE_SCHEMA(ContinuousSharedRow,value);
struct ContinuousLocalRow {std::string value;};
LATTICE_SCHEMA(ContinuousLocalRow,value);
namespace {
using namespace lattice;
using namespace lattice::detail;
class continuity_queue final:public scheduler {
    std::mutex mutex_;std::deque<std::function<void()>> jobs_;bool stopped_=false;
    static thread_local const continuity_queue* current_;
public:
    void invoke(std::function<void()>&& job)override {std::lock_guard<std::mutex> lock(mutex_);if(!stopped_){if(jobs_.size()==256)throw db_error("continuity fixture queue full");jobs_.push_back(std::move(job));}}
    bool is_on_thread()const noexcept override{return current_==this;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
    void shutdown()override{std::deque<std::function<void()>> old;{std::lock_guard<std::mutex> lock(mutex_);stopped_=true;old.swap(jobs_);}}
    void drain(){for(size_t n=0;n<256;++n){std::function<void()> job;{std::lock_guard<std::mutex> lock(mutex_);if(jobs_.empty())return;job=std::move(jobs_.front());jobs_.pop_front();}
        struct restore{const continuity_queue* old;~restore(){current_=old;}} prior{current_};current_=this;job();}throw db_error("continuity fixture turn budget exceeded");}
};
thread_local const continuity_queue* continuity_queue::current_=nullptr;
struct continuity_wire_state {
    std::mutex mutex;sync_transport::on_open_handler opened;std::function<void()> sending;
    sync_transport::on_message_handler received;
    std::vector<std::string> frames;std::atomic<transport_state> state{transport_state::closed};
    std::promise<void> destroyed;std::shared_future<void> destruction=destroyed.get_future().share();
    void open(){sync_transport::on_open_handler callback;{std::lock_guard<std::mutex> lock(mutex);callback=opened;}state=transport_state::open;callback();}
    size_t count(){std::lock_guard<std::mutex> lock(mutex);return frames.size();}
    void ack(const std::vector<std::string>& ids){sync_transport::on_message_handler callback;
        {std::lock_guard<std::mutex> lock(mutex);callback=received;}
        if(!callback)throw db_error("continuity fixture lacks actual receive callback");
        callback(transport_message::from_string(server_sent_event::make_ack(ids).to_json()));}
    std::vector<std::vector<std::string>> audit_batches(){std::vector<std::string> copied;
        {std::lock_guard<std::mutex> lock(mutex);copied=frames;}
        std::vector<std::vector<std::string>> batches;
        for(const auto& raw:copied){const auto event=server_sent_event::from_json(raw);
            if(!event||event->event_type!=server_sent_event::type::audit_log)continue;
            std::vector<std::string> ids;for(const auto& entry:event->audit_logs)ids.push_back(entry.global_id);batches.push_back(std::move(ids));}
        return batches;}
};
class continuity_wire final:public sync_transport {
    std::shared_ptr<continuity_wire_state> state_;
public:
    explicit continuity_wire(std::shared_ptr<continuity_wire_state> state):state_(std::move(state)){}
    ~continuity_wire(){state_->destroyed.set_value();}
    void connect(const std::string&,const std::map<std::string,std::string>&)override{state_->state=transport_state::connecting;}
    void disconnect()override{state_->state=transport_state::closed;}
    transport_state state()const override{return state_->state.load();}
    bool supports_reconnect()const override{return false;}
    void send(const transport_message& message)override {std::function<void()> callback;{std::lock_guard<std::mutex> lock(state_->mutex);state_->frames.push_back(message.as_string());callback=state_->sending;}if(callback)callback();}
    void set_on_open(on_open_handler value)override{std::lock_guard<std::mutex> lock(state_->mutex);state_->opened=std::move(value);}
    void set_on_message(on_message_handler value)override{std::lock_guard<std::mutex> lock(state_->mutex);state_->received=std::move(value);}
    void set_on_error(on_error_handler)override{}
    void set_on_close(on_close_handler)override{}
};
class continuity_factory final:public network_factory {
public:
    std::vector<std::shared_ptr<continuity_wire_state>> wires;
    std::unique_ptr<http_client> create_http_client()override{return std::make_unique<null_http_client>();}
    std::unique_ptr<sync_transport> create_sync_transport()override {auto state=std::make_shared<continuity_wire_state>();wires.push_back(state);return std::make_unique<continuity_wire>(std::move(state));}
};
// This selection fixture owns the send/ACK order. Park the existing retry
// worker before its clock starts; do not lengthen production ACK deadlines.
struct continuity_ack_pause {
    struct state {std::mutex mutex;std::condition_variable ready;bool released=false,timed_out=false;size_t finished=0;};
    std::shared_ptr<state> held=std::make_shared<state>();
    std::shared_ptr<const sync_background_test_hooks::ack_schedule> prior=sync_background_test_hooks::ack;
    std::vector<std::unique_ptr<synchronizer>>& senders;
    std::shared_ptr<continuity_factory> factory;
    continuity_ack_pause(std::vector<std::unique_ptr<synchronizer>>& s,std::shared_ptr<continuity_factory> f):senders(s),factory(std::move(f)){
        const auto gate=held;auto schedule=std::make_shared<sync_background_test_hooks::ack_schedule>();
        schedule->before_expiry=[gate]{std::unique_lock<std::mutex> lock(gate->mutex);
            if(!gate->ready.wait_for(lock,std::chrono::seconds(30),[&]{return gate->released;})){
                gate->timed_out=true;throw db_error("continuous selection fixture ACK hold expired");}};
        schedule->completed=[gate]{std::lock_guard<std::mutex> lock(gate->mutex);++gate->finished;gate->ready.notify_all();};
        sync_background_test_hooks::ack=std::move(schedule);
    }
    ~continuity_ack_pause(){
        // Close actual owner lifetimes first, including every assertion exit.
        // Released workers then observe retirement before touching the sender.
        senders.clear();sync_background_test_hooks::ack=prior;
        size_t expected=0;for(const auto& wire:factory->wires)expected+=wire->audit_batches().size();
        std::unique_lock<std::mutex> lock(held->mutex);held->released=true;held->ready.notify_all();
        const bool completed=held->ready.wait_for(lock,std::chrono::seconds(5),[&]{return held->finished==expected;});
        const bool timed_out=held->timed_out;lock.unlock();
        EXPECT_TRUE(completed);EXPECT_FALSE(timed_out);
    }
};
void known_commit(const recovery_install_result& result) {
    if(result.state!=recovery_install_state::committed){if(result.primary_error)std::rethrow_exception(result.primary_error);throw db_error("continuity fixture operation did not commit");}
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
}
int64_t number(database& db,const std::string& sql){return std::get<int64_t>(db.query(sql).at(0).at("n"));}
// Failure-only diagnostics for the actual read-only constructor. Do not add a
// preliminary SQLite open, change flags, or manufacture missing WAL sidecars.
database diagnostic_readonly_open(const std::string& path,const char* phase) {
    try { return database(path,database::open_mode::read_only); }
    catch(const db_error& original) {
        std::string context="readonly fixture phase="+std::string(phase)+" path="+path;
        for(const auto& suffix:{std::string(),std::string("-wal"),std::string("-shm")}) {
            std::error_code status_error,size_error;
            const auto state=std::filesystem::symlink_status(path+suffix,status_error);
            const auto size=std::filesystem::file_size(path+suffix,size_error);
            context+=" [after-failure suffix="+(suffix.empty()?std::string("main"):suffix)+
                " type="+std::to_string(static_cast<int>(state.type()))+
                " permissions="+std::to_string(static_cast<unsigned>(state.permissions()))+
                " status_error="+std::to_string(status_error.value())+
                " size="+std::to_string(size)+" size_error="+std::to_string(size_error.value())+"]";
        }
        throw db_error(context+": "+original.what());
    }
}
struct continuity_fault {
    recovery_local_producer_test_hooks::authorizer_fault fault;
    const recovery_local_producer_test_hooks::authorizer_fault* prior;
    continuity_fault(const lattice_db* owner,int (*callback)(int,const char*,const char*,const char*) noexcept):fault{owner,callback},prior(recovery_local_producer_test_hooks::fault){recovery_local_producer_test_hooks::fault=&fault;}
    ~continuity_fault(){recovery_local_producer_test_hooks::fault=prior;}
};
thread_local std::function<void()> continuity_claim_action;
struct continuity_claim_hook {
    void(*prior)()=recovery_export_test_hooks::before_claim_commit;
    std::function<void()> old=std::move(continuity_claim_action);
    explicit continuity_claim_hook(std::function<void()> action){continuity_claim_action=std::move(action);recovery_export_test_hooks::before_claim_commit=[] {continuity_claim_action();};}
    ~continuity_claim_hook(){recovery_export_test_hooks::before_claim_commit=prior;continuity_claim_action=std::move(old);}
};
thread_local bool continuity_deny_stamp=false;
thread_local size_t continuity_stamp_denials=0;
struct continuity_final_stamp_fault {
    bool old=continuity_deny_stamp;size_t old_denials=continuity_stamp_denials;
    continuity_fault fault;
    explicit continuity_final_stamp_fault(const lattice_db* owner):fault(owner,[](int action,const char* table,const char*,const char*)noexcept{
        if(continuity_deny_stamp&&action==SQLITE_READ&&table&&std::strcmp(table,"_lattice_obligation_producer_stamp")==0){++continuity_stamp_denials;return SQLITE_DENY;}
        return SQLITE_OK;
    }){continuity_deny_stamp=false;continuity_stamp_denials=0;}
    ~continuity_final_stamp_fault(){continuity_deny_stamp=old;continuity_stamp_denials=old_denials;}
};
class RecoveryProducerContinuity:public ::testing::Test {
protected:
    TempDB unique{"continuous"};
    std::filesystem::path container=unique.str()+".lattice-continuous";
    std::shared_ptr<continuity_queue> queue=std::make_shared<continuity_queue>();
    std::shared_ptr<network_factory> prior_factory;
    std::shared_ptr<continuity_factory> factory=std::make_shared<continuity_factory>();
    std::vector<std::unique_ptr<synchronizer>> senders;
    std::vector<std::shared_ptr<lattice_db>> facades;
    std::shared_ptr<lattice_db> owner;
    bool preserve_container=false;
    recovery_continuous_policy policy;
    configuration config() {configuration c((container/"store.sqlite").string(),queue);c.audit_retention_seconds=0;c.busy_timeout_ms=100;return c;}
    void SetUp()override {
        prior_factory=get_network_factory();set_network_factory(factory);
        policy.limits={{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
        policy.owners=8;policy.physical_routes=4;policy.operations=4;policy.frozen_entries=128;policy.frozen_bytes=2*1024*1024;
        for(const auto* name:{"a","b"}){
            const std::string id=name;policy.contributions.push_back({{{id,"authority","source","epoch","scope-"+id,"schema"},"grant-"+id,"shared-receipts"},{"ContinuousSharedRow"},{'g'}});
            policy.routes.push_back({"wss:wss://continuous.invalid/"+id,"wss://continuous.invalid/"+id});
        }
    }
    void open(){auto result=recovery_continuous_producer::open(config(),policy);known_commit(result.settlement);
    ASSERT_TRUE(result.owner);owner=std::move(result.owner);stop_notifier();}
    void stop_notifier(){auto* notifier=instance_registry::instance().get_or_create_notifier(owner->config().path);if(notifier)notifier->stop_listening();}
    std::shared_ptr<lattice_db> facade(){auto result=recovery_continuous_producer::open(config(),policy);known_commit(result.settlement);if(!result.owner)throw db_error("missing admitted facade");facades.push_back(result.owner);return result.owner;}
    recovery_continuous_quiescence freeze(int64_t attempt=1){auto start=recovery_continuous_producer::begin(owner,attempt);known_commit(start.settlement);if(!start.barrier)throw db_error("missing barrier");auto done=recovery_continuous_producer::finish(*start.barrier);known_commit(done.settlement);return done;}
    auto snapshot(){std::vector<std::vector<database::row_t>> rows;for(const char* name:{"_lattice_producer_continuity","_lattice_obligation_store","_lattice_obligation_scope","_lattice_obligation_entry","_lattice_obligation_producer_store","_lattice_obligation_producer_profile","_lattice_obligation_producer_stamp","_lattice_install_store","_lattice_install_channel","AuditLog","ContinuousSharedRow"})rows.push_back(owner->db().query(std::string("SELECT * FROM ")+name+" ORDER BY 1,2"));return rows;}
    void connect(size_t index=0){sync_config c;c.sync_id=policy.routes.at(index).sync_id;c.websocket_url=policy.routes.at(index).endpoint;c.checkpoint_passive_interval_ms=0;c.upload_coalesce_ms=0;
        auto sync=std::make_unique<synchronizer>(owner,c);sync->connect();queue->drain();factory->wires.back()->open();queue->drain();senders.push_back(std::move(sync));}
    void connect_replacement(){
        // The first sender owns shutdown of its scheduler. Retain a real
        // admitted co-facade whose scheduler is fresh; assigning only queue
        // would leave synchronizer(owner, ...) bound to the stopped queue.
        queue=std::make_shared<continuity_queue>();auto fresh_owner=facade();
        sync_config c;c.sync_id=policy.routes.at(0).sync_id;c.websocket_url=policy.routes.at(0).endpoint;c.checkpoint_passive_interval_ms=0;c.upload_coalesce_ms=0;
        auto sync=std::make_unique<synchronizer>(std::move(fresh_owner),c);sync->connect();queue->drain();factory->wires.back()->open();queue->drain();senders.push_back(std::move(sync));
    }
    void TearDown()override {
        senders.clear();queue->shutdown();facades.clear();if(owner)owner->close();owner.reset();
        for(const auto& wire:factory->wires)EXPECT_EQ(wire->destruction.wait_for(std::chrono::seconds(5)),std::future_status::ready);
        set_network_factory(prior_factory);std::error_code error;if(!preserve_container)std::filesystem::remove_all(container,error);
    }
};
}
TEST_F(RecoveryProducerContinuity, GenuineSharedOriginalHasOneAuditTwoStampsAndOneLocalUnsentIdentity) {
    open();owner->add(ContinuousSharedRow{"shared"});owner->add(ContinuousLocalRow{"local-only"});
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM AuditLog"),2);
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_obligation_producer_stamp"),2);
    auto done=freeze();
    ASSERT_TRUE(done.unsent);
    ASSERT_EQ(done.unsent->frozen_journals().size(),2u);
    EXPECT_EQ(done.unsent->canonical_originals().size(),1u);
    known_commit(recovery_writer_access::install(owner,[&](database&){recovery_continuous_producer::verify_for_owned_write(*done.unsent);}));
    EXPECT_THROW(owner->add(ContinuousSharedRow{"closed"}),db_error);
    EXPECT_NO_THROW(owner->add(ContinuousLocalRow{"unrelated local"}));
    EXPECT_FALSE(recovery_continuous_producer::source_authentication_capability);
    EXPECT_FALSE(recovery_continuous_producer::automatic_recovery_capability);
}
TEST_F(RecoveryProducerContinuity, AdmittedCoWritersShareRowsAndBarrierWithoutLegacySiblingAdoption) {
    open();auto sibling=facade();owner->add(ContinuousSharedRow{"first"});sibling->add(ContinuousSharedRow{"second"});
    auto done=freeze();
    ASSERT_TRUE(done.unsent);
    EXPECT_EQ(done.unsent->canonical_originals().size(),2u);
    EXPECT_THROW(sibling->add(ContinuousSharedRow{"blocked"}),db_error);
    EXPECT_THROW((lattice_db(config())),db_error);
    configuration reader=config();reader.read_only=true;
    EXPECT_NO_THROW((lattice_db(reader)));
    known_commit(recovery_continuous_producer::cancel(*done.barrier));
    EXPECT_NO_THROW(sibling->add(ContinuousSharedRow{"resumed"}));
    const auto stale=recovery_writer_access::install(owner,[&](database&){recovery_continuous_producer::verify_for_owned_write(*done.unsent);});
    EXPECT_NE(stale.state,recovery_install_state::committed);
}
TEST_F(RecoveryProducerContinuity, ExistingLegacyDataAndOrphanContainersAreNeverAdopted) {
    TempDB legacy{"continuous_legacy"};{lattice_db old(legacy.str());old.add(ContinuousSharedRow{"preserved"});}
    auto c=config();c.path=legacy.str();auto result=recovery_continuous_producer::open(c,policy);
    EXPECT_FALSE(result.owner);
    {lattice_db unchanged(legacy.str());
    EXPECT_EQ(number(unchanged.db(),"SELECT COUNT(*) AS n FROM ContinuousSharedRow"),1);}
    std::filesystem::create_directory(container);auto orphan=recovery_continuous_producer::open(config(),policy);
    EXPECT_FALSE(orphan.owner);
    EXPECT_FALSE(std::filesystem::exists(container/"store.sqlite"));
}
TEST_F(RecoveryProducerContinuity, PathAliasesRawHandlesAndLegacySerializersRefuseBeforeEffects) {
    open();owner->add(ContinuousSharedRow{"kept"});const auto before=snapshot();
    EXPECT_THROW(owner->db().handle(),db_error);
    EXPECT_THROW(query_audit_log(owner->db()),db_error);
    EXPECT_THROW(events_after(owner->db(),std::nullopt),db_error);
    EXPECT_THROW(query_audit_log_for_sync(owner->db(),policy.routes[0].sync_id),db_error);
    EXPECT_THROW((database("file:"+config().path+"?mode=rw")),db_error);
    const auto alias=container.parent_path()/(unique.path.filename().string()+"-alias");std::filesystem::create_directory_symlink(container,alias);
    EXPECT_THROW((database((alias/"store.sqlite").string())),db_error);std::filesystem::remove(alias);
    EXPECT_EQ(snapshot(),before);
}
TEST_F(RecoveryProducerContinuity, DirectPrimitiveMetadataCannotForgeClaimCancelOrCoverage) {
    open();owner->add(ContinuousSharedRow{"kept"});const auto before=snapshot();
    auto attempted=recovery_writer_access::install(owner,[&](database& db){db.execute("UPDATE _lattice_producer_continuity SET phase=2");});
    EXPECT_NE(attempted.state,recovery_install_state::committed);
    attempted=recovery_writer_access::install(owner,[&](database&){recovery_obligation_store journal(owner,policy.limits.obligations,policy.limits.installations);auto scope=journal.read("a");journal.freeze(scope->address,1);});
    EXPECT_NE(attempted.state,recovery_install_state::committed);
    EXPECT_EQ(snapshot(),before);auto done=freeze();
    ASSERT_TRUE(done.unsent);
    EXPECT_EQ(done.unsent->canonical_originals().size(),1u);
}
TEST_F(RecoveryProducerContinuity, IgnoredBeginPreservesDurableRowsAndLeavesRuntimeClosedUntilExplicitRetry) {
    open();owner->add(ContinuousSharedRow{"kept"});const auto before=snapshot();
    {continuity_fault fault(owner.get(),[](int action,const char* table,const char* column,const char*)noexcept{return action==SQLITE_UPDATE&&table&&column&&std::strcmp(table,"_lattice_producer_continuity")==0&&std::strcmp(column,"phase")==0?SQLITE_IGNORE:SQLITE_OK;});
     auto start=recovery_continuous_producer::begin(owner,1);
    EXPECT_NE(start.settlement.state,recovery_install_state::committed);
    EXPECT_FALSE(start.unsent);}
    EXPECT_EQ(snapshot(),before);
    EXPECT_THROW(owner->add(ContinuousSharedRow{"still closed"}),db_error);
    auto done=freeze(1);
    ASSERT_TRUE(done.unsent);known_commit(recovery_continuous_producer::cancel(*done.barrier));
    EXPECT_NO_THROW(owner->add(ContinuousSharedRow{"explicitly reopened"}));
}
TEST_F(RecoveryProducerContinuity, FailedCommitDoesNotPublishBarrierOrUnsent) {
    open();owner->add(ContinuousSharedRow{"kept"});const auto before=snapshot();
    {continuity_fault fault(owner.get(),[](int action,const char* command,const char*,const char*)noexcept{return action==SQLITE_TRANSACTION&&command&&std::strcmp(command,"COMMIT")==0?SQLITE_DENY:SQLITE_OK;});
     auto start=recovery_continuous_producer::begin(owner,1);
    EXPECT_NE(start.settlement.state,recovery_install_state::committed);
    EXPECT_FALSE(start.barrier);
    EXPECT_FALSE(start.unsent);}
    EXPECT_EQ(snapshot(),before);
    EXPECT_THROW(owner->add(ContinuousSharedRow{"closed after failure"}),db_error);auto done=freeze(1);
    ASSERT_TRUE(done.unsent);
}
TEST_F(RecoveryProducerContinuity, IgnoredFreezeRollsBackEveryJournalAndCanBeInspectedAndRetried) {
    open();owner->add(ContinuousSharedRow{"kept"});auto start=recovery_continuous_producer::begin(owner,1);known_commit(start.settlement);
    ASSERT_TRUE(start.barrier);const auto before=snapshot();
    {continuity_fault fault(owner.get(),[](int action,const char* table,const char* column,const char*)noexcept{return action==SQLITE_UPDATE&&table&&column&&std::strcmp(table,"_lattice_producer_continuity")==0&&std::strcmp(column,"phase")==0?SQLITE_IGNORE:SQLITE_OK;});
     auto failed=recovery_continuous_producer::finish(*start.barrier);
    EXPECT_NE(failed.settlement.state,recovery_install_state::committed);
    EXPECT_FALSE(failed.unsent);}
    EXPECT_EQ(snapshot(),before);auto observed=recovery_continuous_producer::inspect(owner);known_commit(observed.settlement);
    ASSERT_TRUE(observed.barrier);
    auto done=recovery_continuous_producer::finish(*observed.barrier);known_commit(done.settlement);
    ASSERT_TRUE(done.unsent);
}
TEST_F(RecoveryProducerContinuity, IgnoredCancelKeepsFrozenProofAndDmlFence) {
    open();owner->add(ContinuousSharedRow{"kept"});auto done=freeze();
    ASSERT_TRUE(done.unsent);const auto before=snapshot();
    {continuity_fault fault(owner.get(),[](int action,const char* table,const char* column,const char*)noexcept{return action==SQLITE_UPDATE&&table&&column&&std::strcmp(table,"_lattice_producer_continuity")==0&&std::strcmp(column,"phase")==0?SQLITE_IGNORE:SQLITE_OK;});
     EXPECT_NE(recovery_continuous_producer::cancel(*done.barrier).state,recovery_install_state::committed);}
    EXPECT_EQ(snapshot(),before);
    EXPECT_THROW(owner->add(ContinuousSharedRow{"closed"}),db_error);
    known_commit(recovery_writer_access::install(owner,[&](database&){recovery_continuous_producer::verify_for_owned_write(*done.unsent);}));
    known_commit(recovery_continuous_producer::cancel(*done.barrier));
    EXPECT_NO_THROW(owner->add(ContinuousSharedRow{"open"}));
}
TEST_F(RecoveryProducerContinuity, NewPhysicalSessionPreservesClosedQAndAdvancesIncarnation) {
    int64_t prior=0;std::vector<std::string> originals;
    {open();owner->add(ContinuousSharedRow{"kept"});auto done=freeze();
    ASSERT_TRUE(done.unsent);originals=done.unsent->canonical_originals();prior=number(owner->db(),"SELECT incarnation AS n FROM _lattice_producer_continuity");}
    owner->close();owner.reset();open();
    EXPECT_GT(number(owner->db(),"SELECT incarnation AS n FROM _lattice_producer_continuity"),prior);
    EXPECT_THROW(owner->add(ContinuousSharedRow{"restart stays closed"}),db_error);auto observed=recovery_continuous_producer::inspect(owner);known_commit(observed.settlement);
    ASSERT_TRUE(observed.barrier);
    auto resumed=recovery_continuous_producer::finish(*observed.barrier);known_commit(resumed.settlement);
    ASSERT_TRUE(resumed.unsent);
    EXPECT_EQ(resumed.unsent->canonical_originals(),originals);
}
TEST_F(RecoveryProducerContinuity, WrongPolicyAndCapacityRefuseWithoutChangingTheExistingOwner) {
    policy.owners=2;open();auto second=facade();const auto before=snapshot();auto over=recovery_continuous_producer::open(config(),policy);
    EXPECT_FALSE(over.owner);
    auto changed=policy;changed.contributions[0].profile.receipt_namespace="different";auto wrong=recovery_continuous_producer::open(config(),changed);
    EXPECT_FALSE(wrong.owner);
    EXPECT_EQ(snapshot(),before);
    EXPECT_NO_THROW(second->add(ContinuousSharedRow{"still admitted"}));
}
TEST_F(RecoveryProducerContinuity, EveryRealWssRouteClaimsSharedOriginalBeforeHandoff) {
    open();connect(0);connect(1);owner->add(ContinuousSharedRow{"shared"});queue->drain();
    ASSERT_EQ(factory->wires.size(),2u);
    EXPECT_GT(factory->wires[0]->count(),0u);
    EXPECT_GT(factory->wires[1]->count(),0u);
    auto done=freeze();
    ASSERT_TRUE(done.unsent);
    EXPECT_TRUE(done.unsent->canonical_originals().empty());
    for(const auto& journal:done.unsent->frozen_journals()){ASSERT_EQ(journal.entries.size(),1u);
    EXPECT_TRUE(journal.entries[0].first_export_claim);}
}
TEST_F(RecoveryProducerContinuity, LiveHandoffWaitsOutsideLocksAndFailedDeliveryRemainsUnknown) {
    open();connect();std::optional<recovery_continuous_barrier> barrier;bool waiting=false,cancel_refused=false;
    factory->wires[0]->sending=[&]{auto start=recovery_continuous_producer::begin(owner,1);known_commit(start.settlement);barrier=start.barrier;
    ASSERT_TRUE(barrier);
        auto pending=recovery_continuous_producer::finish(*barrier);waiting=pending.waiting;
    EXPECT_FALSE(pending.unsent);
        cancel_refused=recovery_continuous_producer::cancel(*barrier).state!=recovery_install_state::committed;
        throw db_error("fixture physical send failed after claim");};
    owner->add(ContinuousSharedRow{"claimed"});queue->drain();
    ASSERT_TRUE(barrier);
    EXPECT_TRUE(waiting);
    EXPECT_TRUE(cancel_refused);
    factory->wires[0]->sending={};auto done=recovery_continuous_producer::finish(*barrier);known_commit(done.settlement);
    ASSERT_TRUE(done.unsent);
    EXPECT_TRUE(done.unsent->canonical_originals().empty());
}
TEST_F(RecoveryProducerContinuity, UnregisteredRoutesAndLegacyExportsRefuseBeforeFactoryOrSinkEffects) {
    open();sync_config bad;bad.sync_id="unknown";bad.websocket_url="wss://unknown.invalid";
    EXPECT_THROW((synchronizer(owner,bad)),db_error);
    EXPECT_TRUE(factory->wires.empty());
    EXPECT_THROW(recovery_export_adapter::prepare_pending(owner,policy.routes[0].sync_id,1,8,{},false),db_error);
    EXPECT_THROW(owner->db().execute("PRAGMA synchronous=OFF"),db_error);
    EXPECT_THROW(owner->db().execute("PRAGMA journal_mode=DELETE"),db_error);
    auto done=freeze();
    ASSERT_TRUE(done.unsent);
    EXPECT_TRUE(done.unsent->canonical_originals().empty());
}
TEST_F(RecoveryProducerContinuity, LogicalCloseNeverMakesOldBarrierCurrentOrClaimUnsent) {
    open();owner->add(ContinuousSharedRow{"kept"});auto start=recovery_continuous_producer::begin(owner,1);known_commit(start.settlement);
    ASSERT_TRUE(start.barrier);owner->close();
    auto done=recovery_continuous_producer::finish(*start.barrier);
    EXPECT_NE(done.settlement.state,recovery_install_state::committed);
    EXPECT_FALSE(done.unsent);
}

TEST_F(RecoveryProducerContinuity, PreparingCancellationRetiresExactSequenceWithoutPublishingUnsent) {
    open();owner->add(ContinuousSharedRow{"kept"});auto start=recovery_continuous_producer::begin(owner,1);known_commit(start.settlement);
    ASSERT_TRUE(start.barrier);
    EXPECT_FALSE(start.unsent);
    known_commit(recovery_continuous_producer::cancel(*start.barrier));
    EXPECT_EQ(number(owner->db(),"SELECT MIN(last_sequence) AS n FROM _lattice_install_channel"),1);
    auto done=freeze(2);
    ASSERT_TRUE(done.unsent);
    EXPECT_EQ(done.unsent->canonical_originals().size(),1u);
}
TEST_F(RecoveryProducerContinuity, ImpossibleFrozenCapacityRefusesBeforeEnrollment) {
    policy.frozen_entries=1;auto refused=recovery_continuous_producer::open(config(),policy);
    EXPECT_FALSE(refused.owner);
    EXPECT_FALSE(std::filesystem::exists(container));
}
TEST_F(RecoveryProducerContinuity, ConfiguredUnsupportedRouteRefusesBeforeContainerCreation) {
    auto c=config();c.websocket_url="wss://outside.invalid";c.authorization_token="configured-route-fixture";auto refused=recovery_continuous_producer::open(c,policy);
    EXPECT_FALSE(refused.owner);
    EXPECT_FALSE(std::filesystem::exists(container));
    EXPECT_TRUE(factory->wires.empty());
}
TEST_F(RecoveryProducerContinuity, MissingGuardReopenRefusesWithoutCleanupOrFingerprintRewrite) {
    open();owner->add(ContinuousSharedRow{"kept"});owner->close();owner.reset();
    // Fault fixture only: tamper through a separately SQLite-managed handle
    // after all admitted owners are gone, then test fail-closed bootstrap.
    sqlite3* raw=nullptr;
    ASSERT_EQ(sqlite3_open_v2(config().path.c_str(),&raw,SQLITE_OPEN_READWRITE,nullptr),SQLITE_OK);
    ASSERT_NE(raw,nullptr);
    const std::unique_ptr<sqlite3,decltype(&sqlite3_close)> held(raw,&sqlite3_close);
    ASSERT_EQ(sqlite3_exec(raw,"DROP TRIGGER _lattice_producer_continuity_UPDATE",nullptr,nullptr,nullptr),SQLITE_OK);
    sqlite3_stmt* statement=nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(raw,"PRAGMA schema_version",-1,&statement,nullptr),SQLITE_OK);
    ASSERT_EQ(sqlite3_step(statement),SQLITE_ROW);
    const auto cookie=sqlite3_column_int64(statement,0);sqlite3_finalize(statement);
    auto refused=recovery_continuous_producer::open(config(),policy);
    EXPECT_FALSE(refused.owner);
    ASSERT_EQ(sqlite3_prepare_v2(raw,"PRAGMA schema_version",-1,&statement,nullptr),SQLITE_OK);
    ASSERT_EQ(sqlite3_step(statement),SQLITE_ROW);
    EXPECT_EQ(sqlite3_column_int64(statement,0),cookie);sqlite3_finalize(statement);
    ASSERT_EQ(sqlite3_prepare_v2(raw,"SELECT COUNT(*) FROM ContinuousSharedRow",-1,&statement,nullptr),SQLITE_OK);
    ASSERT_EQ(sqlite3_step(statement),SQLITE_ROW);
    EXPECT_EQ(sqlite3_column_int64(statement,0),1);sqlite3_finalize(statement);
}
TEST_F(RecoveryProducerContinuity, DirectoryMovementAndAliasesRefuseBeforeBarrierMutation) {
    open();owner->add(ContinuousSharedRow{"kept"});const auto moved=std::filesystem::path(container.string()+"-moved.lattice-continuous");
    std::filesystem::rename(container,moved);
    auto refused=recovery_continuous_producer::begin(owner,1);
    EXPECT_NE(refused.settlement.state,recovery_install_state::committed);
    EXPECT_FALSE(refused.barrier);
    std::filesystem::rename(moved,container);
    EXPECT_EQ(number(owner->db(),"SELECT phase AS n FROM _lattice_producer_continuity"),0);
    auto done=freeze();
    ASSERT_TRUE(done.unsent);
}

TEST_F(RecoveryProducerContinuity, ProcessHelper) {
    const auto* value=std::getenv("LATTICE_CONTINUITY_PROCESS");if(!value)GTEST_SKIP()<<"invoked only by owned fresh-process parent";
    const std::string invocation(value);
    ASSERT_GT(invocation.size(),2u);
    ASSERT_EQ(invocation[1],':');container=invocation.substr(2);preserve_container=true;
    if(invocation[0]=='s') {
        open();owner->add(ContinuousSharedRow{"durable before abrupt process exit"});auto done=freeze();
        ASSERT_TRUE(done.unsent);
        ASSERT_EQ(done.unsent->canonical_originals().size(),1u);
        {std::ofstream receipt(container/"expected-original.txt");receipt<<done.unsent->canonical_originals()[0]<<'\n';receipt.flush();
    ASSERT_TRUE(receipt.good());}
        std::_Exit(::testing::Test::HasFailure()?1:0);
    }
    ASSERT_EQ(invocation[0],'r');open();
    EXPECT_EQ(number(owner->db(),"SELECT incarnation AS n FROM _lattice_producer_continuity"),2);
    EXPECT_THROW(owner->add(ContinuousSharedRow{"restart remains closed"}),db_error);
    auto observed=recovery_continuous_producer::inspect(owner);known_commit(observed.settlement);
    ASSERT_TRUE(observed.barrier);
    auto resumed=recovery_continuous_producer::finish(*observed.barrier);known_commit(resumed.settlement);
    ASSERT_TRUE(resumed.unsent);
    ASSERT_EQ(resumed.unsent->canonical_originals().size(),1u);
    std::ifstream receipt(container/"expected-original.txt");std::string expected;std::getline(receipt,expected);
    EXPECT_EQ(resumed.unsent->canonical_originals()[0],expected);
    known_commit(recovery_continuous_producer::cancel(*resumed.barrier));
    EXPECT_NO_THROW(owner->add(ContinuousSharedRow{"explicitly resumed after inspection"}));
}
TEST_F(RecoveryProducerContinuity, FreshProcessesPreserveClosedQAfterKnownCommitAndAbruptExit) {
    char executable[4096];
#ifdef __APPLE__
    uint32_t length=sizeof(executable);
    ASSERT_EQ(_NSGetExecutablePath(executable,&length),0);
#else
    const auto length=readlink("/proc/self/exe",executable,sizeof(executable)-1);
    ASSERT_GT(length,0);
    ASSERT_LT(length,static_cast<ssize_t>(sizeof(executable)-1));executable[length]=0;
#endif
    const auto run=[&](const char* phase){
        const std::string phase_name(phase),variable="LATTICE_CONTINUITY_PROCESS=";
        const auto* configured=std::getenv("LATTICE_TEST_LOG_PATH");
        const auto native=configured?std::string(configured)+".continuous-"+unique.path.filename().string()+"-"+phase_name:unique.str()+"-"+phase_name+"-native.log";
        std::vector<std::string> values;
        for(char** item=environ;*item;++item)if(std::strncmp(*item,variable.c_str(),variable.size())&&std::strncmp(*item,"LATTICE_TEST_LOG_PATH=",22))values.emplace_back(*item);
        values.push_back(variable+(phase_name=="seed"?"s:":"r:")+container.string());values.push_back("LATTICE_TEST_LOG_PATH="+native);
        std::vector<char*> environment;for(auto& value:values)environment.push_back(value.data());environment.push_back(nullptr);
        std::string filter="--gtest_filter=RecoveryProducerContinuity.ProcessHelper",color="--gtest_color=no",repeat="--gtest_repeat=1",output="--gtest_output=";
        char* arguments[]={executable,filter.data(),color.data(),repeat.data(),output.data(),nullptr};
        posix_spawn_file_actions_t actions;
        ASSERT_EQ(posix_spawn_file_actions_init(&actions),0);
        struct destroy{posix_spawn_file_actions_t& value;~destroy(){posix_spawn_file_actions_destroy(&value);}} cleanup{actions};
        const auto terminal=unique.str()+"-"+phase_name+"-terminal.log";
        ASSERT_EQ(posix_spawn_file_actions_addopen(&actions,STDOUT_FILENO,terminal.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600),0);
        ASSERT_EQ(posix_spawn_file_actions_adddup2(&actions,STDOUT_FILENO,STDERR_FILENO),0);
        pid_t child=-1;
        ASSERT_EQ(posix_spawn(&child,executable,&actions,nullptr,arguments,environment.data()),0);
        int status=0;pid_t waited=0;const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(30);
        do {waited=waitpid(child,&status,WNOHANG);if(waited==child||(waited<0&&errno!=EINTR))break;std::this_thread::sleep_for(std::chrono::milliseconds(10));}while(std::chrono::steady_clock::now()<deadline);
        ASSERT_FALSE(waited<0&&errno!=EINTR)<<"owned child observation failed; no unverifiable signal";
        if(waited!=child){kill(child,SIGKILL);do{waited=waitpid(child,&status,0);}while(waited<0&&errno==EINTR);FAIL()<<"continuous child deadline: "<<terminal;}
        ASSERT_EQ(waited,child);
        ASSERT_TRUE(WIFEXITED(status))<<terminal;
        ASSERT_EQ(WEXITSTATUS(status),0)<<terminal;
    };
    run("seed");if(::testing::Test::HasFatalFailure())return;run("reopen");
}

TEST_F(RecoveryProducerContinuity, PhysicalRouteCapacityRefusesBeforeAnotherTransportIsCreated) {
    policy.physical_routes=1;open();connect();
    sync_config config;config.sync_id=policy.routes[1].sync_id;config.websocket_url=policy.routes[1].endpoint;
    EXPECT_THROW((synchronizer(owner,config)),db_error);
    EXPECT_EQ(factory->wires.size(),1u);
    owner->add(ContinuousSharedRow{"existing route still works"});queue->drain();
    EXPECT_GT(factory->wires[0]->count(),0u);
}

TEST_F(RecoveryProducerContinuity, StructurallyValidButContradictoryRestartStateRefusesBeforeIncarnationAdvance) {
    std::string guard;
    {open();owner->add(ContinuousSharedRow{"frozen"});auto done=freeze();ASSERT_TRUE(done.unsent);
     guard=std::get<std::string>(owner->db().query("SELECT sql FROM sqlite_schema WHERE name='_lattice_producer_continuity_UPDATE'").at(0).at("sql"));}
    owner->close();owner.reset();sqlite3* raw=nullptr;
    ASSERT_EQ(sqlite3_open_v2(config().path.c_str(),&raw,SQLITE_OPEN_READWRITE,nullptr),SQLITE_OK);
    const std::unique_ptr<sqlite3,decltype(&sqlite3_close)> held(raw,&sqlite3_close);
    ASSERT_EQ(sqlite3_exec(raw,"DROP TRIGGER _lattice_producer_continuity_UPDATE; UPDATE _lattice_producer_continuity SET phase=0",nullptr,nullptr,nullptr),SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(raw,guard.c_str(),nullptr,nullptr,nullptr),SQLITE_OK);
    auto refused=recovery_continuous_producer::open(config(),policy);
    EXPECT_FALSE(refused.owner);
    sqlite3_stmt* statement=nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(raw,"SELECT incarnation,phase FROM _lattice_producer_continuity",-1,&statement,nullptr),SQLITE_OK);
    ASSERT_EQ(sqlite3_step(statement),SQLITE_ROW);
    EXPECT_EQ(sqlite3_column_int64(statement,0),1);
    EXPECT_EQ(sqlite3_column_int64(statement,1),0);sqlite3_finalize(statement);
}

TEST_F(RecoveryProducerContinuity, GeneratedDmlCapacityRollsBackOriginalAndEverySharedTail) {
    policy.limits.obligations.records=2;policy.limits.producers.stamps=2;open();owner->add(ContinuousSharedRow{"one original fills both contributions"});
    const auto before=snapshot();
    EXPECT_THROW(owner->add(ContinuousSharedRow{"over capacity"}),db_error);
    EXPECT_EQ(snapshot(),before);
    auto done=freeze();
    ASSERT_TRUE(done.unsent);
    EXPECT_EQ(done.unsent->canonical_originals().size(),1u);
}
TEST_F(RecoveryProducerContinuity, ActualRoutePagesBeyondQualificationCapWithSharedAckedAndInflightOriginals) {
    auto pause=std::make_unique<continuity_ack_pause>(senders,factory);
    constexpr size_t originals=2051;
    policy.limits.obligations.records=2*(originals+1);policy.limits.producers.stamps=2*(originals+1);
    policy.frozen_entries=2*(originals+1);
    open();std::vector<ContinuousSharedRow> rows;for(size_t i=0;i<originals;++i)rows.push_back({"retained-"+std::to_string(i)});
    owner->add_bulk(std::move(rows));
    const auto audit_before=owner->db().query("SELECT * FROM AuditLog ORDER BY id");
    ASSERT_EQ(audit_before.size(),originals);
    std::vector<std::string> expected;for(const auto& row:audit_before)expected.push_back(std::get<std::string>(row.at("globalId")));
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0"),2*originals);
    connect();ASSERT_EQ(factory->wires.size(),1u);auto wire=factory->wires[0];auto batches=wire->audit_batches();
    ASSERT_EQ(batches.size(),1u);ASSERT_EQ(batches[0].size(),1000u);
    EXPECT_EQ(batches[0],(std::vector<std::string>(expected.begin(),expected.begin()+1000)));
    senders[0]->sync_now();queue->drain();batches=wire->audit_batches();
    ASSERT_EQ(batches.size(),2u);ASSERT_EQ(batches[1].size(),1000u);
    EXPECT_EQ(batches[1],(std::vector<std::string>(expected.begin()+1000,expected.begin()+2000)));
    senders[0]->sync_now();queue->drain();EXPECT_EQ(wire->audit_batches(),batches); // full actual in-flight window
    wire->ack(batches[0]);queue->drain();batches=wire->audit_batches();
    ASSERT_EQ(batches.size(),3u);EXPECT_EQ(batches[2],(std::vector<std::string>(expected.begin()+2000,expected.end())));
    // ACKed originals remain raw stage-0 obligations; the other 1000 are
    // still in flight, yet the exact last 51 were selected and sent once.
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0"),2*originals);
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE is_synchronized=1"),1000);
    wire->ack(batches[1]);queue->drain();wire->ack(batches[2]);queue->drain();
    senders[0]->sync_now();queue->drain();EXPECT_EQ(wire->audit_batches(),batches);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog ORDER BY id"),audit_before);
    // A's legacy ACK does not exclude the same originals from actual route B.
    connect(1);ASSERT_EQ(factory->wires.size(),2u);const auto other=factory->wires[1]->audit_batches();
    ASSERT_EQ(other.size(),1u);EXPECT_EQ(other[0],batches[0]);
    senders.clear();queue->drain();
    pause.reset(); // Retired senders no longer need held retries during freeze/cancel work.
    owner->add(ContinuousSharedRow{"never handed to any route"});
    const auto full=snapshot();
    EXPECT_THROW(owner->add(ContinuousSharedRow{"over actual admitted cap"}),db_error);
    EXPECT_EQ(snapshot(),full);
    auto done=freeze();ASSERT_TRUE(done.unsent);ASSERT_EQ(done.unsent->frozen_journals().size(),2u);
    EXPECT_EQ(done.unsent->canonical_originals().size(),1u);
    for(const auto& journal:done.unsent->frozen_journals()){
        ASSERT_EQ(journal.entries.size(),originals+1);
        for(size_t i=0;i<journal.entries.size();++i){const auto& entry=journal.entries[i];
            EXPECT_EQ(entry.stage,recovery_obligation_stage::open);EXPECT_FALSE(entry.acknowledged);EXPECT_EQ(entry.settled_install_sequence,0);
            if(i<originals){EXPECT_TRUE(entry.first_export_claim);EXPECT_EQ(entry.record.original_id,expected[i]);}
            else EXPECT_FALSE(entry.first_export_claim);
        }
    }
    known_commit(recovery_writer_access::install(owner,[&](database&){recovery_continuous_producer::verify_for_owned_write(*done.unsent);}));
    const auto entries_before_resume=owner->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY 1,2");
    const auto audit_before_resume=owner->db().query("SELECT * FROM AuditLog ORDER BY id");
    known_commit(recovery_continuous_producer::cancel(*done.barrier));
    EXPECT_EQ(number(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0"),2*(originals+1));
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_obligation_entry ORDER BY 1,2"),entries_before_resume);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog ORDER BY id"),audit_before_resume);
}
TEST_F(RecoveryProducerContinuity, ActualRouteFinalSelectorChangeRollsBackClaimsAndRouteAck) {
    open();owner->add(ContinuousSharedRow{"preserved"});
    const auto id=number(owner->db(),"SELECT id AS n FROM AuditLog");
    const auto before=snapshot();const auto route_before=owner->db().query("SELECT * FROM _lattice_sync_state ORDER BY 1,2");size_t invoked=0;bool changed=false;
    {continuity_claim_hook hook([&]{++invoked;
        owner->db().execute("INSERT INTO _lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,1)",{id,policy.routes[0].sync_id});
        changed=number(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE is_synchronized=1")==1;
    });connect();}
    EXPECT_EQ(invoked,1u);EXPECT_TRUE(changed);EXPECT_TRUE(factory->wires.back()->audit_batches().empty());
    EXPECT_EQ(snapshot(),before);EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_sync_state ORDER BY 1,2"),route_before);
    senders.clear();queue->drain();
    connect_replacement();const auto batches=factory->wires.back()->audit_batches();ASSERT_EQ(batches.size(),1u);EXPECT_EQ(batches[0].size(),1u);
    auto done=freeze();ASSERT_TRUE(done.unsent);EXPECT_TRUE(done.unsent->canonical_originals().empty());
}
TEST_F(RecoveryProducerContinuity, ActualRouteFinalSelectedStampReadFailureRollsBackEveryClaim) {
    open();owner->add(ContinuousSharedRow{"preserved"});const auto before=snapshot();size_t invoked=0;
    {continuity_final_stamp_fault fault(owner.get());continuity_claim_hook hook([&]{++invoked;continuity_deny_stamp=true;});connect();
     EXPECT_EQ(invoked,1u);EXPECT_EQ(continuity_stamp_denials,1u);}
    EXPECT_TRUE(factory->wires.back()->audit_batches().empty());EXPECT_EQ(snapshot(),before);
    senders.clear();queue->drain();
    connect_replacement();const auto batches=factory->wires.back()->audit_batches();ASSERT_EQ(batches.size(),1u);EXPECT_EQ(batches[0].size(),1u);
    auto done=freeze();ASSERT_TRUE(done.unsent);EXPECT_TRUE(done.unsent->canonical_originals().empty());
}

namespace {
struct readonly_classification_fault {
    sqlite3* handle;
    size_t denied=0;
    explicit readonly_classification_fault(database& db):handle(canonical_writer_custody_test_access::fault_handle(db)) {
        sqlite3_set_authorizer(handle,[](void* context,int action,const char*,const char*,const char*,const char*)noexcept {
            auto& self=*static_cast<readonly_classification_fault*>(context);
            if(action==SQLITE_SELECT){++self.denied;return SQLITE_DENY;}return SQLITE_OK;
        },this);
    }
    ~readonly_classification_fault(){sqlite3_set_authorizer(handle,nullptr,nullptr);}
};
}
TEST_F(RecoveryProducerContinuity, OrdinaryReadOnlyFirstRawBoundaryClassifiesOnceAfterInternalReads) {
    TempDB ordinary{"continuous_readonly_ordinary"};
    lattice_db writer(ordinary.str());writer.add(ContinuousSharedRow{"ordinary"});
    database reader(ordinary.str(),database::open_mode::read_only);
    EXPECT_EQ(number(reader,"SELECT COUNT(*) AS n FROM ContinuousSharedRow"),1);
    const auto before=database::thread_statement_count();
    ASSERT_NE(reader.handle(),nullptr);
    EXPECT_EQ(database::thread_statement_count()-before,1u);
    const auto classified=database::thread_statement_count();
    ASSERT_NE(reader.handle(),nullptr);
    EXPECT_EQ(database::thread_statement_count(),classified);
    EXPECT_EQ(query_audit_log(reader).size(),1u);
    EXPECT_EQ(events_after(reader,std::nullopt).size(),1u);
}
TEST_F(RecoveryProducerContinuity, ReadOnlyProtectedStoreAllowsReadsButRefusesEveryLegacyBoundary) {
    open();owner->add(ContinuousSharedRow{"protected"});const auto before=snapshot();
    database reader(config().path,database::open_mode::read_only);
    EXPECT_EQ(number(reader,"SELECT COUNT(*) AS n FROM ContinuousSharedRow"),1);
    const auto count=database::thread_statement_count();
    EXPECT_THROW(reader.handle(),db_error);
    EXPECT_EQ(database::thread_statement_count()-count,1u);
    const auto classified=database::thread_statement_count();
    EXPECT_THROW(query_audit_log(reader),db_error);
    EXPECT_THROW(query_audit_log_for_sync(reader,policy.routes[0].sync_id),db_error);
    EXPECT_THROW(events_after(reader,std::nullopt),db_error);
    EXPECT_EQ(database::thread_statement_count(),classified);
    EXPECT_EQ(snapshot(),before);
}
TEST_F(RecoveryProducerContinuity, ReadOnlyUnknownAndKnownClassificationFollowPhysicalMoves) {
    TempDB ordinary{"continuous_readonly_move"};
    lattice_db writer(ordinary.str());writer.add(ContinuousSharedRow{"ordinary"});
    database original(ordinary.str(),database::open_mode::read_only);
    database moved(std::move(original));
    const auto count=database::thread_statement_count();ASSERT_NE(moved.handle(),nullptr);
    EXPECT_EQ(database::thread_statement_count()-count,1u);
    database destination(ordinary.str(),database::open_mode::read_only);destination=std::move(moved);
    const auto known=database::thread_statement_count();ASSERT_NE(destination.handle(),nullptr);
    EXPECT_EQ(database::thread_statement_count(),known);
    open();owner->add(ContinuousSharedRow{"protected"});
    database protected_reader(config().path,database::open_mode::read_only);
    destination=std::move(protected_reader);
    const auto unknown=database::thread_statement_count();EXPECT_THROW(destination.handle(),db_error);
    EXPECT_EQ(database::thread_statement_count()-unknown,1u);
    database protected_moved(std::move(destination));
    const auto protected_count=database::thread_statement_count();
    EXPECT_THROW(protected_moved.handle(),db_error);
    EXPECT_THROW(query_audit_log(protected_moved),db_error);
    EXPECT_EQ(database::thread_statement_count(),protected_count);
}
TEST_F(RecoveryProducerContinuity, ReadOnlyClassificationFailuresNeverCacheAbsence) {
    open();owner->add(ContinuousSharedRow{"preserved"});const auto before=snapshot();
    database reader(config().path,database::open_mode::read_only);
    {
        readonly_classification_fault denied(reader);
        EXPECT_THROW(reader.handle(),db_error);
        EXPECT_THROW(query_audit_log(reader),db_error);
        EXPECT_THROW(events_after(reader,std::nullopt),db_error);
        EXPECT_EQ(denied.denied,3u);
    }
    const auto retry=database::thread_statement_count();
    EXPECT_THROW(query_audit_log_for_sync(reader,policy.routes[0].sync_id),db_error);
    EXPECT_EQ(database::thread_statement_count()-retry,1u);
    EXPECT_EQ(snapshot(),before);
}
TEST_F(RecoveryProducerContinuity, ReadOnlyCopiedAndAliasedProtectedFilesStillRefuseRawAndLegacyExport) {
    open();owner->add(ContinuousSharedRow{"copied"});owner->close();owner.reset();
    TempDB copied{"continuous_readonly_copy"};
    std::filesystem::copy_file(config().path,copied.str(),std::filesystem::copy_options::overwrite_existing);
    {
        database copy=diagnostic_readonly_open(copied.str(),"copied-main");
        EXPECT_EQ(number(copy,"SELECT COUNT(*) AS n FROM ContinuousSharedRow"),1);
        EXPECT_THROW(copy.handle(),db_error);
        EXPECT_THROW(query_audit_log(copy),db_error);
        EXPECT_THROW(query_audit_log_for_sync(copy,policy.routes[0].sync_id),db_error);
        EXPECT_THROW(events_after(copy,std::nullopt),db_error);
    }
    const auto alias=container.parent_path()/(unique.path.filename().string()+"-readonly-alias");
    std::filesystem::create_directory_symlink(container,alias);
    struct remove_alias {std::filesystem::path path;~remove_alias(){std::error_code error;std::filesystem::remove(path,error);}} cleanup{alias};
    database reader=diagnostic_readonly_open((alias/"store.sqlite").string(),"aliased-main");
    EXPECT_EQ(number(reader,"SELECT COUNT(*) AS n FROM ContinuousSharedRow"),1);
    EXPECT_THROW(reader.handle(),db_error);
    EXPECT_THROW(events_after(reader,std::nullopt),db_error);
}

TEST_F(RecoveryProducerContinuity, ReadOnlyProtectedRouteRefusesBeforeFactoryPublication) {
    // Executing a pre-init route refusal also requires root's separate C7
    // partial-synchronizer destructor correction; no such fix is copied here.
    open();owner->add(ContinuousSharedRow{"not-exported"});const auto before=snapshot();
    auto reader_config=config();reader_config.read_only=true;
    auto reader=std::make_shared<lattice_db>(reader_config);
    sync_config route;route.sync_id=policy.routes[0].sync_id;route.websocket_url=policy.routes[0].endpoint;
    const auto created=factory->wires.size();
    EXPECT_THROW((void)std::make_unique<synchronizer>(reader,route),db_error);
    EXPECT_EQ(factory->wires.size(),created);EXPECT_EQ(snapshot(),before);reader->close();
}
#endif
