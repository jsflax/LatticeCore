#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include <array>
#include <deque>
#include <functional>
#include <utility>

#ifndef __EMSCRIPTEN__
struct ExportCoverageRow {std::string value;};
LATTICE_SCHEMA(ExportCoverageRow,value);
struct ExportCoverageOther {std::string value;};
LATTICE_SCHEMA(ExportCoverageOther,value);
namespace {
using namespace lattice;
using namespace lattice::detail;
constexpr const char* route_channel="coverage-route";
void committed(const recovery_install_result& result){
    if(result.primary_error)std::rethrow_exception(result.primary_error);
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
    if(result.state!=recovery_install_state::committed)throw std::runtime_error("fixture transaction did not commit");
}
void refused_at(const std::function<void()>& action,const char* reason){
    try{action();FAIL()<<"expected refusal: "<<reason;}
    catch(const db_error& error){EXPECT_NE(std::string(error.what()).find(reason),std::string::npos)<<error.what();}
}
class coverage_scheduler final:public scheduler {
    std::mutex mutex_;bool paused_=false,closed_=false;std::deque<std::function<void()>> work_;
    static thread_local const coverage_scheduler* current_;
public:
    void invoke(std::function<void()>&& fn)override{
        {std::lock_guard<std::mutex> lock(mutex_);if(closed_)return;if(paused_){work_.push_back(std::move(fn));return;}}
        struct restore {const coverage_scheduler* prior;~restore(){current_=prior;}} scope{current_};current_=this;fn();
    }
    void pause(bool value){std::lock_guard<std::mutex> lock(mutex_);paused_=value;}
    bool is_on_thread()const noexcept override{return current_==this;}
    bool can_invoke()const noexcept override{return true;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    void shutdown()override{std::deque<std::function<void()>> old;{std::lock_guard<std::mutex> lock(mutex_);closed_=true;old.swap(work_);}}
};
thread_local const coverage_scheduler* coverage_scheduler::current_=nullptr;
struct coverage_wire_state {
    sync_transport::on_open_handler opened;std::vector<std::string> frames;
    std::atomic<transport_state> state{transport_state::closed};
};
class coverage_wire final:public sync_transport {
    std::shared_ptr<coverage_wire_state> state_;
public:
    explicit coverage_wire(std::shared_ptr<coverage_wire_state> state):state_(std::move(state)){}
    void connect(const std::string&,const std::map<std::string,std::string>&)override{state_->state=transport_state::connecting;}
    void disconnect()override{state_->state=transport_state::closed;}
    transport_state state()const override{return state_->state.load();}
    bool supports_reconnect()const override{return false;}
    void send(const transport_message& message)override{state_->frames.push_back(message.as_string());}
    void set_on_open(on_open_handler fn)override{state_->opened=std::move(fn);}
    void set_on_message(on_message_handler)override{}
    void set_on_error(on_error_handler)override{}
    void set_on_close(on_close_handler)override{}
};
class coverage_sender final:public synchronizer {
public:
    using synchronizer::synchronizer;
    std::shared_ptr<lattice_db> retained_owner(){return owned_db_;}
    uint64_t generation()const{return reconnect_lifecycle_.load();}
    bool handoff(committed_export_frame frame){return recovery_export_route_->handoff(std::move(frame));}
};
thread_local std::function<void()> claim_action;
struct claim_hook {
    void(*prior)()=recovery_export_test_hooks::before_claim_commit;
    std::function<void()> old=std::move(claim_action);
    explicit claim_hook(std::function<void()> action){claim_action=std::move(action);recovery_export_test_hooks::before_claim_commit=[] {claim_action();};}
    ~claim_hook(){recovery_export_test_hooks::before_claim_commit=prior;claim_action=std::move(old);}
};
class RecoveryExportCoverage:public ::testing::Test {
protected:
    recovery_obligation_producer_discovery_limits caps{{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
    std::shared_ptr<coverage_scheduler> scheduler=std::make_shared<coverage_scheduler>();
    std::shared_ptr<lattice_db> owner;
    recovery_obligation_address address;
    void SetUp()override{
        configuration cfg(":memory:");cfg.audit_retention_seconds=0;cfg.sched=scheduler;
        owner=std::make_shared<lattice_db>(cfg);bind();
    }
    void TearDown()override{owner->close();owner.reset();scheduler->shutdown();}
    void bind(){
        committed(recovery_writer_access::install(owner,[&](database&){
            receive_install_store receiver(owner,caps.installations);receiver.initialize();
            receive_install_binding binding{"coverage-contribution","authority","source","epoch","scope","schema"};receiver.bind(binding);
            recovery_obligation_store journal(owner,caps.obligations,caps.installations);journal.initialize();address=journal.bind({binding,"grant","receipts"}).address;
        }));
    }
    void enroll(){committed(recovery_local_producer_adapter::enroll_for_qualification(owner,{address,{"ExportCoverageRow"},{'c'}},caps));}
    recovery_obligation_record add(const std::string& value="original"){
        const auto row=owner->add(ExportCoverageRow{value});
        const auto values=owner->db().query("SELECT id,globalId FROM AuditLog WHERE tableName='ExportCoverageRow' AND globalRowId=? ORDER BY id DESC LIMIT 1",{row.global_id()});
        if(values.size()!=1)throw std::runtime_error("fixture lacks actual generated original");
        return {std::get<int64_t>(values[0].at("id")),std::get<std::string>(values[0].at("globalId")),"ExportCoverageRow",row.global_id(),recovery_obligation_origin::local_candidate};
    }
    recovery_export_preparation prepare(size_t cap=4096,const std::vector<int64_t>& in_flight={},size_t count=1000,uint64_t generation=1){
        recovery_export_limits limits;limits.coverage_candidates=cap;
        return recovery_export_adapter::prepare_pending(owner,route_channel,generation,count,in_flight,false,limits);
    }
    void state(const recovery_obligation_record& row,int value){
        owner->db().execute("INSERT INTO _lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,?) ON CONFLICT(audit_entry_id,sync_id) DO UPDATE SET is_synchronized=excluded.is_synchronized",{row.audit_id,std::string(route_channel),static_cast<int64_t>(value)});
    }
    auto snapshot(){
        std::vector<std::vector<database::row_t>> result;
        for(const auto* table:{"_lattice_obligation_store","_lattice_obligation_scope","_lattice_obligation_entry","_lattice_obligation_producer_store","_lattice_obligation_producer_stamp","_lattice_sync_state","AuditLog"})
            result.push_back(owner->db().query(std::string("SELECT * FROM ")+table));
        return result;
    }
    void refusal(const char* reason,size_t cap=4096,const std::vector<int64_t>& in_flight={}){
        const auto before=snapshot();refused_at([&]{prepare(cap,in_flight);},reason);EXPECT_EQ(snapshot(),before);EXPECT_FALSE(owner->db().is_in_transaction());
    }
};
}

TEST_F(RecoveryExportCoverage, PreEnrollmentOriginalRefusesInsteadOfQuiet){
    add();register_replication_slot(owner->db(),route_channel);
    owner->db().execute("UPDATE _lattice_replication_slots SET upload_floor=99999 WHERE sync_id=?",{std::string(route_channel)});
    enroll();refusal("lacks current open obligation");
}
TEST_F(RecoveryExportCoverage, OrdinaryRecordedOriginalDoesNotBecomeGeneratedStamp){
    const auto old=add();
    committed(recovery_writer_access::install(owner,[&](database&){recovery_obligation_store(owner,caps.obligations,caps.installations).record(address,old);}));
    enroll();refusal("lacks generated stamp");
}
TEST_F(RecoveryExportCoverage, UnenrolledModelPendingAfterEnrollmentRefuses){
    enroll();owner->add(ExportCoverageOther{"uncovered"});refusal("no admitted contribution");
}
TEST_F(RecoveryExportCoverage, MissingJournalEntryCannotHideBehindInFlight){
    enroll();const auto row=add();
    committed(recovery_writer_access::install(owner,[&](database& db){db.execute("DELETE FROM _lattice_obligation_entry WHERE audit_id=?",{row.audit_id});}));
    refusal("lacks current open obligation",4096,{row.audit_id});
}
TEST_F(RecoveryExportCoverage, PreEnrollmentPendingInFlightStillRefuses){
    const auto row=add();enroll();refusal("lacks current open obligation",4096,{row.audit_id});
}
TEST_F(RecoveryExportCoverage, RemotePendingLegacyShapeRefuses){
    const auto row=add();owner->db().execute("UPDATE AuditLog SET isFromRemote=1 WHERE id=?",{row.audit_id});enroll();
    refusal("unsupported pending original");
}
TEST_F(RecoveryExportCoverage, SyntheticPendingLegacyShapeRefuses){
    const auto row=add();owner->db().execute("UPDATE AuditLog SET synthesized=1 WHERE id=?",{row.audit_id});enroll();
    refusal("unsupported pending original");
}
TEST_F(RecoveryExportCoverage, GlobalSynchronizedWithoutChannelStateIsResolved){
    enroll();const auto row=add();owner->db().execute("UPDATE AuditLog SET isSynchronized=1 WHERE id=?",{row.audit_id});
    const auto before=snapshot();const auto result=prepare();EXPECT_TRUE(result.protected_store);EXPECT_FALSE(result.frame);EXPECT_EQ(snapshot(),before);
}
TEST_F(RecoveryExportCoverage, ExplicitPendingOverridesGlobalSynchronizedAndUploadFloor){
    register_replication_slot(owner->db(),route_channel);
    owner->db().execute("UPDATE _lattice_replication_slots SET upload_floor=99999 WHERE sync_id=?",{std::string(route_channel)});
    enroll();const auto row=add();owner->db().execute("UPDATE AuditLog SET isSynchronized=1 WHERE id=?",{row.audit_id});state(row,0);
    const auto result=prepare();ASSERT_TRUE(result.frame);ASSERT_EQ(result.frame->entries().size(),1u);EXPECT_EQ(result.frame->entries()[0].id,row.audit_id);
}
TEST_F(RecoveryExportCoverage, GlobalSynchronizedExplicitLegacyPendingStillRefuses){
    const auto row=add();owner->db().execute("UPDATE AuditLog SET isSynchronized=1 WHERE id=?",{row.audit_id});state(row,0);enroll();
    refusal("lacks current open obligation");
}
TEST_F(RecoveryExportCoverage, GlobalPendingExplicitAcknowledgmentIsResolved){
    const auto row=add();state(row,1);enroll();const auto before=snapshot();const auto result=prepare();
    EXPECT_TRUE(result.protected_store);EXPECT_FALSE(result.frame);EXPECT_EQ(snapshot(),before);
}
TEST_F(RecoveryExportCoverage, GlobalRawStreamCapPrecedesAcknowledgmentExclusion){
    add("one");add("two");
    for(const auto& row:owner->db().query("SELECT id FROM AuditLog WHERE tableName='ExportCoverageRow'"))owner->db().execute("INSERT INTO _lattice_sync_state VALUES(?,?,1)",{std::get<int64_t>(row.at("id")),std::string(route_channel)});
    enroll();refusal("coverage budget exceeded",1);EXPECT_FALSE(prepare(2).frame);
}
TEST_F(RecoveryExportCoverage, ExplicitRawStreamCapPrecedesMissingAuditJoin){
    enroll();owner->db().execute("INSERT INTO _lattice_sync_state VALUES(9991,?,0),(9992,?,0)",{std::string(route_channel),std::string(route_channel)});
    refusal("coverage budget exceeded",1);refusal("pending audit disappeared",2);
}
TEST_F(RecoveryExportCoverage, InvalidCoverageCapsCannotDisableContainment){
    enroll();refusal("independent limits exceeded",0);refusal("independent limits exceeded",4097);
}
TEST_F(RecoveryExportCoverage, OversizedExplicitIdentityRefusesAtIntegerProjection){
    enroll();owner->db().execute("INSERT INTO _lattice_sync_state VALUES(?,?,0)",{std::string(262144,'x'),std::string(route_channel)});
    // The SELECT projects CASE/NULL, not the oversized stored TEXT. This
    // failure-stage oracle is not a runtime allocation/RSS measurement.
    refusal("expected INTEGER",1);
}
TEST_F(RecoveryExportCoverage, ValidInFlightOriginalIsCheckedButNotClaimedAgain){
    enroll();const auto row=add();const auto before=snapshot();const auto waiting=prepare(1,{row.audit_id});
    EXPECT_TRUE(waiting.protected_store);EXPECT_FALSE(waiting.frame);EXPECT_EQ(snapshot(),before);
    auto ready=prepare(1);ASSERT_TRUE(ready.frame);ASSERT_EQ(ready.frame->entries().size(),1u);EXPECT_EQ(ready.frame->entries()[0].id,row.audit_id);
}
TEST_F(RecoveryExportCoverage, ExactCandidateCapClaimsValidOriginalsAndAckThenIdles){
    enroll();const auto first=add("one"),second=add("two");auto result=prepare(2);ASSERT_TRUE(result.frame);ASSERT_EQ(result.frame->entries().size(),2u);
    EXPECT_EQ(result.frame->entries()[0].id,first.audit_id);EXPECT_EQ(result.frame->entries()[1].id,second.audit_id);
    recovery_export_adapter::acknowledge_legacy(owner,route_channel,{first.original_id,second.original_id});
    const auto before=snapshot();const auto idle=prepare(2);EXPECT_TRUE(idle.protected_store);EXPECT_FALSE(idle.frame);EXPECT_EQ(snapshot(),before);
}
TEST_F(RecoveryExportCoverage, UnsupportedLaterOriginalRefusesBeforeAnyPageClaim){
    enroll();add();owner->add(ExportCoverageOther{"after-valid"});const auto before=snapshot();
    refused_at([&]{prepare(4096,{},1);},"no admitted contribution");EXPECT_EQ(snapshot(),before);
}
TEST_F(RecoveryExportCoverage, BeforeClaimCommitUncoveredWriteRollsBackWholeAttempt){
    enroll();add();const auto before=snapshot();bool entered=false;
    {claim_hook hook([&]{entered=true;owner->add(ExportCoverageOther{"late"});});refused_at([&]{prepare();},"no admitted contribution");}
    EXPECT_TRUE(entered);EXPECT_EQ(snapshot(),before);EXPECT_TRUE(owner->db().query("SELECT id FROM ExportCoverageOther").empty());
    EXPECT_TRUE(prepare().frame);
}
TEST_F(RecoveryExportCoverage, RequiredPendingIndexDefinitionCannotBeSilentlyReplaced){
    owner->db().execute("DROP INDEX idx_audit_log_pending_sync");
    owner->db().execute("CREATE INDEX idx_audit_log_pending_sync ON AuditLog(isSynchronized) WHERE isSynchronized=1");
    enroll();refusal("required pending index differs");
}
TEST_F(RecoveryExportCoverage, ActualSenderReportsPreEnrollmentCoverageFailureWithoutFrame){
    owner->close();owner.reset();scheduler=std::make_shared<coverage_scheduler>();
    configuration cfg(":memory:");cfg.audit_retention_seconds=0;cfg.sched=scheduler;
    auto wire=std::make_shared<coverage_wire_state>();sync_config config;config.sync_id=route_channel;config.upload_coalesce_ms=0;config.checkpoint_passive_interval_ms=0;
    auto sender=std::make_unique<coverage_sender>(std::make_unique<lattice_db>(cfg),config,std::make_unique<coverage_wire>(wire));
    owner=sender->retained_owner();bind();add();enroll();std::vector<std::string> errors;
    sender->set_on_error([&](const std::string& error){errors.push_back(error);});sender->connect();ASSERT_TRUE(wire->opened);wire->state=transport_state::open;wire->opened();
    EXPECT_TRUE(wire->frames.empty());ASSERT_FALSE(errors.empty());EXPECT_NE(errors.back().find("coverage"),std::string::npos);
    sender.reset();
}
TEST_F(RecoveryExportCoverage, MovedFromPermitRefusesBeforeOneValidTransportHandoff){
    owner->close();owner.reset();scheduler=std::make_shared<coverage_scheduler>();
    configuration cfg(":memory:");cfg.audit_retention_seconds=0;cfg.sched=scheduler;
    auto wire=std::make_shared<coverage_wire_state>();sync_config config;config.sync_id=route_channel;config.upload_coalesce_ms=0;config.checkpoint_passive_interval_ms=0;
    auto sender=std::make_unique<coverage_sender>(std::make_unique<lattice_db>(cfg),config,std::make_unique<coverage_wire>(wire));
    owner=sender->retained_owner();bind();enroll();sender->connect();ASSERT_TRUE(wire->opened);wire->state=transport_state::open;wire->opened();ASSERT_TRUE(wire->frames.empty());
    scheduler->pause(true);add();auto prepared=prepare(4096,{},1000,sender->generation());ASSERT_TRUE(prepared.frame);
    auto moved=std::move(*prepared.frame);refused_at([&]{sender->handoff(std::move(*prepared.frame));},"permit already consumed");EXPECT_TRUE(wire->frames.empty());
    auto assigned=prepare(4096,{},1000,sender->generation());ASSERT_TRUE(assigned.frame);*assigned.frame=std::move(moved);
    refused_at([&]{sender->handoff(std::move(moved));},"permit already consumed");EXPECT_TRUE(wire->frames.empty());
    EXPECT_TRUE(sender->handoff(std::move(*assigned.frame)));EXPECT_EQ(wire->frames.size(),1u);
    refused_at([&]{sender->handoff(std::move(*assigned.frame));},"permit already consumed");EXPECT_EQ(wire->frames.size(),1u);
    sender.reset();
}
#endif
