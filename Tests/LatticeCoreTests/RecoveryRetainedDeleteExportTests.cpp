#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include <deque>
#include <functional>
#include <limits>

#ifndef __EMSCRIPTEN__
struct RetainedDeleteRow {std::string title;std::string body;std::string memo;};
LATTICE_SCHEMA(RetainedDeleteRow,title,body,memo);
namespace {
using namespace lattice;
using namespace lattice::detail;
const bool retained_schema=[] {
    auto schema=managed<RetainedDeleteRow>::schema();schema.properties[1].no_history=true;schema.properties[2].no_history=true;
    schema_registry::instance().register_model(typeid(RetainedDeleteRow),std::move(schema));return true;
}();
void retained_committed(const recovery_install_result& result){
    if(result.primary_error)std::rethrow_exception(result.primary_error);
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
    if(result.state!=recovery_install_state::committed)throw std::runtime_error("retained fixture did not commit");
}
// Inline only for opening an empty endpoint; thereafter bounded queued work
// never races the explicit private retained preparation/handoff under test.
class retained_scheduler final:public scheduler {
    std::mutex mutex_;bool paused_=false,closed_=false;std::deque<std::function<void()>> work_;
    static thread_local const retained_scheduler* current_;
public:
    void invoke(std::function<void()>&& fn)override{
        {std::lock_guard<std::mutex> lock(mutex_);if(closed_)return;
         if(paused_){if(work_.size()==256)throw std::runtime_error("retained fixture queue bound");work_.push_back(std::move(fn));return;}}
        struct restore {const retained_scheduler* old;~restore(){current_=old;}} turn{current_};current_=this;fn();
    }
    void pause(){std::lock_guard<std::mutex> lock(mutex_);paused_=true;}
    bool is_on_thread()const noexcept override{return current_==this;}
    bool can_invoke()const noexcept override{return true;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    void shutdown()override{std::deque<std::function<void()>> old;{std::lock_guard<std::mutex> lock(mutex_);closed_=true;old.swap(work_);}}
};
thread_local const retained_scheduler* retained_scheduler::current_=nullptr;
struct retained_wire_state {
    sync_transport::on_open_handler opened;std::vector<std::string> frames;
    std::function<void()> before_send;bool throw_send=false;
    std::atomic<transport_state> state{transport_state::closed};
};
class retained_wire final:public sync_transport {
    std::shared_ptr<retained_wire_state> shared_;
public:
    explicit retained_wire(std::shared_ptr<retained_wire_state> value):shared_(std::move(value)){}
    void connect(const std::string&,const std::map<std::string,std::string>&)override{shared_->state=transport_state::connecting;}
    void disconnect()override{shared_->state=transport_state::closed;}
    transport_state state()const override{return shared_->state.load();}
    bool supports_reconnect()const override{return false;}
    void send(const transport_message& message)override{
        if(shared_->before_send)shared_->before_send();shared_->frames.push_back(message.as_string());
        if(shared_->throw_send)throw std::runtime_error("retained physical send failed");
    }
    void set_on_open(on_open_handler fn)override{shared_->opened=std::move(fn);}
    void set_on_message(on_message_handler)override{}
    void set_on_close(on_close_handler)override{}
    void set_on_error(on_error_handler)override{}
};
class retained_sender final:public synchronizer {
public:
    using synchronizer::synchronizer;
    std::shared_ptr<lattice_db> owner(){return owned_db_;}
    uint64_t generation()const{return reconnect_lifecycle_.load();}
    bool handoff(committed_export_frame frame){return recovery_export_route_->handoff(std::move(frame));}
};
thread_local std::function<void()> retained_before_action,retained_after_action;
struct retained_hooks {
    void(*before)()=recovery_export_test_hooks::before_claim_commit;
    void(*after)()=recovery_export_test_hooks::after_claim_commit;
    std::function<void()> old_before=std::move(retained_before_action),old_after=std::move(retained_after_action);
    retained_hooks(std::function<void()> one={},std::function<void()> two={}){
        retained_before_action=std::move(one);retained_after_action=std::move(two);
        recovery_export_test_hooks::before_claim_commit=[] {if(retained_before_action)retained_before_action();};
        recovery_export_test_hooks::after_claim_commit=[] {if(retained_after_action)retained_after_action();};
    }
    ~retained_hooks(){
        recovery_export_test_hooks::before_claim_commit=before;recovery_export_test_hooks::after_claim_commit=after;
        retained_before_action=std::move(old_before);retained_after_action=std::move(old_after);
    }
};
struct retained_fault {
    enum class kind { second_claim, final_read, model_read, commit } mode;
    bool enabled=true;int writes=0,hits=0;
    recovery_local_producer_test_hooks::authorizer_fault fault;
    const recovery_local_producer_test_hooks::authorizer_fault* previous;
    retained_fault* old;static thread_local retained_fault* current;
    static int restrict_action(int action,const char* one,const char* two,const char*)noexcept{
        auto& self=*current;if(!self.enabled)return SQLITE_OK;
        if(self.mode==kind::second_claim && action==SQLITE_UPDATE && one && two &&
           !std::strcmp(one,"_lattice_obligation_entry") && !std::strcmp(two,"first_export") && ++self.writes==2){++self.hits;return SQLITE_DENY;}
        if(self.mode==kind::final_read && action==SQLITE_READ && one && two &&
           !std::strcmp(one,"AuditLog") && !std::strcmp(two,"changedFields")){++self.hits;return SQLITE_DENY;}
        if(self.mode==kind::model_read && action==SQLITE_READ && one && !std::strcmp(one,"RetainedDeleteRow")){++self.hits;return SQLITE_DENY;}
        if(self.mode==kind::commit && action==SQLITE_TRANSACTION && one && !std::strcmp(one,"COMMIT")){++self.hits;return SQLITE_DENY;}
        return SQLITE_OK;
    }
    retained_fault(const lattice_db* owner,kind value):mode(value),fault{owner,&restrict_action},
        previous(recovery_local_producer_test_hooks::fault),old(current){current=this;recovery_local_producer_test_hooks::fault=&fault;}
    ~retained_fault(){recovery_local_producer_test_hooks::fault=previous;current=old;}
};
thread_local retained_fault* retained_fault::current=nullptr;
class RecoveryRetainedDeleteExport:public ::testing::TestWithParam<bool> {
protected:
    TempDB file{"retained_delete_client"},source_file{"retained_delete_source"};
    std::shared_ptr<retained_scheduler> queue=std::make_shared<retained_scheduler>();
    std::shared_ptr<retained_wire_state> wire=std::make_shared<retained_wire_state>();
    std::unique_ptr<retained_sender> sender;
    std::shared_ptr<lattice_db> owner,source;
    std::unique_ptr<canonical_writer_adapter> upstream;
    recovery_obligation_producer_discovery_limits caps{{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
    recovery_obligation_address address;
    canonical_writer_profile profile{{"retained-source","epoch","retained-model","schema"},
        {1024,262144,4096,1048576,64,64,64},{"RetainedDeleteRow"},true};
    template<class F> void transaction(F&& fn){retained_committed(recovery_writer_access::install(owner,std::forward<F>(fn)));}
    recovery_obligation_store journal(){return {owner,caps.obligations,caps.installations};}
    static void quiet(const std::shared_ptr<lattice_db>& value){
        if(value->config().is_in_memory())return;
        auto* notifier=instance_registry::instance().get_or_create_notifier(value->config().path);
        if(notifier)notifier->stop_listening();
    }
    configuration source_configuration(){configuration cfg(GetParam()?source_file.str():":memory:");cfg.audit_retention_seconds=0;cfg.busy_timeout_ms=100;return cfg;}
    void attach_source(){
        source=std::make_shared<lattice_db>(source_configuration());quiet(source);
        upstream=canonical_writer_adapter::attach_upstream_for_qualification(source,profile,{512,65536,1048576});
    }
    void SetUp()override{
        configuration cfg(GetParam()?file.str():":memory:");cfg.audit_retention_seconds=0;cfg.busy_timeout_ms=100;cfg.sched=queue;
        sync_config config;config.sync_id="retained-route";config.upload_coalesce_ms=0;config.checkpoint_passive_interval_ms=0;
        sender=std::make_unique<retained_sender>(std::make_unique<lattice_db>(cfg),config,std::make_unique<retained_wire>(wire));owner=sender->owner();quiet(owner);
        transaction([&](database&){receive_install_binding binding{"retained-contribution","authority","source","epoch","scope","schema"};
            receive_install_store receiver(owner,caps.installations);receiver.initialize();receiver.bind(binding);
            auto storage=journal();storage.initialize();address=storage.bind({binding,"grant","receipts"}).address;});
        retained_committed(recovery_local_producer_adapter::enroll_for_qualification(owner,{address,{"RetainedDeleteRow"},{'r'}},caps));
        sender->connect();ASSERT_TRUE(wire->opened);wire->state=transport_state::open;wire->opened();ASSERT_TRUE(wire->frames.empty());queue->pause();
        attach_source();
    }
    void TearDown()override{
        wire->before_send={};sender.reset();queue->shutdown();
        upstream.reset();if(source){source->close();source.reset();}if(owner){owner->close();owner.reset();}
    }
    recovery_obligation_record latest(){
        const auto rows=owner->db().query("SELECT id,globalId,tableName,globalRowId FROM AuditLog ORDER BY id DESC LIMIT 1");
        if(rows.size()!=1)throw std::runtime_error("missing retained original");const auto& r=rows[0];
        return {std::get<int64_t>(r.at("id")),std::get<std::string>(r.at("globalId")),std::get<std::string>(r.at("tableName")),
            std::get<std::string>(r.at("globalRowId")),recovery_obligation_origin::local_candidate};
    }
    recovery_obligation_record add(){owner->add(RetainedDeleteRow{"seed-title","seed-body","seed-memo"});return latest();}
    recovery_obligation_record update(const recovery_obligation_record& row,bool mixed=false){
        if(mixed)owner->db().execute("UPDATE RetainedDeleteRow SET title=?,body=?,memo=? WHERE globalId=?",
            {std::string("ordinary\0value",14),std::string("private-body"),std::string("private-memo"),row.target_id});
        else owner->db().execute("UPDATE RetainedDeleteRow SET body='private-body' WHERE globalId=?",{row.target_id});
        return latest();
    }
    recovery_obligation_record erase(const recovery_obligation_record& row){owner->db().execute("DELETE FROM RetainedDeleteRow WHERE globalId=?",{row.target_id});return latest();}
    recovery_export_preparation page(int64_t after=0,size_t count=1000,const recovery_export_limits& limits={}){
        return recovery_export_adapter::prepare_retained_page(owner,sender->generation(),after,count,limits);
    }
    recovery_obligation_entry obligation(const recovery_obligation_record& row){
        std::optional<recovery_obligation_entry> found;transaction([&](database&){auto storage=journal();found=storage.find(address,row.original_id);
            if(!found||!storage.pins_audit(row.audit_id,row.original_id))throw std::runtime_error("retained original not pinned");});return *found;
    }
    auto originals(){return owner->db().query("SELECT * FROM AuditLog ORDER BY id");}
    auto snapshot(){
        std::vector<std::vector<database::row_t>> result;
        for(const auto* table:{"_lattice_obligation_store","_lattice_obligation_scope","_lattice_obligation_entry","_lattice_obligation_producer_store",
            "_lattice_obligation_producer_stamp","_lattice_sync_state","AuditLog","RetainedDeleteRow"})
            result.push_back(owner->db().query(std::string("SELECT * FROM ")+table+" ORDER BY 1,2"));
        return result;
    }
    void refuse_page(const char* message,int64_t after,size_t count=1000,const recovery_export_limits& limits={}){
        const auto before=snapshot();const auto sent_count=wire->frames.size();bool refused=false;
        try{auto unexpected=page(after,count,limits);(void)unexpected;}
        catch(const std::exception& error){refused=true;EXPECT_NE(std::string(error.what()).find(message),std::string::npos)<<error.what();}
        EXPECT_TRUE(refused);EXPECT_EQ(snapshot(),before);EXPECT_EQ(wire->frames.size(),sent_count);EXPECT_FALSE(owner->db().is_in_transaction());
    }
    std::vector<audit_log_entry> sent(size_t index){
        const auto result=server_sent_event::from_json(wire->frames.at(index));
        if(!result||result->event_type!=server_sent_event::type::audit_log)throw std::runtime_error("invalid retained wire frame");return result->audit_logs;
    }
    std::vector<std::string> accept(size_t frame){return upstream->apply_upstream_owned(source,sent(frame));}
    std::optional<canonical_receipt> receipt(const std::string& original){
        std::optional<canonical_receipt> result;
        retained_committed(recovery_writer_access::install(source,[&](database&){canonical_change_store store(*source,profile.binding,profile.limits);
            result=store.receipt(canonical_writer_adapter::uuid_key(original));}));return result;
    }
    canonical_store_state source_state(){canonical_store_state result;
        retained_committed(recovery_writer_access::install(source,[&](database&){canonical_change_store store(*source,profile.binding,profile.limits);store.audit();result=store.state();}));return result;
    }
    void seed_source(){auto seed=page(0,1);if(!seed.frame||!sender->handoff(std::move(*seed.frame)))throw std::runtime_error("seed handoff failed");
        if(accept(wire->frames.size()-1).size()!=1)throw std::runtime_error("seed source acceptance failed");}
    void positive(const recovery_obligation_record& original,canonical_receipt_outcome outcome){
        const auto value=receipt(original.original_id);ASSERT_TRUE(value);EXPECT_GT(value->position,0);
        EXPECT_EQ(value->original.outcome,outcome);EXPECT_EQ(value->original.target,(canonical_identity{original.table,canonical_writer_adapter::uuid_key(original.target_id)}));
        EXPECT_EQ(value->original.original_id,canonical_writer_adapter::uuid_key(original.original_id));
    }
};
}

TEST_P(RecoveryRetainedDeleteExport, EmptyUpdateAndDeleteReachRealSourceAndLostAckRetry){
    const auto inserted=add();seed_source();const auto updated=update(inserted),deleted=erase(inserted);const auto before=originals();
    auto ready=page(inserted.audit_id,2);ASSERT_TRUE(ready.protected_store);ASSERT_TRUE(ready.frame);ASSERT_EQ(ready.frame->entries().size(),2u);
    EXPECT_EQ(ready.frame->entries()[0].id,updated.audit_id);EXPECT_EQ(ready.frame->entries()[1].id,deleted.audit_id);
    EXPECT_EQ(ready.frame->entries()[0].global_id,updated.original_id);EXPECT_EQ(ready.frame->entries()[1].global_id,deleted.original_id);
    const auto retained_body=owner->db().query("SELECT json_type(changedFields,'$.body') AS kind FROM AuditLog WHERE id=?",{updated.audit_id});
    ASSERT_EQ(retained_body.size(),1u);EXPECT_EQ(std::get<std::string>(retained_body[0].at("kind")),"null");
    EXPECT_EQ(ready.frame->last_audit_id(),deleted.audit_id);EXPECT_TRUE(ready.frame->entries()[0].changed_fields_names.empty());
    EXPECT_EQ(ready.frame->entries()[0].changed_fields.count("body"),0u);EXPECT_EQ(ready.frame->entries()[0].changed_fields.count("title"),1u);
    const auto uclaim=obligation(updated).first_export_claim,dclaim=obligation(deleted).first_export_claim;ASSERT_TRUE(uclaim);ASSERT_TRUE(dclaim);
    bool claimed_before_send=false;wire->before_send=[&]{claimed_before_send=!owner->db().is_in_transaction()&&obligation(updated).first_export_claim==uclaim&&obligation(deleted).first_export_claim==dclaim;};
    ASSERT_TRUE(sender->handoff(std::move(*ready.frame)));wire->before_send={};EXPECT_TRUE(claimed_before_send);
    const auto first_frame=wire->frames.size()-1;EXPECT_EQ(accept(first_frame),(std::vector<std::string>{updated.original_id,deleted.original_id}));
    positive(updated,canonical_receipt_outcome::no_op);positive(deleted,canonical_receipt_outcome::applied);
    const auto ureceipt=receipt(updated.original_id),dreceipt=receipt(deleted.original_id);ASSERT_TRUE(ureceipt);ASSERT_TRUE(dreceipt);
    EXPECT_LT(ureceipt->position,dreceipt->position);EXPECT_TRUE(source->objects<RetainedDeleteRow>().empty());
    const auto committed=source_state();const auto source_audit=source->db().query("SELECT * FROM AuditLog ORDER BY id");
    // Deliberately lose both acknowledgments: no client acknowledgment call.
    auto retry=page(inserted.audit_id,2);ASSERT_TRUE(retry.frame);EXPECT_TRUE(sender->handoff(std::move(*retry.frame)));
    EXPECT_EQ(wire->frames[first_frame],wire->frames.back());EXPECT_EQ(accept(wire->frames.size()-1),(std::vector<std::string>{updated.original_id,deleted.original_id}));
    EXPECT_EQ(source_state(),committed);EXPECT_EQ(source->db().query("SELECT * FROM AuditLog ORDER BY id"),source_audit);
    EXPECT_EQ(obligation(updated).first_export_claim,uclaim);EXPECT_EQ(obligation(deleted).first_export_claim,dclaim);
    EXPECT_EQ(obligation(updated).stage,recovery_obligation_stage::open);EXPECT_EQ(obligation(deleted).stage,recovery_obligation_stage::open);EXPECT_EQ(originals(),before);
    EXPECT_FALSE(recovery_local_producer_adapter::all_route_capability);EXPECT_FALSE(canonical_writer_adapter::serving_capability);
}
TEST_P(RecoveryRetainedDeleteExport, MixedMasksKeepHistoricalOrdinaryValueAndOmitOnlyUnavailableNoHistory){
    const auto inserted=add();seed_source();const auto updated=update(inserted,true),deleted=erase(inserted);const auto before=originals();
    auto ready=page(inserted.audit_id,2);ASSERT_TRUE(ready.frame);const auto& projected=ready.frame->entries()[0];
    EXPECT_EQ(projected.changed_fields_names,(std::vector<std::string>{"title"}));EXPECT_EQ(std::get<std::string>(projected.changed_fields.at("title").value),std::string("ordinary\0value",14));
    EXPECT_EQ(projected.changed_fields.count("body"),0u);EXPECT_EQ(projected.changed_fields.count("memo"),0u);
    ASSERT_TRUE(sender->handoff(std::move(*ready.frame)));const auto wire_rows=sent(wire->frames.size()-1);ASSERT_EQ(wire_rows.size(),2u);
    EXPECT_EQ(upstream->apply_upstream_owned(source,{wire_rows[0]}),(std::vector<std::string>{updated.original_id}));
    const auto public_rows=source->objects<RetainedDeleteRow>();ASSERT_EQ(public_rows.size(),1u);EXPECT_EQ(std::string(public_rows[0].title),std::string("ordinary\0value",14));
    positive(updated,canonical_receipt_outcome::applied);EXPECT_EQ(upstream->apply_upstream_owned(source,{wire_rows[1]}),(std::vector<std::string>{deleted.original_id}));
    positive(deleted,canonical_receipt_outcome::applied);EXPECT_TRUE(source->objects<RetainedDeleteRow>().empty());EXPECT_EQ(originals(),before);
}
TEST_P(RecoveryRetainedDeleteExport, AbsentSourceCommitsTwoRealNoopReceipts){
    const auto inserted=add();const auto updated=update(inserted),deleted=erase(inserted);auto ready=page(inserted.audit_id,2);ASSERT_TRUE(ready.frame);
    ASSERT_TRUE(sender->handoff(std::move(*ready.frame)));EXPECT_EQ(accept(0),(std::vector<std::string>{updated.original_id,deleted.original_id}));
    positive(updated,canonical_receipt_outcome::no_op);positive(deleted,canonical_receipt_outcome::no_op);EXPECT_EQ(source_state().receipts,2);EXPECT_TRUE(source->objects<RetainedDeleteRow>().empty());
}
TEST_P(RecoveryRetainedDeleteExport, PageBoundaryBeforeDeleteRefusesWithoutClaimsThenLargerBoundSucceeds){
    const auto inserted=add();const auto updated=update(inserted),deleted=erase(inserted);
    refuse_page("NoHistory current row is absent",inserted.audit_id,1);EXPECT_FALSE(obligation(updated).first_export_claim);EXPECT_FALSE(obligation(deleted).first_export_claim);
    auto ready=page(inserted.audit_id,2);ASSERT_TRUE(ready.frame);EXPECT_EQ(ready.frame->entries().size(),2u);
}
TEST_P(RecoveryRetainedDeleteExport, WrongTargetDeleteCannotJustifyMissingCurrentValue){
    const auto first=add();const auto updated=update(first);const auto other=add();const auto other_delete=erase(other);
    // Trusted owned-install fixture effect, not a received legacy frame and
    // not a generated local DELETE. This deliberately provides no delete proof.
    transaction([&](database& db){db.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        db.execute("DELETE FROM RetainedDeleteRow WHERE globalId=?",{first.target_id});
        db.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");});
    refuse_page("NoHistory current row is absent",first.audit_id,3);EXPECT_FALSE(obligation(updated).first_export_claim);EXPECT_FALSE(obligation(other_delete).first_export_claim);
}
TEST_P(RecoveryRetainedDeleteExport, UnstampedCallerDeleteCannotJustifyProjection){
    const auto inserted=add();const auto updated=update(inserted);
    owner->db().execute("INSERT INTO AuditLog(globalId,tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,timestamp,isFromRemote,isSynchronized,synthesized) "
        "SELECT 'cccccccc-0000-4000-8000-000000000001',tableName,'DELETE',rowId,globalRowId,changedFields,changedFieldsNames,timestamp,0,0,0 FROM AuditLog WHERE id=?",{updated.audit_id});
    const auto fake=latest();transaction([&](database&){journal().record(address,fake);});
    refuse_page("lacks generated stamp",inserted.audit_id,2);EXPECT_FALSE(obligation(updated).first_export_claim);EXPECT_FALSE(obligation(fake).first_export_claim);
}
TEST_P(RecoveryRetainedDeleteExport, ExistingCurrentValueStillLateBindsWithoutSelectedDelete){
    const auto inserted=add();const auto updated=update(inserted,true);const auto before=originals();auto ready=page(inserted.audit_id,1);ASSERT_TRUE(ready.frame);
    const auto& row=ready.frame->entries()[0];EXPECT_EQ(std::get<std::string>(row.changed_fields.at("body").value),"private-body");EXPECT_EQ(std::get<std::string>(row.changed_fields.at("memo").value),"private-memo");
    EXPECT_EQ(row.global_id,updated.original_id);EXPECT_EQ(row.changed_fields_names.size(),3u);EXPECT_EQ(originals(),before);
}
TEST_P(RecoveryRetainedDeleteExport, InitialCurrentRowReadFailureIsNotAbsenceAndRollsBack){
    const auto inserted=add();const auto updated=update(inserted);erase(inserted);const auto before=snapshot();int hits=0;
    {retained_fault fault(owner.get(),retained_fault::kind::model_read);
     EXPECT_THROW(page(inserted.audit_id,2),db_error);
     hits=fault.hits;}
    EXPECT_GT(hits,0);EXPECT_EQ(snapshot(),before);EXPECT_FALSE(obligation(updated).first_export_claim);EXPECT_TRUE(wire->frames.empty());EXPECT_TRUE(page(inserted.audit_id,2).frame);
}
TEST_P(RecoveryRetainedDeleteExport, FinalCurrentRowReadFailureCannotTurnIntoOmission){
    const auto inserted=add();const auto updated=update(inserted),deleted=erase(inserted);const auto before=snapshot();int hits=0;bool claims_present=false;
    {retained_fault fault(owner.get(),retained_fault::kind::model_read);fault.enabled=false;
     retained_hooks hooks([&]{const auto rows=owner->db().query("SELECT first_export FROM _lattice_obligation_entry WHERE audit_id IN (?,?) ORDER BY audit_id",{updated.audit_id,deleted.audit_id});
         claims_present=rows.size()==2&&std::holds_alternative<int64_t>(rows[0].at("first_export"))&&std::holds_alternative<int64_t>(rows[1].at("first_export"));fault.enabled=true;});
     EXPECT_THROW(page(inserted.audit_id,2),db_error);
     hits=fault.hits;}
    EXPECT_TRUE(claims_present);EXPECT_GT(hits,0);EXPECT_EQ(snapshot(),before);EXPECT_FALSE(obligation(updated).first_export_claim);EXPECT_FALSE(obligation(deleted).first_export_claim);EXPECT_TRUE(page(inserted.audit_id,2).frame);
}
TEST_P(RecoveryRetainedDeleteExport, ReentrantReinsertBeforeCommitRefusesAndRollsBackBothClaimsAndRow){
    const auto inserted=add();const auto updated=update(inserted);erase(inserted);const auto before=snapshot();bool inserted_again=false;
    {retained_hooks hooks([&]{owner->db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        owner->db().execute("INSERT INTO RetainedDeleteRow(globalId,title,body,memo) VALUES(?,'new','new-body','new-memo')",{inserted.target_id});
        owner->db().execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");inserted_again=true;});
     EXPECT_THROW(page(inserted.audit_id,2),db_error);}
    EXPECT_TRUE(inserted_again);EXPECT_EQ(snapshot(),before);EXPECT_FALSE(obligation(updated).first_export_claim);EXPECT_TRUE(page(inserted.audit_id,2).frame);
}
TEST_P(RecoveryRetainedDeleteExport, IndependentEntryRawWireAndCursorLimitsLeaveNoClaim){
    const auto inserted=add();const auto updated=update(inserted);erase(inserted);recovery_export_limits limits;limits.entries=1;
    refuse_page("independent limits exceeded",inserted.audit_id,2,limits);
    limits={};limits.field_bytes=16;refuse_page("byte budget exceeded before copy",inserted.audit_id,2,limits);
    limits={};limits.field_bytes=256;limits.raw_bytes=256;refuse_page("byte budget exceeded before copy",inserted.audit_id,2,limits);
    limits={};limits.wire_bytes=64;refuse_page("wire budget exceeded before serialization",inserted.audit_id,2,limits);
    refuse_page("nonnegative PK",-1,2);refuse_page("independent limits exceeded",inserted.audit_id,0);EXPECT_FALSE(obligation(updated).first_export_claim);
}
TEST_P(RecoveryRetainedDeleteExport, CommitDenialReturnsNoFrameAndKeepsOriginalsUnclaimed){
    const auto inserted=add();const auto updated=update(inserted);erase(inserted);const auto before=snapshot();int hits=0;
    {retained_fault fault(owner.get(),retained_fault::kind::commit);
     EXPECT_THROW(page(inserted.audit_id,2),db_error);
     hits=fault.hits;}
    EXPECT_GT(hits,0);EXPECT_EQ(snapshot(),before);EXPECT_FALSE(obligation(updated).first_export_claim);EXPECT_TRUE(wire->frames.empty());EXPECT_TRUE(page(inserted.audit_id,2).frame);
}
TEST_P(RecoveryRetainedDeleteExport, FreezeAfterPreparationRefusesHandoffAndPreservesClaims){
    const auto inserted=add();const auto updated=update(inserted);erase(inserted);auto ready=page(inserted.audit_id,2);ASSERT_TRUE(ready.frame);const auto claim=obligation(updated).first_export_claim;ASSERT_TRUE(claim);
    transaction([&](database&){address=journal().freeze(address,1).address;});
    EXPECT_THROW(sender->handoff(std::move(*ready.frame)),db_error);
    EXPECT_TRUE(wire->frames.empty());EXPECT_EQ(obligation(updated).first_export_claim,claim);
}
TEST_P(RecoveryRetainedDeleteExport, FileSourceReopenKeepsActualReceiptAndDuplicateDoesNotRepeatEffects){
    if(!GetParam())GTEST_SKIP()<<"durable reopen requires the file parameter";
    const auto inserted=add();seed_source();const auto updated=update(inserted),deleted=erase(inserted);auto ready=page(inserted.audit_id,2);ASSERT_TRUE(ready.frame);
    ASSERT_TRUE(sender->handoff(std::move(*ready.frame)));const auto frame=wire->frames.size()-1;ASSERT_EQ(accept(frame).size(),2u);const auto before=source_state();
    upstream.reset();source->close();source.reset();attach_source();EXPECT_EQ(source_state(),before);EXPECT_TRUE(source->objects<RetainedDeleteRow>().empty());
    positive(updated,canonical_receipt_outcome::no_op);positive(deleted,canonical_receipt_outcome::applied);EXPECT_EQ(accept(frame).size(),2u);EXPECT_EQ(source_state(),before);
}
INSTANTIATE_TEST_SUITE_P(MemoryAndFile,RecoveryRetainedDeleteExport,::testing::Values(false,true),
    [](const ::testing::TestParamInfo<bool>& info){return info.param?"File":"Memory";});
#endif
