#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include <deque>
#include <functional>
#include <limits>

#ifndef __EMSCRIPTEN__
struct ExportHistoryRow {std::string title;std::string body;};
LATTICE_SCHEMA(ExportHistoryRow,title,body);
struct ExportHistoryOther {std::string value;};
LATTICE_SCHEMA(ExportHistoryOther,value);
namespace {
using namespace lattice;
using namespace lattice::detail;
const bool history_schema=[] {
    auto schema=managed<ExportHistoryRow>::schema();schema.properties[1].no_history=true;
    schema_registry::instance().register_model(typeid(ExportHistoryRow),std::move(schema));return true;
}();
void history_committed(const recovery_install_result& result){
    if(result.primary_error)std::rethrow_exception(result.primary_error);
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
    if(result.state!=recovery_install_state::committed)throw std::runtime_error("history fixture did not commit");
}
// Inline only for opening an empty endpoint; thereafter bounded queued work
// never races the explicit private history preparation/handoff under test.
class history_scheduler final:public scheduler {
    std::mutex mutex_;bool paused_=false,closed_=false;std::deque<std::function<void()>> work_;
    static thread_local const history_scheduler* current_;
public:
    void invoke(std::function<void()>&& fn)override{
        {std::lock_guard<std::mutex> lock(mutex_);if(closed_)return;
         if(paused_){if(work_.size()==256)throw std::runtime_error("history fixture queue bound");work_.push_back(std::move(fn));return;}}
        struct restore {const history_scheduler* old;~restore(){current_=old;}} turn{current_};current_=this;fn();
    }
    void pause(){std::lock_guard<std::mutex> lock(mutex_);paused_=true;}
    bool is_on_thread()const noexcept override{return current_==this;}
    bool can_invoke()const noexcept override{return true;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    void shutdown()override{std::deque<std::function<void()>> old;{std::lock_guard<std::mutex> lock(mutex_);closed_=true;old.swap(work_);}}
};
thread_local const history_scheduler* history_scheduler::current_=nullptr;
struct history_wire_state {
    sync_transport::on_open_handler opened;std::vector<std::string> frames;
    std::function<void()> before_send;bool throw_send=false;
    std::atomic<transport_state> state{transport_state::closed};
};
class history_wire final:public sync_transport {
    std::shared_ptr<history_wire_state> shared_;
public:
    explicit history_wire(std::shared_ptr<history_wire_state> value):shared_(std::move(value)){}
    void connect(const std::string&,const std::map<std::string,std::string>&)override{shared_->state=transport_state::connecting;}
    void disconnect()override{shared_->state=transport_state::closed;}
    transport_state state()const override{return shared_->state.load();}
    bool supports_reconnect()const override{return false;}
    void send(const transport_message& message)override{
        if(shared_->before_send)shared_->before_send();shared_->frames.push_back(message.as_string());
        if(shared_->throw_send)throw std::runtime_error("history physical send failed");
    }
    void set_on_open(on_open_handler fn)override{shared_->opened=std::move(fn);}
    void set_on_message(on_message_handler)override{}
    void set_on_close(on_close_handler)override{}
    void set_on_error(on_error_handler)override{}
};
class history_sender final:public synchronizer {
public:
    using synchronizer::synchronizer;
    std::shared_ptr<lattice_db> owner(){return owned_db_;}
    uint64_t generation()const{return reconnect_lifecycle_.load();}
    bool handoff(committed_export_frame frame){return recovery_export_route_->handoff(std::move(frame));}
};
thread_local std::function<void()> history_before_action,history_after_action;
struct history_hooks {
    void(*before)()=recovery_export_test_hooks::before_claim_commit;
    void(*after)()=recovery_export_test_hooks::after_claim_commit;
    std::function<void()> old_before=std::move(history_before_action),old_after=std::move(history_after_action);
    history_hooks(std::function<void()> one={},std::function<void()> two={}){
        history_before_action=std::move(one);history_after_action=std::move(two);
        recovery_export_test_hooks::before_claim_commit=[] {if(history_before_action)history_before_action();};
        recovery_export_test_hooks::after_claim_commit=[] {if(history_after_action)history_after_action();};
    }
    ~history_hooks(){
        recovery_export_test_hooks::before_claim_commit=before;recovery_export_test_hooks::after_claim_commit=after;
        history_before_action=std::move(old_before);history_after_action=std::move(old_after);
    }
};
struct history_fault {
    enum class kind { second_claim, final_read, commit } mode;
    bool enabled=true;int writes=0,hits=0;
    recovery_local_producer_test_hooks::authorizer_fault fault;
    const recovery_local_producer_test_hooks::authorizer_fault* previous;
    history_fault* old;static thread_local history_fault* current;
    static int restrict_action(int action,const char* one,const char* two,const char*)noexcept{
        auto& self=*current;if(!self.enabled)return SQLITE_OK;
        if(self.mode==kind::second_claim && action==SQLITE_UPDATE && one && two &&
           !std::strcmp(one,"_lattice_obligation_entry") && !std::strcmp(two,"first_export") && ++self.writes==2){++self.hits;return SQLITE_DENY;}
        if(self.mode==kind::final_read && action==SQLITE_READ && one && two &&
           !std::strcmp(one,"AuditLog") && !std::strcmp(two,"changedFields")){++self.hits;return SQLITE_DENY;}
        if(self.mode==kind::commit && action==SQLITE_TRANSACTION && one && !std::strcmp(one,"COMMIT")){++self.hits;return SQLITE_DENY;}
        return SQLITE_OK;
    }
    history_fault(const lattice_db* owner,kind value):mode(value),fault{owner,&restrict_action},
        previous(recovery_local_producer_test_hooks::fault),old(current){current=this;recovery_local_producer_test_hooks::fault=&fault;}
    ~history_fault(){recovery_local_producer_test_hooks::fault=previous;current=old;}
};
thread_local history_fault* history_fault::current=nullptr;
class RecoveryExportHistory:public ::testing::TestWithParam<bool> {
protected:
    TempDB file{"history_export"};
    std::shared_ptr<history_scheduler> queue=std::make_shared<history_scheduler>();
    std::shared_ptr<history_wire_state> wire=std::make_shared<history_wire_state>();
    std::unique_ptr<history_sender> sender;std::shared_ptr<lattice_db> owner;
    recovery_obligation_producer_discovery_limits caps{{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
    recovery_obligation_address address,other_address;
    static receive_install_binding binding(const std::string& channel){return {channel,"authority","source","epoch",channel+"-scope","schema"};}
    template<class F> void transaction(F&& fn){history_committed(recovery_writer_access::install(owner,std::forward<F>(fn)));}
    recovery_obligation_store journal(){return {owner,caps.obligations,caps.installations};}
    recovery_obligation_address bind(const std::string& channel){
        recovery_obligation_address result;
        transaction([&](database&){receive_install_store receiver(owner,caps.installations);receiver.initialize();receiver.bind(binding(channel));
            auto storage=journal();storage.initialize();result=storage.bind({binding(channel),"grant","receipts"}).address;});return result;
    }
    void SetUp()override{
        configuration cfg(GetParam()?file.str():":memory:");cfg.audit_retention_seconds=0;cfg.busy_timeout_ms=100;cfg.sched=queue;
        sync_config config;config.sync_id="history-route";config.upload_coalesce_ms=0;config.checkpoint_passive_interval_ms=0;
        sender=std::make_unique<history_sender>(std::make_unique<lattice_db>(cfg),config,std::make_unique<history_wire>(wire));owner=sender->owner();
        if(GetParam()){auto* notifier=instance_registry::instance().get_or_create_notifier(file.str());if(notifier)notifier->stop_listening();}
        address=bind("a-history");
        history_committed(recovery_local_producer_adapter::enroll_for_qualification(owner,{address,{"ExportHistoryRow"},{'h'}},caps));
        sender->connect();ASSERT_TRUE(wire->opened);wire->state=transport_state::open;wire->opened();ASSERT_TRUE(wire->frames.empty());queue->pause();
    }
    void TearDown()override{wire->before_send={};sender.reset();queue->shutdown();if(owner){owner->close();owner.reset();}}
    recovery_obligation_record actual(int64_t id){
        const auto rows=owner->db().query("SELECT id,globalId,tableName,globalRowId FROM AuditLog WHERE id=?",{id});
        if(rows.size()!=1)throw std::runtime_error("missing history original");const auto& r=rows[0];
        return {std::get<int64_t>(r.at("id")),std::get<std::string>(r.at("globalId")),std::get<std::string>(r.at("tableName")),
            std::get<std::string>(r.at("globalRowId")),recovery_obligation_origin::local_candidate};
    }
    recovery_obligation_record latest(){const auto rows=owner->db().query("SELECT MAX(id) AS id FROM AuditLog");return actual(std::get<int64_t>(rows.at(0).at("id")));}
    recovery_obligation_record add(const std::string& title="one",const std::string& body="seed"){owner->add(ExportHistoryRow{title,body});return latest();}
    recovery_export_preparation page(int64_t after=0,size_t count=1000,const recovery_export_limits& limits={}){
        return recovery_export_adapter::prepare_history_page(owner,sender->generation(),after,count,limits);
    }
    recovery_obligation_entry entry(const recovery_obligation_record& row,const recovery_obligation_address* target=nullptr){
        std::optional<recovery_obligation_entry> found;transaction([&](database&){found=journal().find(target?*target:address,row.original_id);});
        if(!found)throw std::runtime_error("missing history obligation");return *found;
    }
    auto snapshot(){
        std::vector<std::vector<database::row_t>> result;
        for(const auto* table:{"_lattice_obligation_store","_lattice_obligation_scope","_lattice_obligation_entry","_lattice_obligation_producer_store",
            "_lattice_obligation_producer_stamp","_lattice_sync_state","AuditLog"})result.push_back(owner->db().query(std::string("SELECT * FROM ")+table+" ORDER BY 1,2"));
        return result;
    }
    void refuse_page(const char* message,int64_t after=0,size_t count=1000,const recovery_export_limits& limits={}){
        const auto before=snapshot();const auto sent=wire->frames.size();bool refused=false;
        try{auto unexpected=page(after,count,limits);(void)unexpected;}
        catch(const std::exception& error){refused=true;EXPECT_NE(std::string(error.what()).find(message),std::string::npos)<<error.what();}
        EXPECT_TRUE(refused);EXPECT_EQ(snapshot(),before);EXPECT_EQ(wire->frames.size(),sent);EXPECT_FALSE(owner->db().is_in_transaction());
    }
    std::vector<audit_log_entry> sent(size_t index){
        const auto result=server_sent_event::from_json(wire->frames.at(index));
        if(!result||result->event_type!=server_sent_event::type::audit_log)throw std::runtime_error("invalid history wire frame");return result->audit_logs;
    }
    static std::string value(const audit_log_entry& row,const char* name){return std::get<std::string>(row.changed_fields.at(name).value);}
    recovery_obligation_record legacy_middle(const recovery_obligation_record& first,int remote,int synthetic){
        // A genuine persisted legacy/imported/synthetic AuditLog shape, NOT
        // a generated-origin stamp; no raw handle or authorizer bypass.
        owner->db().execute("INSERT INTO AuditLog(globalId,tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,timestamp,isFromRemote,isSynchronized,synthesized) "
            "SELECT 'a0000000-0000-4000-8000-000000000001',tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,timestamp,?,0,? FROM AuditLog WHERE id=?",
            {int64_t(remote),int64_t(synthetic),first.audit_id});return latest();
    }
    void freeze(){recovery_obligation_address next;transaction([&](database&){next=journal().freeze(address,1).address;});address=next;}
};
}

TEST_P(RecoveryExportHistory, LegacyAckedOpenOriginalIsHistoryEvenWhenPendingIsEmpty){
    const auto row=add();auto first=page();ASSERT_TRUE(first.frame);const auto claim=entry(row).first_export_claim;ASSERT_TRUE(claim);
    recovery_export_adapter::acknowledge_legacy(owner,"history-route",{row.original_id});
    const auto pending=recovery_export_adapter::prepare_pending(owner,"history-route",sender->generation(),1000,{},false);EXPECT_FALSE(pending.frame);
    owner->db().execute("UPDATE AuditLog SET isSynchronized=1 WHERE id=?",{row.audit_id});
    auto history=page();ASSERT_TRUE(history.protected_store);ASSERT_TRUE(history.frame);ASSERT_EQ(history.frame->entries().size(),1u);
    EXPECT_EQ(history.frame->last_audit_id(),row.audit_id);EXPECT_EQ(history.frame->entries()[0].global_id,row.original_id);
    EXPECT_EQ(entry(row).first_export_claim,claim);EXPECT_TRUE(sender->handoff(std::move(*history.frame)));
    ASSERT_EQ(wire->frames.size(),1u);EXPECT_EQ(sent(0)[0].global_id,row.original_id);EXPECT_EQ(value(sent(0)[0],"title"),"one");
}
TEST_P(RecoveryExportHistory, ResolvedPkPagesDoNotUsePendingCoverageOrAdvanceBeyondSelectedRows){
    const auto one=add("one"),two=add("two"),three=add("three");recovery_export_limits limits;limits.coverage_candidates=1;
    auto first=page(0,2,limits);ASSERT_TRUE(first.frame);ASSERT_EQ(first.frame->entries().size(),2u);EXPECT_EQ(first.frame->last_audit_id(),two.audit_id);
    EXPECT_EQ(first.frame->entries()[0].id,one.audit_id);EXPECT_TRUE(entry(one).first_export_claim);EXPECT_TRUE(entry(two).first_export_claim);EXPECT_FALSE(entry(three).first_export_claim);
    auto second=page(two.audit_id,2,limits);ASSERT_TRUE(second.frame);ASSERT_EQ(second.frame->entries().size(),1u);EXPECT_EQ(second.frame->last_audit_id(),three.audit_id);
    const auto before=snapshot();const auto empty=page(three.audit_id,2,limits);EXPECT_TRUE(empty.protected_store);EXPECT_FALSE(empty.frame);EXPECT_EQ(snapshot(),before);
    // An empty sampled page is deliberately not an authority/frontier receipt.
    EXPECT_FALSE(page(std::numeric_limits<int64_t>::max(),1,limits).frame);
}
TEST_P(RecoveryExportHistory, ImportedMiddleRowRefusesWholePageWithoutFiltering){
    const auto first=add();const auto middle=legacy_middle(first,1,0);const auto last=add("last");
    ASSERT_LT(first.audit_id,middle.audit_id);ASSERT_LT(middle.audit_id,last.audit_id);refuse_page("local persisted original shape");
    EXPECT_FALSE(entry(first).first_export_claim);EXPECT_FALSE(entry(last).first_export_claim);
}
TEST_P(RecoveryExportHistory, SyntheticMiddleRowRefusesWholePageWithoutFiltering){
    const auto first=add();const auto middle=legacy_middle(first,0,1);const auto last=add("last");
    ASSERT_LT(first.audit_id,middle.audit_id);ASSERT_LT(middle.audit_id,last.audit_id);refuse_page("local persisted original shape");
}
TEST_P(RecoveryExportHistory, UnstampedCallerRecordedMiddleCannotBecomeExportable){
    const auto first=add();const auto middle=legacy_middle(first,0,0);const auto last=add("last");
    transaction([&](database&){journal().record(address,middle);});
    ASSERT_LT(first.audit_id,middle.audit_id);ASSERT_LT(middle.audit_id,last.audit_id);refuse_page("lacks generated stamp");
    EXPECT_FALSE(entry(first).first_export_claim);EXPECT_FALSE(entry(middle).first_export_claim);EXPECT_FALSE(entry(last).first_export_claim);
}
TEST_P(RecoveryExportHistory, UnrecordedMiddleCannotReportFalseEmptyOrSkip){
    const auto first=add();const auto middle=legacy_middle(first,0,0);add("last");refuse_page("lacks current open obligation");
    auto prefix=page(0,1);ASSERT_TRUE(prefix.frame);EXPECT_EQ(prefix.frame->last_audit_id(),first.audit_id);
    refuse_page("lacks current open obligation",first.audit_id,1);EXPECT_GT(middle.audit_id,first.audit_id);
}
TEST_P(RecoveryExportHistory, CanonicalAcknowledgedMiddleRemainsPinnedButCannotExport){
    add("first");const auto middle=add("middle");add("last");
    transaction([&](database&){journal().claim_export(address,{middle.original_id});
        journal().acknowledge(address,{middle.original_id,"receipts",5,recovery_obligation_outcome::applied});});
    // This is explicitly trusted storage-fixture state, not a bare wire ACK.
    EXPECT_EQ(entry(middle).stage,recovery_obligation_stage::acknowledged_awaiting_install);refuse_page("lacks current open obligation");
}
TEST_P(RecoveryExportHistory, SettledMiddleAfterResumeCannotBeSilentlySkipped){
    const auto first=add("first"),middle=add("middle"),last=add("last");
    receive_install_identity identity{1,0,{},9,receive_install_mode::full,"Q","E","C","M"};freeze();
    recovery_obligation_address resumed;
    transaction([&](database&){auto storage=journal();const auto snapshot=storage.snapshot_for_install(address,1);
        receive_install_store receiver(owner,caps.installations);receiver.apply_if_new(binding(address.channel),identity,{},[](database&){});
        storage.settle_install(address,snapshot.scope.revision,identity,{{middle.original_id,"receipts",5,recovery_obligation_outcome::applied}});
        resumed=storage.resume(address,identity).address;});address=resumed;
    EXPECT_EQ(entry(middle).stage,recovery_obligation_stage::settled);EXPECT_EQ(entry(first).stage,recovery_obligation_stage::open);EXPECT_EQ(entry(last).stage,recovery_obligation_stage::open);
    refuse_page("lacks current open obligation");
}
TEST_P(RecoveryExportHistory, SameViewNoHistoryAndNulBytesRemainImmutableAcrossPostCommitWriter){
    const std::string title("ti\0tle",6),seed("se\0ed",5),selected("selected\0body",13),later("later\0body",10);
    const auto inserted=add(title,seed);owner->db().execute("UPDATE ExportHistoryRow SET body=? WHERE globalId=?",{selected,inserted.target_id});const auto updated=latest();
    auto before=owner->db().query("SELECT * FROM AuditLog ORDER BY id");bool committed_claim=false;
    {
        history_hooks hook({},[&]{
            committed_claim=entry(updated).first_export_claim.has_value();std::exception_ptr failure;
            std::thread writer([&]{try{owner->db().execute("UPDATE ExportHistoryRow SET body=? WHERE globalId=?",{later,inserted.target_id});}catch(...){failure=std::current_exception();}});
            writer.join();if(failure)std::rethrow_exception(failure);
        });
        auto prepared=page(0,2);ASSERT_TRUE(prepared.frame);EXPECT_TRUE(committed_claim);EXPECT_EQ(prepared.frame->last_audit_id(),updated.audit_id);
        EXPECT_TRUE(sender->handoff(std::move(*prepared.frame)));
    }
    ASSERT_EQ(wire->frames.size(),1u);const auto wire_rows=sent(0);ASSERT_EQ(wire_rows.size(),2u);
    EXPECT_EQ(value(wire_rows[0],"title"),title);EXPECT_EQ(value(wire_rows[0],"body"),seed);EXPECT_EQ(value(wire_rows[1],"body"),selected);
    const auto after=owner->db().query("SELECT * FROM AuditLog ORDER BY id");ASSERT_EQ(after.size(),3u);EXPECT_EQ(after[0],before[0]);EXPECT_EQ(after[1],before[1]);
    const auto next=latest();EXPECT_FALSE(entry(next).first_export_claim);auto page_two=page(updated.audit_id,1);ASSERT_TRUE(page_two.frame);
    EXPECT_EQ(value(page_two.frame->entries()[0],"body"),later);EXPECT_EQ(page_two.frame->last_audit_id(),next.audit_id);
}
TEST_P(RecoveryExportHistory, MissingNoHistoryCurrentRowRefusesAllSelectedClaims){
    const auto row=add();owner->db().execute("UPDATE ExportHistoryRow SET body='updated' WHERE globalId=?",{row.target_id});
    owner->db().execute("DELETE FROM ExportHistoryRow WHERE globalId=?",{row.target_id});
    refuse_page("NoHistory current row is absent");EXPECT_FALSE(entry(row).first_export_claim);
}
TEST_P(RecoveryExportHistory, IndependentByteAndCountCapsRefuseBeforeClaims){
    const auto row=add("ordinary",std::string(300,'b'));recovery_export_limits limits;
    limits.field_bytes=256;refuse_page("byte budget exceeded before copy",0,1000,limits);
    limits.field_bytes=1024;limits.raw_bytes=1024;add("second",std::string(300,'c'));refuse_page("byte budget exceeded before copy",0,1000,limits);
    limits={};limits.wire_bytes=64;refuse_page("wire budget exceeded before serialization",0,1000,limits);
    limits={};limits.entries=1;refuse_page("independent limits exceeded",0,2,limits);
    refuse_page("independent limits exceeded",0,0);refuse_page("independent limits exceeded",0,1001);refuse_page("nonnegative PK",-1,1);
    EXPECT_FALSE(entry(row).first_export_claim);auto valid=page();ASSERT_TRUE(valid.frame);EXPECT_EQ(valid.frame->entries().size(),2u);
}
TEST_P(RecoveryExportHistory, LaterContributionClaimFailureRollsBackEarlierPositiveClaim){
    other_address=bind("z-history");history_committed(recovery_local_producer_adapter::enroll_for_qualification(owner,{other_address,{"ExportHistoryOther"},{'o'}},caps));
    const auto first=add();owner->add(ExportHistoryOther{"other"});const auto second=latest();const auto before=snapshot();int hits=0,writes=0;
    {history_fault fault(owner.get(),history_fault::kind::second_claim);EXPECT_THROW(page(),db_error);hits=fault.hits;writes=fault.writes;}
    EXPECT_EQ(hits,1);EXPECT_EQ(writes,2);EXPECT_EQ(snapshot(),before);EXPECT_FALSE(entry(first).first_export_claim);EXPECT_FALSE(entry(second,&other_address).first_export_claim);EXPECT_TRUE(wire->frames.empty());
    auto retry=page();ASSERT_TRUE(retry.frame);ASSERT_EQ(retry.frame->entries().size(),2u);EXPECT_TRUE(entry(first).first_export_claim);EXPECT_TRUE(entry(second,&other_address).first_export_claim);
}
TEST_P(RecoveryExportHistory, FinalOriginalReadFailureRollsBackAllClaimsAndAllowsRetry){
    const auto first=add(),second=add("second");const auto before=snapshot();int hits=0;bool claimed=false;
    {history_fault fault(owner.get(),history_fault::kind::final_read);fault.enabled=false;
     history_hooks hook([&]{const auto rows=owner->db().query("SELECT first_export FROM _lattice_obligation_entry ORDER BY audit_id");
         claimed=rows.size()==2&&std::holds_alternative<int64_t>(rows[0].at("first_export"))&&std::holds_alternative<int64_t>(rows[1].at("first_export"));fault.enabled=true;});
     EXPECT_THROW(page(),db_error);hits=fault.hits;}
    EXPECT_TRUE(claimed);EXPECT_GT(hits,0);EXPECT_EQ(snapshot(),before);EXPECT_TRUE(wire->frames.empty());
    EXPECT_FALSE(entry(first).first_export_claim);EXPECT_FALSE(entry(second).first_export_claim);EXPECT_TRUE(page().frame);
}
TEST_P(RecoveryExportHistory, PageMutationDuringClaimsRefusesInsteadOfReturningOldEmptyTail){
    add();const auto before=snapshot();bool added=false;
    {history_hooks hook([&]{owner->db().execute("INSERT INTO AuditLog(globalId,tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,timestamp,isFromRemote,isSynchronized,synthesized) "
         "VALUES('b0000000-0000-4000-8000-000000000001','ExportHistoryRow','INSERT',0,'b0000000-0000-4000-8000-000000000002','{}','[]',1,0,0,0)");added=true;});
     EXPECT_THROW(page(),db_error);}
    EXPECT_TRUE(added);EXPECT_EQ(snapshot(),before);EXPECT_TRUE(wire->frames.empty());EXPECT_TRUE(page().frame);
}
TEST_P(RecoveryExportHistory, ClaimCommitDenialReturnsNoPermitAndRollsBackBeforeRetry){
    const auto row=add();const auto before=snapshot();int hits=0;
    {history_fault fault(owner.get(),history_fault::kind::commit);EXPECT_THROW(page(),db_error);hits=fault.hits;}
    EXPECT_GT(hits,0);EXPECT_EQ(snapshot(),before);EXPECT_FALSE(entry(row).first_export_claim);EXPECT_TRUE(wire->frames.empty());EXPECT_TRUE(page().frame);
}
TEST_P(RecoveryExportHistory, DurableClaimPrecedesSendAndSendFailureKeepsExactRetry){
    const auto row=add();auto ready=page();ASSERT_TRUE(ready.frame);const auto cursor=ready.frame->last_audit_id();ASSERT_TRUE(cursor);const auto claim=entry(row).first_export_claim;ASSERT_TRUE(claim);
    bool durable=false;wire->before_send=[&]{durable=entry(row).first_export_claim==claim&&!owner->db().is_in_transaction();};wire->throw_send=true;
    EXPECT_THROW(sender->handoff(std::move(*ready.frame)),std::runtime_error);EXPECT_TRUE(durable);ASSERT_EQ(wire->frames.size(),1u);EXPECT_EQ(entry(row).first_export_claim,claim);
    EXPECT_FALSE(ready.frame->last_audit_id());wire->throw_send=false;auto retry=page();ASSERT_TRUE(retry.frame);EXPECT_EQ(retry.frame->last_audit_id(),cursor);EXPECT_EQ(entry(row).first_export_claim,claim);
    EXPECT_TRUE(sender->handoff(std::move(*retry.frame)));ASSERT_EQ(wire->frames.size(),2u);EXPECT_EQ(wire->frames[0],wire->frames[1]);
}
TEST_P(RecoveryExportHistory, FreezeAfterPreparationRefusesPhysicalHandoffWithClaimPinned){
    const auto row=add();auto ready=page();ASSERT_TRUE(ready.frame);const auto claim=entry(row).first_export_claim;ASSERT_TRUE(claim);freeze();
    EXPECT_THROW(sender->handoff(std::move(*ready.frame)),db_error);EXPECT_TRUE(wire->frames.empty());EXPECT_EQ(entry(row).first_export_claim,claim);
}
TEST_P(RecoveryExportHistory, ClosedOwnerCannotSendAlreadyPreparedPage){
    const auto row=add();auto ready=page();ASSERT_TRUE(ready.frame);EXPECT_TRUE(entry(row).first_export_claim);owner->close();
    EXPECT_FALSE(sender->handoff(std::move(*ready.frame)));EXPECT_TRUE(wire->frames.empty());
}
TEST_P(RecoveryExportHistory, NormalizedStampLookupPreservesActualPersistedUuidSpelling){
    const std::string target="ABCDEFAB-1234-4ABC-8DEF-ABCDEFABCDEF";
    owner->db().execute("INSERT INTO ExportHistoryRow(globalId,title,body) VALUES(?,?,?)",{target,std::string("case"),std::string("body")});
    const auto original=latest();const auto before=owner->db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto stored=entry(original);EXPECT_EQ(stored.record.target_id,target);EXPECT_EQ(stored.canonical_target_id,"abcdefab-1234-4abc-8def-abcdefabcdef");
    auto ready=page();ASSERT_TRUE(ready.frame);ASSERT_EQ(ready.frame->entries().size(),1u);EXPECT_EQ(ready.frame->entries()[0].global_row_id,target);
    EXPECT_TRUE(sender->handoff(std::move(*ready.frame)));ASSERT_EQ(wire->frames.size(),1u);EXPECT_EQ(sent(0)[0].global_row_id,target);EXPECT_EQ(sent(0)[0].global_id,original.original_id);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog ORDER BY id"),before);
}
TEST_P(RecoveryExportHistory, ExistingCallerWriteTransactionIsRefusedWithoutSettlement){
    owner->begin_transaction();const auto row=add("caller pending");const auto inside=snapshot();
    EXPECT_ANY_THROW(page());EXPECT_TRUE(owner->db().is_in_transaction());EXPECT_EQ(snapshot(),inside);EXPECT_TRUE(wire->frames.empty());
    const auto claims=owner->db().query("SELECT first_export FROM _lattice_obligation_entry WHERE audit_id=?",{row.audit_id});
    ASSERT_EQ(claims.size(),1u);EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(claims[0].at("first_export")));
    owner->rollback();EXPECT_TRUE(owner->db().query("SELECT id FROM AuditLog").empty());EXPECT_FALSE(page().frame);
}
INSTANTIATE_TEST_SUITE_P(MemoryAndFile,RecoveryExportHistory,::testing::Values(false,true),
    [](const ::testing::TestParamInfo<bool>& info){return info.param?"File":"Memory";});
#endif
