#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include <condition_variable>
#include <deque>

#ifndef __EMSCRIPTEN__
struct ExportPayloadDoc {std::string title;std::string body;};
LATTICE_SCHEMA(ExportPayloadDoc,title,body);
struct ExportPayloadRoot {std::string name;};
LATTICE_SCHEMA(ExportPayloadRoot,name);
struct ExportPayloadLeaf {std::string name;};
LATTICE_SCHEMA(ExportPayloadLeaf,name);

namespace {
using namespace lattice;
using namespace lattice::detail;
using namespace std::chrono_literals;
const bool register_export_payload_shapes=[] {
    auto doc=managed<ExportPayloadDoc>::schema();doc.properties[1].no_history=true;
    schema_registry::instance().register_model(typeid(ExportPayloadDoc),std::move(doc));
    auto root=managed<ExportPayloadRoot>::schema();property_descriptor link{};
    link.name="leaf";link.kind=property_kind::link;link.type=column_type::integer;
    link.nullable=true;link.target_table="ExportPayloadLeaf";root.properties.push_back(link);
    schema_registry::instance().register_model(typeid(ExportPayloadRoot),std::move(root));return true;
}();
constexpr const char* payload_link="_ExportPayloadRoot_ExportPayloadLeaf_leaf";
class payload_scheduler final:public scheduler {
    std::mutex mutex_;bool paused_=false,closed_=false;std::deque<std::function<void()>> pending_;
    static thread_local const payload_scheduler* current_;
public:
    void invoke(std::function<void()>&& job)override {
        {std::lock_guard<std::mutex> lock(mutex_);if(closed_)return;if(paused_){pending_.push_back(std::move(job));return;}}
        struct restore {const payload_scheduler* prior;~restore(){current_=prior;}} turn{current_};current_=this;job();
    }
    void pause(bool value){std::lock_guard<std::mutex> lock(mutex_);paused_=value;}
    bool is_on_thread()const noexcept override{return current_==this;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
    void shutdown()override {
        std::deque<std::function<void()>> old;{std::lock_guard<std::mutex> lock(mutex_);closed_=true;old.swap(pending_);}
    }
};
thread_local const payload_scheduler* payload_scheduler::current_=nullptr;
struct payload_pause {
    std::shared_ptr<payload_scheduler> queue;
    explicit payload_pause(std::shared_ptr<payload_scheduler> q):queue(std::move(q)){queue->pause(true);}
    ~payload_pause(){queue->pause(false);}
};
struct payload_wire_state {
    std::mutex mutex;sync_transport::on_open_handler opened;sync_transport::on_message_handler message;
    std::vector<std::string> frames;bool throw_send=false;std::atomic<transport_state> state{transport_state::closed};
    void open(){sync_transport::on_open_handler callback;{std::lock_guard<std::mutex> lock(mutex);callback=opened;}state=transport_state::open;callback();}
    void ack(const std::string& original) {
        sync_transport::on_message_handler callback;{std::lock_guard<std::mutex> lock(mutex);callback=message;}
        const auto encoded=server_sent_event::make_ack({original}).to_json();
        const auto frame=transport_message::from_binary({encoded.begin(),encoded.end()});callback(frame);
    }
    void fail(bool value){std::lock_guard<std::mutex> lock(mutex);throw_send=value;}
    std::vector<std::string> sent(){std::lock_guard<std::mutex> lock(mutex);return frames;}
};
class payload_wire final:public sync_transport {
    std::shared_ptr<payload_wire_state> shared_;
public:
    explicit payload_wire(std::shared_ptr<payload_wire_state> shared):shared_(std::move(shared)){}
    void connect(const std::string&,const std::map<std::string,std::string>&)override{shared_->state=transport_state::connecting;}
    void disconnect()override{shared_->state=transport_state::closed;}
    transport_state state()const override{return shared_->state.load();}
    bool supports_reconnect()const override{return false;}
    void send(const transport_message& frame)override {
        bool fail;{std::lock_guard<std::mutex> lock(shared_->mutex);shared_->frames.push_back(frame.as_string());fail=shared_->throw_send;}
        if(fail)throw std::runtime_error("actual payload send interrupted");
    }
    void set_on_open(on_open_handler fn)override{std::lock_guard<std::mutex> lock(shared_->mutex);shared_->opened=std::move(fn);}
    void set_on_message(on_message_handler fn)override{std::lock_guard<std::mutex> lock(shared_->mutex);shared_->message=std::move(fn);}
    void set_on_error(on_error_handler)override{}
    void set_on_close(on_close_handler)override{}
};
class payload_sender final:public synchronizer {
public:
    using synchronizer::synchronizer;
    std::shared_ptr<lattice_db> retained_owner(){return owned_db_;}
    void page_size(size_t n){config_.chunk_size=n;}
};
struct payload_ack_hold {
    std::mutex mutex;std::condition_variable changed;bool released=false,timed_out=false;size_t completed=0;
    void wait(){std::unique_lock<std::mutex> lock(mutex);if(!changed.wait_for(lock,5s,[&]{return released;})){timed_out=true;throw std::runtime_error("payload ACK gate timed out");}}
    void finish(){std::lock_guard<std::mutex> lock(mutex);++completed;changed.notify_all();}
    bool release_and_wait(size_t expected){
        std::unique_lock<std::mutex> lock(mutex);released=true;changed.notify_all();
        return changed.wait_for(lock,5s,[&]{return completed==expected;})&&!timed_out;
    }
};
thread_local std::function<void()> payload_after_claim;
struct payload_claim_hook {
    void (*prior)()=recovery_export_test_hooks::after_claim_commit;
    std::function<void()> old=std::move(payload_after_claim);
    explicit payload_claim_hook(std::function<void()> action){payload_after_claim=std::move(action);recovery_export_test_hooks::after_claim_commit=[] {payload_after_claim();};}
    ~payload_claim_hook(){recovery_export_test_hooks::after_claim_commit=prior;payload_after_claim=std::move(old);}
};
void payload_commit(const recovery_install_result& result) {
    if(result.state!=recovery_install_state::committed){if(result.primary_error)std::rethrow_exception(result.primary_error);throw std::runtime_error("payload fixture transaction did not commit");}
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
}
const audit_log_entry& payload_entry(const std::vector<audit_log_entry>& entries,const std::string& original) {
    for(const auto& entry:entries)if(entry.global_id==original)return entry;
    throw std::runtime_error("actual payload frame lacks selected original");
}
std::string payload_string(const audit_log_entry& entry,const std::string& field){return std::get<std::string>(entry.changed_fields.at(field).value);}
class RecoveryExportPayload:public ::testing::Test {
protected:
    std::shared_ptr<payload_scheduler> queue=std::make_shared<payload_scheduler>();
    std::shared_ptr<payload_wire_state> transport=std::make_shared<payload_wire_state>();
    std::shared_ptr<payload_ack_hold> ack_hold=std::make_shared<payload_ack_hold>();
    std::shared_ptr<const sync_background_test_hooks::ack_schedule> prior_ack=sync_background_test_hooks::ack;
    std::unique_ptr<payload_sender> sender;std::shared_ptr<lattice_db> owner;
    recovery_obligation_producer_discovery_limits caps{{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
    recovery_obligation_address address;
    void SetUp()override {
        configuration cfg(":memory:");cfg.audit_retention_seconds=0;cfg.sched=queue;
        sync_config config;config.sync_id="payload-route";config.upload_coalesce_ms=0;config.checkpoint_passive_interval_ms=0;
        sender=std::make_unique<payload_sender>(std::make_unique<lattice_db>(cfg),config,std::make_unique<payload_wire>(transport));
        owner=sender->retained_owner();
        payload_commit(recovery_writer_access::install(owner,[&](database&) {
            receive_install_store receiver(owner,caps.installations);receiver.initialize();
            receive_install_binding binding{"payload-contribution","authority","source","epoch","scope","schema"};receiver.bind(binding);
            recovery_obligation_store journal(owner,caps.obligations,caps.installations);journal.initialize();address=journal.bind({binding,"grant","receipts"}).address;
        }));
        payload_commit(recovery_local_producer_adapter::enroll_for_qualification(owner,
            {address,{"ExportPayloadDoc","ExportPayloadRoot","ExportPayloadLeaf"},{'p'}},caps));
        const auto hold=ack_hold;auto hook=std::make_shared<sync_background_test_hooks::ack_schedule>();
        hook->before_expiry=[hold]{hold->wait();};hook->completed=[hold]{hold->finish();};sync_background_test_hooks::ack=std::move(hook);
        sender->connect();transport->open();ASSERT_TRUE(transport->sent().empty());
    }
    void TearDown()override {
        sender.reset();EXPECT_TRUE(ack_hold->release_and_wait(transport->sent().size()));
        sync_background_test_hooks::ack=std::move(prior_ack);queue->shutdown();
    }
    void send(){sender->drain(std::chrono::steady_clock::now());}
    std::vector<database::row_t> originals(){return owner->db().query("SELECT * FROM AuditLog ORDER BY id");}
    std::vector<audit_log_entry> frame(size_t at) {
        const auto frames=transport->sent();if(at>=frames.size())throw std::runtime_error("missing actual payload frame");
        const auto event=server_sent_event::from_json(frames[at]);
        if(!event||event->event_type!=server_sent_event::type::audit_log)throw std::runtime_error("invalid actual payload frame");
        return event->audit_logs;
    }
    recovery_obligation_entry obligation(const std::string& original) {
        std::optional<recovery_obligation_entry> entry;
        payload_commit(recovery_writer_access::install(owner,[&](database&) {
            recovery_obligation_store journal(owner,caps.obligations,caps.installations);entry=journal.find(address,original);
            if(!entry||!journal.pins_audit(entry->record.audit_id,entry->record.original_id))throw std::runtime_error("actual original is not retained/pinned");
        }));return *entry;
    }
    auto snapshot() {
        std::vector<std::vector<database::row_t>> rows;
        for(const auto* name:{"_lattice_obligation_store","_lattice_obligation_scope","_lattice_obligation_entry",
            "_lattice_obligation_producer_store","_lattice_obligation_producer_stamp","AuditLog"})
            rows.push_back(owner->db().query(std::string("SELECT * FROM ")+name+" ORDER BY 1,2"));
        return rows;
    }
};
}

TEST_F(RecoveryExportPayload, GenuineNoHistoryInsertAndUpdateRetryKeepOriginalsAndStickyClaims) {
    std::string target;
    {payload_pause pause(queue);auto row=owner->add(ExportPayloadDoc{"title","insert-body"});target=row.global_id();row.body="first-current-body";}
    const auto before=originals();ASSERT_EQ(before.size(),2u);
    const auto insert_id=std::get<std::string>(before[0].at("globalId"));const auto update_id=std::get<std::string>(before[1].at("globalId"));
    EXPECT_EQ(std::get<std::string>(owner->db().query("SELECT json_type(changedFields,'$.body') AS kind FROM AuditLog WHERE globalId=?",{update_id})[0].at("kind")),"null");
    transport->fail(true);EXPECT_THROW(send(),std::runtime_error);ASSERT_EQ(transport->sent().size(),1u);
    const auto first=frame(0);ASSERT_EQ(first.size(),2u);
    EXPECT_EQ(payload_string(payload_entry(first,insert_id),"body"),"insert-body");
    EXPECT_EQ(payload_string(payload_entry(first,update_id),"body"),"first-current-body");
    const auto inserted_claim=obligation(insert_id);const auto updated_claim=obligation(update_id);
    ASSERT_TRUE(inserted_claim.first_export_claim);ASSERT_TRUE(updated_claim.first_export_claim);EXPECT_EQ(originals(),before);
    {payload_pause pause(queue);owner->db().execute("UPDATE ExportPayloadDoc SET body=? WHERE globalId=?",{std::string("retry-current-body"),target});}
    const auto retry_originals=originals();ASSERT_EQ(retry_originals.size(),3u);
    EXPECT_EQ(retry_originals[0],before[0]);EXPECT_EQ(retry_originals[1],before[1]);
    transport->fail(false);EXPECT_NO_THROW(send());ASSERT_EQ(transport->sent().size(),2u);
    const auto retry=frame(1);ASSERT_EQ(retry.size(),3u);
    EXPECT_EQ(payload_string(payload_entry(retry,insert_id),"body"),"insert-body");
    EXPECT_EQ(payload_string(payload_entry(retry,update_id),"body"),"retry-current-body");
    const auto latest_id=std::get<std::string>(retry_originals[2].at("globalId"));
    EXPECT_EQ(payload_string(payload_entry(retry,latest_id),"body"),"retry-current-body");
    EXPECT_EQ(obligation(insert_id).first_export_claim,inserted_claim.first_export_claim);
    EXPECT_EQ(obligation(update_id).first_export_claim,updated_claim.first_export_claim);
    EXPECT_EQ(originals(),retry_originals);EXPECT_EQ(obligation(update_id).stage,recovery_obligation_stage::open);
}

TEST_F(RecoveryExportPayload, ActualRegularLinkUsesPositiveAuditIdWithZeroModelRowId) {
    std::string lhs,rhs;const std::string relation="aaaaaaaa-0000-4000-8000-000000000881";
    {payload_pause pause(queue);auto root=owner->add(ExportPayloadRoot{"parent"});auto leaf=owner->add(ExportPayloadLeaf{"child"});
     lhs=root.global_id();rhs=leaf.global_id();owner->ensure_link_table(payload_link,"ExportPayloadRoot","ExportPayloadLeaf");
     owner->db().execute("INSERT INTO "+std::string(payload_link)+"(globalId,lhs,rhs) VALUES(?,?,?)",{relation,lhs,rhs});}
    const auto before=originals();ASSERT_EQ(before.size(),3u);
    const auto row=owner->db().query("SELECT id,globalId,rowId FROM AuditLog WHERE tableName=?",{std::string(payload_link)});
    ASSERT_EQ(row.size(),1u);const auto original=std::get<std::string>(row[0].at("globalId"));
    ASSERT_GT(std::get<int64_t>(row[0].at("id")),0);ASSERT_EQ(std::get<int64_t>(row[0].at("rowId")),0);
    EXPECT_NO_THROW(send());ASSERT_EQ(transport->sent().size(),1u);
    const auto entries=frame(0);ASSERT_EQ(entries.size(),3u);const auto& link=payload_entry(entries,original);
    EXPECT_EQ(link.id,std::get<int64_t>(row[0].at("id")));EXPECT_EQ(link.row_id,0);
    EXPECT_EQ(link.table_name,payload_link);EXPECT_EQ(link.global_row_id,relation);EXPECT_EQ(link.operation,"INSERT");
    EXPECT_EQ(payload_string(link,"lhs"),lhs);EXPECT_EQ(payload_string(link,"rhs"),rhs);
    const auto claimed=obligation(original);ASSERT_TRUE(claimed.first_export_claim);
    EXPECT_EQ(claimed.record.audit_id,link.id);EXPECT_EQ(claimed.record.target_id,relation);EXPECT_EQ(originals(),before);
}

TEST_F(RecoveryExportPayload, ActualTransportPreservesEmbeddedNulOrdinaryAndNoHistoryText) {
    const std::string title("left\0right",10),seed("seed\0body",9),latest("new\0suffix",10);std::string target;
    {payload_pause pause(queue);auto row=owner->add(ExportPayloadDoc{title,seed});target=row.global_id();row.body=latest;}
    const auto before=originals();ASSERT_EQ(before.size(),2u);
    const auto insert_id=std::get<std::string>(before[0].at("globalId"));const auto update_id=std::get<std::string>(before[1].at("globalId"));
    const auto current=owner->db().query("SELECT title,body FROM ExportPayloadDoc WHERE globalId=?",{target});ASSERT_EQ(current.size(),1u);
    ASSERT_EQ(std::get<std::string>(current[0].at("title")),title);ASSERT_EQ(std::get<std::string>(current[0].at("body")),latest);
    EXPECT_NO_THROW(send());ASSERT_EQ(transport->sent().size(),1u);const auto entries=frame(0);ASSERT_EQ(entries.size(),2u);
    EXPECT_EQ(payload_string(payload_entry(entries,insert_id),"title"),title);
    EXPECT_EQ(payload_string(payload_entry(entries,insert_id),"body"),seed);
    EXPECT_EQ(payload_string(payload_entry(entries,update_id),"body"),latest);
    EXPECT_EQ(originals(),before);EXPECT_TRUE(obligation(insert_id).first_export_claim);EXPECT_TRUE(obligation(update_id).first_export_claim);
}

TEST_F(RecoveryExportPayload, OversizedLiveNoHistoryRefusesBeforeAnyClaimOrPhysicalSend) {
    const auto bytes=recovery_export_limits{}.field_bytes+1;std::string target;
    {payload_pause pause(queue);auto row=owner->add(ExportPayloadDoc{"small","seed"});target=row.global_id();row.body=std::string(bytes,'x');}
    const auto before=snapshot();const auto audit=originals();ASSERT_EQ(audit.size(),2u);
    std::string refusal;
    try{send();ADD_FAILURE()<<"oversized current body unexpectedly exported";}
    catch(const db_error& error){refusal=error.what();}
    EXPECT_NE(refusal.find("byte budget exceeded before copy"),std::string::npos);
    EXPECT_TRUE(transport->sent().empty());EXPECT_EQ(snapshot(),before);EXPECT_FALSE(owner->db().is_in_transaction());
    for(const auto& row:audit)EXPECT_FALSE(obligation(std::get<std::string>(row.at("globalId"))).first_export_claim);
    EXPECT_EQ(std::get<int64_t>(owner->db().query("SELECT length(CAST(body AS BLOB)) AS n FROM ExportPayloadDoc WHERE globalId=?",{target})[0].at("n")),static_cast<int64_t>(bytes));
}

TEST_F(RecoveryExportPayload, ActualOneEntryPagesClaimOnlyTheirSelectedOriginal) {
    sender->page_size(1);
    {payload_pause pause(queue);owner->add(ExportPayloadDoc{"first","one"});owner->add(ExportPayloadDoc{"second","two"});owner->add(ExportPayloadDoc{"third","three"});}
    const auto before=originals();ASSERT_EQ(before.size(),3u);std::vector<std::optional<int64_t>> first_claims(3);
    for(size_t page=0;page<3;++page) {
        SCOPED_TRACE(page);EXPECT_NO_THROW(send());ASSERT_EQ(transport->sent().size(),page+1);
        const auto entries=frame(page);ASSERT_EQ(entries.size(),1u);
        EXPECT_EQ(entries[0].global_id,std::get<std::string>(before[page].at("globalId")));
        for(size_t row=0;row<3;++row) {
            const auto state=obligation(std::get<std::string>(before[row].at("globalId")));
            if(row<=page){ASSERT_TRUE(state.first_export_claim);if(!first_claims[row])first_claims[row]=state.first_export_claim;EXPECT_EQ(state.first_export_claim,first_claims[row]);}
            else EXPECT_FALSE(state.first_export_claim);
        }
    }
    EXPECT_EQ(originals(),before);
}

TEST_F(RecoveryExportPayload, PostClaimLocalWriteCannotMixSelectedOriginalAndNextPagePayload) {
    sender->page_size(1);std::string target;
    {payload_pause pause(queue);auto row=owner->add(ExportPayloadDoc{"title","seed"});target=row.global_id();}
    EXPECT_NO_THROW(send());ASSERT_EQ(transport->sent().size(),1u);const auto inserted=frame(0);ASSERT_EQ(inserted.size(),1u);
    EXPECT_NO_THROW(transport->ack(inserted[0].global_id));
    const auto ack=owner->db().query("SELECT is_synchronized FROM _lattice_sync_state WHERE audit_entry_id=? AND sync_id='payload-route'",{inserted[0].id});
    ASSERT_EQ(ack.size(),1u);ASSERT_EQ(std::get<int64_t>(ack[0].at("is_synchronized")),1);
    {payload_pause pause(queue);owner->db().execute("UPDATE ExportPayloadDoc SET body='selected-body' WHERE globalId=?",{target});}
    const auto before=originals();ASSERT_EQ(before.size(),2u);const auto selected_id=std::get<std::string>(before[1].at("globalId"));
    int writes=0;
    {payload_claim_hook hook([&]{++writes;payload_pause pause(queue);owner->db().execute("UPDATE ExportPayloadDoc SET body='next-page-body' WHERE globalId=?",{target});});
     EXPECT_NO_THROW(send());}
    ASSERT_EQ(writes,1);ASSERT_EQ(transport->sent().size(),2u);const auto selected=frame(1);ASSERT_EQ(selected.size(),1u);
    EXPECT_EQ(selected[0].global_id,selected_id);EXPECT_EQ(payload_string(selected[0],"body"),"selected-body");
    const auto after=originals();ASSERT_EQ(after.size(),3u);EXPECT_EQ(after[0],before[0]);EXPECT_EQ(after[1],before[1]);
    const auto next_id=std::get<std::string>(after[2].at("globalId"));const auto selected_claim=obligation(selected_id);
    ASSERT_TRUE(selected_claim.first_export_claim);EXPECT_FALSE(obligation(next_id).first_export_claim);
    EXPECT_NO_THROW(send());ASSERT_EQ(transport->sent().size(),3u);const auto next=frame(2);ASSERT_EQ(next.size(),1u);
    EXPECT_EQ(next[0].global_id,next_id);EXPECT_EQ(payload_string(next[0],"body"),"next-page-body");
    EXPECT_TRUE(obligation(next_id).first_export_claim);EXPECT_EQ(obligation(selected_id).first_export_claim,selected_claim.first_export_claim);
    EXPECT_EQ(originals(),after);
}
#endif
