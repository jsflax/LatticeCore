#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_export_adapter.hpp"
#include <deque>

#ifndef __EMSCRIPTEN__
struct ExportPublicCallbackRow {std::string value;};
LATTICE_SCHEMA(ExportPublicCallbackRow,value);

namespace {
using namespace lattice;
using namespace lattice::detail;
class public_callback_scheduler final:public scheduler {
    std::mutex mutex_;bool closed_=false,inline_=false;std::deque<std::function<void()>> jobs_;
    static thread_local const public_callback_scheduler* current_;
public:
    void invoke(std::function<void()>&& job)override {
        {std::lock_guard<std::mutex> lock(mutex_);if(closed_)return;if(!inline_){jobs_.push_back(std::move(job));return;}}
        run(std::move(job));
    }
    void inline_dispatch(bool value){std::lock_guard<std::mutex> lock(mutex_);inline_=value;}
    bool is_on_thread()const noexcept override{return current_==this;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
    void shutdown()override {
        std::deque<std::function<void()>> old;{std::lock_guard<std::mutex> lock(mutex_);closed_=true;old.swap(jobs_);}
    }
    std::function<void()> take(){std::lock_guard<std::mutex> lock(mutex_);if(jobs_.empty())return {};auto job=std::move(jobs_.front());jobs_.pop_front();return job;}
    void run(std::function<void()> job) {
        struct restore {const public_callback_scheduler* prior;~restore(){current_=prior;}} turn{current_};current_=this;job();
    }
    void drain(){for(size_t i=0;i<128;++i){auto job=take();if(!job)return;run(std::move(job));}throw std::runtime_error("public callback scheduler budget exceeded");}
};
thread_local const public_callback_scheduler* public_callback_scheduler::current_=nullptr;
struct public_wire_state {
    std::mutex mutex;sync_transport::on_open_handler opened;size_t frames=0;
    std::atomic<transport_state> state{transport_state::closed};
    void open(){sync_transport::on_open_handler callback;{std::lock_guard<std::mutex> lock(mutex);callback=opened;}state=transport_state::open;callback();}
    size_t count(){std::lock_guard<std::mutex> lock(mutex);return frames;}
};
class public_callback_wire final:public sync_transport {
    std::shared_ptr<public_wire_state> state_;
public:
    explicit public_callback_wire(std::shared_ptr<public_wire_state> state):state_(std::move(state)){}
    void connect(const std::string&,const std::map<std::string,std::string>&)override{state_->state=transport_state::connecting;}
    void disconnect()override{state_->state=transport_state::closed;}
    transport_state state()const override{return state_->state.load();}
    bool supports_reconnect()const override{return false;}
    void send(const transport_message&)override{std::lock_guard<std::mutex> lock(state_->mutex);++state_->frames;}
    void set_on_open(on_open_handler callback)override{std::lock_guard<std::mutex> lock(state_->mutex);state_->opened=std::move(callback);}
    void set_on_message(on_message_handler)override{}
    void set_on_error(on_error_handler)override{}
    void set_on_close(on_close_handler)override{}
};
using filter_view=std::optional<std::vector<std::pair<std::string,std::optional<std::string>>>>;
class public_callback_sender final:public synchronizer {
public:
    using synchronizer::synchronizer;
    std::shared_ptr<lattice_db> retained_owner(){return owned_db_;}
    filter_view configured_filter()const {
        if(!config_.sync_filter)return {};filter_view result=std::vector<std::pair<std::string,std::optional<std::string>>>{};
        for(const auto& item:*config_.sync_filter)result->emplace_back(item.table_name,item.where_clause);return result;
    }
};
struct public_errors {
    std::mutex mutex;std::vector<std::string> values;
    void add(const std::string& value){std::lock_guard<std::mutex> lock(mutex);values.push_back(value);}
    auto copy(){std::lock_guard<std::mutex> lock(mutex);return values;}
};
void public_commit(const recovery_install_result& result) {
    if(result.state!=recovery_install_state::committed){if(result.primary_error)std::rethrow_exception(result.primary_error);throw std::runtime_error("public callback fixture did not commit");}
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
}
class RecoveryExportPublicCallback:public ::testing::Test {
protected:
    std::shared_ptr<public_callback_scheduler> queue=std::make_shared<public_callback_scheduler>();
    std::shared_ptr<public_wire_state> transport=std::make_shared<public_wire_state>();
    std::shared_ptr<public_errors> errors=std::make_shared<public_errors>();
    std::unique_ptr<public_callback_sender> sender;std::shared_ptr<lattice_db> owner;
    recovery_obligation_producer_discovery_limits caps{{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
    recovery_obligation_address address;
    void create(bool with_filter) {
        configuration cfg(":memory:");cfg.audit_retention_seconds=0;cfg.sched=queue;
        sync_config config;config.sync_id="public-callback-route";config.upload_coalesce_ms=0;config.checkpoint_passive_interval_ms=0;
        if(with_filter)config.sync_filter=std::vector<sync_filter_entry>{{"ExportPublicCallbackRow",std::string("value='kept'")}};
        sender=std::make_unique<public_callback_sender>(std::make_unique<lattice_db>(cfg),config,std::make_unique<public_callback_wire>(transport));
        owner=sender->retained_owner();const auto observed=errors;
        sender->set_on_error([observed](const std::string& message){observed->add(message);});
        public_commit(recovery_writer_access::install(owner,[&](database&) {
            receive_install_store receiver(owner,caps.installations);receiver.initialize();
            receive_install_binding binding{"public-contribution","authority","source","epoch","scope","schema"};receiver.bind(binding);
            recovery_obligation_store journal(owner,caps.obligations,caps.installations);journal.initialize();address=journal.bind({binding,"grant","receipts"}).address;
        }));
        public_commit(recovery_local_producer_adapter::enroll_for_qualification(owner,{address,{"ExportPublicCallbackRow"},{'p'}},caps));
        queue->drain();
    }
    void TearDown()override{sender.reset();queue->shutdown();}
    void seed_filter_metadata() {
        const auto row=owner->add(ExportPublicCallbackRow{"kept"});queue->drain();
        const auto audit=owner->db().query("SELECT id FROM AuditLog WHERE tableName='ExportPublicCallbackRow' AND globalRowId=?",{row.global_id()});
        if(audit.size()!=1)throw std::runtime_error("missing genuine filter fixture original");
        const auto id=std::get<int64_t>(audit[0].at("id"));
        owner->db().execute("INSERT INTO _lattice_replication_slots(sync_id,confirmed_audit_id,upload_floor) VALUES('public-callback-route',0,0)");
        owner->db().execute("INSERT INTO _lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,'public-callback-route',0)",{id});
        owner->db().execute("INSERT INTO _lattice_sync_set(sync_id,table_name,global_row_id) VALUES('public-callback-route','ExportPublicCallbackRow',?)",{row.global_id()});
        queue->drain();
    }
    auto snapshot() {
        std::vector<std::vector<database::row_t>> result;
        for(const auto* table:{"_lattice_obligation_producer_profile","_lattice_obligation_producer_store","_lattice_obligation_producer_stamp",
            "_lattice_obligation_store","_lattice_obligation_scope","_lattice_obligation_entry","_lattice_sync_state","_lattice_sync_set",
            "_lattice_replication_slots","AuditLog","ExportPublicCallbackRow"})
            result.push_back(owner->db().query(std::string("SELECT * FROM ")+table+" ORDER BY 1,2"));
        return result;
    }
    void expect_error(const std::string& stage,const std::string& reason) {
        const auto reported=errors->copy();ASSERT_EQ(reported.size(),1u);
        EXPECT_NE(reported[0].find(stage),std::string::npos);EXPECT_NE(reported[0].find(reason),std::string::npos);
    }
    void usable_after_filter_refusal() {
        ASSERT_FALSE(owner->is_closed());ASSERT_FALSE(owner->db().is_in_transaction());
        EXPECT_NO_THROW(owner->add(ExportPublicCallbackRow{"ordinary local successor"}));
        EXPECT_NO_THROW(queue->drain());
        EXPECT_EQ(std::get<int64_t>(owner->db().query("SELECT COUNT(*) AS n FROM ExportPublicCallbackRow")[0].at("n")),2);
        EXPECT_EQ(std::get<int64_t>(owner->db().query("SELECT COUNT(*) AS n FROM _lattice_obligation_producer_stamp")[0].at("n")),2);
        EXPECT_EQ(transport->count(),0u);EXPECT_EQ(errors->copy().size(),1u);
    }
    void open_then_freeze_empty() {
        create(false);sender->connect();transport->open();queue->drain();
        if(transport->count()!=0||!errors->copy().empty())throw std::runtime_error("empty protected public route did not open");
        recovery_obligation_address next;
        public_commit(recovery_writer_access::install(owner,[&](database&) {
            recovery_obligation_store journal(owner,caps.obligations,caps.installations);next=journal.freeze(address,1).address;
        }));address=std::move(next);queue->drain();
    }
};
}

TEST_F(RecoveryExportPublicCallback, DeferredPublicFilterUpdateReportsAndPreservesFilterAndStore) {
    create(true);seed_filter_metadata();const auto before=snapshot();const auto filter=sender->configured_filter();ASSERT_TRUE(filter);
    EXPECT_NO_THROW(sender->update_sync_filter({{"ExportPublicCallbackRow",std::string("value='replacement'")}}));
    EXPECT_TRUE(errors->copy().empty());EXPECT_EQ(sender->configured_filter(),filter);EXPECT_EQ(snapshot(),before);
    auto work=queue->take();ASSERT_TRUE(work);
    EXPECT_NO_THROW(queue->run(std::move(work)));
    EXPECT_NO_THROW(queue->drain());
    expect_error("filter update","protected export route refuses legacy filter mutation");
    EXPECT_EQ(sender->configured_filter(),filter);EXPECT_EQ(snapshot(),before);EXPECT_EQ(transport->count(),0u);
    usable_after_filter_refusal();
}

TEST_F(RecoveryExportPublicCallback, DeferredPublicFilterClearReportsAndPreservesFilterAndStore) {
    create(true);seed_filter_metadata();const auto before=snapshot();const auto filter=sender->configured_filter();ASSERT_TRUE(filter);
    EXPECT_NO_THROW(sender->clear_sync_filter());
    EXPECT_TRUE(errors->copy().empty());EXPECT_EQ(sender->configured_filter(),filter);EXPECT_EQ(snapshot(),before);
    auto work=queue->take();ASSERT_TRUE(work);
    EXPECT_NO_THROW(queue->run(std::move(work)));
    EXPECT_NO_THROW(queue->drain());
    expect_error("filter clear","protected export route refuses legacy filter mutation");
    EXPECT_EQ(sender->configured_filter(),filter);EXPECT_EQ(snapshot(),before);EXPECT_EQ(transport->count(),0u);
    usable_after_filter_refusal();
}

TEST_F(RecoveryExportPublicCallback, DeferredPublicSyncNowAfterFreezeContainsFailureAndMakesNoClaim) {
    open_then_freeze_empty();const auto before=snapshot();
    EXPECT_NO_THROW(sender->sync_now());EXPECT_TRUE(errors->copy().empty());EXPECT_EQ(snapshot(),before);
    auto work=queue->take();ASSERT_TRUE(work);
    EXPECT_NO_THROW(queue->run(std::move(work)));
    EXPECT_NO_THROW(queue->drain());
    expect_error("background upload","frozen or installed");EXPECT_EQ(snapshot(),before);EXPECT_EQ(transport->count(),0u);
    EXPECT_EQ(std::get<int64_t>(owner->db().query("SELECT export_sequence AS n FROM _lattice_obligation_store WHERE id=1")[0].at("n")),0);
    EXPECT_TRUE(owner->db().query("SELECT 1 FROM _lattice_obligation_entry").empty());
    EXPECT_FALSE(owner->is_closed());EXPECT_FALSE(owner->db().is_in_transaction());
}

TEST_F(RecoveryExportPublicCallback, TrueInlinePublicSyncNowAfterFreezeStillThrowsToCaller) {
    open_then_freeze_empty();const auto before=snapshot();queue->inline_dispatch(true);
    std::string failure;
    try{sender->sync_now();ADD_FAILURE()<<"true inline foreground refusal did not throw";}
    catch(const db_error& error){failure=error.what();}
    EXPECT_NE(failure.find("frozen or installed"),std::string::npos);
    EXPECT_TRUE(errors->copy().empty());EXPECT_EQ(snapshot(),before);EXPECT_EQ(transport->count(),0u);
    EXPECT_FALSE(owner->db().is_in_transaction());
}
#endif
