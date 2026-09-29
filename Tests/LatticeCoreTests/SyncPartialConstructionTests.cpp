#include "TestHelpers.hpp"

#ifndef __EMSCRIPTEN__
namespace {
using namespace lattice;

struct construction_probe {
    int factories=0, transports_destroyed=0, disconnects=0;
};
class construction_transport final : public mock_sync_transport {
    std::shared_ptr<construction_probe> probe_;
public:
    explicit construction_transport(std::shared_ptr<construction_probe> probe):probe_(std::move(probe)){}
    ~construction_transport()override{++probe_->transports_destroyed;}
    void disconnect()override{++probe_->disconnects;mock_sync_transport::disconnect();}
};
class construction_factory final : public network_factory {
public:
    enum class behavior { fail, empty, succeed } mode=behavior::fail;
    const std::shared_ptr<construction_probe> probe=std::make_shared<construction_probe>();
    std::unique_ptr<http_client> create_http_client()override{return std::make_unique<null_http_client>();}
    std::unique_ptr<sync_transport> create_sync_transport()override{
        ++probe->factories;
        if(mode==behavior::fail)throw db_error("fixture transport factory failed");
        if(mode==behavior::empty)return nullptr;
        return std::make_unique<construction_transport>(probe);
    }
};
struct construction_factory_scope {
    std::shared_ptr<network_factory> prior=get_network_factory();
    const std::shared_ptr<construction_factory> factory=std::make_shared<construction_factory>();
    construction_factory_scope(){set_network_factory(factory);}
    ~construction_factory_scope(){set_network_factory(prior);}
};
}

TEST(SyncPartialConstruction, NullOwnersRefuseBeforeFactoryAndReleaseInjectedTransport) {
    construction_factory_scope scope;
    const lattice::sync_config config;
    EXPECT_THROW((void)lattice::synchronizer(std::shared_ptr<lattice::lattice_db>{},config),lattice::db_error);
    EXPECT_THROW((void)lattice::synchronizer(std::unique_ptr<lattice::lattice_db>{},config),lattice::db_error);
    EXPECT_THROW((void)lattice::synchronizer(std::unique_ptr<lattice::lattice_db>{},config,
        std::make_unique<construction_transport>(scope.factory->probe)),lattice::db_error);
    EXPECT_EQ(scope.factory->probe->factories,0);
    EXPECT_EQ(scope.factory->probe->transports_destroyed,1);
    EXPECT_EQ(scope.factory->probe->disconnects,0);
}

TEST(SyncPartialConstruction, ThrowingAndEmptyFactoryLeaveOwnerUsableForLaterConstruction) {
    construction_factory_scope scope;
    TempDB file("sync_partial_factory");
    auto owner=std::make_shared<lattice::lattice_db>(lattice::configuration(file.str()));
    lattice::sync_config config;config.checkpoint_passive_interval_ms=0;
    EXPECT_THROW((void)lattice::synchronizer(owner,config),lattice::db_error);
    scope.factory->mode=construction_factory::behavior::empty;
    EXPECT_THROW((void)lattice::synchronizer(owner,config),lattice::db_error);
    EXPECT_EQ(scope.factory->probe->factories,2);
    EXPECT_EQ(scope.factory->probe->transports_destroyed,0);
    EXPECT_FALSE(owner->is_closed());
    owner->add(TestPerson{"after-refusal",42,std::nullopt});
    scope.factory->mode=construction_factory::behavior::succeed;
    {lattice::synchronizer live(owner,config);EXPECT_FALSE(live.is_connected());}
    EXPECT_EQ(scope.factory->probe->factories,3);
    EXPECT_EQ(scope.factory->probe->disconnects,1);
    EXPECT_EQ(scope.factory->probe->transports_destroyed,1);
    const auto rows=owner->db().query("SELECT name FROM TestPerson");
    ASSERT_EQ(rows.size(),1u);
    EXPECT_EQ(std::get<std::string>(rows.front().at("name")),"after-refusal");
}

TEST(SyncPartialConstruction, EmptyInjectedTransportUnwindsWithoutPoisoningStore) {
    construction_factory_scope scope;
    TempDB file("sync_partial_injected");
    auto owner=std::make_unique<lattice::lattice_db>(lattice::configuration(file.str()));
    owner->add(TestPerson{"survives",7,std::nullopt});
    const lattice::sync_config config;
    EXPECT_THROW((void)lattice::synchronizer(std::move(owner),config,
        std::unique_ptr<lattice::sync_transport>{}),lattice::db_error);
    EXPECT_FALSE(owner);
    EXPECT_EQ(scope.factory->probe->factories,0);
    lattice::lattice_db reopened{lattice::configuration(file.str())};
    const auto rows=reopened.db().query("SELECT name FROM TestPerson");
    ASSERT_EQ(rows.size(),1u);
    EXPECT_EQ(std::get<std::string>(rows.front().at("name")),"survives");
}
#endif

#ifndef __EMSCRIPTEN__
#include "../../Sources/LatticeCore/src/sync_callback_lifetime.hpp"
#include <future>
namespace {
using namespace std::chrono_literals;
struct ownership_wire {
    std::mutex mutex;std::condition_variable changed;
    std::vector<platform_transport_callbacks> dials;std::vector<std::string> frames;
    struct holder {std::shared_ptr<ownership_wire> state;};
    static std::unique_ptr<sync_transport> transport(const std::shared_ptr<ownership_wire>& state) {
        // Same owned native attempt boundary used by the public connect
        // fixtures. These ordinary-store cases confer no TLS/source authority.
        return std::unique_ptr<sync_transport>(make_owned_platform_sync_transport(new holder{state},
            [](void* p,const void*,const void*,const void* endpoint) {
                const auto state=static_cast<holder*>(p)->state;
                {std::lock_guard lock(state->mutex);if(state->dials.size()>=16)throw db_error("ownership fixture dial bound");
                    state->dials.push_back(*static_cast<const platform_transport_callbacks*>(endpoint));}
                state->changed.notify_all();
            },[](void*){},
            [](void* p,const void* message,const void* endpoint) {
                if(!static_cast<const platform_transport_callbacks*>(endpoint)->is_current())return;
                const auto state=static_cast<holder*>(p)->state;const auto& frame=*static_cast<const transport_message*>(message);
                if(frame.data.size()>8388608)throw db_error("ownership fixture frame bound");
                {std::lock_guard lock(state->mutex);if(state->frames.size()>=64)throw db_error("ownership fixture frame count bound");state->frames.push_back(frame.as_string());}
                state->changed.notify_all();
            },[](void* p){delete static_cast<holder*>(p);}));
    }
    bool wait_dial() {std::unique_lock lock(mutex);return changed.wait_for(lock,5s,[&]{return !dials.empty();});}
    platform_transport_callbacks endpoint(){std::lock_guard lock(mutex);return dials.at(0);}
    size_t dial_count(){std::lock_guard lock(mutex);return dials.size();}
    std::vector<audit_log_entry> uploaded() {
        std::vector<std::string> copied;{std::lock_guard lock(mutex);copied=frames;}
        std::vector<audit_log_entry> result;for(const auto& raw:copied){const auto event=server_sent_event::from_json(raw);
            if(event&&event->event_type==server_sent_event::type::audit_log)result.insert(result.end(),event->audit_logs.begin(),event->audit_logs.end());}
        return result;
    }
};
class ownership_factory final:public network_factory {
    std::mutex mutex_;construction_factory::behavior mode_=construction_factory::behavior::succeed;
    std::vector<std::shared_ptr<ownership_wire>> wires_;
    std::shared_ptr<scheduler> selected_;
public:
    void mode(construction_factory::behavior mode){std::lock_guard lock(mutex_);mode_=mode;}
    std::shared_ptr<ownership_wire> last(){std::lock_guard lock(mutex_);return wires_.back();}
    std::shared_ptr<scheduler> selected(){std::lock_guard lock(mutex_);return selected_;}
    std::unique_ptr<http_client> create_http_client()override{return std::make_unique<null_http_client>();}
    std::unique_ptr<sync_transport> create_sync_transport()override {
        std::shared_ptr<ownership_wire> state;
        {std::lock_guard lock(mutex_);
            if(mode_==construction_factory::behavior::fail)throw db_error("ownership fixture factory failed");
            if(mode_==construction_factory::behavior::empty)return {};
            if(wires_.size()>=16)throw db_error("ownership fixture owner bound");
            state=std::make_shared<ownership_wire>();wires_.push_back(state);}
        return ownership_wire::transport(state);
    }
    std::unique_ptr<sync_transport> create_sync_transport(std::shared_ptr<scheduler> scheduled)override {
        {std::lock_guard lock(mutex_);selected_=std::move(scheduled);}return create_sync_transport();
    }
};
class ownership_sync final:public synchronizer {
public:
    using synchronizer::synchronizer;
    auto lifetime()const{return callback_lifetime_;}
    auto dispatch()const{return scheduler_;}
    auto owner()const{return owned_db_;}
};
struct ownership_pause {
    std::mutex mutex;std::condition_variable changed;bool arrived=false,released=false,timed_out=false;
    void wait(){std::unique_lock lock(mutex);arrived=true;changed.notify_all();if(!changed.wait_for(lock,5s,[&]{return released;}))timed_out=true;}
    bool await(){std::unique_lock lock(mutex);return changed.wait_for(lock,5s,[&]{return arrived;});}
    void release(){std::lock_guard lock(mutex);released=true;changed.notify_all();}
    bool failed(){std::lock_guard lock(mutex);return timed_out;}
};
class SyncSharedSchedulerOwnership:public ::testing::Test {
protected:
    std::shared_ptr<network_factory> prior;
    std::shared_ptr<ownership_factory> factory=std::make_shared<ownership_factory>();
    std::shared_ptr<std_thread_scheduler> scheduled=std::make_shared<std_thread_scheduler>();
    std::shared_ptr<lattice_db> owner;
    std::vector<std::unique_ptr<ownership_sync>> clients;
    std::vector<std::shared_ptr<ownership_wire>> wires;
    std::mutex error_mutex;std::vector<std::string> errors;
    void SetUp()override {
        prior=get_network_factory();set_network_factory(factory);
        configuration cfg(":memory:",scheduled);cfg.audit_retention_seconds=0;cfg.busy_timeout_ms=100;
        owner=std::make_shared<lattice_db>(cfg);
    }
    void TearDown()override {clients.clear();if(owner)owner->close();owner.reset();set_network_factory(prior);}
    sync_config config(const std::string& channel) {
        sync_config c;c.sync_id=channel;c.websocket_url="wss://scheduler.example/"+channel;
        c.all_active_sync_ids={"shared-a","shared-b"};c.upload_coalesce_ms=0;c.checkpoint_passive_interval_ms=0;return c;
    }
    size_t create(const std::string& channel,bool open=true) {
        const auto index=clients.size();auto client=std::make_unique<ownership_sync>(owner,config(channel));
        client->set_on_error([this](const std::string& error){std::lock_guard lock(error_mutex);errors.push_back(error);});
        clients.push_back(std::move(client));wires.push_back(factory->last());clients[index]->connect();
        if(!wires[index]->wait_dial())throw db_error("ownership fixture actual connect did not dial");
        if(open&&!wires[index]->endpoint().trigger_on_open())throw db_error("ownership fixture actual open retired");
        return index;
    }
    bool until(const std::function<bool()>& predicate) {
        const auto end=std::chrono::steady_clock::now()+5s;
        do {if(predicate())return true;std::this_thread::sleep_for(1ms);}while(std::chrono::steady_clock::now()<end);return predicate();
    }
    bool barrier() {
        auto arrived=std::make_shared<std::promise<void>>();auto done=arrived->get_future();
        scheduled->invoke([arrived]{arrived->set_value();});return done.wait_for(5s)==std::future_status::ready;
    }
    void expect_no_errors(){std::lock_guard lock(error_mutex);EXPECT_TRUE(errors.empty());}
    void upload_and_ack(size_t index,const std::string& name) {
        owner->add(TestPerson{name,7,std::nullopt});clients[index]->sync_now();
        ASSERT_TRUE(until([&]{return !wires[index]->uploaded().empty();}));const auto sent=wires[index]->uploaded();ASSERT_EQ(sent.size(),1u);
        const auto original=owner->db().query("SELECT globalId FROM AuditLog WHERE tableName='TestPerson' ORDER BY id");ASSERT_EQ(original.size(),1u);
        EXPECT_EQ(sent[0].global_id,std::get<std::string>(original[0].at("globalId")));EXPECT_EQ(sent[0].table_name,"TestPerson");
        EXPECT_NE(sent[0].changed_fields_to_json().find(name),std::string::npos);
        ASSERT_TRUE(wires[index]->endpoint().trigger_on_message(transport_message::from_string(server_sent_event::make_ack({sent[0].global_id}).to_json())));
        ASSERT_TRUE(until([&]{const auto p=clients[index]->get_progress();return p.acked==1&&p.pending_upload==0;}));
        EXPECT_EQ(wires[index]->uploaded().size(),1u);EXPECT_EQ(wires[index]->dial_count(),1u);expect_no_errors();
    }
};
TEST_F(SyncSharedSchedulerOwnership, DestroyingOneSharedOwnerChannelKeepsActualSiblingUploadAndAckLive) {
    const auto a=create("shared-a"),b=create("shared-b");ASSERT_TRUE(barrier());const auto old=wires[a]->endpoint();
    clients[a].reset();ASSERT_TRUE(scheduled->can_invoke());ASSERT_TRUE(barrier());EXPECT_FALSE(owner->is_closed());
    EXPECT_FALSE(old.trigger_on_message(transport_message::from_string(server_sent_event::make_ack({"retired"}).to_json())));
    upload_and_ack(b,"sibling-after-external-retirement");
}
TEST_F(SyncSharedSchedulerOwnership, RemovedChannelCanReconnectOnTheSameRetainedOwnerAndScheduler) {
    const auto old_index=create("shared-a");ASSERT_TRUE(barrier());const auto old=wires[old_index]->endpoint();
    clients[old_index].reset();ASSERT_TRUE(scheduled->can_invoke());const auto replacement=create("shared-a");
    EXPECT_FALSE(old.matches(wires[replacement]->endpoint()));EXPECT_FALSE(old.trigger_on_open());
    upload_and_ack(replacement,"same-owner-replacement");EXPECT_EQ(wires[old_index]->dial_count(),1u);
}
TEST_F(SyncSharedSchedulerOwnership, FailedAndNullFactoriesCannotShutdownTheRetainedOwnerOrSibling) {
    const auto b=create("shared-b");ASSERT_TRUE(barrier());
    factory->mode(construction_factory::behavior::fail);EXPECT_THROW((void)ownership_sync(owner,config("shared-a")),db_error);
    ASSERT_TRUE(scheduled->can_invoke());ASSERT_TRUE(barrier());EXPECT_FALSE(owner->is_closed());
    factory->mode(construction_factory::behavior::empty);EXPECT_THROW((void)ownership_sync(owner,config("shared-a")),db_error);
    ASSERT_TRUE(scheduled->can_invoke());ASSERT_TRUE(barrier());EXPECT_FALSE(owner->is_closed());
    factory->mode(construction_factory::behavior::succeed);upload_and_ack(b,"after-shared-construction-failure");
}
TEST_F(SyncSharedSchedulerOwnership, ExternalRetirementWaitsForActualAdmittedCallbackWithoutStoppingSiblingWork) {
    const auto a=create("shared-a",false),b=create("shared-b");ASSERT_TRUE(barrier());
    const auto pause=std::make_shared<ownership_pause>();clients[a]->set_on_state_change([pause](bool connected){if(connected)pause->wait();});
    const auto life=clients[a]->lifetime();const auto generation=life->dispatch_generation();
    ASSERT_TRUE(wires[a]->endpoint().trigger_on_open());ASSERT_TRUE(pause->await());
    std::promise<void> finished;auto done=finished.get_future();std::exception_ptr error;std::thread retiring;
    struct settle {std::shared_ptr<ownership_pause> pause;std::thread& worker;~settle(){pause->release();if(worker.joinable())worker.join();}} cleanup{pause,retiring};
    retiring=std::thread([&]{try{clients[a].reset();}catch(...){error=std::current_exception();}finished.set_value();});
    ASSERT_TRUE(until([&]{return !life->current(generation);}));EXPECT_EQ(done.wait_for(0ms),std::future_status::timeout);
    auto sibling=std::make_shared<std::promise<void>>();auto progressed=sibling->get_future();scheduled->invoke([sibling]{sibling->set_value();});
    pause->release();ASSERT_EQ(done.wait_for(5s),std::future_status::ready);retiring.join();EXPECT_FALSE(error);EXPECT_FALSE(pause->failed());
    ASSERT_TRUE(scheduled->can_invoke());ASSERT_EQ(progressed.wait_for(5s),std::future_status::ready);ASSERT_TRUE(barrier());
    upload_and_ack(b,"after-admitted-foreign-callback");
}
TEST_F(SyncSharedSchedulerOwnership, UniqueDedicatedOwnerStillShutsDownItsSchedulerEvenWithRetainedDatabase) {
    auto dedicated=std::make_shared<std_thread_scheduler>();configuration cfg(":memory:",dedicated);cfg.audit_retention_seconds=0;
    auto client=std::make_unique<ownership_sync>(std::make_unique<lattice_db>(cfg),config("dedicated"));
    auto retained=client->owner();ASSERT_TRUE(dedicated->can_invoke());client.reset();
    EXPECT_FALSE(dedicated->can_invoke());EXPECT_FALSE(retained->is_closed());retained->close();
    EXPECT_TRUE(scheduled->can_invoke());EXPECT_TRUE(barrier());
}
TEST_F(SyncSharedSchedulerOwnership, SharedImmediateOwnerShutsDownOnlyItsPrivateAdapterIncludingFailedInit) {
    configuration cfg(":memory:");cfg.audit_retention_seconds=0;auto immediate_owner=std::make_shared<lattice_db>(cfg);
    auto client=std::make_unique<ownership_sync>(immediate_owner,config("private-adapter"));const auto adapter=client->dispatch();
    EXPECT_TRUE(adapter->can_invoke());client.reset();EXPECT_FALSE(adapter->can_invoke());EXPECT_TRUE(immediate_owner->get_scheduler()->can_invoke());
    factory->mode(construction_factory::behavior::fail);EXPECT_THROW((void)ownership_sync(immediate_owner,config("private-failed")),db_error);
    ASSERT_TRUE(factory->selected());EXPECT_FALSE(factory->selected()->can_invoke());EXPECT_TRUE(immediate_owner->get_scheduler()->can_invoke());
    factory->mode(construction_factory::behavior::succeed);immediate_owner->add(TestPerson{"still-usable",1,std::nullopt});
    EXPECT_EQ(immediate_owner->objects<TestPerson>().size(),1u);immediate_owner->close();
}
}
#endif
