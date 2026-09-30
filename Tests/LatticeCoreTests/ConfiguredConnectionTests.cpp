#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/configured_attempt_custody.hpp"
#include "../../Sources/LatticeCore/src/configured_recovery_connection.hpp"
#include "../../Sources/LatticeCore/src/recovery_producer_continuity.hpp"
#include "../../Sources/LatticeCore/src/sync_callback_lifetime.hpp"
#include <condition_variable>
#include <optional>

using namespace lattice;
using namespace lattice::detail;

TEST(ConfiguredConnectionCustody, CommandLimitRefusesWithoutEvictingExistingUses) {
    auto custody=std::make_shared<configured_attempt_custody>();
    std::vector<configured_attempt_custody::lease> held;
    for(size_t i=0;i<128;++i)held.push_back(custody->admit(configured_attempt_custody::kind::command));
    EXPECT_EQ(custody->snapshot().commands,128u);
    EXPECT_THROW(custody->admit(configured_attempt_custody::kind::command),std::runtime_error);
    EXPECT_EQ(custody->snapshot().commands,128u);EXPECT_NE(custody->snapshot().first_error,0);
    custody->close();EXPECT_FALSE(custody->admit(configured_attempt_custody::kind::command));
    held.clear();EXPECT_EQ(custody->snapshot().commands,0u);
}
TEST(ConfiguredConnectionCustody, PayloadLimitIsReservedBeforeRetainedCaptureConstruction) {
    auto custody=std::make_shared<configured_attempt_custody>();
    std::vector<configured_attempt_custody::lease> held;
    for(size_t i=0;i<4096;++i)held.push_back(custody->admit(configured_attempt_custody::kind::payload));
    int allocated=0;
    EXPECT_THROW({
        auto charge=custody->admit(configured_attempt_custody::kind::payload);
        ++allocated;
        (void)retain_configured_payload(std::move(charge),[]{});
    },std::runtime_error);
    EXPECT_EQ(allocated,0);EXPECT_EQ(custody->snapshot().payloads,4096u);
    held.clear();EXPECT_EQ(custody->snapshot().payloads,0u);
}
TEST(ConfiguredConnectionCustody, AllCallableCopiesRetainOneChargeUntilActualCaptureDestruction) {
    auto custody=std::make_shared<configured_attempt_custody>();
    size_t at_destruction=0;int destroyed=0;
    struct capture {
        std::shared_ptr<configured_attempt_custody> custody;size_t* count;int* destroyed;
        ~capture(){*count=custody->snapshot().payloads;++*destroyed;}
    };
    auto charge=custody->admit(configured_attempt_custody::kind::payload);
    auto object=std::shared_ptr<capture>(new capture{custody,&at_destruction,&destroyed});
    auto first=retain_configured_payload(std::move(charge),[object]{});object.reset();
    auto copied=first;first=nullptr;
    EXPECT_EQ(destroyed,0);EXPECT_EQ(custody->snapshot().payloads,1u);
    copied=nullptr;
    EXPECT_EQ(destroyed,1);EXPECT_EQ(at_destruction,1u);EXPECT_EQ(custody->snapshot().payloads,0u);
}
namespace {
class rejecting_configured_scheduler final:public scheduler {
public:
    void invoke(std::function<void()>&&)override{throw db_error("actual configured test submission rejected");}
    bool is_on_thread()const noexcept override{return false;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
};
}
TEST(ConfiguredConnectionCustody, ThrowingSubmissionDestroysPayloadAndReleasesExactlyOnce) {
    auto custody=std::make_shared<configured_attempt_custody>();
    auto lifetime=std::make_shared<sync_callback_lifetime>(nullptr,std::shared_ptr<lattice_db>{});
    lifetime->configure(custody,{});
    auto target=make_sync_lifetime_scheduler(std::make_shared<rejecting_configured_scheduler>(),lifetime);
    int released=0;
    auto token=std::shared_ptr<int>(new int(1),[&](int* p){++released;delete p;});
    EXPECT_THROW(target->invoke([held=std::move(token)]{}),db_error);
    EXPECT_EQ(released,1);EXPECT_EQ(custody->snapshot().payloads,0u);
    EXPECT_NE(custody->snapshot().first_error,0);
}
TEST(ConfiguredConnectionCustody, FinalSealRefusesEvenTerminalBypassPayloads) {
    auto custody=std::make_shared<configured_attempt_custody>();
    custody->close();
    auto terminal=custody->admit(configured_attempt_custody::kind::payload,true);
    EXPECT_TRUE(terminal);EXPECT_FALSE(custody->admit(configured_attempt_custody::kind::payload));
    custody->seal_payloads();
    EXPECT_FALSE(custody->admit(configured_attempt_custody::kind::payload,true));
    EXPECT_EQ(custody->snapshot().payloads,1u);
}
#ifndef __EMSCRIPTEN__
TEST(ConfiguredConnectionCustody, CaptureSettlementDoesNotSubstituteForActualWorkerJoin) {
    const auto registry=configured_retirement_registry::instance();auto reservation=registry->reserve();
    const auto receipt=reservation.begin_attempt();auto custody=registry->attempt_custody(receipt);
    struct observed {std::mutex mutex;std::condition_variable changed;std::atomic<bool> thread_exited{false};};
    auto result=std::make_shared<observed>();
    custody->bind_wakeup([result]{result->changed.notify_all();});
    auto charge=custody->admit(configured_attempt_custody::kind::payload);
    custody->launch_worker(std::move(charge),[result]{
        struct thread_exit {std::shared_ptr<observed> result;~thread_exit(){result->thread_exited=true;result->changed.notify_all();}};
        thread_local thread_exit actual{result};
    });
    bool finished=false;
    {
        std::unique_lock<std::mutex> lock(result->mutex);
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        // The notification is advisory; use short bounded waits because the
        // production custody leaf cannot acquire this separate fixture leaf.
        while(std::chrono::steady_clock::now()<deadline){
            if(custody->snapshot().finished_workers==1){finished=true;break;}
            result->changed.wait_until(lock,std::min(deadline,std::chrono::steady_clock::now()+std::chrono::milliseconds(10)));
        }
    }
    EXPECT_TRUE(finished);EXPECT_EQ(custody->snapshot().payloads,0u);
    EXPECT_EQ(custody->snapshot().workers,1u);
    EXPECT_TRUE(custody->join_finished_workers());
    EXPECT_EQ(custody->snapshot().workers,0u);EXPECT_TRUE(result->thread_exited);
    registry->request_retirement(receipt);
    // An unproved thread exit remains in the registry's fixed charged slot.
    // This fixture creates no platform resource or callback endpoint.
    if(custody->snapshot().workers==0){
        EXPECT_TRUE(receipt.complete_adapter_cleanup(0));EXPECT_TRUE(registry->complete_native_cleanup(receipt,0));
        EXPECT_TRUE(registry->collect_completed(receipt));
    }
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
struct ConfiguredConnectionRow {std::string value;};
LATTICE_SCHEMA(ConfiguredConnectionRow,value);
namespace {
// This is a native ownership fixture with no OS socket. It exercises the real
// configured owner/typed bridge and actual object destruction. It is NOT stock
// Apple/NIO, authenticated receiver recovery, or hosted runtime acceptance.
struct configured_wire {
    std::mutex mutex;std::condition_variable changed;
    size_t factories=0,dials=0,requests=0,releases=0,legacy=0;
    struct counts {size_t factories,dials,requests,releases,legacy;};
    counts snapshot(){std::lock_guard<std::mutex> lock(mutex);return {factories,dials,requests,releases,legacy};}
    std::vector<platform_transport_callbacks> endpoints;
    bool wait(size_t dial_count,size_t release_count=0){
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_for(lock,std::chrono::seconds(5),[&]{return dials>=dial_count&&releases>=release_count;});
    }
    platform_transport_callbacks endpoint(size_t index){std::lock_guard<std::mutex> lock(mutex);return endpoints.at(index);}
};
struct configured_box {
    std::shared_ptr<configured_wire> wire;platform_retirement_receipt receipt;
    bool requested=false;
    ~configured_box(){std::lock_guard<std::mutex> lock(wire->mutex);++wire->releases;wire->changed.notify_all();}
};
class configured_test_factory final:public network_factory,public configured_platform_factory {
public:
    std::shared_ptr<configured_wire> wire=std::make_shared<configured_wire>();
    std::unique_ptr<http_client> create_http_client()override{return std::make_unique<null_http_client>();}
    std::unique_ptr<sync_transport> create_sync_transport()override{
        std::lock_guard<std::mutex> lock(wire->mutex);++wire->legacy;return {};
    }
    std::unique_ptr<sync_transport> create_configured_sync_transport(std::shared_ptr<scheduler> actual,
        const platform_retirement_receipt& receipt)override{
        if(!actual||!receipt.valid())throw db_error("actual configured factory custody missing");
        {std::lock_guard<std::mutex> lock(wire->mutex);++wire->factories;}
        auto registration=register_configured_system_tls_platform_transport(receipt,new configured_box{wire,receipt},
            [](void* p,const void*,const void*,const void* c){
                auto* box=static_cast<configured_box*>(p);const auto endpoint=*static_cast<const platform_transport_callbacks*>(c);
                {std::lock_guard<std::mutex> lock(box->wire->mutex);++box->wire->dials;box->wire->endpoints.push_back(endpoint);}
                box->wire->changed.notify_all();
            },[](void*){},[](void*,const void*,const void*){},[](void* p){delete static_cast<configured_box*>(p);},
            [](void*,const void*,const void*)->int32_t{return 1;},
            [](void* p,const void* r){
                auto* box=static_cast<configured_box*>(p);const auto receipt=*static_cast<const platform_retirement_receipt*>(r);
                if(box->requested||!box->receipt.matches(receipt))return false;box->requested=true;
                {std::lock_guard<std::mutex> lock(box->wire->mutex);++box->wire->requests;}
                // No OS resource or async callback is constructed by this test
                // adapter. Actual native calls remain separately counted.
                return receipt.complete_adapter_cleanup(0);
            });
        return std::unique_ptr<sync_transport>(make_configured_system_tls_platform_sync_transport(registration));
    }
};
class ConfiguredConnectionOwner:public ::testing::Test {
protected:
    TempDB unique{"configured-owner"};
    std::filesystem::path container=unique.str()+".continuous";
    std::shared_ptr<network_factory> prior;
    std::shared_ptr<configured_test_factory> factory=std::make_shared<configured_test_factory>();
    std::shared_ptr<lattice_db> owner;
    void SetUp()override{prior=get_network_factory();set_network_factory(factory);}
    void open(){
        recovery_continuous_policy policy;
        policy.limits={{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
        policy.owners=8;policy.physical_routes=4;policy.operations=4;policy.frozen_entries=128;policy.frozen_bytes=2*1024*1024;
        policy.contributions.push_back({{{"a","authority","source","epoch","scope","schema"},"grant","receipts"},{"ConfiguredConnectionRow"},{'g'}});
        policy.routes.push_back({"wss:wss://configured.invalid/a","wss://configured.invalid/a"});
        configuration config((container/"store.sqlite").string(),std::make_shared<immediate_scheduler>());
        config.audit_retention_seconds=0;config.busy_timeout_ms=100;
        config.websocket_url=policy.routes[0].endpoint;config.authorization_token="fixture-token";
        config.tuning.base_delay_seconds=0.01;config.tuning.max_delay_seconds=0.01;
        config.tuning.checkpoint_passive_interval_ms=0;
        auto opened=recovery_continuous_producer::open(config,policy);
        ASSERT_EQ(opened.settlement.state,recovery_install_state::committed);
        ASSERT_FALSE(opened.settlement.primary_error);ASSERT_FALSE(opened.settlement.postcommit_error);
        ASSERT_TRUE(opened.owner);owner=std::move(opened.owner);
        ASSERT_TRUE(factory->wire->wait(1));
    }
    void TearDown()override{
        bool settled=true;
        if(owner){const auto closed=owner->close_checked();settled=closed.cleanup_complete;EXPECT_TRUE(settled);owner.reset();}
        set_network_factory(prior);
        // An unproved cleanup preserves the actual container for diagnosis.
        if(settled){std::error_code ignored;std::filesystem::remove_all(container,ignored);}
    }
};
}
TEST_F(ConfiguredConnectionOwner, NormalConfiguredFacadeUsesTypedFactoryAndImmutableScope) {
    open();ASSERT_TRUE(owner);EXPECT_TRUE(owner->is_sync_agent());
    EXPECT_EQ(factory->wire->snapshot().legacy,0u);EXPECT_EQ(factory->wire->snapshot().factories,1u);
    owner->connect_sync();owner->connect_sync();
    EXPECT_EQ(factory->wire->snapshot().dials,1u);
    EXPECT_THROW(owner->update_sync_filter("",{}),db_error);
    EXPECT_THROW(owner->clear_sync_filter(""),db_error);
    EXPECT_FALSE(owner->config().sync_filter.has_value());
}
TEST_F(ConfiguredConnectionOwner, PreServiceFailureReturnsBothEmptyReservationsBeforeAnotherOpen) {
    auto parent=std::make_shared<lattice_db>(configuration(unique.str(),std::make_shared<immediate_scheduler>()));
    const auto registry=configured_retirement_registry::instance();const auto before=registry->charged_owners();
    size_t entered=0,children=0;
    struct restore_hook {
        std::function<void()> prior=std::move(configured_recovery_test_hooks::before_service_registration);
        ~restore_hook(){configured_recovery_test_hooks::before_service_registration=std::move(prior);}
    } restore;
    configured_recovery_test_hooks::before_service_registration=[&]{++entered;throw db_error("actual pre-service registration failure");};
    for(size_t i=0;i<65;++i){
        EXPECT_THROW(configured_recovery_connection::create(parent,sync_config{},factory,factory.get(),[&]{++children;return parent;}),db_error);
        EXPECT_EQ(registry->charged_owners(),before);
    }
    EXPECT_EQ(entered,65u);EXPECT_EQ(children,0u);EXPECT_EQ(factory->wire->snapshot().factories,0u);
    configured_recovery_test_hooks::before_service_registration={};
    open();ASSERT_TRUE(owner); // Actual normal route still reserves and dials.
    EXPECT_TRUE(parent->close_checked().cleanup_complete);
}
TEST_F(ConfiguredConnectionOwner, ScopedSuccessorUsesFreshEndpointOnlyAfterOldContextRelease) {
    configured_recovery_test_hooks::successor_permission qualification;
    open();ASSERT_TRUE(owner);
    const auto retired=factory->wire->endpoint(0);
    retired.trigger_on_close(1006,"fixture disconnect");
    ASSERT_TRUE(factory->wire->wait(2,1));
    EXPECT_EQ(factory->wire->snapshot().factories,2u);EXPECT_EQ(factory->wire->snapshot().requests,1u);
    const auto fresh=factory->wire->endpoint(1);
    EXPECT_FALSE(retired.matches(fresh));
    retired.trigger_on_close(1006,"stale duplicate");
    EXPECT_EQ(factory->wire->snapshot().dials,2u);
    owner->disconnect_sync();
    ASSERT_TRUE(factory->wire->wait(2,2));
    EXPECT_EQ(factory->wire->snapshot().dials,2u);
}
TEST_F(ConfiguredConnectionOwner, CompletedCloseReleasesLogicalCapacityWhileFacadeIsRetained) {
    const auto registry=configured_retirement_registry::instance();const auto before=registry->charged_owners();
    open();ASSERT_TRUE(owner);EXPECT_EQ(registry->charged_owners(),before+1);
    const auto closed=owner->close_checked();EXPECT_TRUE(closed.cleanup_complete);
    EXPECT_TRUE(owner);EXPECT_EQ(registry->charged_owners(),before);
    EXPECT_EQ(factory->wire->snapshot().releases,1u);EXPECT_EQ(factory->wire->snapshot().dials,1u);
    EXPECT_THROW(owner->connect_sync(),db_error);
}
TEST_F(ConfiguredConnectionOwner, NewExplicitIntentCannotInheritUnpublishedOldBackoff) {
    configured_recovery_test_hooks::successor_permission qualification;
    struct rendezvous {
        std::mutex mutex;std::condition_variable changed;std::array<uint64_t,3> epochs{},retries{};
        size_t observations=0;bool held=false,released=false,safety_release=false;
    };
    const auto gate=std::make_shared<rendezvous>();
    struct restore_probe {
        std::shared_ptr<rendezvous> gate;
        std::shared_ptr<const std::function<void(uint64_t,uint64_t)>> prior=std::move(configured_recovery_test_hooks::before_backoff_publication);
        ~restore_probe(){
            {std::lock_guard<std::mutex> lock(gate->mutex);gate->released=true;}gate->changed.notify_all();
            configured_recovery_test_hooks::before_backoff_publication=std::move(prior);
        }
    } restore{gate};
    configured_recovery_test_hooks::before_backoff_publication=std::make_shared<const std::function<void(uint64_t,uint64_t)>>([gate](uint64_t epoch,uint64_t retry){
        std::unique_lock<std::mutex> lock(gate->mutex);
        const auto index=gate->observations++;
        if(index<gate->epochs.size()){gate->epochs[index]=epoch;gate->retries[index]=retry;}
        if(index==1){
            gate->held=true;gate->changed.notify_all();
            if(!gate->changed.wait_for(lock,std::chrono::seconds(5),[&]{return gate->released;}))gate->safety_release=true;
        }
        gate->changed.notify_all();
    });
    open();ASSERT_TRUE(owner);
    factory->wire->endpoint(0).trigger_on_close(1006,"first real replacement");
    ASSERT_TRUE(factory->wire->wait(2,1));
    factory->wire->endpoint(1).trigger_on_close(1006,"old second backoff");
    {
        std::unique_lock<std::mutex> lock(gate->mutex);
        ASSERT_TRUE(gate->changed.wait_for(lock,std::chrono::seconds(5),[&]{return gate->held;}));
        EXPECT_EQ(gate->retries[0],0u);EXPECT_EQ(gate->retries[1],1u);
    }
    owner->disconnect_sync();owner->connect_sync();
    {std::lock_guard<std::mutex> lock(gate->mutex);gate->released=true;}gate->changed.notify_all();
    {
        std::unique_lock<std::mutex> lock(gate->mutex);
        ASSERT_TRUE(gate->changed.wait_for(lock,std::chrono::seconds(5),[&]{return gate->observations>=3;}));
        EXPECT_FALSE(gate->safety_release);EXPECT_NE(gate->epochs[2],gate->epochs[1]);EXPECT_EQ(gate->retries[2],0u);
    }
    ASSERT_TRUE(factory->wire->wait(3,2));EXPECT_EQ(factory->wire->snapshot().dials,3u);
    owner->disconnect_sync();
}
TEST_F(ConfiguredConnectionOwner, CopiedTerminalCallbackCanCloseWithoutJoiningItsOwnScheduler) {
    configured_recovery_test_hooks::successor_permission qualification;
    open();ASSERT_TRUE(owner);
    struct result_state {std::mutex mutex;std::condition_variable ready;bool called=false;lattice_close_result observed;};
    const auto result=std::make_shared<result_state>();const std::weak_ptr<lattice_db> weak=owner;
    owner->set_on_sync_state_change([result,weak](bool connected){
        if(connected)return;
        const auto retained=weak.lock();if(!retained)return;
        const auto actual=retained->close_checked();
        {std::lock_guard<std::mutex> lock(result->mutex);result->observed=actual;result->called=true;}
        result->ready.notify_all();
    });
    factory->wire->endpoint(0).trigger_on_close(1006,"actual old terminal callback");
    {
        std::unique_lock<std::mutex> lock(result->mutex);
        ASSERT_TRUE(result->ready.wait_for(lock,std::chrono::seconds(5),[&]{return result->called;}));
        EXPECT_FALSE(result->observed.cleanup_complete);EXPECT_EQ(result->observed.sync,sync_drain_state::reentrant_pending);
    }
    ASSERT_TRUE(factory->wire->wait(1,1));
    EXPECT_TRUE(owner->close_checked().cleanup_complete);
    EXPECT_EQ(factory->wire->snapshot().dials,1u);EXPECT_EQ(factory->wire->snapshot().requests,1u);
}
#endif
