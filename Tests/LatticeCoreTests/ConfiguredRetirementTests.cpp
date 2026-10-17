#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/configured_retirement.hpp"
#include <lattice/network.hpp>
#include <limits>
#include <stdexcept>
#include <vector>

namespace lattice::detail {
struct configured_retirement_test_access {
    static std::shared_ptr<configured_retirement_registry> make(size_t capacity=64){
        return std::shared_ptr<configured_retirement_registry>(new configured_retirement_registry(capacity));
    }
    static void exhaust_owner(configured_retirement_registry& r){r.next_owner_=std::numeric_limits<uint64_t>::max();}
    static void exhaust_attempt(configured_retirement_registry& r){r.next_attempt_=std::numeric_limits<uint64_t>::max();}
};
}
namespace {
using registry=lattice::detail::configured_retirement_registry;
using retirement_test_access=lattice::detail::configured_retirement_test_access;
using receipt=lattice::platform_retirement_receipt;
class retained_transport final:public lattice::sync_transport {
    std::function<void()> destroyed_;
public:
    explicit retained_transport(std::function<void()> destroyed):destroyed_(std::move(destroyed)){}
    ~retained_transport() override {if(destroyed_)destroyed_();}
    void connect(const std::string&,const lattice::HeadersMap&) override {}
    void disconnect() override {}
    lattice::transport_state state()const override{return lattice::transport_state::closed;}
    void send(const lattice::transport_message&) override {}
    void set_on_open(on_open_handler) override {}
    void set_on_message(on_message_handler) override {}
    void set_on_error(on_error_handler) override {}
    void set_on_close(on_close_handler) override {}
};
void settle(const std::shared_ptr<registry>& r,const receipt& value){
    EXPECT_TRUE(r->request_retirement(value));
    EXPECT_TRUE(value.complete_adapter_cleanup(0));
    EXPECT_TRUE(r->complete_native_cleanup(value,0));
}
}

TEST(ConfiguredRetirement, DefaultReceiptCannotSignalAnyAttempt) {
    receipt invalid;
    EXPECT_FALSE(invalid.valid());EXPECT_FALSE(invalid.matches(invalid));
    EXPECT_FALSE(invalid.retirement_requested());EXPECT_FALSE(invalid.complete_adapter_cleanup(0));
}
TEST(ConfiguredRetirement, EarlyCompletionRemainsRetryableAfterActualRequest) {
    auto r=retirement_test_access::make();auto owner=r->reserve();const auto value=owner.begin_attempt();
    EXPECT_FALSE(value.complete_adapter_cleanup(71));
    EXPECT_FALSE(r->complete_native_cleanup(value,72));
    const auto before=r->snapshot(value);
    EXPECT_FALSE(before.adapter_complete);EXPECT_FALSE(before.native_complete);EXPECT_EQ(before.first_error,0);
    settle(r,value);EXPECT_TRUE(r->collect_completed(value));
}
TEST(ConfiguredRetirement, AdapterCompletionCannotReleaseBeforeNativeSettlement) {
    auto r=retirement_test_access::make();auto owner=r->reserve();const auto value=owner.begin_attempt();
    ASSERT_TRUE(r->request_retirement(value));ASSERT_TRUE(value.complete_adapter_cleanup(0));
    EXPECT_FALSE(r->collect_completed(value));EXPECT_EQ(r->charged_owners(),1u);
    EXPECT_THROW(owner.begin_attempt(),std::logic_error);
    ASSERT_TRUE(r->complete_native_cleanup(value,0));EXPECT_TRUE(r->collect_completed(value));
}
TEST(ConfiguredRetirement, NativeSettlementCannotReleaseBeforeAdapterCompletion) {
    auto r=retirement_test_access::make();auto owner=r->reserve();const auto value=owner.begin_attempt();
    ASSERT_TRUE(r->request_retirement(value));ASSERT_TRUE(r->complete_native_cleanup(value,0));
    EXPECT_FALSE(r->collect_completed(value));EXPECT_EQ(r->charged_owners(),1u);
    ASSERT_TRUE(value.complete_adapter_cleanup(0));EXPECT_TRUE(r->collect_completed(value));
}
TEST(ConfiguredRetirement, FreshAttemptRejectsOldCompletionAndCrossRegistryIdentity) {
    auto r=retirement_test_access::make();auto owner=r->reserve();const auto old=owner.begin_attempt();
    const auto copied=old;EXPECT_TRUE(old.matches(copied));settle(r,old);ASSERT_TRUE(r->collect_completed(old));
    const auto fresh=owner.begin_attempt();EXPECT_FALSE(old.valid());EXPECT_FALSE(old.matches(fresh));
    EXPECT_FALSE(old.complete_adapter_cleanup(0));EXPECT_FALSE(r->request_retirement(old));
    auto foreign=retirement_test_access::make();auto other=foreign->reserve();const auto same_numbers=other.begin_attempt();
    EXPECT_FALSE(fresh.matches(same_numbers));EXPECT_FALSE(r->request_retirement(same_numbers));
    settle(r,fresh);EXPECT_TRUE(r->collect_completed(fresh));settle(foreign,same_numbers);EXPECT_TRUE(foreign->collect_completed(same_numbers));
}
TEST(ConfiguredRetirement, RequestIsDeliveredOnceAndReentrantCompletionDoesNotCollect) {
    auto r=retirement_test_access::make();auto owner=r->reserve();const auto value=owner.begin_attempt();int calls=0;
    ASSERT_TRUE(r->bind_request(value,[&](receipt actual){
        ++calls;EXPECT_TRUE(actual.matches(value));EXPECT_TRUE(actual.retirement_requested());
        EXPECT_TRUE(actual.complete_adapter_cleanup(0));EXPECT_TRUE(r->complete_native_cleanup(actual,0));
        EXPECT_FALSE(r->collect_completed(actual));
    }));
    EXPECT_EQ(calls,0);EXPECT_TRUE(r->request_retirement(value));EXPECT_EQ(calls,1);
    EXPECT_FALSE(r->request_retirement(value));EXPECT_FALSE(value.complete_adapter_cleanup(0));
    EXPECT_TRUE(r->collect_completed(value));EXPECT_EQ(calls,1);
}
TEST(ConfiguredRetirement, LateRequestRegistrationReceivesAlreadyRequestedAttempt) {
    auto r=retirement_test_access::make();auto owner=r->reserve();const auto value=owner.begin_attempt();int calls=0;
    ASSERT_TRUE(r->request_retirement(value));
    ASSERT_TRUE(r->bind_request(value,[&](receipt actual){++calls;EXPECT_TRUE(actual.matches(value));EXPECT_TRUE(actual.retirement_requested());}));
    EXPECT_FALSE(r->bind_request(value,[&](receipt){++calls;}));EXPECT_EQ(calls,1);
    EXPECT_TRUE(value.complete_adapter_cleanup(0));EXPECT_TRUE(r->complete_native_cleanup(value,0));
    EXPECT_TRUE(r->collect_completed(value));
}
TEST(ConfiguredRetirement, FirstCleanupErrorQuarantinesAndDuplicateCannotEraseIt) {
    auto r=retirement_test_access::make(1);auto owner=r->reserve();const auto value=owner.begin_attempt();
    ASSERT_TRUE(r->request_retirement(value));ASSERT_TRUE(value.complete_adapter_cleanup(41));
    EXPECT_FALSE(value.complete_adapter_cleanup(0));ASSERT_TRUE(r->complete_native_cleanup(value,42));
    EXPECT_EQ(r->snapshot(value).first_error,41);EXPECT_TRUE(r->snapshot(value).quarantined);
    EXPECT_FALSE(r->collect_completed(value));EXPECT_THROW(owner.begin_attempt(),std::logic_error);
    EXPECT_THROW(r->reserve(),std::runtime_error);EXPECT_EQ(r->charged_owners(),1u);
}
TEST(ConfiguredRetirement, NativeFirstErrorSurvivesLaterAdapterFailure) {
    auto r=retirement_test_access::make();auto owner=r->reserve();const auto value=owner.begin_attempt();
    ASSERT_TRUE(r->request_retirement(value));ASSERT_TRUE(r->complete_native_cleanup(value,51));
    ASSERT_TRUE(value.complete_adapter_cleanup(52));EXPECT_EQ(r->snapshot(value).first_error,51);
    EXPECT_FALSE(r->complete_native_cleanup(value,0));EXPECT_FALSE(r->collect_completed(value));
}
TEST(ConfiguredRetirement, ThrowingRequestDeliveryNeverMakesCapacityAvailable) {
    auto r=retirement_test_access::make(1);auto owner=r->reserve();const auto value=owner.begin_attempt();
    ASSERT_TRUE(r->bind_request(value,[](receipt){throw std::runtime_error("delivery failed");}));
    ASSERT_TRUE(r->request_retirement(value));EXPECT_EQ(r->snapshot(value).first_error,registry::request_delivery_failed);
    EXPECT_TRUE(value.complete_adapter_cleanup(0));EXPECT_TRUE(r->complete_native_cleanup(value,0));
    EXPECT_FALSE(r->collect_completed(value));EXPECT_THROW(r->reserve(),std::runtime_error);
}
TEST(ConfiguredRetirement, AbandonedPendingOwnerStaysChargedUntilExplicitCollection) {
    auto r=retirement_test_access::make(1);receipt value;int requests=0;
    {auto owner=r->reserve();value=owner.begin_attempt();ASSERT_TRUE(r->bind_request(value,[&](receipt){++requests;}));}
    EXPECT_EQ(requests,1);EXPECT_TRUE(value.retirement_requested());EXPECT_FALSE(r->snapshot(value).owner_live);
    EXPECT_THROW(r->reserve(),std::runtime_error);EXPECT_TRUE(value.complete_adapter_cleanup(0));
    EXPECT_TRUE(r->complete_native_cleanup(value,0));EXPECT_EQ(r->charged_owners(),1u);
    EXPECT_TRUE(r->collect_completed(value));EXPECT_EQ(r->charged_owners(),0u);
    auto next=r->reserve();EXPECT_EQ(r->charged_owners(),1u);
}
TEST(ConfiguredRetirement, UnstartedOwnerAndAttemptHaveDistinctCleanupObligations) {
    auto r=retirement_test_access::make(1);
    {auto owner=r->reserve();EXPECT_EQ(r->charged_owners(),1u);}
    EXPECT_EQ(r->charged_owners(),0u);
    auto owner=r->reserve();const auto never_dialed=owner.begin_attempt();
    EXPECT_FALSE(r->collect_completed(never_dialed));settle(r,never_dialed);
    EXPECT_TRUE(r->collect_completed(never_dialed));EXPECT_EQ(r->charged_owners(),1u);
}
TEST(ConfiguredRetirement, AllSixtyFourLogicalSlotsRefuseBeforeFactoryAllocation) {
    auto r=retirement_test_access::make();std::vector<registry::reservation> owners;owners.reserve(64);
    for(size_t i=0;i<64;++i)owners.push_back(r->reserve());
    int factory_allocations=0;
    EXPECT_THROW(([&]{auto owner=r->reserve();++factory_allocations;}()),std::runtime_error);
    EXPECT_EQ(factory_allocations,0);EXPECT_EQ(r->charged_owners(),64u);
    owners.clear();EXPECT_EQ(r->charged_owners(),0u);
}
TEST(ConfiguredRetirement, SerialExhaustionNeverReusesOldAuthority) {
    auto owners=retirement_test_access::make();retirement_test_access::exhaust_owner(*owners);
    EXPECT_THROW(owners->reserve(),std::overflow_error);EXPECT_EQ(owners->charged_owners(),0u);
    auto r=retirement_test_access::make();auto owner=r->reserve();retirement_test_access::exhaust_attempt(*r);
    EXPECT_THROW(owner.begin_attempt(),std::overflow_error);EXPECT_EQ(r->charged_owners(),1u);
}
TEST(ConfiguredRetirement, CustodyIsDestroyedOffLeafAndChargedThroughDestruction) {
    auto r=retirement_test_access::make(1);auto owner=r->reserve();const auto value=owner.begin_attempt();int destroyed=0;
    ASSERT_TRUE(r->retain_transport(value,std::make_shared<retained_transport>([&]{
        ++destroyed;EXPECT_EQ(r->charged_owners(),1u);EXPECT_TRUE(value.valid());
        EXPECT_THROW(owner.begin_attempt(),std::logic_error);EXPECT_FALSE(r->collect_completed(value));
    })));
    settle(r,value);EXPECT_EQ(destroyed,0);ASSERT_TRUE(r->collect_completed(value));
    EXPECT_EQ(destroyed,1);EXPECT_FALSE(value.valid());
}
TEST(ConfiguredRetirement, RequestCaptureDestructionCannotObserveReleasedCapacity) {
    auto r=retirement_test_access::make(1);auto owner=r->reserve();const auto value=owner.begin_attempt();int destroyed=0;
    auto captured=std::shared_ptr<int>(new int(1),[&](int* p){
        ++destroyed;EXPECT_EQ(r->charged_owners(),1u);EXPECT_THROW(owner.begin_attempt(),std::logic_error);delete p;
    });
    ASSERT_TRUE(r->bind_request(value,[captured](receipt){}));captured.reset();
    settle(r,value);EXPECT_EQ(destroyed,0);EXPECT_TRUE(r->collect_completed(value));EXPECT_EQ(destroyed,1);
}
TEST(ConfiguredRetirement, LateTransportMustPrecedeBothCleanupAssertions) {
    auto r=retirement_test_access::make();auto owner=r->reserve();const auto value=owner.begin_attempt();int destroyed=0;
    ASSERT_TRUE(r->request_retirement(value));
    ASSERT_TRUE(r->retain_transport(value,std::make_shared<retained_transport>([&]{++destroyed;})));
    EXPECT_TRUE(r->snapshot(value).transport_retained);EXPECT_EQ(destroyed,0);
    EXPECT_TRUE(value.complete_adapter_cleanup(0));EXPECT_TRUE(r->complete_native_cleanup(value,0));
    EXPECT_TRUE(r->collect_completed(value));EXPECT_EQ(destroyed,1);
    const auto next=owner.begin_attempt();ASSERT_TRUE(r->request_retirement(next));
    ASSERT_TRUE(next.complete_adapter_cleanup(0));
    EXPECT_FALSE(r->retain_transport(next,std::make_shared<retained_transport>([&]{++destroyed;})));
    EXPECT_EQ(destroyed,2);EXPECT_TRUE(r->complete_native_cleanup(next,0));EXPECT_TRUE(r->collect_completed(next));
}
