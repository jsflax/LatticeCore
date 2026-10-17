#include "ObserverDeliveryBridgeTestSupport.hpp"
#ifndef __linux__
#include <lattice.hpp>
#include <dynamic_object.hpp>
#include <observer_delivery_setter.hpp>
#include <array>
#include <atomic>
#include <thread>

namespace lattice {
// Test DATA manipulation of an actual pre-existing cache; no profile/admission
// flag, target identity, Native SQL outcome or owner constructor is fabricated.
struct observer_delivery_cache_test_access {
    static size_t grow_buckets(dynamic_object_ref& object,size_t requested) {
        auto& cache=object.impl_->managed_.source.values;
        cache.rehash(requested);return cache.bucket_count();
    }
    static size_t grow_text(dynamic_object_ref& object,size_t requested) {
        auto& text=std::get<std::string>(object.impl_->managed_.source.values.at("text"));
        text.reserve(requested);return text.capacity();
    }
};
}
namespace {
// This deliberately uses the genuine normal Bridge factory and its COMPLETE
// retained catalog. It neither prunes the global Native registry nor fabricates
// an enabled bit. Activation against the full CoreTests registry remains an
// explicit setup prerequisite until its actual supported catalog is qualified.
struct BridgeSetterFixture {
    using Connection=lattice::detail::observer_delivery_connection;
    delivery_bridge_testing::File file{"delivery_bridge_setter"};
    std::shared_ptr<delivery_bridge_testing::Queue> queue=std::make_shared<delivery_bridge_testing::Queue>();
    std::unique_ptr<lattice::swift_lattice_ref> owner;
    BridgeSetterFixture() {
        lattice::swift_schema_entry entry;entry.table_name="DeliverySetterRow";
        lattice::property_descriptor text;text.name="text";text.type=lattice::column_type::text;
        text.kind=lattice::property_kind::primitive;entry.properties.emplace(text.name,text);
        lattice::property_descriptor blob;blob.name="bytes";blob.type=lattice::column_type::blob;
        blob.kind=lattice::property_kind::primitive;entry.properties.emplace(blob.name,blob);
        lattice::swift_configuration config(file.str());config.sched=queue;config.audit_retention_seconds=0;
#if LATTICE_HAS_FRT
        owner.reset(lattice::swift_lattice_ref::create_uncached(config,{entry}));
#else
        owner=std::make_unique<lattice::swift_lattice_ref>(lattice::swift_lattice_ref::create_uncached(config,{entry}));
#endif
        if(!owner||!owner->get())throw std::runtime_error("actual Bridge setter owner unavailable");
        owner->get()->stop_audit_maintenance();
        lattice::instance_registry::instance().get_or_create_notifier(file.str())->stop_listening();queue->drain();
        owner->get()->db().execute("INSERT INTO DeliverySetterRow(globalId,text,bytes) VALUES('setter-original','seed',X'010203')");
        queue->drain();
        Connection::policy policy{{2,8,{16,2*1024*1024},{16,2*1024*1024}},
            {16,256*1024,1024*1024},std::chrono::milliseconds(1000)};
        lattice::detail::observer_delivery_connection_test_access::enroll(*owner->get(),policy);
    }
    ~BridgeSetterFixture(){if(owner)lattice::detail::end_fixture_owner_checked(owner->get(),"delivery-bridge-setter");}
    std::unique_ptr<lattice::dynamic_object_ref> object() {
        auto rows=owner->get()->objects("DeliverySetterRow");
        if(rows.size()!=1)throw std::runtime_error("actual Bridge setter row missing");
#if LATTICE_HAS_FRT
        return std::unique_ptr<lattice::dynamic_object_ref>(lattice::dynamic_object_ref::wrap_managed(rows[0]));
#else
        return std::make_unique<lattice::dynamic_object_ref>(lattice::dynamic_object_ref::wrap_managed(rows[0]));
#endif
    }
};
const uint8_t* bytes(const char* text){return reinterpret_cast<const uint8_t*>(text);}

TEST(ObserverDeliveryBridgeSetter, CopiedPreparedHolderCannotSupplyOrExecuteTwice) {
    BridgeSetterFixture actual;auto object=actual.object();
    auto first=object->prepare_delivery_text(bytes("text"),4,6);
    ASSERT_FALSE(lattice::last_bridge_call_failed());ASSERT_TRUE(first.prepared());
    const auto copy=first;
    copy.apply_text(bytes("winner"),6);
    ASSERT_FALSE(lattice::last_bridge_call_failed());
    first.apply_text(bytes("second"),6);
    EXPECT_TRUE(lattice::last_bridge_call_failed());
    EXPECT_EQ(object->get_string("text"),"winner");
    const auto rows=actual.owner->get()->db().query("SELECT text FROM DeliverySetterRow");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::string>(rows[0].at("text")),"winner");
}

TEST(ObserverDeliveryBridgeSetter, TwoCopiedCallersRaceOnlyOneActualCoreSupplyAndExecute) {
    BridgeSetterFixture actual;auto object=actual.object();
    auto first=object->prepare_delivery_text(bytes("text"),4,3);
    ASSERT_FALSE(lattice::last_bridge_call_failed());ASSERT_TRUE(first.prepared());
    const auto second=first;
    std::atomic<bool> start{false};std::array<std::atomic<bool>,2> failed{};
    std::array<std::thread,2> workers;
    struct Ending {
        std::atomic<bool>& start;std::array<std::thread,2>& workers;
        ~Ending()noexcept {
            start.store(true,std::memory_order_release);
            for(auto& worker:workers)if(worker.joinable()) {
                try{worker.join();}catch(...){std::fprintf(stderr,"delivery setter worker-ending-debt namespaceRetained=1\n");std::fflush(stderr);::_exit(96);}
            }
        }
    } ending{start,workers};
    workers[0]=std::thread([&]{while(!start.load(std::memory_order_acquire))std::this_thread::yield();first.apply_text(bytes("one"),3);failed[0]=lattice::last_bridge_call_failed();});
    workers[1]=std::thread([&]{while(!start.load(std::memory_order_acquire))std::this_thread::yield();second.apply_text(bytes("two"),3);failed[1]=lattice::last_bridge_call_failed();});
    start.store(true,std::memory_order_release);
    for(auto& worker:workers)worker.join();
    ASSERT_EQ(unsigned(failed[0].load())+unsigned(failed[1].load()),1u);
    const auto expected=failed[0].load()?"two":"one";
    EXPECT_EQ(object->get_string("text"),expected);
    const auto rows=actual.owner->get()->db().query("SELECT text FROM DeliverySetterRow");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::string>(rows[0].at("text")),expected);
}

TEST(ObserverDeliveryBridgeSetter, ActualSuppliedExtentIsCheckedBeforeCacheOrSqlEffects) {
    BridgeSetterFixture actual;auto object=actual.object();object->enable_row_cache();
    auto limited=object->prepare_delivery_text(bytes("text"),4,3);
    ASSERT_FALSE(lattice::last_bridge_call_failed());ASSERT_TRUE(limited.prepared());
    limited.apply_text(bytes("four"),4);
    EXPECT_TRUE(lattice::last_bridge_call_failed());
    EXPECT_EQ(object->get_string("text"),"seed");
    const auto rows=actual.owner->get()->db().query("SELECT text FROM DeliverySetterRow");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::string>(rows[0].at("text")),"seed");
}

TEST(ObserverDeliveryBridgeSetter, AdmittedBinarySupplyUsesActualBytesAndCache) {
    BridgeSetterFixture actual;auto object=actual.object();object->enable_row_cache();
    const std::array<uint8_t,5> requested{0,255,1,0,7};
    auto setter=object->prepare_delivery_bytes(bytes("bytes"),5,requested.size());
    ASSERT_FALSE(lattice::last_bridge_call_failed());ASSERT_TRUE(setter.prepared());
    setter.apply_bytes(requested.data(),requested.size());
    ASSERT_FALSE(lattice::last_bridge_call_failed());
    EXPECT_EQ(object->get_data("bytes"),(std::vector<uint8_t>(requested.begin(),requested.end())));
    const auto rows=actual.owner->get()->db().query("SELECT bytes FROM DeliverySetterRow");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::vector<uint8_t>>(rows[0].at("bytes")),(std::vector<uint8_t>(requested.begin(),requested.end())));
}
TEST(ObserverDeliveryBridgeSetter, TinyCacheWithLargeActualBucketCapacityRefusesBeforeEffects) {
    BridgeSetterFixture actual;auto object=actual.object();object->enable_row_cache();
    const auto buckets=lattice::observer_delivery_cache_test_access::grow_buckets(*object,1u<<18);
    ASSERT_GE(buckets,1u<<18);
    auto setter=object->prepare_delivery_text(bytes("text"),4,3);
    ASSERT_FALSE(lattice::last_bridge_call_failed());ASSERT_TRUE(setter.prepared());
    setter.apply_text(bytes("new"),3);
    EXPECT_TRUE(lattice::last_bridge_call_failed());
    EXPECT_EQ(object->get_string("text"),"seed");
    const auto rows=actual.owner->get()->db().query("SELECT text FROM DeliverySetterRow");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::string>(rows[0].at("text")),"seed");
}

TEST(ObserverDeliveryBridgeSetter, TinyCacheValueWithLargeActualStringCapacityRefusesBeforeEffects) {
    BridgeSetterFixture actual;auto object=actual.object();object->enable_row_cache();
    const auto capacity=lattice::observer_delivery_cache_test_access::grow_text(*object,2u*1024*1024);
    ASSERT_GE(capacity,2u*1024*1024);
    auto setter=object->prepare_delivery_text(bytes("text"),4,3);
    ASSERT_FALSE(lattice::last_bridge_call_failed());ASSERT_TRUE(setter.prepared());
    setter.apply_text(bytes("new"),3);
    EXPECT_TRUE(lattice::last_bridge_call_failed());
    EXPECT_EQ(object->get_string("text"),"seed");
    const auto rows=actual.owner->get()->db().query("SELECT text FROM DeliverySetterRow");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::string>(rows[0].at("text")),"seed");
}

TEST(ObserverDeliveryBridgeSetter, PreparedTokenDoesNotRetainAnExpiredModelOrExecuteItsSql) {
    BridgeSetterFixture actual;
    lattice::swift_delivery_setter token;
    {
        auto object=actual.object();
        token=object->prepare_delivery_text(bytes("text"),4,3);
        ASSERT_FALSE(lattice::last_bridge_call_failed());ASSERT_TRUE(token.prepared());
    } // The actual sole object box dies; only weak model custody remains.
    token.apply_text(bytes("new"),3);
    EXPECT_TRUE(lattice::last_bridge_call_failed());
    const auto rows=actual.owner->get()->db().query("SELECT text FROM DeliverySetterRow");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::string>(rows[0].at("text")),"seed");
}

}
#endif
