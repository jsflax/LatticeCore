#include "TestHelpers.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "ObserverDeliveryCustodyTestAccess.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_store.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_original.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_plan.hpp"
#include <array>
#include <deque>
#include <stdexcept>

namespace {
using Store=lattice::detail::observer_delivery_store;
using Original=lattice::detail::observer_delivery_original;
using Status=Store::status;
using Units=Store::units;

// Uses the real public scheduler handoff. No alternate observer dispatcher or
// synthesized Native transaction/ending event is used by these controls.
class CreditQueue final:public lattice::scheduler {
public:
    std::deque<std::function<void()>> pending;
    void invoke(std::function<void()>&& work)override{pending.push_back(std::move(work));}
    bool is_on_thread()const noexcept override{return true;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
    std::function<void()> take(){auto value=std::move(pending.front());pending.pop_front();return value;}
    void cancel(){pending.clear();}
    void drain(){while(!pending.empty()){auto work=take();work();}}
};
class CreditOwner final:public lattice::lattice_db {
public:
    using lattice_db::lattice_db;
    using lattice_db::with_mutation_operation;
};
std::shared_ptr<CreditOwner> credit_owner(const std::string& path,
    const std::shared_ptr<CreditQueue>& queue) {
    lattice::configuration config(path);config.sched=queue;config.audit_retention_seconds=0;
    auto owner=std::make_shared<CreditOwner>(config);
    lattice::instance_registry::instance().get_or_create_notifier(path)->stop_listening();
    queue->drain();return owner;
}

#if defined(__APPLE__) || defined(__linux__)
TEST(ObserverDeliveryOriginal, RealCommittedBatchKeepsCreditThroughCallbackReturnUntilLastCallableRetires) {
    int callbacks=0;
    TempDB file("observer_delivery_original_commit");auto queue=std::make_shared<CreditQueue>();
    auto owner=credit_owner(file.str(),queue);
    auto store=Store::bind(owner->db(),{1,2,{1,4096},{}});auto client=store.register_client().value;
    const auto token=owner->add_table_observer("TestPerson",[&](const auto& rows){++callbacks;EXPECT_EQ(rows.size(),1u);});
    {
        auto admitted=client.try_acquire({1,4096});ASSERT_EQ(admitted.code,Status::admitted);
        Original::begin_offer offer(owner->db(),admitted.held,{1,1024,3072});admitted.held.reset();
        owner->begin_transaction();owner->add(TestPerson{"committed",7,std::nullopt});owner->commit();
        EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_EQ(callbacks,0);ASSERT_EQ(queue->pending.size(),1u);
    }
    owner->remove_table_observer("TestPerson",token); // queued delivery remains accepted
    EXPECT_EQ(store.inspect().used,(Units{1,4096}));
    auto work=queue->take();work();EXPECT_EQ(callbacks,1);
    EXPECT_EQ(store.inspect().used,(Units{1,4096}))<<"callback return is not capture retirement";
    auto copy=work;work={};EXPECT_EQ(store.inspect().used,(Units{1,4096}));
    copy={};EXPECT_EQ(store.inspect().used,(Units{}));
    const auto rows=owner->db().query("SELECT name FROM TestPerson");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::string>(rows[0].at("name")),"committed");
    owner->close();
}
TEST(ObserverDeliveryOriginal, ThrowingRealQueuedCallbackRetainsCreditAndCommittedDataUntilCancellationTail) {
    TempDB file("observer_delivery_original_throw");auto queue=std::make_shared<CreditQueue>();
    auto owner=credit_owner(file.str(),queue);
    auto store=Store::bind(owner->db(),{1,2,{1,4096},{}});auto client=store.register_client().value;
    const auto token=owner->add_table_observer("TestPerson",[](const auto&){throw std::runtime_error("actual queued observer failure");});
    {
        auto admitted=client.try_acquire({1,4096});ASSERT_EQ(admitted.code,Status::admitted);
        Original::begin_offer offer(owner->db(),admitted.held,{1,1024,3072});admitted.held.reset();
        owner->begin_transaction();owner->add(TestPerson{"durable",7,std::nullopt});owner->commit();
    }
    owner->remove_table_observer("TestPerson",token);ASSERT_EQ(queue->pending.size(),1u);
    auto work=queue->take();EXPECT_THROW(work(),std::runtime_error);
    EXPECT_EQ(store.inspect().used,(Units{1,4096}));
    EXPECT_EQ(owner->db().query("SELECT name FROM TestPerson").size(),1u);
    work={};EXPECT_EQ(store.inspect().used,(Units{}));owner->close();
}
TEST(ObserverDeliveryOriginal, RealQueuedWorkCopiesPreserveIndependentMutableOpaqueCallbackValues) {
    std::vector<int> observed;
    TempDB file("observer_delivery_original_callback_copy");auto queue=std::make_shared<CreditQueue>();
    auto owner=credit_owner(file.str(),queue);
    auto store=Store::bind(owner->db(),{1,2,{1,4096},{}});auto client=store.register_client().value;
    const auto token=owner->add_table_observer("TestPerson",[count=0,&observed](const auto&)mutable{observed.push_back(++count);});
    {
        auto admitted=client.try_acquire({1,4096});ASSERT_EQ(admitted.code,Status::admitted);
        Original::begin_offer offer(owner->db(),admitted.held,{1,1024,3072});admitted.held.reset();
        owner->begin_transaction();owner->add(TestPerson{"copied",7,std::nullopt});owner->commit();
    }
    owner->remove_table_observer("TestPerson",token);ASSERT_EQ(queue->pending.size(),1u);
    auto first=queue->take();auto second=first;
    first();second();EXPECT_EQ(observed,(std::vector<int>{1,1}));
    first={};EXPECT_EQ(store.inspect().used,(Units{1,4096}));
    second={};EXPECT_EQ(store.inspect().used,(Units{}));owner->close();
}
TEST(ObserverDeliveryOriginal, ActualUninvokedCaptureDestructorSeesHeldCreditAndCannotWaitForItself) {
    // All marker targets outlive every early-assertion queue/owner tail.
    int destroyed=0;Status nested=Status::invalid;Units during{};
    TempDB file("observer_delivery_original_cancel");auto queue=std::make_shared<CreditQueue>();
    auto owner=credit_owner(file.str(),queue);
    auto store=Store::bind(owner->db(),{1,2,{1,4096},{}});auto client=store.register_client().value;
    struct Marker {
        Store store;Store::client client;int& destroyed;Status& nested;Units& during;
        Marker(Store s,Store::client c,int& d,Status& n,Units& u)
            :store(std::move(s)),client(std::move(c)),destroyed(d),nested(n),during(u){}
        ~Marker(){during=store.inspect().used;nested=client.acquire({1,4096},Store::clock::now()+std::chrono::seconds(1)).code;++destroyed;}
    };
    auto marker=std::make_shared<Marker>(store,client,destroyed,nested,during);
    const auto token=owner->add_table_observer("TestPerson",[marker](const auto&){});
    {
        auto admitted=client.try_acquire({1,4096});ASSERT_EQ(admitted.code,Status::admitted);
        Original::begin_offer offer(owner->db(),admitted.held,{1,1024,3072});admitted.held.reset();
        owner->begin_transaction();owner->add(TestPerson{"cancelled callback",7,std::nullopt});owner->commit();
    }
    owner->remove_table_observer("TestPerson",token);marker.reset();
    ASSERT_EQ(queue->pending.size(),1u);EXPECT_EQ(destroyed,0);
    queue->cancel();EXPECT_EQ(destroyed,1);EXPECT_EQ(during,(Units{1,4096}));
    EXPECT_EQ(nested,Status::reentrant_would_block);EXPECT_EQ(store.inspect().used,(Units{}));owner->close();
}
TEST(ObserverDeliveryOriginal, ActualSavepointFailurePreservesCallerPrefixAndSameOriginalCredit) {
    TempDB file("observer_delivery_original_savepoint");auto queue=std::make_shared<CreditQueue>();
    auto owner=credit_owner(file.str(),queue);auto store=Store::bind(owner->db(),{1,2,{2,8192},{}});
    auto client=store.register_client().value;std::vector<std::string> names;
    const auto token=owner->add_table_observer("TestPerson",[&](const auto& batch){
        for(const auto& item:batch)names.push_back(std::get<1>(item));
    });
    {
        auto admitted=client.try_acquire({2,8192});ASSERT_EQ(admitted.code,Status::admitted);
        Original::begin_offer offer(owner->db(),admitted.held,{2,2048,6144});admitted.held.reset();
        owner->begin_transaction();owner->add(TestPerson{"prefix",7,std::nullopt});
        EXPECT_THROW(owner->with_mutation_operation([&](lattice::database&){
            owner->add(TestPerson{"rolled back suffix",8,std::nullopt});
            throw std::runtime_error("actual savepoint body failure");
        }),std::runtime_error);
        EXPECT_TRUE(owner->db().is_in_transaction());EXPECT_EQ(store.inspect().used,(Units{2,8192}));
        const auto rows=owner->db().query("SELECT name FROM TestPerson");
        ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::string>(rows[0].at("name")),"prefix");
        owner->commit();
    }
    ASSERT_EQ(queue->pending.size(),1u);queue->drain();
    EXPECT_EQ(names,(std::vector<std::string>{"INSERT"}));EXPECT_EQ(store.inspect().used,(Units{}));
    owner->remove_table_observer("TestPerson",token);owner->close();
}
TEST(ObserverDeliveryOriginal, RolledBackOriginalReturnsCreditAfterRealCaptureWithoutPublishingRows) {
    TempDB file("observer_delivery_original_rollback");auto queue=std::make_shared<CreditQueue>();
    auto owner=credit_owner(file.str(),queue);auto store=Store::bind(owner->db(),{1,2,{1,4096},{}});
    auto client=store.register_client().value;
    {
        auto admitted=client.try_acquire({1,4096});ASSERT_EQ(admitted.code,Status::admitted);
        Original::begin_offer offer(owner->db(),admitted.held,{1,1024,3072});admitted.held.reset();
        owner->begin_transaction();owner->add(TestPerson{"rolled back",7,std::nullopt});owner->rollback();
        EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_EQ(store.inspect().used,(Units{1,4096}));
    }
    EXPECT_TRUE(owner->db().query("SELECT name FROM TestPerson").empty());
    EXPECT_TRUE(queue->pending.empty());EXPECT_EQ(store.inspect().used,(Units{}));owner->close();
}
TEST(ObserverDeliveryOriginal, OneOfferCannotRenewItsCreditForARealSuccessorBegin) {
    TempDB file("observer_delivery_original_successor");auto queue=std::make_shared<CreditQueue>();
    auto owner=credit_owner(file.str(),queue);auto store=Store::bind(owner->db(),{1,2,{1,4096},{}});
    auto client=store.register_client().value;
    {
        auto admitted=client.try_acquire({1,4096});ASSERT_EQ(admitted.code,Status::admitted);
        Original::begin_offer offer(owner->db(),admitted.held,{1,1024,3072});admitted.held.reset();
        owner->begin_transaction();owner->commit();
        EXPECT_THROW(owner->begin_transaction(),std::runtime_error);
        EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_EQ(store.inspect().used,(Units{1,4096}));
    }
    EXPECT_EQ(store.inspect().used,(Units{}));
    auto successor=client.try_acquire({1,4096});ASSERT_EQ(successor.code,Status::admitted);
    {Original::begin_offer offer(owner->db(),successor.held,{1,1024,3072});successor.held.reset();owner->begin_transaction();owner->rollback();}
    EXPECT_EQ(store.inspect().used,(Units{}));owner->close();
}
TEST(ObserverDeliveryOriginal, StructuredInputOriginalSharesOneCreditWithRealSqlAndCannotIssueTwiceThroughCopies) {
    using Plan=lattice::detail::observer_delivery_plan;
    TempDB file("observer_delivery_original_plan");auto queue=std::make_shared<CreditQueue>();
    auto owner=credit_owner(file.str(),queue);auto store=Store::bind(owner->db(),{1,2,{4,16384},{}});
    auto client=store.register_client().value;
    std::string name("old\0name",8);
    const std::array<Plan::field_view,2> fields{{{"name",std::string_view(name)},{"age",int64_t{7}}}};
    const std::array<Plan::row_view,1> input{{{"TestPerson",fields}}};
    auto prepared=Plan::prepare(client,{4,4096,12288},Store::clock::now()+std::chrono::seconds(1),input);
    ASSERT_EQ(prepared.code,Status::admitted);auto same=prepared.plan;name="later";
    {
        Original::begin_offer offer(owner->db(),prepared.plan);
        owner->with_mutation_operation([&](lattice::database& writer){
            const auto& row=prepared.plan.rows().at(0);
            writer.execute("INSERT INTO TestPerson(name,age) VALUES(?,?)",
                {std::get<std::string>(row.fields.at(0).contents),std::get<int64_t>(row.fields.at(1).contents)});
        });
    }
    EXPECT_THROW((Original::begin_offer(owner->db(),same)),std::runtime_error);
    EXPECT_FALSE(owner->db().is_in_transaction());
    const auto rows=owner->db().query("SELECT name,age FROM TestPerson");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::string>(rows[0].at("name")),std::string("old\0name",8));
    EXPECT_EQ(std::get<int64_t>(rows[0].at("age")),7);
    EXPECT_EQ(store.inspect().used,(Units{4,16384}));prepared.plan={};
    EXPECT_EQ(store.inspect().used,(Units{4,16384}))<<"actual immutable input copy remains live";
    same={};EXPECT_EQ(store.inspect().used,(Units{}));owner->close();
}
// The registration is never invoked/replaced/removed by this control. SQLite
// alone calls xDestroy on genuine native teardown. The target is atomic DATA,
// not a strong writer/original/retirement witness that could prevent ending.
int observe_credit_native_close(lattice::lattice_db& owner,
    const std::shared_ptr<std::atomic<size_t>>& observed) {
    auto* native=lattice::detail::canonical_writer_custody_test_access::fault_handle(owner.db());
    auto* data=new std::shared_ptr<std::atomic<size_t>>(observed);
    return sqlite3_create_function_v2(native,"_delivery_native_retirement_observer",0,SQLITE_UTF8,data,
        +[](sqlite3_context* result,int,sqlite3_value**)noexcept{sqlite3_result_null(result);},
        nullptr,nullptr,+[](void* raw)noexcept {
            auto* value=static_cast<std::shared_ptr<std::atomic<size_t>>*>(raw);
            (*value)->fetch_add(1,std::memory_order_release);delete value;
        });
}
TEST(ObserverDeliveryOriginal, TrueOwnerDropEndsActualSqliteAndFinalUnpublishedPayloadBeforeReturningCredit) {
    auto closed=std::make_shared<std::atomic<size_t>>(0);
    TempDB file("observer_delivery_original_drop");auto queue=std::make_shared<CreditQueue>();
    auto owner=credit_owner(file.str(),queue);auto store=Store::bind(owner->db(),{1,2,{1,4096},{}});
    auto client=store.register_client().value;
    ASSERT_EQ(observe_credit_native_close(*owner,closed),SQLITE_OK);
    {
        auto admitted=client.try_acquire({1,4096});ASSERT_EQ(admitted.code,Status::admitted);
        Original::begin_offer offer(owner->db(),admitted.held,{1,1024,3072});admitted.held.reset();
        owner->begin_transaction();owner->add(TestPerson{"uncommitted",7,std::nullopt});
    }
    EXPECT_TRUE(owner->db().is_in_transaction());EXPECT_EQ(store.inspect().used,(Units{1,4096}));
    const auto weak=lattice::observer_delivery_custody_test_access::writer(*owner);
    owner.reset(); // no explicit SQL COMMIT/ROLLBACK and no strong test witness
    EXPECT_TRUE(weak.expired());EXPECT_EQ(closed->load(std::memory_order_acquire),1u);
    EXPECT_TRUE(queue->pending.empty());EXPECT_EQ(store.inspect().used,(Units{}));
    // Actual native/resource ending is not a forged transaction COMMIT.
    lattice::database verify(file.str());EXPECT_TRUE(verify.query("SELECT name FROM TestPerson").empty());
}
TEST(ObserverDeliveryOriginal, ActualWriterRetirementCannotRefundTheStillOwnedLatticeRowBuffer) {
    auto closed=std::make_shared<std::atomic<size_t>>(0);
    TempDB file("observer_delivery_original_writer_retire");auto queue=std::make_shared<CreditQueue>();
    auto owner=credit_owner(file.str(),queue);auto store=Store::bind(owner->db(),{1,2,{1,4096},{}});
    auto client=store.register_client().value;ASSERT_EQ(observe_credit_native_close(*owner,closed),SQLITE_OK);
    {
        auto admitted=client.try_acquire({1,4096});ASSERT_EQ(admitted.code,Status::admitted);
        Original::begin_offer offer(owner->db(),admitted.held,{1,1024,3072});admitted.held.reset();
        owner->begin_transaction();owner->add(TestPerson{"buffer outlives writer",7,std::nullopt});
    }
    const auto weak=lattice::observer_delivery_custody_test_access::writer(*owner);
    owner->close_write_db();EXPECT_TRUE(weak.expired());
    EXPECT_EQ(closed->load(std::memory_order_acquire),1u);
    EXPECT_EQ(store.inspect().used,(Units{1,4096}))<<"physical writer close is not row-buffer retirement";
    owner.reset();EXPECT_TRUE(queue->pending.empty());EXPECT_EQ(store.inspect().used,(Units{}));
}
TEST(ObserverDeliveryOriginal, RealBusyCheckedCloseQuarantinesCreditEvenAfterZombieStatementFinallyEnds) {
    auto closed=std::make_shared<std::atomic<size_t>>(0);
    TempDB file("observer_delivery_original_busy_close");auto queue=std::make_shared<CreditQueue>();
    auto owner=credit_owner(file.str(),queue);auto store=Store::bind(owner->db(),{1,2,{1,4096},{}});
    auto client=store.register_client().value;ASSERT_EQ(observe_credit_native_close(*owner,closed),SQLITE_OK);
    {
        auto admitted=client.try_acquire({1,4096});ASSERT_EQ(admitted.code,Status::admitted);
        Original::begin_offer offer(owner->db(),admitted.held,{1,1024,3072});admitted.held.reset();
        owner->begin_transaction();owner->add(TestPerson{"busy owner",7,std::nullopt});
    }
    sqlite3_stmt* pending=nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(lattice::detail::canonical_writer_custody_test_access::fault_handle(owner->db()),
        "SELECT name FROM TestPerson",-1,&pending,nullptr),SQLITE_OK);
    const auto weak=lattice::observer_delivery_custody_test_access::writer(*owner);
    owner.reset();EXPECT_TRUE(weak.expired());EXPECT_EQ(closed->load(std::memory_order_acquire),0u);
    EXPECT_EQ(store.inspect().used,(Units{1,4096}));
    EXPECT_EQ(sqlite3_finalize(pending),SQLITE_OK);pending=nullptr;
    EXPECT_EQ(closed->load(std::memory_order_acquire),1u);
    EXPECT_EQ(store.inspect().used,(Units{1,4096}))<<"zombie ending never rewrites first checked-close failure";
    EXPECT_EQ(client.try_acquire({1,4096}).code,Status::unavailable);
}
#endif
}
