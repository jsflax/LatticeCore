#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_store.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_meter.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_plan.hpp"
#include <array>
#include <limits>
#include <thread>
#if defined(__APPLE__) || defined(__linux__)
#include <unistd.h>
#endif

namespace {
using Store=lattice::detail::observer_delivery_store;
using Meter=lattice::detail::observer_delivery_meter;
using Status=Store::status;
using Units=Store::units;

#if defined(__APPLE__) || defined(__linux__)
TEST(ObserverDeliveryStore, RealSameFileAliasesUseOnePhysicalDomainAndExactConfiguration) {
    TempDB file("observer_delivery_store_alias");
    lattice::database first(file.str());first.execute("CREATE TABLE Rows(value INTEGER)");
    lattice::database second(file.str());
    auto a=Store::bind(first,{2,4,{1,10},{1,10}});
    auto b=Store::bind(second,{2,4,{1,10},{1,10}});
    ASSERT_TRUE(a.same_domain(b));ASSERT_TRUE(a.current_for(second));ASSERT_TRUE(b.current_for(first));
    auto client=a.register_client();ASSERT_EQ(client.code,Status::admitted);
    auto held=client.value.try_acquire({2,20});ASSERT_EQ(held.code,Status::admitted);
    EXPECT_EQ(b.inspect().used,(Units{2,20}));
    const Store::domain::limits incompatible{2,4,{1,10},{2,10}};
    EXPECT_THROW(Store::bind(second,incompatible),std::runtime_error);
    held.held.reset();EXPECT_EQ(a.inspect().used,(Units{}));
}
TEST(ObserverDeliveryStore, ActualLeaseOnlyTailKeepsDomainAndClientSlotUntilItsLastRetirement) {
    TempDB file("observer_delivery_store_tail");
    lattice::database writer(file.str());writer.execute("CREATE TABLE Rows(value INTEGER)");
    Store::lease tail;
    {
        auto store=Store::bind(writer,{1,2,{1,10},{1,10}});
        auto client=store.register_client();ASSERT_EQ(client.code,Status::admitted);
        auto held=client.value.try_acquire({2,20});ASSERT_EQ(held.code,Status::admitted);
        tail=held.held;held.held.reset();client.value.reset();
    }
    auto reopened=Store::bind(writer,{1,2,{1,10},{1,10}});
    EXPECT_EQ(reopened.inspect().used,(Units{2,20}));
    EXPECT_EQ(reopened.register_client().code,Status::client_capacity);
    auto copied_tail=tail;tail.reset();EXPECT_EQ(reopened.inspect().used,(Units{2,20}));
    copied_tail.reset();EXPECT_EQ(reopened.inspect().used,(Units{}));
    EXPECT_EQ(reopened.register_client().code,Status::admitted);
}
TEST(ObserverDeliveryStore, DifferentActualFilesHaveSeparateBudgetsAndForeignCancellationRefuses) {
    TempDB first_file("observer_delivery_store_first"),second_file("observer_delivery_store_second");
    lattice::database first(first_file.str()),second(second_file.str());
    first.execute("CREATE TABLE Rows(value INTEGER)");second.execute("CREATE TABLE Rows(value INTEGER)");
    auto a=Store::bind(first,{1,1,{1,10},{}}),b=Store::bind(second,{1,1,{1,10},{}});
    EXPECT_FALSE(a.same_domain(b));EXPECT_FALSE(a.current_for(second));
    auto ac=a.register_client().value,bc=b.register_client().value;
    auto held=ac.try_acquire({1,10});ASSERT_EQ(held.code,Status::admitted);
    EXPECT_EQ(bc.try_acquire({1,10}).code,Status::admitted);
    Store::cancellation foreign(b);
    EXPECT_EQ(ac.acquire({1,10},Store::clock::now()+std::chrono::seconds(1),&foreign).code,Status::invalid);
    held.held.reset();EXPECT_EQ(a.inspect().used,(Units{}));
}
TEST(ObserverDeliveryStore, LeaseAndClientAssignmentRetireDisplacedCreditWithoutLosingPhysicalPins) {
    TempDB a_file("observer_delivery_store_assign_a"),b_file("observer_delivery_store_assign_b");
    lattice::database aw(a_file.str()),bw(b_file.str());aw.execute("CREATE TABLE Rows(v)");bw.execute("CREATE TABLE Rows(v)");
    auto a=Store::bind(aw,{1,1,{1,10},{}}),b=Store::bind(bw,{1,1,{1,10},{}});
    auto ac=a.register_client().value,bc=b.register_client().value;
    auto ah=ac.try_acquire({1,10}),bh=bc.try_acquire({1,10});
    ASSERT_EQ(ah.code,Status::admitted);ASSERT_EQ(bh.code,Status::admitted);
    ah.held=bh.held;EXPECT_EQ(a.inspect().used,(Units{}));EXPECT_EQ(b.inspect().used,(Units{1,10}));
    ac=bc;EXPECT_EQ(a.register_client().code,Status::admitted);
    bh.held.reset();EXPECT_EQ(b.inspect().used,(Units{1,10}));
    ah.held.reset();EXPECT_EQ(b.inspect().used,(Units{}));
}
TEST(ObserverDeliveryStore, OwningStructuredInputReservesBeforeCopyAndKeepsOriginalBytesAcrossLaterMutation) {
    using Plan=lattice::detail::observer_delivery_plan;
    TempDB file("observer_delivery_plan");lattice::database writer(file.str());writer.execute("CREATE TABLE Rows(value TEXT)");
    auto store=Store::bind(writer,{1,2,{1,8192},{1,8192}});auto client=store.register_client().value;
    std::string source("old\0value",9);std::array<Plan::field_view,1> fields{{{"value",std::string_view(source)}}};
    std::array<Plan::row_view,1> input{{{"Rows",fields}}};
    auto prepared=Plan::prepare(client,{1,128,4096},Store::clock::now()+std::chrono::seconds(1),input);
    ASSERT_EQ(prepared.code,Status::admitted);ASSERT_TRUE(prepared.plan.valid());
    EXPECT_EQ(store.inspect().used,(Units{1,4224}));
    source.assign("later"); // no borrowed text survives the synchronous call
    ASSERT_EQ(prepared.plan.rows().size(),1u);ASSERT_EQ(prepared.plan.rows()[0].fields.size(),1u);
    EXPECT_EQ(std::get<std::string>(prepared.plan.rows()[0].fields[0].contents),std::string("old\0value",9));
    EXPECT_EQ(prepared.plan.rows()[0].insert_sql,"INSERT INTO \"Rows\"(\"value\") VALUES(?)");
    EXPECT_EQ(prepared.plan.rows()[0].bind_bytes,10u);
    EXPECT_EQ(prepared.plan.rows()[0].largest_value_bytes,10u);
    auto tail=prepared.plan;prepared.plan={};EXPECT_EQ(store.inspect().used,(Units{1,4224}));
    tail={};EXPECT_EQ(store.inspect().used,(Units{}));
}
TEST(ObserverDeliveryStore, OversizedStructuredInputRefusesBeforeAnyCreditOrSqlEffectAndLeavesAllOffers) {
    using Plan=lattice::detail::observer_delivery_plan;
    TempDB file("observer_delivery_plan_refusal");lattice::database writer(file.str());writer.execute("CREATE TABLE Rows(value TEXT)");
    auto store=Store::bind(writer,{1,1,{1,8192},{}});auto client=store.register_client().value;
    const std::string source(8192,'x');std::array<Plan::field_view,1> fields{{{"value",std::string_view(source)}}};
    std::array<Plan::row_view,1> input{{{"Rows",fields}}};
    auto refused=Plan::prepare(client,{1,128,4096},Store::clock::now()+std::chrono::seconds(1),input);
    EXPECT_EQ(refused.code,Status::oversized);EXPECT_FALSE(refused.plan.valid());
    EXPECT_EQ(store.inspect().used,(Units{}));EXPECT_EQ(source.size(),8192u);
    EXPECT_TRUE(writer.query("SELECT value FROM Rows").empty());
}
TEST(ObserverDeliveryStore, ActualAdmissionWaitRecountsChangedBorrowedExtentBeforeOwningCopy) {
    using Plan=lattice::detail::observer_delivery_plan;
    TempDB file("observer_delivery_changed_during_admission");
    lattice::database writer(file.str());writer.execute("CREATE TABLE Rows(value TEXT)");
    auto store=Store::bind(writer,{1,1,{1,8192},{}});auto client=store.register_client().value;
    std::string source="small";std::array<Plan::field_view,1> fields{{{"value",std::string_view(source)}}};
    const std::array<Plan::row_view,1> input{{{"Rows",fields}}};
    auto blocking=client.try_acquire({1,8192});ASSERT_EQ(blocking.code,Status::admitted);
    std::atomic<bool> done{false};std::exception_ptr error;Plan::result result;std::thread worker;
    const auto ending=[&]()noexcept {
        blocking.held.reset();
        if(!worker.joinable())return;
        const auto end=Store::clock::now()+std::chrono::seconds(3);
        while(!done.load(std::memory_order_acquire)&&Store::clock::now()<end)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        if(!done.load(std::memory_order_acquire)) {
            std::fprintf(stderr,"delivery-plan-worker-ending-unknown pid=%ld store=%s\n",long(::getpid()),file.str().c_str());
            ::_exit(94); // Preserve actual owner/namespace; no guessed retirement.
        }
        try{worker.join();}catch(...) {
            std::fprintf(stderr,"delivery-plan-worker-join-debt pid=%ld store=%s\n",long(::getpid()),file.str().c_str());
            ::_exit(94);
        }
    };
    struct Guard{const decltype(ending)& end;~Guard()noexcept{end();}} guard{ending};
    worker=std::thread([&]{
        try{result=Plan::prepare(client,{1,128,4096},Store::clock::now()+std::chrono::seconds(2),input);}
        catch(...){error=std::current_exception();}
        done.store(true,std::memory_order_release);
    });
    const auto admission_end=Store::clock::now()+std::chrono::seconds(1);
    while(store.inspect().waiters!=1&&!done.load(std::memory_order_acquire)&&Store::clock::now()<admission_end)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    const bool waiting=store.inspect().waiters==1;
    if(waiting) {
        // Intentional adversarial violation of immutable-borrow semantics,
        // synchronized ONLY while the actual domain waiter cannot read input.
        // Publication precedes the same domain-mutex release of its blocker.
        source.assign(8192,'x');fields[0].contents=std::string_view(source);
    }
    ending();ASSERT_TRUE(waiting);ASSERT_TRUE(error);
    EXPECT_THROW(std::rethrow_exception(error),std::length_error);
    EXPECT_FALSE(result.plan.valid());EXPECT_EQ(store.inspect().used,(Units{}));
    EXPECT_TRUE(writer.query("SELECT value FROM Rows").empty());
}
#endif

TEST(ObserverDeliveryMeter, DeclarationArithmeticNeverPromotesOrWraps) {
    EXPECT_THROW(Meter::admission_charge({0,1,1}),std::length_error);
    EXPECT_THROW(Meter::admission_charge({1,0,1}),std::length_error);
    EXPECT_THROW(Meter::admission_charge({1,1,0}),std::length_error);
    EXPECT_THROW(Meter::admission_charge({1,Meter::integer_ceiling,1}),std::length_error);
    EXPECT_THROW(Meter::admission_charge({UINT64_MAX,1,1}),std::length_error);
    EXPECT_EQ(Meter::admission_charge({3,20,10}),(Units{3,30}));
    EXPECT_EQ(Meter::admission_charge({Meter::integer_ceiling,1,Meter::integer_ceiling-1}),
              (Units{Meter::integer_ceiling,Meter::integer_ceiling}));
}
TEST(ObserverDeliveryMeter, ActualUtf8NulControlAndQuoteBytesAreChargedWithoutBuildingJson) {
    const std::string value("\xc3\xa9\0\n\"\\",6);
    EXPECT_EQ(Meter::json_string_bytes(value),20u);
    TempDB file("observer_delivery_meter_values");lattice::database db(file.str());
    // Real SQLite value conversion and byte length, including embedded NUL.
    // This standalone value test does not enroll raw access into bounded mode.
    sqlite3_stmt* raw=nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(db.handle(),"SELECT ?",-1,&raw,nullptr),SQLITE_OK);
    std::unique_ptr<sqlite3_stmt,decltype(&sqlite3_finalize)> statement(raw,sqlite3_finalize);
    ASSERT_EQ(sqlite3_bind_text64(raw,1,value.data(),value.size(),SQLITE_TRANSIENT,SQLITE_UTF8),SQLITE_OK);
    ASSERT_EQ(sqlite3_step(raw),SQLITE_ROW);
    std::array<std::string_view,1> names{"v"};std::array<sqlite3_value*,1> values{sqlite3_column_value(raw,0)};
    EXPECT_EQ(Meter::row_bytes("Rows",names,values),159u);
}
TEST(ObserverDeliveryMeter, ActualSqlStorageTypesHaveConservativeDistinctByteCharges) {
    TempDB file("observer_delivery_meter_types");lattice::database db(file.str());sqlite3_stmt* raw=nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(db.handle(),"SELECT NULL,-9223372036854775808,1.25,x'00ff'",-1,&raw,nullptr),SQLITE_OK);
    std::unique_ptr<sqlite3_stmt,decltype(&sqlite3_finalize)> statement(raw,sqlite3_finalize);
    ASSERT_EQ(sqlite3_step(raw),SQLITE_ROW);
    std::array<std::string_view,1> names{"v"};
    const std::array<uint64_t,4> payload{4,21,32,6};
    for(size_t i=0;i<payload.size();++i){std::array<sqlite3_value*,1> values{sqlite3_column_value(raw,static_cast<int>(i))};
        EXPECT_EQ(Meter::row_bytes("Rows",names,values),139+payload[i]);}
}
TEST(ObserverDeliveryMeter, StrictUtf8ProfileRejectsConversionsAndInvalidScalarsBeforeInputCopies) {
    EXPECT_TRUE(Meter::valid_utf8(std::string("a\0b",3)));
    EXPECT_TRUE(Meter::valid_utf8("\xc3\xa9\xe2\x82\xac\xf0\x9f\x92\xa9"));
    for(const auto& invalid:std::vector<std::string>{"\x80","\xc0\x80","\xe0\x80\x80","\xed\xa0\x80","\xf4\x90\x80\x80","\xf5\x80\x80\x80","\xe2\x82"})
        EXPECT_FALSE(Meter::valid_utf8(invalid));
#if defined(__APPLE__) || defined(__linux__)
    using Plan=lattice::detail::observer_delivery_plan;
    TempDB file("observer_delivery_invalid_utf8");lattice::database db(file.str());db.execute("CREATE TABLE Rows(value TEXT)");
    auto store=Store::bind(db,{1,1,{1,8192},{}});auto client=store.register_client().value;
    const std::string invalid("\xc0\x80",2);
    const std::array<Plan::field_view,1> fields{{{"value",std::string_view(invalid)}}};
    const std::array<Plan::row_view,1> rows{{{"Rows",fields}}};
    EXPECT_THROW(Plan::prepare(client,{1,128,4096},Store::clock::now()+std::chrono::seconds(1),rows),std::invalid_argument);
    EXPECT_EQ(store.inspect().used,(Units{}));EXPECT_TRUE(db.query("SELECT value FROM Rows").empty());
#endif
}
TEST(ObserverDeliveryMeter, WideActualCatalogProgramsStayWithinHistoricalFunctionArgumentCeiling) {
    lattice::detail::observer_delivery_meter_table table{"Rows",{}};
    for(size_t i=0;i<128;++i)table.columns.push_back({"field"+std::to_string(i)});
    const auto programs=Meter::programs({table});ASSERT_EQ(programs.size(),3u);
    for(const auto& program:programs) {
        // Inspect the generated closed row-cost calls, whose arguments contain
        // no nested expression. All128 actual columns must appear exactly in
        // each complete row-version cost; no property was dropped to fit121.
        size_t at=0,calls=0;const std::string prefix="lattice_observer_delivery_row_bytes_v1(";
        while((at=program.sql.find(prefix,at))!=std::string::npos) {
            const auto end=program.sql.find(')',at);ASSERT_NE(end,std::string::npos);
            const auto body=program.sql.substr(at+prefix.size(),end-at-prefix.size());
            EXPECT_LE(static_cast<size_t>(std::count(body.begin(),body.end(),','))+1,121u);
            ++calls;at=end+1;
        }
        // Row expression occurs in SET and the two fail-closed WHERE checks;
        // UPDATE accounts both OLD and NEW on each occurrence.
        EXPECT_EQ(calls,program.sql.find(" BEFORE UPDATE ")!=std::string::npos?18u:9u);
        EXPECT_NE(program.sql.find("\"field127\""),std::string::npos);
    }
}
TEST(ObserverDeliveryMeter, ActualInvalidTextAndNonfiniteRealRefuseWithoutAJsonProducer) {
    TempDB file("observer_delivery_meter_invalid_values");lattice::database db(file.str());
    sqlite3_stmt* raw=nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(db.handle(),"SELECT ?,1e999",-1,&raw,nullptr),SQLITE_OK);
    std::unique_ptr<sqlite3_stmt,decltype(&sqlite3_finalize)> statement(raw,sqlite3_finalize);
    const char malformed[]{char(0xc0),char(0x80)};
    ASSERT_EQ(sqlite3_bind_text64(raw,1,malformed,sizeof(malformed),SQLITE_TRANSIENT,SQLITE_UTF8),SQLITE_OK);
    ASSERT_EQ(sqlite3_step(raw),SQLITE_ROW);
    ASSERT_EQ(sqlite3_column_type(raw,0),SQLITE_TEXT);ASSERT_EQ(sqlite3_column_type(raw,1),SQLITE_FLOAT);
    const std::array<std::string_view,1> names{"value"};
    for(int index:{0,1}) {
        const std::array<sqlite3_value*,1> values{sqlite3_column_value(raw,index)};
        EXPECT_THROW(Meter::row_bytes("Rows",names,values),std::length_error);
    }
}
TEST(ObserverDeliveryMeter, SQLiteCaseAliasesAndReservedNameVariantsCannotMintAnotherBundle) {
    using Table=lattice::detail::observer_delivery_meter_table;
    EXPECT_THROW(Meter::programs({Table{"Rows",{{"value"}}},Table{"rows",{{"value"}}}}),std::length_error);
    EXPECT_THROW(Meter::programs({Table{"Rows",{{"Value"},{"value"}}}}),std::length_error);
    for(const char* table:{"SQLITE_sequence","_LATTICE_OBSERVER_DELIVERY_meter_v1","_Lattice_Observer_Delivery_other"})
        EXPECT_THROW(Meter::programs({Table{table,{{"value"}}}}),std::length_error);
    // Different exact admitted names remain a complete deterministic bundle.
    const auto bundle=Meter::programs({Table{"Zed",{{"value"}}},Table{"Alpha",{{"value"}}}});
    ASSERT_EQ(bundle.size(),6u);EXPECT_EQ(bundle.front().table,"Alpha");EXPECT_EQ(bundle.back().table,"Zed");
}

}

#if defined(__APPLE__) || defined(__linux__)
TEST(ObserverDeliveryStore, ActualDataOnlyTailKeepsOneDomainAcrossAllNativeOwnerRetirementAndReopen) {
    TempDB file("observer_delivery_data_anchor_retired_native");Store::lease tail;
    {
        lattice::database writer(file.str());writer.execute("CREATE TABLE Rows(value INTEGER)");
        auto store=Store::bind(writer,{1,2,{1,10},{1,10}});
        auto client=store.register_client();ASSERT_EQ(client.code,Status::admitted);
        auto held=client.value.try_acquire({2,20});ASSERT_EQ(held.code,Status::admitted);
        tail=held.held;held.held.reset();client.value.reset();
    } // Actual database/Store/client wrappers have ended; only DATA lease remains.
    ASSERT_TRUE(tail.valid());
    lattice::database reopened(file.str());
    auto same=Store::bind(reopened,{1,2,{1,10},{1,10}});
    EXPECT_TRUE(same.current_for(reopened));EXPECT_EQ(same.inspect().used,(Units{2,20}));
    EXPECT_EQ(same.inspect().clients,1u);EXPECT_EQ(same.inspect().leases,1u);
    EXPECT_EQ(same.register_client().code,Status::client_capacity);
    const Store::domain::limits different{1,2,{1,10},{2,10}};
    EXPECT_THROW(Store::bind(reopened,different),std::runtime_error);
    auto copied=tail;tail.reset();EXPECT_EQ(same.inspect().used,(Units{2,20}));
    copied.reset();EXPECT_EQ(same.inspect().used,(Units{}));EXPECT_EQ(same.inspect().leases,0u);
    EXPECT_EQ(same.register_client().code,Status::admitted);
}
#endif
