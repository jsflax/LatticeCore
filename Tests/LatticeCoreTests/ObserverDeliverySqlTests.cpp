#include "TestHelpers.hpp"
#include "FixtureCheckedOwnerEnding.hpp"
#include "ObserverDeliveryRegistrationObservation.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_meter.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_original.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_plan.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_sql_runtime.hpp"
#include <deque>

namespace lattice::detail {
// Test-only entry into the real mechanism. These controls deliberately do not
// stand in for the unfinished complete catalog/enrollment/closed producer API.
// The scope validates the genuine current Core original and actual statement;
// it never sets a BEGIN, ending, capture, refund or permission outcome.
struct observer_delivery_sql_test_access {
    using Runtime=observer_delivery_sql_runtime;
    static std::shared_ptr<Runtime> install(database& writer){
        return observer_delivery_registration_testing::observe([&]{return Runtime::install(writer);});
    }
    static auto prepare(Runtime& runtime,database& writer,const observer_delivery_plan& input) {
        return runtime.prepare(writer,input);
    }
    static void begin_explicit(lattice_db& owner,observer_delivery_store::lease credit,
        observer_delivery_meter_budget budget) {
        // Actual pre-admitted test entry; only the genuine public BEGIN can
        // claim this pure DATA offer. No issued/ending/outcome bit is set.
        observer_delivery_original::begin_offer offer(owner.db(),std::move(credit),budget,{},true);
        owner.begin_transaction();
    }
    static auto prepare_borrowed(database& writer,const observer_delivery_store::client& client,
        observer_delivery_meter_budget budget,std::span<const observer_delivery_plan::row_view> input) {
        std::shared_ptr<const observer_delivery_original> original;
        {
            database::catalog_native_admission_ admitted(writer);
            const auto& actual=writer.core_transaction_original_;
            if(!actual||!actual->current_for(writer)||!actual->observer_delivery)
                throw db_error("fixture requires actual issued caller original");
            original=actual->observer_delivery; // DATA only, no Native original retained
        }
        return observer_delivery_plan::prepare_insert(client,budget,input,original);
    }
    static auto update(database& writer,const observer_delivery_store::client& client,
        observer_delivery_meter_budget budget,int64_t rowid,std::string_view gid,size_t kind,uint64_t maximum,
        bool borrow=false) {
        std::shared_ptr<const observer_delivery_original> original;
        if(borrow) {
            database::catalog_native_admission_ admitted(writer);
            const auto& actual=writer.core_transaction_original_;
            if(!actual||!actual->current_for(writer)||!actual->observer_delivery)
                throw db_error("fixture requires actual current UPDATE original");
            original=actual->observer_delivery;
        }
        return observer_delivery_plan::prepare_update(client,budget,
            observer_delivery_store::clock::now()+std::chrono::seconds(1),
            "TestPerson","name",rowid,gid,kind,maximum,original);
    }
    static void supply(const observer_delivery_plan& plan,const observer_delivery_plan::value_view& value){plan.supply_update(value);}
    static bool row_present(const observer_delivery_plan& plan){return plan.update_row_present();}
    static void execute(Runtime& runtime,database& writer,const char* sql,
                        const std::vector<column_value_t>& parameters={}) {
        auto* native=runtime.native(writer);sqlite3_stmt* raw=nullptr;
        int rc=sqlite3_prepare_v2(native,sql,-1,&raw,nullptr);
        std::unique_ptr<sqlite3_stmt,decltype(&database::finalize_mutation_statement_)>
            statement(raw,database::finalize_mutation_statement_);
        if(rc!=SQLITE_OK||!raw)throw db_error("real meter control prepare failed");
        try {
            Runtime::statement_scope admitted(runtime,writer,raw);
            if(sqlite3_bind_parameter_count(raw)!=int(parameters.size()))throw db_error("meter fixture parameter count");
            for(size_t i=0;i<parameters.size();++i) {
                rc=writer.bind_value_checked_(raw,int(i+1),parameters[i]);
                if(rc!=SQLITE_OK)throw db_error("real meter control bind failed");
            }
            rc=writer.step_statement_(raw);
            if(rc!=SQLITE_DONE)throw db_error(std::string("real meter control step: ")+sqlite3_errmsg(native));
        }catch(...) {
            const auto first=std::current_exception();
            database::finalize_failed_mutation_statement_(statement.release(),rc,first);
            std::rethrow_exception(first);
        }
        const int ended=database::finalize_mutation_statement_(statement.release());
        if(ended!=SQLITE_OK)throw db_error("real meter control finalize failed");
    }
};
}
namespace {
using Store=lattice::detail::observer_delivery_store;
using Original=lattice::detail::observer_delivery_original;
using Meter=lattice::detail::observer_delivery_meter;
using Access=lattice::detail::observer_delivery_sql_test_access;
using Status=Store::status;
using Units=Store::units;
class SqlMeterQueue final:public lattice::scheduler {
public:
    std::deque<std::function<void()>> pending;
    void invoke(std::function<void()>&& work)override{pending.push_back(std::move(work));}
    bool is_on_thread()const noexcept override{return true;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return true;}
    void drain(){while(!pending.empty()){auto work=std::move(pending.front());pending.pop_front();work();}}
};
lattice::detail::recovery_owner_schema sql_meter_declarations() {
    const auto* actual=lattice::schema_registry::instance().get_schema(typeid(TestPerson));
    lattice::detail::recovery_owner_schema catalog;
    if(!actual||!catalog.admit_model(*actual))throw std::runtime_error("actual meter fixture declaration unavailable");
    return catalog;
}
class SqlMeterOwner final:public lattice::lattice_db {
public:
    using lattice_db::lattice_db;using lattice_db::with_mutation_operation;
    explicit SqlMeterOwner(const lattice::configuration& config):lattice_db(config,false,sql_meter_declarations()){}
};
struct SqlMeterFixture {
    TempDB file{"observer_delivery_sql"};
    std::shared_ptr<SqlMeterQueue> queue=std::make_shared<SqlMeterQueue>();
    std::shared_ptr<SqlMeterOwner> owner;
    Store store;Store::client client;
    std::shared_ptr<lattice::detail::observer_delivery_sql_runtime> runtime;
    explicit SqlMeterFixture(bool corrupt_count=false) {
        lattice::configuration config(file.str());config.sched=queue;config.audit_retention_seconds=0;
        owner=std::make_shared<SqlMeterOwner>(config);
        lattice::instance_registry::instance().get_or_create_notifier(file.str())->stop_listening();queue->drain();
        // Force the normal production model/table/audit setup, then remove
        // only this fixture's seed before installing any new guard.
        {auto seed=owner->add(TestPerson{"seed",0,std::nullopt});owner->remove(seed);}
        owner->db().execute("DELETE FROM AuditLog WHERE tableName='TestPerson'");queue->drain();
        // Two known genuine tables isolate row/ABORT mechanics. A future
        // production factory must regenerate+validate the WHOLE catalog and
        // refresh exact fingerprints atomically; this helper claims neither.
        std::vector<lattice::detail::observer_delivery_meter_table> tables{
            {"TestPerson",{{"id"},{"globalId"},{"name"},{"age"},{"email"}}},
            {"AuditLog",{{"id"},{"globalId"},{"tableName"},{"operation"},{"rowId"},{"globalRowId"},
                {"changedFields"},{"changedFieldsNames"},{"isFromRemote"},{"isSynchronized"},{"timestamp"},{"synthesized"}}}};
        owner->with_mutation_operation([&](lattice::database& writer){
            writer.execute(std::string(Meter::ledger_ddl()));
            writer.execute("INSERT INTO main._lattice_observer_delivery_meter_v1 VALUES(1,zeroblob(32),0,0)");
            for(const auto& program:Meter::programs(tables))writer.execute(program.sql);
        });queue->drain();
        if(corrupt_count) {
            // Actual stored malformed state; bypass CHECK only to build the
            // adversarial fixture, before runtime installation. No outcome or
            // permission is fabricated and ordinary mode cannot do this later.
            owner->db().execute("PRAGMA ignore_check_constraints=ON");
            owner->db().execute("UPDATE main._lattice_observer_delivery_meter_v1 SET events=1.5");
            owner->db().execute("PRAGMA ignore_check_constraints=OFF");queue->drain();
        }
        store=Store::bind(owner->db(),{1,4,{4,65536},{4,65536}});client=store.register_client().value;
        runtime=Access::install(owner->db());
    }
    ~SqlMeterFixture(){if(owner)lattice::detail::end_fixture_owner_checked(owner.get(),"observer-delivery-sql");}
    int64_t count()const {
        const auto rows=owner->db().query("SELECT events FROM main._lattice_observer_delivery_meter_v1");
        if(rows.size()!=1)throw std::runtime_error("actual ledger row missing");return std::get<int64_t>(rows[0].at("events"));
    }
    void insert(const std::string& name) {
        Access::execute(*runtime,owner->db(),"INSERT INTO TestPerson(name,age,email) VALUES(?,7,NULL)",{name});
    }
};
#if defined(__APPLE__) || defined(__linux__)
TEST(ObserverDeliverySql, RealModelAndGeneratedAuditConsumeTwoEventsWithoutRecursiveLedgerOffers) {
    size_t batches=0,events=0;
    SqlMeterFixture fixture;
    const auto token=fixture.owner->add_table_observer("TestPerson",[&](const auto& batch){++batches;events+=batch.size();});
    {
        auto held=fixture.client.try_acquire({2,32768});ASSERT_EQ(held.code,Status::admitted);
        Original::begin_offer offer(fixture.owner->db(),held.held,{2,8192,24576});held.held.reset();
        fixture.owner->with_mutation_operation([&](lattice::database&){fixture.insert("real model and audit");});
        EXPECT_EQ(fixture.count(),2);EXPECT_EQ(batches,0u);ASSERT_EQ(fixture.queue->pending.size(),1u);
    }
    fixture.owner->remove_table_observer("TestPerson",token);
    EXPECT_EQ(fixture.store.inspect().used,(Units{2,32768}));fixture.queue->drain();
    EXPECT_EQ(batches,1u);EXPECT_EQ(events,1u);EXPECT_EQ(fixture.store.inspect().used,(Units{}));
    EXPECT_EQ(fixture.owner->db().query("SELECT id FROM TestPerson").size(),1u);
}
TEST(ObserverDeliverySql, ActualSavepointBudgetAbortRestoresSameCallerPrefixAndLedger) {
    SqlMeterFixture fixture;
    auto held=fixture.client.try_acquire({2,32768});ASSERT_EQ(held.code,Status::admitted);
    {
        Original::begin_offer offer(fixture.owner->db(),held.held,{2,8192,24576});held.held.reset();
        fixture.owner->begin_transaction();
        fixture.owner->with_mutation_operation([&](lattice::database&){fixture.insert("prefix");});
        ASSERT_EQ(fixture.count(),2);
        EXPECT_THROW(fixture.owner->with_mutation_operation([&](lattice::database&){fixture.insert("over declaration");}),lattice::db_error);
        EXPECT_TRUE(fixture.owner->db().is_in_transaction());EXPECT_EQ(fixture.count(),2);
        const auto rows=fixture.owner->db().query("SELECT name FROM TestPerson");ASSERT_EQ(rows.size(),1u);
        EXPECT_EQ(std::get<std::string>(rows[0].at("name")),"prefix");fixture.owner->commit();
    }
    fixture.queue->drain();EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}
TEST(ObserverDeliverySql, CatchingActualRowAbortCannotCommitAnAcceptedPrefixOfThatUnit) {
    SqlMeterFixture fixture;
    auto held=fixture.client.try_acquire({2,32768});ASSERT_EQ(held.code,Status::admitted);
    {
        Original::begin_offer offer(fixture.owner->db(),held.held,{2,8192,24576});held.held.reset();
        EXPECT_THROW(fixture.owner->with_mutation_operation([&](lattice::database&){
            fixture.insert("would be prefix");
            try{fixture.insert("actual ABORT");}catch(const lattice::db_error&){}
        }),lattice::db_error);
        EXPECT_FALSE(fixture.owner->db().is_in_transaction());
        EXPECT_EQ(fixture.count(),0);EXPECT_TRUE(fixture.owner->db().query("SELECT name FROM TestPerson").empty());
    }
    EXPECT_TRUE(fixture.queue->pending.empty());EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}
TEST(ObserverDeliverySql, PublicPreparedDirectSqlAndRawBeginRefuseButActualDiagnosticReadsRemain) {
    SqlMeterFixture fixture;
    EXPECT_THROW(fixture.owner->db().begin_transaction(),lattice::db_error);
    EXPECT_THROW(fixture.owner->db().handle(),lattice::db_error);
    EXPECT_THROW(fixture.owner->db().execute("UPDATE main._lattice_observer_delivery_meter_v1 SET events=0"),lattice::db_error);
    EXPECT_THROW(fixture.owner->db().execute("INSERT INTO TestPerson(name,age) VALUES(?,7)",{std::string("unsupported")}),lattice::db_error);
    EXPECT_THROW(fixture.owner->db().query("INSERT INTO TestPerson(name,age) VALUES('unsupported',7) RETURNING id"),lattice::db_error);
    EXPECT_FALSE(fixture.owner->db().is_in_transaction());EXPECT_EQ(fixture.count(),0);
    const auto diagnostic=fixture.owner->db().query("SELECT 7 AS value");ASSERT_EQ(diagnostic.size(),1u);
    EXPECT_EQ(std::get<int64_t>(diagnostic[0].at("value")),7);
}
TEST(ObserverDeliverySql, ActualByteRefusalHappensBeforeAuditJsonAndRollsBackItsModelRow) {
    const std::string supplied(8192,'x');SqlMeterFixture fixture;
    auto held=fixture.client.try_acquire({2,33024});ASSERT_EQ(held.code,Status::admitted);
    {
        Original::begin_offer offer(fixture.owner->db(),held.held,{2,256,32768});held.held.reset();
        EXPECT_THROW(fixture.owner->with_mutation_operation([&](lattice::database&){fixture.insert(supplied);}),lattice::db_error);
        EXPECT_EQ(fixture.count(),0);EXPECT_TRUE(fixture.owner->db().query("SELECT name FROM TestPerson").empty());
        EXPECT_TRUE(fixture.owner->db().query("SELECT id FROM AuditLog WHERE tableName='TestPerson'").empty());
    }
    EXPECT_EQ(supplied.size(),8192u);EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}
TEST(ObserverDeliverySql, RealStoredNonintegerCounterCannotPromoteOrResetIntoAnAdmission) {
    SqlMeterFixture fixture(true);
    auto held=fixture.client.try_acquire({2,32768});ASSERT_EQ(held.code,Status::admitted);
    {
        Original::begin_offer offer(fixture.owner->db(),held.held,{2,8192,24576});held.held.reset();
        EXPECT_THROW(fixture.owner->with_mutation_operation([&](lattice::database&){fixture.insert("must refuse corrupt ledger");}),lattice::db_error);
        const auto rows=fixture.owner->db().query("SELECT typeof(events) AS kind,events FROM main._lattice_observer_delivery_meter_v1");
        ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::string>(rows[0].at("kind")),"real");
        EXPECT_DOUBLE_EQ(std::get<double>(rows[0].at("events")),1.5);
        EXPECT_TRUE(fixture.owner->db().query("SELECT id FROM TestPerson").empty());
    }
}
TEST(ObserverDeliverySql, IndependentRawWriterCannotExecuteMeterWithoutConnectionIssuedFunctions) {
    SqlMeterFixture fixture;sqlite3* raw=nullptr;
    ASSERT_EQ(sqlite3_open(fixture.file.str().c_str(),&raw),SQLITE_OK);
    std::unique_ptr<sqlite3,decltype(&sqlite3_close)> other(raw,sqlite3_close);
    // Satisfy the pre-existing ordinary audit function with this fixture's
    // actual disabled=0 result. The refusal must name the new meter namespace,
    // rather than pass merely because an old audit helper was absent.
    ASSERT_EQ(sqlite3_create_function_v2(raw,"sync_disabled",0,SQLITE_UTF8,nullptr,
        [](sqlite3_context* context,int,sqlite3_value**){sqlite3_result_int(context,0);},nullptr,nullptr,nullptr),SQLITE_OK);
    char* error=nullptr;
    const int rc=sqlite3_exec(raw,"INSERT INTO TestPerson(name,age) VALUES('foreign writer',7)",nullptr,nullptr,&error);
    const std::unique_ptr<char,decltype(&sqlite3_free)> diagnostic(error,sqlite3_free);
    EXPECT_NE(rc,SQLITE_OK);ASSERT_NE(error,nullptr);
    EXPECT_NE(std::string(error).find("lattice_observer_delivery_"),std::string::npos);
    EXPECT_TRUE(sqlite3_get_autocommit(raw));
    EXPECT_TRUE(fixture.owner->db().query("SELECT id FROM TestPerson").empty());EXPECT_EQ(fixture.count(),0);
}
TEST(ObserverDeliverySql, ActualGeneratedStaticBindsKeepAdmittedNulInputUntilPreparedTailRetires) {
    using Plan=lattice::detail::observer_delivery_plan;
    std::string name("old\0value",9);SqlMeterFixture fixture;
    const std::array<Plan::field_view,3> fields{{{"name",std::string_view(name)},{"age",int64_t(7)},{"email",nullptr}}};
    const std::array<Plan::row_view,1> rows{{{"TestPerson",fields}}};
    auto input=Plan::prepare(fixture.client,{2,8192,24576},Store::clock::now()+std::chrono::seconds(1),rows);
    ASSERT_EQ(input.code,Status::admitted);
    {
        auto prepared=Access::prepare(*fixture.runtime,fixture.owner->db(),input.plan);
        EXPECT_THROW(Access::prepare(*fixture.runtime,fixture.owner->db(),input.plan),std::runtime_error);
        name="later caller value";
        {
            Original::begin_offer offer(fixture.owner->db(),input.plan);
            fixture.owner->with_mutation_operation([&](lattice::database& writer){prepared.execute(writer);});
        }
        const auto actual=fixture.owner->db().query("SELECT name FROM TestPerson");ASSERT_EQ(actual.size(),1u);
        EXPECT_EQ(std::get<std::string>(actual[0].at("name")),std::string("old\0value",9));
        EXPECT_EQ(fixture.count(),2);fixture.queue->drain();input.plan={};
        EXPECT_EQ(fixture.store.inspect().used,(Units{2,32768}))<<"STATIC binds/input retire after actual finalize and final prepared owner";
    }
    EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}
TEST(ObserverDeliverySql, EqualPhysicalBudgetDoesNotLetDifferentInputUseAnIssuedOriginal) {
    using Plan=lattice::detail::observer_delivery_plan;
    SqlMeterFixture fixture;
    const std::array<Plan::field_view,2> fields{{{"name",std::string_view("same values")},{"age",int64_t(7)}}};
    const std::array<Plan::row_view,1> rows{{{"TestPerson",fields}}};
    {
        auto first=Plan::prepare(fixture.client,{2,8192,24576},Store::clock::now()+std::chrono::seconds(1),rows);
        auto other=Plan::prepare(fixture.client,{2,8192,24576},Store::clock::now()+std::chrono::seconds(1),rows);
        ASSERT_EQ(first.code,Status::admitted);ASSERT_EQ(other.code,Status::admitted);
        auto prepared=Access::prepare(*fixture.runtime,fixture.owner->db(),other.plan);
        Original::begin_offer offer(fixture.owner->db(),first.plan);
        EXPECT_THROW(fixture.owner->with_mutation_operation([&](lattice::database& writer){prepared.execute(writer);}),lattice::db_error);
        EXPECT_EQ(fixture.count(),0);EXPECT_TRUE(fixture.owner->db().query("SELECT id FROM TestPerson").empty());
        EXPECT_EQ(fixture.store.inspect().used,(Units{4,65536}));
    }
    EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}

TEST(ObserverDeliverySql, ExplicitBeginSubplansUseOneOriginalLeaseThroughRealCommitAndQueuedTail) {
    using Plan=lattice::detail::observer_delivery_plan;
    size_t calls=0;SqlMeterFixture fixture;
    const auto token=fixture.owner->add_table_observer("TestPerson",[&](const auto& rows){calls+=rows.size();});
    const std::array<Plan::field_view,3> fields{{{"name",std::string_view("actual caller")},{"age",int64_t(7)},{"email",nullptr}}};
    const std::array<Plan::row_view,1> rows{{{"TestPerson",fields}}};
    const lattice::detail::observer_delivery_meter_budget budget{4,8192,24576};
    {
        auto admitted=fixture.client.try_acquire({4,32768});ASSERT_EQ(admitted.code,Status::admitted);
        Access::begin_explicit(*fixture.owner,std::move(admitted.held),budget);
        ASSERT_TRUE(fixture.owner->db().is_in_transaction());EXPECT_EQ(fixture.store.inspect().used,(Units{4,32768}));
        auto first=Access::prepare_borrowed(fixture.owner->db(),fixture.client,budget,rows);
        auto second=Access::prepare_borrowed(fixture.owner->db(),fixture.client,budget,rows);
        ASSERT_EQ(first.code,Status::admitted);ASSERT_EQ(second.code,Status::admitted);
        auto one=Access::prepare(*fixture.runtime,fixture.owner->db(),first.plan);
        auto two=Access::prepare(*fixture.runtime,fixture.owner->db(),second.plan);
        fixture.owner->with_mutation_operation([&](lattice::database& writer){one.execute(writer);});
        fixture.owner->with_mutation_operation([&](lattice::database& writer){two.execute(writer);});
        EXPECT_EQ(fixture.store.inspect().used,(Units{4,32768}));EXPECT_EQ(fixture.count(),4);
        fixture.owner->commit();EXPECT_FALSE(fixture.owner->db().is_in_transaction());
        EXPECT_EQ(fixture.owner->db().query("SELECT id FROM TestPerson").size(),2u);EXPECT_EQ(calls,0u);
        ASSERT_EQ(fixture.queue->pending.size(),1u);
    }
    fixture.owner->remove_table_observer("TestPerson",token);
    EXPECT_EQ(fixture.store.inspect().used,(Units{4,32768}));fixture.queue->drain();
    EXPECT_EQ(calls,2u);EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}
TEST(ObserverDeliverySql, ExplicitSubplanAbortRestoresActualCallerPrefixWithoutRenewingLease) {
    using Plan=lattice::detail::observer_delivery_plan;
    SqlMeterFixture fixture;
    const std::array<Plan::field_view,3> fields{{{"name",std::string_view("preserved caller")},{"age",int64_t(7)},{"email",nullptr}}};
    const std::array<Plan::row_view,1> rows{{{"TestPerson",fields}}};
    const lattice::detail::observer_delivery_meter_budget budget{2,8192,24576};
    {
        auto admitted=fixture.client.try_acquire({2,32768});ASSERT_EQ(admitted.code,Status::admitted);
        Access::begin_explicit(*fixture.owner,std::move(admitted.held),budget);
        auto first=Access::prepare_borrowed(fixture.owner->db(),fixture.client,budget,rows);
        auto rejected=Access::prepare_borrowed(fixture.owner->db(),fixture.client,budget,rows);
        ASSERT_EQ(first.code,Status::admitted);ASSERT_EQ(rejected.code,Status::admitted);
        auto one=Access::prepare(*fixture.runtime,fixture.owner->db(),first.plan);
        auto two=Access::prepare(*fixture.runtime,fixture.owner->db(),rejected.plan);
        fixture.owner->with_mutation_operation([&](lattice::database& writer){one.execute(writer);});
        EXPECT_THROW(fixture.owner->with_mutation_operation([&](lattice::database& writer){two.execute(writer);}),lattice::db_error);
        EXPECT_TRUE(fixture.owner->db().is_in_transaction());EXPECT_EQ(fixture.count(),2);
        const auto actual=fixture.owner->db().query("SELECT name FROM TestPerson");ASSERT_EQ(actual.size(),1u);
        EXPECT_EQ(std::get<std::string>(actual[0].at("name")),"preserved caller");
        EXPECT_EQ(fixture.store.inspect().used,(Units{2,32768}));fixture.owner->commit();
        fixture.queue->drain();EXPECT_EQ(fixture.store.inspect().used,(Units{2,32768}));
    }
    EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}
TEST(ObserverDeliverySql, PreparedCallerSubplanCannotBorrowSuccessorOriginalAfterRealEnding) {
    using Plan=lattice::detail::observer_delivery_plan;
    SqlMeterFixture fixture;
    const std::array<Plan::field_view,3> fields{{{"name",std::string_view("stale input")},{"age",int64_t(7)},{"email",nullptr}}};
    const std::array<Plan::row_view,1> rows{{{"TestPerson",fields}}};
    const lattice::detail::observer_delivery_meter_budget budget{2,8192,24576};
    {
        auto old=fixture.client.try_acquire({2,32768});ASSERT_EQ(old.code,Status::admitted);
        Access::begin_explicit(*fixture.owner,std::move(old.held),budget);
        auto input=Access::prepare_borrowed(fixture.owner->db(),fixture.client,budget,rows);
        ASSERT_EQ(input.code,Status::admitted);auto stale=Access::prepare(*fixture.runtime,fixture.owner->db(),input.plan);
        fixture.owner->rollback();
        auto next=fixture.client.try_acquire({2,32768});ASSERT_EQ(next.code,Status::admitted);
        Access::begin_explicit(*fixture.owner,std::move(next.held),budget);
        EXPECT_THROW(fixture.owner->with_mutation_operation([&](lattice::database& writer){stale.execute(writer);}),lattice::db_error);
        EXPECT_TRUE(fixture.owner->db().is_in_transaction());EXPECT_EQ(fixture.count(),0);
        EXPECT_TRUE(fixture.owner->db().query("SELECT id FROM TestPerson").empty());fixture.owner->rollback();
    }
    fixture.queue->drain();EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}
TEST(ObserverDeliverySql, DefaultClosedInsertGeneratesOneIdentityWithoutCopyingIgnoredSuppliedIdentity) {
    using Plan=lattice::detail::observer_delivery_plan;
    const std::string ignored(1048576,'x');SqlMeterFixture fixture;
    const std::array<Plan::field_view,4> fields{{{"name",std::string_view("new identity")},{"age",int64_t(7)},
        {"email",nullptr},{"globalId",std::string_view(ignored)}}};
    const std::array<Plan::row_view,1> rows{{{"TestPerson",fields}}};
    const lattice::detail::observer_delivery_meter_budget budget{2,8192,24576};
    {
        auto admitted=fixture.client.try_acquire({2,32768});ASSERT_EQ(admitted.code,Status::admitted);
        Access::begin_explicit(*fixture.owner,std::move(admitted.held),budget);
        auto input=Access::prepare_borrowed(fixture.owner->db(),fixture.client,budget,rows);
        ASSERT_EQ(input.code,Status::admitted);ASSERT_EQ(input.plan.rows().size(),1u);
        const auto& owned=input.plan.rows()[0].fields.back();ASSERT_EQ(owned.name,"globalId");
        const auto identity=std::get<std::string>(owned.contents);ASSERT_EQ(identity.size(),36u);EXPECT_NE(identity,ignored);
        auto prepared=Access::prepare(*fixture.runtime,fixture.owner->db(),input.plan);
        fixture.owner->with_mutation_operation([&](lattice::database& writer){prepared.execute(writer);});fixture.owner->commit();
        const auto actual=fixture.owner->db().query("SELECT globalId FROM TestPerson");ASSERT_EQ(actual.size(),1u);
        EXPECT_EQ(std::get<std::string>(actual[0].at("globalId")),identity);
    }
    fixture.queue->drain();EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}

struct ClosedUpdateSeed {
    int64_t id;std::string gid;
    explicit ClosedUpdateSeed(SqlMeterFixture& fixture) {
        auto held=fixture.client.try_acquire({2,32768});
        if(held.code!=Status::admitted)throw std::runtime_error("actual seed admission failed");
        {
            Original::begin_offer offer(fixture.owner->db(),std::move(held.held),{2,8192,24576});
            fixture.owner->with_mutation_operation([&](lattice::database&){fixture.insert("seed");});
        }
        fixture.queue->drain();
        const auto rows=fixture.owner->db().query("SELECT id,globalId FROM TestPerson");
        if(rows.size()!=1)throw std::runtime_error("actual UPDATE seed missing");
        id=std::get<int64_t>(rows[0].at("id"));gid=std::get<std::string>(rows[0].at("globalId"));
    }
};
TEST(ObserverDeliverySql, UpdateConversionAllowanceOwnsRealCreditBeforeSupplyAndAnySql) {
    SqlMeterFixture fixture;ClosedUpdateSeed seed(fixture);
    {
        auto input=Access::update(fixture.owner->db(),fixture.client,{2,8192,24576},seed.id,seed.gid,3,1024);
        ASSERT_EQ(input.code,Status::admitted);
        EXPECT_EQ(fixture.store.inspect().used,(Units{2,32768}));
        EXPECT_FALSE(fixture.owner->db().is_in_transaction());
        EXPECT_THROW(Access::prepare(*fixture.runtime,fixture.owner->db(),input.plan),std::runtime_error);
        EXPECT_EQ(std::get<std::string>(fixture.owner->db().query("SELECT name FROM TestPerson")[0].at("name")),"seed");
        Access::supply(input.plan,std::string_view("admitted"));
        EXPECT_THROW(Access::supply(input.plan,std::string_view("second")),std::runtime_error);
        auto prepared=Access::prepare(*fixture.runtime,fixture.owner->db(),input.plan);
        Original::begin_offer offer(fixture.owner->db(),input.plan);
        fixture.owner->with_mutation_operation([&](lattice::database& writer){prepared.execute(writer);});
        EXPECT_TRUE(Access::row_present(input.plan));
        EXPECT_EQ(std::get<std::string>(fixture.owner->db().query("SELECT name FROM TestPerson")[0].at("name")),"admitted");
    }
    fixture.queue->drain();EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}
TEST(ObserverDeliverySql, UpdateSupplyGrowthAndInvalidUtf8RefuseBeforeValueCopyOrEffects) {
    SqlMeterFixture fixture;ClosedUpdateSeed seed(fixture);
    {
        auto small=Access::update(fixture.owner->db(),fixture.client,{2,8192,24576},seed.id,seed.gid,3,1);
        ASSERT_EQ(small.code,Status::admitted);
        EXPECT_THROW(Access::supply(small.plan,std::string_view("too long")),std::length_error);
        EXPECT_THROW(Access::supply(small.plan,std::string_view("x")),std::runtime_error);
        EXPECT_EQ(fixture.store.inspect().used,(Units{2,32768}));
    }
    {
        auto invalid=Access::update(fixture.owner->db(),fixture.client,{2,8192,24576},seed.id,seed.gid,3,2);
        ASSERT_EQ(invalid.code,Status::admitted);
        const std::string bytes("\xC0\xAF",2);
        EXPECT_THROW(Access::supply(invalid.plan,std::string_view(bytes)),std::invalid_argument);
        EXPECT_THROW(Access::prepare(*fixture.runtime,fixture.owner->db(),invalid.plan),std::runtime_error);
    }
    EXPECT_EQ(std::get<std::string>(fixture.owner->db().query("SELECT name FROM TestPerson")[0].at("name")),"seed");
    EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}
TEST(ObserverDeliverySql, ActualMissingUpdateRowIsSuccessfulNoEffectWithoutNewAuditOrCallback) {
    size_t calls=0;SqlMeterFixture fixture;ClosedUpdateSeed seed(fixture);
    auto input=Access::update(fixture.owner->db(),fixture.client,{2,8192,24576},seed.id,seed.gid,3,32);
    ASSERT_EQ(input.code,Status::admitted);Access::supply(input.plan,std::string_view("cache still adopts"));
    {
        auto held=fixture.client.try_acquire({2,32768});ASSERT_EQ(held.code,Status::admitted);
        Original::begin_offer offer(fixture.owner->db(),std::move(held.held),{2,8192,24576});
        fixture.owner->with_mutation_operation([&](lattice::database&){
            Access::execute(*fixture.runtime,fixture.owner->db(),"DELETE FROM TestPerson WHERE id=?",{seed.id});
        });
    }
    fixture.queue->drain();
    const auto audit_before=fixture.owner->db().query("SELECT COUNT(*) AS n FROM AuditLog");
    const auto token=fixture.owner->add_table_observer("TestPerson",[&](const auto&){++calls;});
    {
        auto prepared=Access::prepare(*fixture.runtime,fixture.owner->db(),input.plan);
        Original::begin_offer offer(fixture.owner->db(),input.plan);
        EXPECT_NO_THROW(fixture.owner->with_mutation_operation([&](lattice::database& writer){prepared.execute(writer);}));
        EXPECT_FALSE(Access::row_present(input.plan));
    }
    fixture.owner->remove_table_observer("TestPerson",token);fixture.queue->drain();
    EXPECT_EQ(calls,0u);EXPECT_TRUE(fixture.owner->db().query("SELECT id FROM TestPerson").empty());
    EXPECT_EQ(fixture.owner->db().query("SELECT COUNT(*) AS n FROM AuditLog"),audit_before);
    input.plan={};EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}
TEST(ObserverDeliverySql, ActualReusedIdWithDifferentGlobalIdCannotMutateSuccessor) {
    SqlMeterFixture fixture;ClosedUpdateSeed seed(fixture);
    auto input=Access::update(fixture.owner->db(),fixture.client,{2,8192,24576},seed.id,seed.gid,3,32);
    ASSERT_EQ(input.code,Status::admitted);Access::supply(input.plan,std::string_view("must not overwrite"));
    const std::string successor="00000000-0000-0000-0000-000000000099";
    {
        auto held=fixture.client.try_acquire({4,32768});ASSERT_EQ(held.code,Status::admitted);
        Original::begin_offer offer(fixture.owner->db(),std::move(held.held),{4,8192,24576});
        fixture.owner->with_mutation_operation([&](lattice::database&){
            Access::execute(*fixture.runtime,fixture.owner->db(),"DELETE FROM TestPerson WHERE id=?",{seed.id});
            Access::execute(*fixture.runtime,fixture.owner->db(),"INSERT INTO TestPerson(id,globalId,name,age,email) VALUES(?,?,?,7,NULL)",
                {seed.id,successor,std::string("successor")});
        });
    }
    fixture.queue->drain();
    {
        auto prepared=Access::prepare(*fixture.runtime,fixture.owner->db(),input.plan);
        Original::begin_offer offer(fixture.owner->db(),input.plan);
        EXPECT_THROW(fixture.owner->with_mutation_operation([&](lattice::database& writer){prepared.execute(writer);}),lattice::db_error);
    }
    const auto rows=fixture.owner->db().query("SELECT name,globalId FROM TestPerson");ASSERT_EQ(rows.size(),1u);
    EXPECT_EQ(std::get<std::string>(rows[0].at("name")),"successor");
    EXPECT_EQ(std::get<std::string>(rows[0].at("globalId")),successor);
    input.plan={};EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}
TEST(ObserverDeliverySql, CallerUpdateAbortRestoresActualPriorValueAndDoesNotRenewWorkspace) {
    SqlMeterFixture fixture;ClosedUpdateSeed seed(fixture);
    {
        auto held=fixture.client.try_acquire({2,32768});ASSERT_EQ(held.code,Status::admitted);
        Access::begin_explicit(*fixture.owner,std::move(held.held),{2,8192,24576});
        auto first=Access::update(fixture.owner->db(),fixture.client,{2,8192,24576},seed.id,seed.gid,3,32,true);
        auto second=Access::update(fixture.owner->db(),fixture.client,{2,8192,24576},seed.id,seed.gid,3,32,true);
        ASSERT_EQ(first.code,Status::admitted);ASSERT_EQ(second.code,Status::admitted);
        Access::supply(first.plan,std::string_view("caller prefix"));Access::supply(second.plan,std::string_view("must rollback"));
        auto one=Access::prepare(*fixture.runtime,fixture.owner->db(),first.plan);
        auto two=Access::prepare(*fixture.runtime,fixture.owner->db(),second.plan);
        fixture.owner->with_mutation_operation([&](lattice::database& writer){one.execute(writer);});
        EXPECT_THROW(fixture.owner->with_mutation_operation([&](lattice::database& writer){two.execute(writer);}),lattice::db_error);
        EXPECT_TRUE(fixture.owner->db().is_in_transaction());EXPECT_EQ(fixture.count(),2);
        EXPECT_EQ(std::get<std::string>(fixture.owner->db().query("SELECT name FROM TestPerson")[0].at("name")),"caller prefix");
        EXPECT_EQ(fixture.store.inspect().used,(Units{2,32768}));fixture.owner->commit();fixture.queue->drain();
        EXPECT_EQ(fixture.store.inspect().used,(Units{2,32768}));
    }
    EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}
TEST(ObserverDeliverySql, ActualUpdateQueueTailKeepsCreditThroughUnregisterAndPreparedRetirement) {
    size_t calls=0;SqlMeterFixture fixture;ClosedUpdateSeed seed(fixture);
    const auto token=fixture.owner->add_table_observer("TestPerson",[&](const auto& rows){calls+=rows.size();});
    {
        auto input=Access::update(fixture.owner->db(),fixture.client,{2,8192,24576},seed.id,seed.gid,3,32);
        ASSERT_EQ(input.code,Status::admitted);Access::supply(input.plan,std::string_view("queued actual"));
        {
            auto prepared=Access::prepare(*fixture.runtime,fixture.owner->db(),input.plan);
            Original::begin_offer offer(fixture.owner->db(),input.plan);
            fixture.owner->with_mutation_operation([&](lattice::database& writer){prepared.execute(writer);});
        }
        fixture.owner->remove_table_observer("TestPerson",token);
        EXPECT_EQ(calls,0u);EXPECT_EQ(fixture.store.inspect().used,(Units{2,32768}));
    }
    EXPECT_EQ(fixture.store.inspect().used,(Units{2,32768}));fixture.queue->drain();
    EXPECT_EQ(calls,1u);EXPECT_EQ(fixture.store.inspect().used,(Units{}));
}

// These ordinary owners exercise the actual constructor/catalog contract.
// They never install Runtime over the global RTree owner or change the registry.
class NativeCatalogOwner final:public lattice::lattice_db {
public:
    using lattice_db::lattice_db;
    NativeCatalogOwner(const lattice::configuration& config,lattice::detail::recovery_owner_schema catalog)
        :lattice_db(config,false,std::move(catalog)){}
    const auto& retained_catalog()const{return recovery_declarations();}
    std::string fingerprint_key()const{return compute_core_fingerprint_key();}
};
struct NativeCatalogFixture {
    TempDB file{"observer_native_catalog"};
    std::shared_ptr<SqlMeterQueue> queue=std::make_shared<SqlMeterQueue>();
    std::unique_ptr<NativeCatalogOwner> owner;
    explicit NativeCatalogFixture(std::optional<lattice::detail::recovery_owner_schema> catalog=std::nullopt) {
        lattice::configuration config(file.str());config.sched=queue;config.audit_retention_seconds=0;
        if(catalog)owner=std::make_unique<NativeCatalogOwner>(config,std::move(*catalog));
        else owner=std::make_unique<NativeCatalogOwner>(config);
        lattice::instance_registry::instance().get_or_create_notifier(file.str())->stop_listening();queue->drain();
    }
    ~NativeCatalogFixture(){if(owner)lattice::detail::end_fixture_owner_checked(owner.get(),"observer-native-catalog");}
};
void expect_actual_catalog_fingerprint(NativeCatalogOwner& owner) {
    const auto marker=owner.db().query("SELECT key,value FROM _lattice_meta WHERE key LIKE 'schema_fingerprint:%'");
    ASSERT_EQ(marker.size(),1u);
    EXPECT_EQ(std::get<std::string>(marker[0].at("key")),owner.fingerprint_key());
    const auto cookie=owner.db().query("PRAGMA schema_version");ASSERT_EQ(cookie.size(),1u);
    ASSERT_EQ(cookie[0].size(),1u);
    EXPECT_EQ(std::get<std::string>(marker[0].at("value")),std::to_string(std::get<int64_t>(cookie[0].begin()->second)));
}
TEST(ObserverDeliveryCatalog, ExplicitNativeSnapshotControlsActualTablesAndFingerprint) {
    const auto* registered_region=lattice::schema_registry::instance().get_schema(typeid(TestRegion));
    ASSERT_NE(registered_region,nullptr);
    const auto global_count=lattice::schema_registry::instance().all_schemas().size();
    NativeCatalogFixture fixture(sql_meter_declarations());
    const auto& retained=fixture.owner->retained_catalog();ASSERT_TRUE(retained.valid());
    ASSERT_EQ(retained.models.size(),1u);EXPECT_NE(retained.models.find("TestPerson"),retained.models.end());
    EXPECT_TRUE(fixture.owner->db().table_exists("TestPerson"));EXPECT_TRUE(fixture.owner->db().table_exists("AuditLog"));
    EXPECT_FALSE(fixture.owner->db().table_exists("TestDog"));EXPECT_FALSE(fixture.owner->db().table_exists("TestRegion"));
    EXPECT_FALSE(fixture.owner->db().table_exists("_TestRegion_zones_rtree"));
    expect_actual_catalog_fingerprint(*fixture.owner);
    EXPECT_EQ(lattice::schema_registry::instance().all_schemas().size(),global_count);
    EXPECT_EQ(lattice::schema_registry::instance().get_schema(typeid(TestRegion)),registered_region);
}
TEST(ObserverDeliveryCatalog, DefaultOwnerStillCapturesEveryActualGlobalNativeDeclaration) {
    const auto global=lattice::schema_registry::instance().all_schemas();
    ASSERT_NE(lattice::schema_registry::instance().get_schema(typeid(TestRegion)),nullptr);
    NativeCatalogFixture fixture;
    const auto& retained=fixture.owner->retained_catalog();ASSERT_TRUE(retained.valid());
    ASSERT_EQ(retained.models.size(),global.size());
    for(const auto* declared:global) {
        ASSERT_NE(declared,nullptr);EXPECT_NE(retained.models.find(declared->table_name),retained.models.end());
        EXPECT_TRUE(fixture.owner->db().table_exists(declared->table_name))<<declared->table_name;
    }
    EXPECT_TRUE(fixture.owner->db().table_exists("_TestRegion_zones_rtree"));
    expect_actual_catalog_fingerprint(*fixture.owner);
    EXPECT_EQ(lattice::schema_registry::instance().all_schemas().size(),global.size());
}
TEST(ObserverDeliveryCatalog, UnavailableBoundedSnapshotKeepsOrdinaryGlobalFallback) {
    auto unavailable=sql_meter_declarations();unavailable.refuse();
    const auto global=lattice::schema_registry::instance().all_schemas();
    NativeCatalogFixture fixture(std::move(unavailable));
    EXPECT_FALSE(fixture.owner->retained_catalog().valid());
    ASSERT_EQ(fixture.owner->retained_catalog().models.size(),1u);
    for(const auto* declared:global) {
        ASSERT_NE(declared,nullptr);EXPECT_TRUE(fixture.owner->db().table_exists(declared->table_name))<<declared->table_name;
    }
    EXPECT_TRUE(fixture.owner->db().table_exists("TestDog"));
    EXPECT_TRUE(fixture.owner->db().table_exists("_TestRegion_zones_rtree"));
    expect_actual_catalog_fingerprint(*fixture.owner);
    EXPECT_EQ(lattice::schema_registry::instance().all_schemas().size(),global.size());
}

#endif
}
