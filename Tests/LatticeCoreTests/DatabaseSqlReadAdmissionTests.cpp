#include "ObserverDeliveryConnectionTestSupport.hpp"
#include "../../Sources/LatticeCore/src/database_sql_request.hpp"
#include "../../Sources/LatticeCore/src/database_sql_read_admission.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_original.hpp"
#include <gtest/gtest.h>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <thread>

namespace lattice::detail {
struct database_sql_read_admission_test_access {
    struct binding_data {const void* binding=nullptr;const void* profile=nullptr;bool current=false;};
    struct request_data {
        const void* budget=nullptr;const void* request=nullptr;uint64_t used=0;
        bool borrowed=false;std::chrono::steady_clock::time_point deadline{};
    };
    static binding_data binding(const database& owner) {
        owner.require_creator_();const auto value=std::atomic_load(&owner.sql_read_binding_);
        return value?binding_data{value.get(),value->profile_.get(),value->current(owner)}:binding_data{};
    }
    static request_data current(database& owner)noexcept {
        if(!owner.creator_current_())return {};
        for(auto* node=database::engine_query_scope::current;node;node=node->previous) {
            if(&node->owner!=&owner||!node->request)continue;
            const auto value=node->request->inspect_admission_for_test_();
            return {value.budget,value.request,value.used,value.borrowed,value.deadline};
        }
        return {};
    }
    static uint64_t remaining(database& owner) {
        database::catalog_native_admission_ frame(owner);
        const auto& actual=owner.core_transaction_original_;
        if(!actual||!actual->current_for(owner)||!actual->observer_delivery)
            throw db_error("test requires its genuine live admitted BEGIN");
        return actual->observer_delivery->remaining_workspace();
    }
    static int function(database& owner,const char* name,void* context,
        void(*call)(sqlite3_context*,int,sqlite3_value**)) {
        database::catalog_native_admission_ frame(owner);
        return sqlite3_create_function_v2(owner.db_,name,0,SQLITE_UTF8,context,call,nullptr,nullptr,nullptr);
    }
    static std::vector<database::row_t> declared(database& owner,const std::string& sql,
        const database_sql_read_limits& limits,const std::vector<column_value_t>& params={}) {
        return owner.query_requested_(sql,params,database_sql_read_kind::diagnostic,&limits);
    }
};
}
namespace {
using Fixture=observer_delivery_testing::Fixture;
using access=lattice::detail::database_sql_read_admission_test_access;
using clock_type=std::chrono::steady_clock;
using lattice::database;
using lattice::db_error;
using lattice::detail::database_native_process;

lattice::detail::database_sql_read_policy policy() {
    return {1024*1024,4096,65536,std::chrono::seconds(5)};
}
struct held_callback {
    const uint64_t creator=database_native_process();
    std::mutex mutex;std::condition_variable changed;
    bool entered=false,released=false,timed_out=false;
    access::request_data observed;
    database* owner=nullptr;
    bool await_entry() {
        std::unique_lock lock(mutex);
        return changed.wait_until(lock,clock_type::now()+std::chrono::seconds(2),[&]{return entered;});
    }
    void release()noexcept {
        if(creator!=database_native_process())return;
        {std::lock_guard lock(mutex);released=true;}changed.notify_all();
    }
    static void call(sqlite3_context* context,int,sqlite3_value**)noexcept {
        auto& state=*static_cast<held_callback*>(sqlite3_user_data(context));
        if(state.creator!=database_native_process())return;
        try {
            state.observed=access::current(*state.owner);
            {std::unique_lock lock(state.mutex);state.entered=true;state.changed.notify_all();
             if(!state.changed.wait_until(lock,clock_type::now()+std::chrono::seconds(2),[&]{return state.released;}))
                 state.timed_out=true;}
            if(state.creator!=database_native_process())return;
            if(state.timed_out)sqlite3_result_error(context,"read control hold exceeded its original deadline",-1);
            else sqlite3_result_text(context,"retained",8,SQLITE_STATIC);
        }catch(...) {if(state.creator==database_native_process())sqlite3_result_error(context,"read control callback failed",-1);}
    }
};
// Declared AFTER the fixture; any early assertion releases and joins before
// owner/namespace teardown. The callback's DATA counters outlive both.
struct read_worker {
    const uint64_t creator=database_native_process();
    held_callback& callback;const char* retained_namespace;
    std::thread thread;std::exception_ptr failure;std::vector<database::row_t> rows;
    read_worker(held_callback& value,database& owner,const char* path):callback(value),retained_namespace(path),
        thread([this,&owner]{try{rows=owner.query("SELECT held_read_credit() AS value");}catch(...){failure=std::current_exception();}}){}
    void join()noexcept {
        if(creator!=database_native_process())return;
        callback.release();
        try {if(thread.joinable())thread.join();}
        catch(...) {
            std::fprintf(stderr,"read admission control unknown worker ending namespace=%s\n",retained_namespace);
            std::fflush(stderr);std::_Exit(94);
        }
    }
    ~read_worker(){join();}
};
struct original_cleanup {
    lattice::lattice_db& owner;bool active=false;
    ~original_cleanup() {
        if(!active)return;
        try {owner.rollback();}
        catch(...) {std::fprintf(stderr,"read admission control unresolved original\n");std::fflush(stderr);std::_Exit(96);}
    }
};
struct nested_read {
    const uint64_t creator=database_native_process();
    database* owner=nullptr;unsigned calls=0;bool failed=false;
    access::request_data outer,inner,after;
    static void inner_call(sqlite3_context* context,int,sqlite3_value**)noexcept {
        auto& state=*static_cast<nested_read*>(sqlite3_user_data(context));
        if(state.creator!=database_native_process())return;
        state.inner=access::current(*state.owner);++state.calls;sqlite3_result_int(context,11);
    }
    static void outer_call(sqlite3_context* context,int,sqlite3_value**)noexcept {
        auto& state=*static_cast<nested_read*>(sqlite3_user_data(context));
        if(state.creator!=database_native_process())return;
        try {
            state.outer=access::current(*state.owner);
            const auto rows=state.owner->query("SELECT nested_read_credit_inner() AS value");
            if(state.creator!=database_native_process())return;
            state.after=access::current(*state.owner);
            if(rows.size()!=1||std::get<int64_t>(rows[0].at("value"))!=11)throw db_error("nested result differs");
            sqlite3_result_int(context,13);
        }catch(...) {if(state.creator==database_native_process()){state.failed=true;sqlite3_result_error(context,"nested read failed",-1);}}
    }
};
struct calls_data {
    const uint64_t creator=database_native_process();unsigned calls=0;
    static void call(sqlite3_context* context,int,sqlite3_value**)noexcept {
        auto& state=*static_cast<calls_data*>(sqlite3_user_data(context));
        if(state.creator!=database_native_process())return;
        ++state.calls;sqlite3_result_int(context,7);
    }
};
}

// True enrollment is intentionally retained, including its current refusal for
// preexisting opaque SQLite statements. No fake profile, pruning or permission.
TEST(DatabaseSqlReadAdmission,ActualWriterReadersShareProfileButKeepDistinctOpenBindings) {
    Fixture fixture(policy());
    auto reader=fixture.owner->borrow_read_connection();
    auto xproc=fixture.owner->borrow_xproc_read_connection();
    ASSERT_NE(reader.get(),&fixture.owner->db());ASSERT_NE(xproc.get(),reader.get());
    const auto writer_data=access::binding(fixture.owner->db());
    const auto reader_data=access::binding(*reader),xproc_data=access::binding(*xproc);
    ASSERT_TRUE(writer_data.current);ASSERT_TRUE(reader_data.current);ASSERT_TRUE(xproc_data.current);
    ASSERT_NE(writer_data.profile,nullptr);EXPECT_EQ(writer_data.profile,reader_data.profile);
    EXPECT_EQ(writer_data.profile,xproc_data.profile);
    EXPECT_NE(writer_data.binding,reader_data.binding);EXPECT_NE(reader_data.binding,xproc_data.binding);
    EXPECT_EQ(fixture.store.inspect().clients,1u);
}
TEST(DatabaseSqlReadAdmission,ActualMaterializationRetainsCreditUntilReturnThenTransfersOwningRows) {
    held_callback callback;Fixture fixture(policy());
    auto reader=fixture.owner->borrow_read_connection();callback.owner=reader.get();
    ASSERT_EQ(access::function(*reader,"held_read_credit",&callback,held_callback::call),SQLITE_OK);
    const auto path=fixture.file.str();read_worker worker(callback,*reader,path.c_str());
    const bool entered=callback.await_entry();
    const auto while_held=fixture.store.inspect();
    worker.join(); // Ending and all Native unlock/finalization precede assertions.
    ASSERT_TRUE(entered);ASSERT_FALSE(worker.failure);EXPECT_FALSE(callback.timed_out);
    EXPECT_EQ(while_held.used.count,1u);EXPECT_EQ(while_held.used.bytes,policy().workspace_bytes);
    EXPECT_NE(callback.observed.budget,nullptr);EXPECT_FALSE(callback.observed.borrowed);
    ASSERT_EQ(worker.rows.size(),1u);EXPECT_EQ(std::get<std::string>(worker.rows[0].at("value")),"retained");
    EXPECT_EQ(fixture.store.inspect().used.count,0u);
    // A bare returned vector is caller-owned after transfer; it is NOT claimed
    // to retain library credit until arbitrary caller destruction.
    EXPECT_EQ(std::get<std::string>(worker.rows[0].at("value")),"retained");
}
TEST(DatabaseSqlReadAdmission,CaughtReadRefusalConsumesSameBeginWorkspaceWithoutReacquiringOrRefund) {
    Fixture fixture(policy());original_cleanup cleanup{*fixture.owner};
    fixture.owner->begin_transaction();cleanup.active=true;
    const auto charge=fixture.store.inspect();ASSERT_EQ(charge.used.count,fixture.policy.original.events);
    auto& writer=fixture.owner->db();const auto before=access::remaining(writer);
    const auto first=writer.query("SELECT 7 AS value");ASSERT_EQ(first.size(),1u);
    const auto after=access::remaining(writer);EXPECT_LT(after,before);
    lattice::detail::database_sql_read_limits declared;
    declared.rows=1;declared.columns=1;declared.result_bytes=4096;
    declared.deadline=clock_type::now()+std::chrono::seconds(5);
    EXPECT_THROW(access::declared(writer,"SELECT 7 AS value UNION ALL SELECT 9",declared),db_error);
    const auto after_refusal=access::remaining(writer);EXPECT_LT(after_refusal,after);
    EXPECT_EQ(fixture.store.inspect().used.count,charge.used.count);
    EXPECT_EQ(fixture.store.inspect().used.bytes,charge.used.bytes);
    const auto again=writer.query("SELECT 11 AS value");ASSERT_EQ(again.size(),1u);
    EXPECT_EQ(std::get<int64_t>(again[0].at("value")),11);
    EXPECT_LT(access::remaining(writer),after_refusal);
    fixture.owner->rollback();cleanup.active=false;fixture.queue->drain();
}
TEST(DatabaseSqlReadAdmission,ActualTransientBindRefusesBeforeValueProducerAndLeavesNoStandaloneLease) {
    calls_data state;Fixture fixture(policy());auto reader=fixture.owner->borrow_read_connection();
    ASSERT_EQ(access::function(*reader,"read_credit_count",&state,calls_data::call),SQLITE_OK);
    lattice::detail::database_sql_read_limits declared;
    declared.parameter_bytes=3;declared.rows=1;declared.columns=1;declared.result_bytes=4096;
    declared.deadline=clock_type::now()+std::chrono::seconds(5);
    EXPECT_THROW(access::declared(*reader,"SELECT read_credit_count()+length(?) AS value",declared,{std::string("four")}),db_error);
    EXPECT_EQ(state.calls,0u);EXPECT_EQ(fixture.store.inspect().used.count,0u);
    EXPECT_EQ(reader->query("SELECT 17 AS value").size(),1u);
}
TEST(DatabaseSqlReadAdmission,ActualPublicReentryUsesDistinctRequestWithSameBudgetAndAbsoluteDeadline) {
    nested_read state;Fixture fixture(policy());auto reader=fixture.owner->borrow_read_connection();state.owner=reader.get();
    ASSERT_EQ(access::function(*reader,"nested_read_credit_inner",&state,nested_read::inner_call),SQLITE_OK);
    ASSERT_EQ(access::function(*reader,"nested_read_credit_outer",&state,nested_read::outer_call),SQLITE_OK);
    const auto result=reader->query("SELECT nested_read_credit_outer() AS value");
    ASSERT_EQ(result.size(),1u);EXPECT_EQ(std::get<int64_t>(result[0].at("value")),13);
    ASSERT_FALSE(state.failed);EXPECT_EQ(state.calls,1u);ASSERT_NE(state.outer.budget,nullptr);
    EXPECT_EQ(state.outer.budget,state.inner.budget);EXPECT_NE(state.outer.request,state.inner.request);
    EXPECT_EQ(state.outer.deadline,state.inner.deadline);EXPECT_EQ(state.after.request,state.outer.request);
    EXPECT_GT(state.after.used,state.outer.used);EXPECT_EQ(fixture.store.inspect().used.count,0u);
}
TEST(DatabaseSqlReadAdmission,LiteralGettersMetadataAndOrdinaryGroupedPredicateResultsRemainUnrewritten) {
    Fixture fixture(policy());auto reader=fixture.owner->borrow_read_connection();
    const auto plain=reader->query("PRAGMA busy_timeout");const auto terminated=reader->query("PRAGMA busy_timeout;");
    ASSERT_EQ(plain.size(),1u);EXPECT_EQ(plain,terminated);
    const auto metadata=reader->get_table_info("TestPerson");
    ASSERT_EQ(metadata.count("name"),1u);EXPECT_EQ(metadata.at("name"),"TEXT");
    const auto result=reader->query("SELECT DISTINCT name,count(*) AS copies FROM TestPerson WHERE age>=? GROUP BY name ORDER BY name",{int64_t(7)});
    ASSERT_EQ(result.size(),1u);EXPECT_EQ(std::get<std::string>(result[0].at("name")),"seed");
    EXPECT_EQ(std::get<int64_t>(result[0].at("copies")),1);
    EXPECT_EQ(fixture.store.inspect().used.count,0u);
}
