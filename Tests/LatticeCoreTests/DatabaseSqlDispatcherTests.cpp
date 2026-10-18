#include <lattice/db.hpp>
#include "../../Sources/LatticeCore/src/database_sql_dispatcher.hpp"
#include "../../Sources/LatticeCore/src/database_retirement.hpp"
#include <gtest/gtest.h>
#include <atomic>
#include <cstring>
#include <memory>
#include <utility>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <chrono>

namespace lattice::detail {
struct database_sql_dispatch_test_access {
    using callback=int(*)(void*,int,const char*,const char*,const char*,const char*)noexcept;
    static int bind(database& db,database_authorizer_slot role,callback fn,void* context)noexcept {
        return db.set_sql_authorizer_(role,fn,context);
    }
    static int clear(database& db,database_authorizer_slot role,void* context)noexcept {
        return db.clear_sql_authorizer_(role,context);
    }
    enum class prepare_kind {v1,v2,v3};
    static sqlite3_stmt* prepare_original(database& db,const char* sql,prepare_kind kind) {
        db.require_creator_();store_physical_native_scope original(db);
        if(!original.entered())throw db_error("real Core statement original unavailable");
        database::catalog_native_admission_ admitted(db);
        sqlite3_stmt* statement=nullptr;int rc=SQLITE_MISUSE;
        switch(kind){
            case prepare_kind::v1:rc=sqlite3_prepare(db.db_,sql,-1,&statement,nullptr);break;
            case prepare_kind::v2:rc=sqlite3_prepare_v2(db.db_,sql,-1,&statement,nullptr);break;
            case prepare_kind::v3:rc=sqlite3_prepare_v3(db.db_,sql,-1,SQLITE_PREPARE_PERSISTENT,&statement,nullptr);break;
        }
        if(rc!=SQLITE_OK){if(statement)sqlite3_finalize(statement);throw db_error("real Core retained policy prepare failed");}
        return statement;
    }
    static int step_original(database& db,sqlite3_stmt* statement) {
        db.require_creator_();store_physical_native_scope original(db);
        if(!original.entered())throw db_error("real Core retained policy step unavailable");
        database::catalog_native_admission_ admitted(db);return sqlite3_step(statement);
    }
    static int end_original(database& db,sqlite3_stmt*& statement)noexcept {
        if(!db.creator_current_())return SQLITE_MISUSE;
        store_physical_cleanup_scope original(db);if(!original.entered())return SQLITE_MISUSE;
        auto* mutex=sqlite3_db_mutex(db.db_);if(mutex)sqlite3_mutex_enter(mutex);
        const int result=sqlite3_finalize(std::exchange(statement,nullptr));
        if(!db.creator_current_())return SQLITE_MISUSE;
        if(mutex)sqlite3_mutex_leave(mutex);return result;
    }
    static std::pair<int,int> policy_debt(database& db)noexcept {
        if(!db.creator_current_())return {SQLITE_MISUSE,SQLITE_MISUSE};
        auto* cookie=db.sql_dispatcher_.load(std::memory_order_acquire);
        if(!cookie)return {SQLITE_MISUSE,SQLITE_MISUSE};
        auto* mutex=sqlite3_db_mutex(db.db_);if(mutex)sqlite3_mutex_enter(mutex);
        const auto copied=std::pair{cookie->publication_failure_,cookie->expiration_failure_};
        if(mutex)sqlite3_mutex_leave(mutex);return copied;
    }
    static bool inactive(database& db)noexcept {
        if(!db.creator_current_())return false;
        auto* cookie=db.sql_dispatcher_.load(std::memory_order_acquire);
        return cookie&&!cookie->active_.load(std::memory_order_acquire);
    }
    static int progress(database& db,int(*fn)(void*)noexcept,void* context)noexcept {
        return db.set_sql_progress_(database_handler_slot::owned_test,fn,context);
    }
    static int clear_progress(database& db,void* context)noexcept {
        return db.clear_sql_progress_(database_handler_slot::owned_test,context);
    }
    static int busy(database& db,int(*fn)(void*,int)noexcept,void* context)noexcept {
        return db.set_sql_busy_(database_handler_slot::owned_test,fn,context);
    }
    static int clear_busy(database& db,void* context)noexcept {
        return db.clear_sql_busy_(database_handler_slot::owned_test,context,0);
    }
    static bool in_actual_callback(database& db)noexcept {
        return database_sql_dispatcher::callback_active_for_(db);
    }
    static const void* selected_request(database& db)noexcept {
        auto* actual=db.sql_dispatcher_.load(std::memory_order_acquire);
        return actual?actual->request_:nullptr; // caller is the actual serialized callback
    }
    static void observe_destruction(database& db,std::atomic<unsigned>& counter) {
        auto* cookie=db.sql_dispatcher_.load(std::memory_order_acquire);
        if(!cookie)throw db_error("test requires the real installed dispatcher cookie");
        cookie->destroyed_observer_=&counter;
    }
    static sqlite3_stmt* prepare(database& db,const char* sql) {
        sqlite3_stmt* stmt=nullptr;
        const int rc=db.prepare_catalog_checked_(sql,-1,&stmt,nullptr);
        if(rc!=SQLITE_OK){if(stmt)sqlite3_finalize(stmt);throw db_error("actual retained statement prepare failed");}
        return stmt;
    }
};
}
namespace {
using lattice::database;using lattice::detail::database_authorizer_slot;
using access=lattice::detail::database_sql_dispatch_test_access;
struct restriction {
    const char* column=nullptr;int answer=SQLITE_DENY;unsigned seen=0;
    static int authorize(void* raw,int action,const char* table,const char* column,const char*,const char*)noexcept {
        auto& value=*static_cast<restriction*>(raw);
        if(action==SQLITE_READ&&table&&column&&std::strcmp(table,"PolicyRow")==0&&
           value.column&&std::strcmp(column,value.column)==0){++value.seen;return value.answer;}
        return SQLITE_OK;
    }
};
}
TEST(DatabaseSqlDispatcher,IndependentActualPoliciesConjoinAndExactRemovalPreservesOtherPolicy) {
    restriction first{"value"},second{"id"},wrong{"value"};
    database db(":memory:");
    db.execute("CREATE TABLE PolicyRow(id INTEGER,value TEXT)");db.execute("INSERT INTO PolicyRow VALUES(7,'kept')");
    ASSERT_EQ(access::bind(db,database_authorizer_slot::canonical,restriction::authorize,&first),SQLITE_OK);
    ASSERT_EQ(access::bind(db,database_authorizer_slot::local_producer,restriction::authorize,&second),SQLITE_OK);
    EXPECT_THROW(db.query("SELECT value FROM PolicyRow"),lattice::db_error);
    EXPECT_THROW(db.query("SELECT id FROM PolicyRow"),lattice::db_error);
    ASSERT_EQ(access::clear(db,database_authorizer_slot::canonical,&wrong),SQLITE_OK);
    EXPECT_THROW(db.query("SELECT value FROM PolicyRow"),lattice::db_error);
    ASSERT_EQ(access::clear(db,database_authorizer_slot::canonical,&first),SQLITE_OK);
    ASSERT_EQ(db.query("SELECT value FROM PolicyRow").size(),1u);
    EXPECT_THROW(db.query("SELECT id FROM PolicyRow"),lattice::db_error);
    ASSERT_EQ(access::clear(db,database_authorizer_slot::local_producer,&second),SQLITE_OK);
    const auto rows=db.query("SELECT id,value FROM PolicyRow");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<int64_t>(rows[0].at("id")),7);
    EXPECT_EQ(std::get<std::string>(rows[0].at("value")),"kept");
}
TEST(DatabaseSqlDispatcher,IgnoreDoesNotBypassAnotherActualDenyAndIndependentSlotRemoval) {
    restriction ignore{"value",SQLITE_IGNORE},deny{"value",SQLITE_DENY};
    database db(":memory:");db.execute("CREATE TABLE PolicyRow(value TEXT)");db.execute("INSERT INTO PolicyRow VALUES('kept')");
    ASSERT_EQ(access::bind(db,database_authorizer_slot::canonical,restriction::authorize,&ignore),SQLITE_OK);
    ASSERT_EQ(access::bind(db,database_authorizer_slot::local_producer,restriction::authorize,&deny),SQLITE_OK);
    EXPECT_THROW(db.query("SELECT value FROM PolicyRow"),lattice::db_error);
    EXPECT_GT(ignore.seen,0u);EXPECT_GT(deny.seen,0u);
    ASSERT_EQ(access::clear(db,database_authorizer_slot::local_producer,&deny),SQLITE_OK);
    const auto rows=db.query("SELECT value FROM PolicyRow");
    ASSERT_EQ(rows.size(),1u);EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(rows[0].at("value")));
    ASSERT_EQ(access::clear(db,database_authorizer_slot::canonical,&ignore),SQLITE_OK);
    EXPECT_EQ(std::get<std::string>(db.query("SELECT value FROM PolicyRow")[0].at("value")),"kept");
}
TEST(DatabaseSqlDispatcher,ActualBusyCloseKeepsDataCookieUntilLastRealStatementFinalization) {
    // Counter outlives every Native object and statement, including early ASSERT.
    std::atomic<unsigned> destroyed{0};
    restriction accepted{nullptr};auto db=std::make_unique<database>(":memory:");
    ASSERT_EQ(access::bind(*db,database_authorizer_slot::canonical,restriction::authorize,&accepted),SQLITE_OK);
    access::observe_destruction(*db,destroyed);
    auto retirement=lattice::detail::database_retirement_access::retain(*db); // DATA, no writer retained
    auto finalize=[](sqlite3_stmt* stmt){if(stmt)sqlite3_finalize(stmt);};
    std::unique_ptr<sqlite3_stmt,decltype(finalize)> statement(access::prepare(*db,"SELECT 7"),finalize);
    ASSERT_NE(statement.get(),nullptr);
    db.reset();
    EXPECT_EQ(retirement->state(),lattice::detail::database_retirement_state::phase::close_unproved);
    EXPECT_EQ(retirement->checked_close_result(),SQLITE_BUSY);
    EXPECT_EQ(retirement->fallback_close_result(),SQLITE_OK);
    EXPECT_EQ(destroyed.load(std::memory_order_acquire),0u);
    const int rc=sqlite3_finalize(statement.release());
    EXPECT_EQ(rc,SQLITE_OK);
    EXPECT_EQ(destroyed.load(std::memory_order_acquire),1u);
    // Finalization never relabels the first checked-close result or proves refund.
    EXPECT_EQ(retirement->checked_close_result(),SQLITE_BUSY);
}
TEST(DatabaseSqlDispatcher,LegacyRawReplacementCannotFreeInstalledCookieBeforeWrapperRetirement) {
    std::atomic<unsigned> destroyed{0};restriction accepted{nullptr};
    auto db=std::make_unique<database>(":memory:");
    ASSERT_EQ(access::bind(*db,database_authorizer_slot::canonical,restriction::authorize,&accepted),SQLITE_OK);
    access::observe_destruction(*db,destroyed);
    auto* raw=db->handle();ASSERT_NE(raw,nullptr); // genuine legacy escape, never bounded-mode permission
    ASSERT_EQ(sqlite3_create_function_v2(raw,"lattice_owned_sql_dispatch_cookie_v1",0,SQLITE_UTF8,
        nullptr,nullptr,nullptr,nullptr,nullptr),SQLITE_OK);
    EXPECT_EQ(destroyed.load(std::memory_order_acquire),0u); // actual wrapper DATA still owns userdata
    EXPECT_EQ(db->query("SELECT 7").size(),1u);
    db.reset();EXPECT_EQ(destroyed.load(std::memory_order_acquire),1u);
}

namespace {
struct retained_policy_statement {
    database& owner;sqlite3_stmt* value;bool ending_refused=false;
    retained_policy_statement(database& db,const char* sql,access::prepare_kind kind)
        :owner(db),value(access::prepare_original(db,sql,kind)){}
    int finish()noexcept{
        if(ending_refused)return SQLITE_MISUSE; // no repeated ending admission
        const int result=value?access::end_original(owner,value):SQLITE_OK;
        ending_refused=value!=nullptr;return result;
    }
    ~retained_policy_statement()noexcept{
        if(!ending_refused)(void)finish();
        if(value){
            // The actual Native call was never issued. Preserve the retained
            // statement/owner/context instead of unwinding them or claiming
            // ending. Owned test-process death is the only remaining cleanup.
            std::fputs("policy-statement-ending-unproved beforeFinalize=1 retained=1\n",stderr);std::fflush(stderr);
            std::_Exit(96);
        }
    }
};
void retained_second_policy(access::prepare_kind kind){
    restriction neutral{nullptr},deny{"value"};
    database db(":memory:");db.execute("CREATE TABLE PolicyRow(value TEXT)");db.execute("INSERT INTO PolicyRow VALUES('kept')");
    ASSERT_EQ(access::bind(db,database_authorizer_slot::canonical,restriction::authorize,&neutral),SQLITE_OK);
    retained_policy_statement statement(db,"SELECT value FROM PolicyRow",kind);
    ASSERT_NE(statement.value,nullptr);EXPECT_EQ(deny.seen,0u);
    ASSERT_EQ(access::bind(db,database_authorizer_slot::local_producer,restriction::authorize,&deny),SQLITE_OK);
    // SAME actual retained Native original. A new query would hide the stable
    // callback publication gap by compiling afresh; no proxy raw connection.
    const int stepped=access::step_original(db,statement.value);
    EXPECT_EQ(stepped,SQLITE_AUTH);EXPECT_GT(deny.seen,0u);
    EXPECT_EQ(statement.finish(),SQLITE_AUTH);
    ASSERT_EQ(access::clear(db,database_authorizer_slot::local_producer,&deny),SQLITE_OK);
    EXPECT_EQ(std::get<std::string>(db.query("SELECT value FROM PolicyRow")[0].at("value")),"kept");
    ASSERT_EQ(access::clear(db,database_authorizer_slot::canonical,&neutral),SQLITE_OK);
}
}
TEST(DatabaseSqlDispatcher,ActualRetainedV2StatementRechecksTheSecondPolicyPublication){
    retained_second_policy(access::prepare_kind::v2);
}
TEST(DatabaseSqlDispatcher,ActualRetainedV3StatementRechecksTheSecondPolicyPublication){
    retained_second_policy(access::prepare_kind::v3);
}
TEST(DatabaseSqlDispatcher,ActualRetainedV1OriginalCannotRunAcrossTheSecondPolicyPublication){
    restriction neutral{nullptr},deny{"value"};
    database db(":memory:");db.execute("CREATE TABLE PolicyRow(value TEXT)");db.execute("INSERT INTO PolicyRow VALUES('kept')");
    ASSERT_EQ(access::bind(db,database_authorizer_slot::canonical,restriction::authorize,&neutral),SQLITE_OK);
    retained_policy_statement statement(db,"SELECT value FROM PolicyRow",access::prepare_kind::v1);
    ASSERT_EQ(access::bind(db,database_authorizer_slot::local_producer,restriction::authorize,&deny),SQLITE_OK);
    // Legacy prepare cannot automatically recompile. Expiration must refuse
    // the same original instead of running the prior unrestricted program.
    const int stepped=access::step_original(db,statement.value);EXPECT_NE(stepped,SQLITE_ROW);EXPECT_NE(stepped,SQLITE_DONE);
    EXPECT_NE(statement.finish(),SQLITE_OK);
    EXPECT_THROW(db.query("SELECT value FROM PolicyRow"),lattice::db_error);
    ASSERT_EQ(access::clear(db,database_authorizer_slot::local_producer,&deny),SQLITE_OK);
    ASSERT_EQ(access::clear(db,database_authorizer_slot::canonical,&neutral),SQLITE_OK);
}
TEST(DatabaseSqlDispatcher,ActualBusyOriginalRefusesNewPolicyWithoutPublishingItsContext){
    restriction neutral{nullptr},deny{"value"};
    database db(":memory:");db.execute("CREATE TABLE PolicyRow(value TEXT)");db.execute("INSERT INTO PolicyRow VALUES('first'),('second')");
    ASSERT_EQ(access::bind(db,database_authorizer_slot::canonical,restriction::authorize,&neutral),SQLITE_OK);
    retained_policy_statement statement(db,"SELECT value FROM PolicyRow ORDER BY rowid",access::prepare_kind::v2);
    ASSERT_EQ(access::step_original(db,statement.value),SQLITE_ROW);
    ASSERT_EQ(access::bind(db,database_authorizer_slot::local_producer,restriction::authorize,&deny),SQLITE_BUSY);
    EXPECT_EQ(deny.seen,0u);EXPECT_EQ(access::step_original(db,statement.value),SQLITE_ROW);EXPECT_EQ(statement.finish(),SQLITE_OK);
    EXPECT_EQ(db.query("SELECT value FROM PolicyRow").size(),2u);EXPECT_EQ(deny.seen,0u);
    ASSERT_EQ(access::bind(db,database_authorizer_slot::local_producer,restriction::authorize,&deny),SQLITE_OK);
    EXPECT_THROW(db.query("SELECT value FROM PolicyRow"),lattice::db_error);
    ASSERT_EQ(access::clear(db,database_authorizer_slot::local_producer,&deny),SQLITE_OK);
    ASSERT_EQ(access::clear(db,database_authorizer_slot::canonical,&neutral),SQLITE_OK);
}
TEST(DatabaseSqlDispatcher,BusyExactCleanupDropsRetiringContextAndRetainsPermanentPolicyDebt){
    auto neutral=std::make_unique<restriction>();
    database db(":memory:");db.execute("CREATE TABLE PolicyRow(value TEXT)");db.execute("INSERT INTO PolicyRow VALUES('kept')");
    ASSERT_EQ(access::bind(db,database_authorizer_slot::canonical,restriction::authorize,neutral.get()),SQLITE_OK);
    retained_policy_statement idle(db,"SELECT value FROM PolicyRow",access::prepare_kind::v2);
    retained_policy_statement statement(db,"SELECT value FROM PolicyRow",access::prepare_kind::v2);
    ASSERT_EQ(access::step_original(db,statement.value),SQLITE_ROW);
    ASSERT_EQ(access::clear(db,database_authorizer_slot::canonical,neutral.get()),SQLITE_BUSY);
    ASSERT_TRUE(access::inactive(db));neutral.reset(); // actual context destruction precedes another real prepare
    EXPECT_EQ(access::policy_debt(db),(std::pair{SQLITE_BUSY,SQLITE_OK}));
    EXPECT_EQ(access::step_original(db,idle.value),SQLITE_AUTH);EXPECT_EQ(idle.finish(),SQLITE_AUTH);
    EXPECT_EQ(statement.finish(),SQLITE_OK);
    EXPECT_THROW(db.query("SELECT value FROM PolicyRow"),lattice::db_error);
    EXPECT_TRUE(access::inactive(db)); // Ending the VM cannot silently restore permission.
}

TEST(DatabaseSqlDispatcher,BusyEmptyCleanupIsAnExactNoOpWithNoPolicyDebt){
    restriction neutral{nullptr};database db(":memory:");
    db.execute("CREATE TABLE PolicyRow(value TEXT)");db.execute("INSERT INTO PolicyRow VALUES('kept')");
    ASSERT_EQ(access::bind(db,database_authorizer_slot::canonical,restriction::authorize,&neutral),SQLITE_OK);
    retained_policy_statement statement(db,"SELECT value FROM PolicyRow",access::prepare_kind::v2);
    ASSERT_EQ(access::step_original(db,statement.value),SQLITE_ROW);
    EXPECT_EQ(access::clear(db,database_authorizer_slot::observer_delivery,nullptr),SQLITE_OK);
    EXPECT_FALSE(access::inactive(db));EXPECT_EQ(access::policy_debt(db),(std::pair{SQLITE_OK,SQLITE_OK}));
    EXPECT_EQ(statement.finish(),SQLITE_OK);
    ASSERT_EQ(access::clear(db,database_authorizer_slot::canonical,&neutral),SQLITE_OK);
}
namespace {
struct actual_callback_reentry {
    database* owner=nullptr;restriction neutral{nullptr},replacement{"value"};
    unsigned calls=0;int bind_result=SQLITE_OK,clear_result=SQLITE_OK,clear_progress_result=SQLITE_OK,clear_busy_result=SQLITE_OK;
    void attempt()noexcept {
        if(++calls!=1)return;
        bind_result=access::bind(*owner,database_authorizer_slot::local_producer,restriction::authorize,&replacement);
        clear_result=access::clear(*owner,database_authorizer_slot::canonical,&neutral);
        clear_progress_result=access::clear_progress(*owner,this);
        clear_busy_result=access::clear_busy(*owner,this);
    }
    static int progress(void* raw)noexcept{static_cast<actual_callback_reentry*>(raw)->attempt();return 0;}
    static int busy(void* raw,int)noexcept{static_cast<actual_callback_reentry*>(raw)->attempt();return 0;}
};
void expect_callback_refusal(const actual_callback_reentry& state){
    EXPECT_GT(state.calls,0u);EXPECT_EQ(state.bind_result,SQLITE_MISUSE);EXPECT_EQ(state.clear_result,SQLITE_MISUSE);
    EXPECT_EQ(state.clear_progress_result,SQLITE_MISUSE);EXPECT_EQ(state.clear_busy_result,SQLITE_MISUSE);
}
}
TEST(DatabaseSqlDispatcher,ActualProgressCallbackCannotMutateOrClearTheOwningPolicy){
    actual_callback_reentry state;database db(":memory:");state.owner=&db;
    ASSERT_EQ(access::bind(db,database_authorizer_slot::canonical,restriction::authorize,&state.neutral),SQLITE_OK);
    ASSERT_EQ(access::progress(db,actual_callback_reentry::progress,&state),SQLITE_OK);
    const auto rows=db.query("WITH RECURSIVE PolicySeries(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM PolicySeries WHERE x<2000) SELECT sum(x) AS n FROM PolicySeries");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<int64_t>(rows[0].at("n")),2001000);
    expect_callback_refusal(state);EXPECT_FALSE(access::inactive(db));EXPECT_EQ(state.replacement.seen,0u);
    ASSERT_EQ(access::clear_progress(db,&state),SQLITE_OK);
    ASSERT_EQ(access::clear(db,database_authorizer_slot::canonical,&state.neutral),SQLITE_OK);
}

TEST(DatabaseSqlDispatcher,ActualBusyCallbackCannotMutateOrClearTheOwningPolicy){
    actual_callback_reentry state;
    const auto directory=std::filesystem::temp_directory_path()/
        ("actual_sql_policy_busy_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    ASSERT_TRUE(std::filesystem::create_directory(directory));
    const auto path=(directory/"store.sqlite").string();
    std::shared_ptr<const lattice::detail::database_retirement_state> holder_ending,contender_ending;
    {
        database holder(path);holder.execute("CREATE TABLE PolicyRow(value TEXT)");
        database contender(path);state.owner=&contender;
        holder_ending=lattice::detail::database_retirement_access::retain(holder);
        contender_ending=lattice::detail::database_retirement_access::retain(contender);
        ASSERT_EQ(access::bind(contender,database_authorizer_slot::canonical,restriction::authorize,&state.neutral),SQLITE_OK);
        ASSERT_EQ(access::busy(contender,actual_callback_reentry::busy,&state),SQLITE_OK);
        holder.begin_transaction();holder.execute("INSERT INTO PolicyRow VALUES('held')");
        EXPECT_THROW(contender.execute("INSERT INTO PolicyRow VALUES('refused')"),lattice::db_error);
        expect_callback_refusal(state);EXPECT_FALSE(access::inactive(contender));EXPECT_EQ(state.replacement.seen,0u);
        holder.rollback();
        ASSERT_EQ(access::clear_busy(contender,&state),SQLITE_OK);
        ASSERT_EQ(access::clear(contender,database_authorizer_slot::canonical,&state.neutral),SQLITE_OK);
    }
    // No process worker or borrowed source exists in this direct database
    // control. Do not unlink even this test's own MAIN until BOTH actual
    // checked close calls are known SQLITE_OK. A failed setup leaves evidence.
    ASSERT_TRUE(holder_ending);ASSERT_TRUE(contender_ending);
    ASSERT_EQ(holder_ending->checked_close_result(),SQLITE_OK);
    ASSERT_EQ(contender_ending->checked_close_result(),SQLITE_OK);
    std::error_code error;EXPECT_TRUE(std::filesystem::remove(path,error));EXPECT_FALSE(error);
    EXPECT_TRUE(std::filesystem::remove(directory,error));EXPECT_FALSE(error);
}

namespace {
struct actual_request_reentry {
    const uint64_t creator=lattice::detail::database_native_process();
    database* owner=nullptr;database* other=nullptr;
    unsigned calls=0;bool witnessed=false,denied=false,early_denial=false,unexpected=false;
    bool other_succeeded=false;const void* before=nullptr;const void* after=nullptr;
    void attempt()noexcept {
        if(creator!=lattice::detail::database_native_process()||++calls!=1)return;
        witnessed=access::in_actual_callback(*owner);before=access::selected_request(*owner);
        try {(void)owner->query("SELECT 7 AS value");unexpected=true;}
        catch(const lattice::db_error& error) {
            if(creator!=lattice::detail::database_native_process())return;
            denied=true;early_denial=std::strstr(error.what(),"new SQL request refuses reentry")!=nullptr;
        }catch(...) {if(creator==lattice::detail::database_native_process())unexpected=true;}
        if(creator!=lattice::detail::database_native_process())return;
        after=access::selected_request(*owner);
        try {
            // A different genuine owned connection remains usable. The marker
            // denies exact callback provenance, not all SQL on this thread.
            const auto rows=other->query("SELECT 41 AS value");
            if(creator!=lattice::detail::database_native_process())return;
            other_succeeded=rows.size()==1&&std::get<int64_t>(rows[0].at("value"))==41;
        }catch(...) {if(creator==lattice::detail::database_native_process())unexpected=true;}
    }
    static int progress(void* raw)noexcept{static_cast<actual_request_reentry*>(raw)->attempt();return 0;}
    static int busy(void* raw,int)noexcept{static_cast<actual_request_reentry*>(raw)->attempt();return 0;}
};
void expect_request_reentry_refusal(const actual_request_reentry& state) {
    EXPECT_GT(state.calls,0u);EXPECT_TRUE(state.witnessed);EXPECT_TRUE(state.denied);
    EXPECT_TRUE(state.early_denial) << "refusal must precede the new request's admission/Native handler publication";
    EXPECT_FALSE(state.unexpected);EXPECT_TRUE(state.other_succeeded);EXPECT_EQ(state.before,state.after);
}
}
TEST(DatabaseSqlDispatcher,ActualProgressPublicQueryRefusesBeforeRequestAdmissionAndPreservesOtherOwner) {
    actual_request_reentry state;restriction owner_policy{nullptr},other_policy{nullptr};
    database owner(":memory:"),other(":memory:");state.owner=&owner;state.other=&other;
    ASSERT_EQ(access::bind(owner,database_authorizer_slot::canonical,restriction::authorize,&owner_policy),SQLITE_OK);
    ASSERT_EQ(access::bind(other,database_authorizer_slot::canonical,restriction::authorize,&other_policy),SQLITE_OK);
    ASSERT_EQ(access::progress(owner,actual_request_reentry::progress,&state),SQLITE_OK);
    const auto rows=owner.query("WITH RECURSIVE RequestSeries(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM RequestSeries WHERE x<2000) SELECT sum(x) AS n FROM RequestSeries");
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<int64_t>(rows[0].at("n")),2001000);
    expect_request_reentry_refusal(state);ASSERT_NE(state.before,nullptr);
    EXPECT_FALSE(access::in_actual_callback(owner));
    ASSERT_EQ(access::clear_progress(owner,&state),SQLITE_OK);
    EXPECT_EQ(owner.query("SELECT 17 AS value").size(),1u);
    ASSERT_EQ(access::clear(owner,database_authorizer_slot::canonical,&owner_policy),SQLITE_OK);
    ASSERT_EQ(access::clear(other,database_authorizer_slot::canonical,&other_policy),SQLITE_OK);
}
TEST(DatabaseSqlDispatcher,ActualBusyPublicQueryRefusesBeforeRequestAdmissionAndPreservesOtherOwner) {
    actual_request_reentry state;restriction owner_policy{nullptr},other_policy{nullptr};
    database other(":memory:");state.other=&other;
    ASSERT_EQ(access::bind(other,database_authorizer_slot::canonical,restriction::authorize,&other_policy),SQLITE_OK);
    const auto directory=std::filesystem::temp_directory_path()/
        ("actual_sql_request_busy_"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    ASSERT_TRUE(std::filesystem::create_directory(directory));const auto path=(directory/"store.sqlite").string();
    std::shared_ptr<const lattice::detail::database_retirement_state> holder_ending,contender_ending;
    {
        database holder(path);holder.execute("CREATE TABLE PolicyRow(value TEXT)");
        database contender(path);state.owner=&contender;
        holder_ending=lattice::detail::database_retirement_access::retain(holder);
        contender_ending=lattice::detail::database_retirement_access::retain(contender);
        ASSERT_EQ(access::bind(contender,database_authorizer_slot::canonical,restriction::authorize,&owner_policy),SQLITE_OK);
        ASSERT_EQ(access::busy(contender,actual_request_reentry::busy,&state),SQLITE_OK);
        holder.begin_transaction();holder.execute("INSERT INTO PolicyRow VALUES('held')");
        EXPECT_THROW(contender.execute("INSERT INTO PolicyRow VALUES('refused')"),lattice::db_error);
        expect_request_reentry_refusal(state);EXPECT_FALSE(access::in_actual_callback(contender));
        holder.rollback();
        ASSERT_EQ(access::clear_busy(contender,&state),SQLITE_OK);
        EXPECT_EQ(contender.query("SELECT 19 AS value").size(),1u);
        ASSERT_EQ(access::clear(contender,database_authorizer_slot::canonical,&owner_policy),SQLITE_OK);
    }
    ASSERT_TRUE(holder_ending);ASSERT_TRUE(contender_ending);
    ASSERT_EQ(holder_ending->checked_close_result(),SQLITE_OK);
    ASSERT_EQ(contender_ending->checked_close_result(),SQLITE_OK);
    std::error_code error;EXPECT_TRUE(std::filesystem::remove(path,error));EXPECT_FALSE(error);
    EXPECT_TRUE(std::filesystem::remove(directory,error));EXPECT_FALSE(error);
    ASSERT_EQ(access::clear(other,database_authorizer_slot::canonical,&other_policy),SQLITE_OK);
}
