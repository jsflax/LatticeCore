#include <lattice/db.hpp>
#include "../../Sources/LatticeCore/src/database_sql_dispatcher.hpp"
#include "../../Sources/LatticeCore/src/database_sql_request.hpp"
#include "../../Sources/LatticeCore/src/projection_memory.hpp"
#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/database_retirement.hpp"
#include <atomic>
#include <filesystem>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <cstring>
namespace lattice::detail {
struct database_sql_request_test_access {
    static int install(database& db){return db.install_sql_dispatcher_();}
    struct capture_dispatch_state {
        const void* cookie=nullptr;bool active=false,profile=false,runtime=false;
        bool authorizer=false,progress=false,busy=false,request=false;
    };
    static capture_dispatch_state capture_state(database& db) {
        db.require_creator_();database::catalog_native_admission_ admitted(db);
        auto* cookie=db.sql_dispatcher_.load(std::memory_order_acquire);
        capture_dispatch_state result;result.cookie=cookie;
        result.profile=bool(std::atomic_load(&db.sql_read_binding_));
        result.runtime=bool(db.observer_delivery_sql_);
        if(cookie) {
            result.active=cookie->active_.load(std::memory_order_acquire);
            result.authorizer=bool(cookie->slots_[std::size_t(database_authorizer_slot::projection_capture)].call);
            result.progress=bool(cookie->progress_[std::size_t(database_handler_slot::projection_capture)].call);
            result.busy=bool(cookie->busy_[std::size_t(database_handler_slot::projection_capture)].call);
            result.request=cookie->request_!=nullptr;
        }
        return result;
    }
    struct reprepare_observation {database* owner=nullptr;sqlite3_stmt* expected=nullptr;unsigned exact=0;};
    static int observe(void* raw,int action,const char*,const char*,const char*,const char*)noexcept {
        auto& value=*static_cast<reprepare_observation*>(raw);
        if(action==SQLITE_READ&&value.expected) {
            auto* cookie=value.owner->sql_dispatcher_.load(std::memory_order_acquire);
            if(cookie&&cookie->request_&&cookie->request_->matches(value.expected))++value.exact;
        }
        return SQLITE_OK;
    }
    static int bind_observation(database& db,reprepare_observation& value) {
        return db.set_sql_authorizer_(database_authorizer_slot::owned_test_restriction,observe,&value);
    }
    static int clear_observation(database& db,reprepare_observation& value) {
        return db.clear_sql_authorizer_(database_authorizer_slot::owned_test_restriction,&value);
    }
    static int64_t reprepare(database& reader,database& external,reprepare_observation& observation) {
        const std::string sql="SELECT value FROM BusyRequest";
        database_sql_read_limits declaration{1024*1024,1024*1024,128,4096,16*1024*1024,
            std::chrono::steady_clock::now()+std::chrono::seconds(5)};declaration.rows=1;declaration.columns=1;declaration.result_bytes=128;
        database_sql_request request(reader,database_sql_read_kind::diagnostic,sql,{},&declaration);
        database::engine_query_scope actual(reader,false,&request);
        sqlite3_stmt* statement=nullptr;
        const int rc=reader.prepare_catalog_checked_(sql.c_str(),-1,&statement,nullptr,&request);
        actual.adopt(statement);if(rc!=SQLITE_OK)throw db_error("actual reprepare control prepare failed");
        request.columns(statement);observation.expected=statement;
        external.execute("ALTER TABLE BusyRequest ADD COLUMN other INTEGER");
        if(reader.step_statement_(statement)!=SQLITE_ROW)throw db_error("actual reprepare control step failed");
        request.row(statement);const auto value=sqlite3_column_int64(statement,0);
        if(reader.step_statement_(statement)!=SQLITE_DONE)throw db_error("actual reprepare control did not end");
        actual.reset();observation.expected=nullptr;return value;
    }

    static std::vector<database::row_t> query(database& db,const std::string& sql,
        const database_sql_read_limits& declaration,const std::vector<column_value_t>& params={}) {
        return db.query_requested_(sql,params,database_sql_read_kind::diagnostic,&declaration);
    }
    static sqlite3_stmt* prepare(database& db,database_sql_request& request,const char* sql) {
        sqlite3_stmt* stmt=nullptr;
        const int rc=db.prepare_catalog_checked_(sql,-1,&stmt,nullptr,&request);
        if(rc!=SQLITE_OK){if(stmt)sqlite3_finalize(stmt);throw db_error("actual request control prepare failed");}
        request.adopt(stmt);return stmt;
    }
    static void destruction(database& db,std::atomic<unsigned>& counter) {
        db.sql_dispatcher_.load(std::memory_order_acquire)->destroyed_observer_=&counter;
    }
    static int function(database& db,const char* name,void* context,void(*call)(sqlite3_context*,int,sqlite3_value**)) {
        return sqlite3_create_function_v2(db.db_,name,0,SQLITE_UTF8,context,call,nullptr,nullptr,nullptr);
    }
    struct busy_getter_denial {unsigned calls=0;};
    static int deny_busy_getter(void* raw,int action,const char* first,const char* second,const char*,const char*)noexcept {
        if(action==SQLITE_PRAGMA&&first&&!second&&sqlite3_stricmp(first,"busy_timeout")==0){
            ++static_cast<busy_getter_denial*>(raw)->calls;return SQLITE_DENY;
        }
        return SQLITE_OK;
    }
    static int bind_busy_denial(database& db,busy_getter_denial& value) {
        return db.set_sql_authorizer_(database_authorizer_slot::owned_test_restriction,deny_busy_getter,&value);
    }
    static int clear_busy_denial(database& db,busy_getter_denial& value) {
        return db.clear_sql_authorizer_(database_authorizer_slot::owned_test_restriction,&value);
    }
    struct request_identity {
        const void* address=nullptr;bool bounded=false;
        database_sql_read_kind kind=database_sql_read_kind::diagnostic;
    };
    static request_identity identity(database& db)noexcept {
        auto* cookie=db.sql_dispatcher_.load(std::memory_order_acquire);
        auto* request=cookie?cookie->request_:nullptr;
        if(!request||!request->current())return {};
        return {request,request->bounded_,request->kind_};
    }
    struct public_reentry {
        const uint64_t creator=database_native_process();
        database* owner=nullptr;bool inside=false,failed=false;unsigned inner_compiles=0;
        request_identity outer,inner,after;int64_t actual_timeout=-1;
    };
    static int observe_public_reentry(void* raw,int action,const char* first,const char* second,const char*,const char*)noexcept {
        auto& state=*static_cast<public_reentry*>(raw);
        if(state.creator!=database_native_process())return SQLITE_DENY;
        if(state.inside&&action==SQLITE_PRAGMA&&first&&!second&&sqlite3_stricmp(first,"busy_timeout")==0) {
            state.inner=identity(*state.owner);++state.inner_compiles;
        }
        return SQLITE_OK;
    }
    static void public_reentry_call(sqlite3_context* context,int,sqlite3_value**) {
        auto& state=*static_cast<public_reentry*>(sqlite3_user_data(context));
        if(state.creator!=database_native_process())return;
        try {
            state.outer=identity(*state.owner);state.inside=true;
            // The actual public method creates a NEW request. This is not
            // access::query and not a copied/private metadata declaration.
            const auto result=state.owner->query("PRAGMA busy_timeout");
            if(state.creator!=database_native_process())return;
            state.inside=false;state.after=identity(*state.owner);
            if(result.size()!=1||result[0].size()!=1)throw db_error("public getter result shape differs");
            state.actual_timeout=std::get<int64_t>(result[0].begin()->second);
            sqlite3_result_int(context,7);
        }catch(...) {
            if(state.creator!=database_native_process())return;
            state.inside=false;state.failed=true;
            sqlite3_result_error(context,"actual public reentry control failed",-1);
        }
    }
    static int bind_public_reentry(database& db,public_reentry& state) {
        return db.set_sql_authorizer_(database_authorizer_slot::owned_test_restriction,observe_public_reentry,&state);
    }
    static int clear_public_reentry(database& db,public_reentry& state) {
        return db.clear_sql_authorizer_(database_authorizer_slot::owned_test_restriction,&state);
    }
    static int progress(database& db,database_handler_slot slot,int(*fn)(void*)noexcept,void* context) {
        return db.set_sql_progress_(slot,fn,context);
    }
    static int clear_progress(database& db,database_handler_slot slot,void* context) {
        return db.clear_sql_progress_(slot,context);
    }
    static int busy(database& db,database_handler_slot slot,int(*fn)(void*,int)noexcept,void* context) {
        return db.set_sql_busy_(slot,fn,context);
    }
    static int clear_busy(database& db,database_handler_slot slot,void* context,int timeout=0) {
        return db.clear_sql_busy_(slot,context,timeout);
    }
};
}
namespace {
using lattice::database;using lattice::db_error;
using access=lattice::detail::database_sql_request_test_access;
using limits=lattice::detail::database_sql_read_limits;
using slot=lattice::detail::database_handler_slot;
limits bounded_limits() {
    return {1024*1024,1024*1024,128,4096,16*1024*1024,
        std::chrono::steady_clock::now()+std::chrono::seconds(5)};
}
struct progress_state {
    unsigned calls=0;bool stop=false;
    static int check(void* raw)noexcept {auto& value=*static_cast<progress_state*>(raw);++value.calls;return value.stop?1:0;}
};
struct reentry {
    database* owner=nullptr;bool refused=false;unsigned calls=0;
    static void invoke(sqlite3_context* context,int,sqlite3_value**) {
        auto& value=*static_cast<reentry*>(sqlite3_user_data(context));++value.calls;
        try {limits declaration=bounded_limits();declaration.result_bytes=64;declaration.rows=1;declaration.columns=1;
            (void)access::query(*value.owner,"PRAGMA busy_timeout=1",declaration);
        }catch(const db_error&){value.refused=true;}
        sqlite3_result_int(context,7);
    }
};
}
// Mechanism controls: real database/query/SQLite callbacks, not factory/domain
// activation or physical provenance proof. Full profile activation remains RED.
TEST(DatabaseSqlRequest,ActualResultLimitRefusesBeforeNextOwningRowAndLeavesOrdinaryReadUsable) {
    database db(":memory:");ASSERT_EQ(access::install(db),SQLITE_OK);
    db.execute("CREATE TABLE RequestRow(value TEXT)");db.execute("INSERT INTO RequestRow VALUES('a'),('b')");
    limits declaration=bounded_limits();declaration.rows=1;declaration.columns=1;declaration.result_bytes=128;
    EXPECT_THROW(access::query(db,"SELECT value FROM RequestRow ORDER BY rowid",declaration),db_error);
    const auto all=db.query("SELECT value FROM RequestRow ORDER BY rowid");
    ASSERT_EQ(all.size(),2u);EXPECT_EQ(std::get<std::string>(all[0].at("value")),"a");
    EXPECT_EQ(std::get<std::string>(all[1].at("value")),"b");
}
TEST(DatabaseSqlRequest,ActualParameterAndValueBytesAreCheckedBeforeOwnedCopies) {
    database db(":memory:");ASSERT_EQ(access::install(db),SQLITE_OK);
    limits declaration=bounded_limits();declaration.parameter_bytes=3;
    EXPECT_THROW(access::query(db,"SELECT ? AS value",declaration,{std::string("four")}),db_error);
    declaration.parameter_bytes=4;declaration.result_bytes=8; // five-byte name +four value bytes cannot fit
    EXPECT_THROW(access::query(db,"SELECT ? AS value",declaration,{std::string("four")}),db_error);
    declaration.result_bytes=14;
    const auto result=access::query(db,"SELECT ? AS value",declaration,{std::string("four")});
    ASSERT_EQ(result.size(),1u);EXPECT_EQ(std::get<std::string>(result[0].at("value")),"four");
}
TEST(DatabaseSqlRequest,LiteralMetadataPreservesTempResolutionViewAndDeclaredColumnTypes) {
    database db(":memory:");ASSERT_EQ(access::install(db),SQLITE_OK);
    db.execute("CREATE TABLE SameName(main_only INTEGER)");
    db.execute("CREATE TEMP TABLE SameName(temp_only TEXT)");
    db.execute("CREATE TEMP VIEW NamedView AS SELECT temp_only FROM SameName");
    const auto temp=db.get_table_info("SameName");
    EXPECT_EQ(temp.count("main_only"),0u);ASSERT_EQ(temp.count("temp_only"),1u);
    EXPECT_EQ(temp.at("temp_only"),"TEXT");
    const auto view=db.get_table_info("NamedView");ASSERT_EQ(view.count("temp_only"),1u);
    EXPECT_EQ(view.at("temp_only"),"TEXT");
}
TEST(DatabaseSqlRequest,DeclaredReadRefusesPrepareTimeSetterButLegacyGetterAndSetterRemainCompatible) {
    database db(":memory:");db.execute("PRAGMA busy_timeout=47");ASSERT_EQ(access::install(db),SQLITE_OK);
    limits declaration=bounded_limits();declaration.rows=1;declaration.columns=1;declaration.result_bytes=128;
    EXPECT_THROW(access::query(db,"PRAGMA busy_timeout=1",declaration),db_error);
    const auto actual=db.query("PRAGMA busy_timeout");ASSERT_EQ(actual.size(),1u);
    EXPECT_EQ(std::get<int64_t>(actual[0].begin()->second),47);
    db.execute("PRAGMA busy_timeout=53");
    EXPECT_EQ(std::get<int64_t>(db.query("PRAGMA busy_timeout")[0].begin()->second),53);
}
TEST(DatabaseSqlRequest,TwoActualProgressOwnersConjoinAndWrongContextClearCannotRemoveEither) {
    progress_state first,second,wrong;database db(":memory:");
    ASSERT_EQ(access::progress(db,slot::checked_read,progress_state::check,&first),SQLITE_OK);
    ASSERT_EQ(access::progress(db,slot::catalog,progress_state::check,&second),SQLITE_OK);
    ASSERT_EQ(access::clear_progress(db,slot::catalog,&wrong),SQLITE_OK);
    const std::string sql="WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<2000) SELECT sum(x) AS total FROM n";
    ASSERT_EQ(db.query(sql).size(),1u);EXPECT_GT(first.calls,0u);EXPECT_GT(second.calls,0u);
    second.stop=true;EXPECT_THROW(db.query(sql),db_error);
    ASSERT_EQ(access::clear_progress(db,slot::catalog,&second),SQLITE_OK);
    EXPECT_EQ(db.query(sql).size(),1u);
    ASSERT_EQ(access::clear_progress(db,slot::checked_read,&first),SQLITE_OK);
}
TEST(DatabaseSqlRequest,ReentrantPublicQueryCannotInheritOuterExactRequestPurpose) {
    reentry state;database db(":memory:");state.owner=&db;ASSERT_EQ(access::install(db),SQLITE_OK);
    // This test-only actual UDF is deliberately outside the future bounded SQL
    // function profile. It exercises reentrant request isolation, not approval
    // of arbitrary user VM output or callbacks in that profile.
    ASSERT_EQ(access::function(db,"request_reentry",&state,reentry::invoke),SQLITE_OK);
    const auto result=db.query("SELECT request_reentry() AS value");
    ASSERT_EQ(result.size(),1u);EXPECT_EQ(std::get<int64_t>(result[0].at("value")),7);
    EXPECT_TRUE(state.refused);EXPECT_EQ(state.calls,1u);
}
TEST(DatabaseSqlRequest,DuplicateResultColumnNamesRetainActualLastValueBehavior) {
    database db(":memory:");ASSERT_EQ(access::install(db),SQLITE_OK);
    limits declaration=bounded_limits();declaration.rows=1;declaration.columns=2;declaration.result_bytes=128;
    const auto result=access::query(db,"SELECT 7 AS same,9 AS same",declaration);
    ASSERT_EQ(result.size(),1u);ASSERT_EQ(result[0].size(),1u);
    EXPECT_EQ(std::get<int64_t>(result[0].at("same")),9);
}

TEST(DatabaseSqlRequest,RealFinalizationCanEndSQLiteBeforeBindingDataRetiresWithoutNativeReuse) {
    std::atomic<unsigned> destroyed{0};auto owner=std::make_unique<database>(":memory:");
    ASSERT_EQ(access::install(*owner),SQLITE_OK);access::destruction(*owner,destroyed);
    auto ending=lattice::detail::database_retirement_access::retain(*owner);
    lattice::detail::database_sql_request request(*owner,lattice::detail::database_sql_read_kind::diagnostic,"SELECT 7",{});
    {
        lattice::detail::database_sql_request::binding active(&request);
        auto finish=[](sqlite3_stmt* value){if(value)sqlite3_finalize(value);};
        std::unique_ptr<sqlite3_stmt,decltype(finish)> statement(access::prepare(*owner,request,"SELECT 7"),finish);
        ASSERT_NE(statement.get(),nullptr);
        owner.reset();
        EXPECT_EQ(ending->checked_close_result(),SQLITE_BUSY);
        EXPECT_EQ(ending->fallback_close_result(),SQLITE_OK);
        EXPECT_EQ(sqlite3_finalize(statement.release()),SQLITE_OK);
        EXPECT_EQ(destroyed.load(std::memory_order_acquire),0u);
        // Scope exit now releases ONLY its retained DATA reference. The actual
        // Native connection was already destroyed by final statement ending.
    }
    EXPECT_EQ(destroyed.load(std::memory_order_acquire),1u);
    EXPECT_EQ(ending->checked_close_result(),SQLITE_BUSY); // never relabeled refund/ending
}
namespace {
struct actual_busy_state {
    unsigned calls=0;
    static int stop(void* raw,int)noexcept {++static_cast<actual_busy_state*>(raw)->calls;return 0;}
};
struct actual_file_pair {
    std::filesystem::path path;
    std::shared_ptr<const lattice::detail::database_retirement_state> first_ending,second_ending;
    std::unique_ptr<database> first,second;
    actual_file_pair() {
        static std::atomic<uint64_t> sequence{0};
        path=std::filesystem::temp_directory_path()/
            ("owned-request-"+std::to_string(lattice::detail::database_native_process())+"-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count())+
             "-"+std::to_string(sequence.fetch_add(1))+".sqlite");
        first=std::make_unique<database>(path.string(),database::open_mode::read_write,0);
        first_ending=lattice::detail::database_retirement_access::retain(*first);
        first->execute("CREATE TABLE BusyRequest(value INTEGER)");first->execute("INSERT INTO BusyRequest VALUES(7)");
        second=std::make_unique<database>(path.string(),database::open_mode::read_write,0);
        second_ending=lattice::detail::database_retirement_access::retain(*second);
    }
    ~actual_file_pair() {
        second.reset();first.reset();
        if(!first_ending||!second_ending||first_ending->checked_close_result()!=SQLITE_OK||
           second_ending->checked_close_result()!=SQLITE_OK) {
            std::fprintf(stderr,"request-control ending debt path=%s\n",path.string().c_str());std::fflush(stderr);std::_Exit(96);
        }
        std::error_code error;
        for(const auto* suffix:{"","-wal","-shm"}) {
            std::filesystem::remove(path.string()+suffix,error);
            if(error){std::fprintf(stderr,"request-control retained namespace=%s error=%d\n",path.string().c_str(),error.value());std::fflush(stderr);std::_Exit(96);}
        }
    }
};
}
TEST(DatabaseSqlRequest,ActualBusyOverlayRemovalRestoresPriorOwnerWithoutCallingTwoSleepPolicies) {
    actual_busy_state original,inner,wrong;actual_file_pair files;
    ASSERT_EQ(access::busy(*files.second,slot::checked_read,actual_busy_state::stop,&original),SQLITE_OK);
    ASSERT_EQ(access::busy(*files.second,slot::catalog,actual_busy_state::stop,&inner),SQLITE_OK);
    files.first->execute("BEGIN IMMEDIATE");
    EXPECT_THROW(files.second->execute("UPDATE BusyRequest SET value=8"),db_error);
    EXPECT_GT(inner.calls,0u);EXPECT_EQ(original.calls,0u);
    ASSERT_EQ(access::clear_busy(*files.second,slot::catalog,&wrong),SQLITE_OK);
    const auto inner_before=inner.calls;
    EXPECT_THROW(files.second->execute("UPDATE BusyRequest SET value=8"),db_error);
    EXPECT_GT(inner.calls,inner_before);EXPECT_EQ(original.calls,0u);
    ASSERT_EQ(access::clear_busy(*files.second,slot::catalog,&inner),SQLITE_OK);
    EXPECT_THROW(files.second->execute("UPDATE BusyRequest SET value=8"),db_error);
    EXPECT_GT(original.calls,0u);
    files.first->execute("ROLLBACK");
    ASSERT_EQ(access::clear_busy(*files.second,slot::checked_read,&original),SQLITE_OK);
    files.second->execute("UPDATE BusyRequest SET value=9");
    const auto result=files.second->query("SELECT value FROM BusyRequest");ASSERT_EQ(result.size(),1u);
    EXPECT_EQ(std::get<int64_t>(result[0].at("value")),9);
}

TEST(DatabaseSqlRequest,ActualSchemaAutoReprepareUsesTheSameExactRegisteredReadRequest) {
    access::reprepare_observation observation;actual_file_pair files;observation.owner=files.second.get();
    ASSERT_EQ(access::bind_observation(*files.second,observation),SQLITE_OK);
    EXPECT_EQ(access::reprepare(*files.second,*files.first,observation),7);
    EXPECT_GT(observation.exact,0u) << "the genuine post-schema-change authorizer must see the exact engine_reads_ statement";
    ASSERT_EQ(access::clear_observation(*files.second,observation),SQLITE_OK);
}

TEST(DatabaseSqlRequest,DeclaredDiagnosticRejectsActualDmlAndDdlBeforeAnyModelEffect) {
    database db(":memory:");ASSERT_EQ(access::install(db),SQLITE_OK);
    db.execute("CREATE TABLE RefusedRead(value INTEGER)");db.execute("INSERT INTO RefusedRead VALUES(7)");
    const auto declaration=bounded_limits();
    EXPECT_THROW(access::query(db,"UPDATE RefusedRead SET value=9 RETURNING value",declaration),db_error);
    EXPECT_THROW(access::query(db,"CREATE TABLE UnexpectedRead(value INTEGER)",declaration),db_error);
    EXPECT_FALSE(db.table_exists("UnexpectedRead"));
    const auto actual=db.query("SELECT value FROM RefusedRead");ASSERT_EQ(actual.size(),1u);
    EXPECT_EQ(std::get<int64_t>(actual[0].at("value")),7);
}
TEST(DatabaseSqlRequest,RealOwnedGetterDenialDoesNotRecursivelyResampleDuringBindingUnwind) {
    access::busy_getter_denial denial;database db(":memory:");db.execute("PRAGMA busy_timeout=37");
    ASSERT_EQ(access::bind_busy_denial(db,denial),SQLITE_OK);
    const auto declaration=bounded_limits();
    EXPECT_THROW(access::query(db,"SELECT 7 AS value",declaration),db_error);
    EXPECT_EQ(denial.calls,1u) << "failure restore must not retry the same actual denied getter";
    ASSERT_EQ(access::clear_busy_denial(db,denial),SQLITE_OK);
    const auto actual=db.query("PRAGMA busy_timeout");ASSERT_EQ(actual.size(),1u);
    EXPECT_EQ(std::get<int64_t>(actual[0].begin()->second),37);
    EXPECT_EQ(access::query(db,"SELECT 7 AS value",declaration).size(),1u);
}
TEST(DatabaseSqlRequest,ClosedBusyGetterCannotTurnAnotherSelectIntoNumericPolicyEvidence) {
    database db(":memory:");ASSERT_EQ(access::install(db),SQLITE_OK);const auto declaration=bounded_limits();
    EXPECT_THROW((lattice::detail::database_sql_request(db,
        lattice::detail::database_sql_read_kind::busy_timeout_getter,"SELECT 53",{},&declaration)),db_error);
    const auto actual=db.query("PRAGMA busy_timeout");ASSERT_EQ(actual.size(),1u);
    EXPECT_EQ(std::get<int64_t>(actual[0].begin()->second),lattice::kDefaultBusyTimeoutMs);
}

TEST(DatabaseSqlRequest,ActualPublicReentryGetsDistinctPurposeAndRestoresOuterDeclaredRequest) {
    access::public_reentry state;database db(":memory:");state.owner=&db;
    ASSERT_EQ(access::bind_public_reentry(db,state),SQLITE_OK);
    ASSERT_EQ(access::function(db,"actual_public_request_reentry",&state,access::public_reentry_call),SQLITE_OK);
    const auto declaration=bounded_limits();
    const auto result=access::query(db,"SELECT actual_public_request_reentry() AS value",declaration);
    ASSERT_EQ(result.size(),1u);EXPECT_EQ(std::get<int64_t>(result[0].at("value")),7);
    EXPECT_FALSE(state.failed);ASSERT_NE(state.outer.address,nullptr);ASSERT_NE(state.inner.address,nullptr);
    EXPECT_NE(state.outer.address,state.inner.address);EXPECT_EQ(state.inner_compiles,1u);
    EXPECT_TRUE(state.outer.bounded);EXPECT_FALSE(state.inner.bounded);
    EXPECT_EQ(state.outer.kind,lattice::detail::database_sql_read_kind::diagnostic);
    EXPECT_EQ(state.inner.kind,lattice::detail::database_sql_read_kind::busy_timeout_getter);
    EXPECT_EQ(state.after.address,state.outer.address);EXPECT_TRUE(state.after.bounded);
    EXPECT_EQ(state.after.kind,state.outer.kind);
    EXPECT_EQ(state.actual_timeout,lattice::kDefaultBusyTimeoutMs);
    ASSERT_EQ(access::clear_public_reentry(db,state),SQLITE_OK);
}


// These controls construct genuine memory writers. No file installation,
// delivery profile, substitute client, or direct dispatcher install is used.
TEST(DatabaseSqlRequest,FirstMemoryCaptureInstallsItsActualDispatcherBeforeTheFiniteGetter) {
    database db(":memory:");
    db.execute("CREATE TABLE CapturePrerequisite(value INTEGER)");
    db.execute("INSERT INTO CapturePrerequisite VALUES(71)");
    db.execute("PRAGMA busy_timeout=43");
    const auto before=access::capture_state(db);
    ASSERT_EQ(before.cookie,nullptr);ASSERT_FALSE(before.profile);ASSERT_FALSE(before.runtime);
    auto control=std::make_shared<lattice::database_read_control>();
    control->deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
    sqlite3_stmt* statement=nullptr;const void* actual_cookie=nullptr;
    {
        lattice::database_projection_capture capture(db,control,statement);
        const auto during=access::capture_state(db);
        ASSERT_NE(during.cookie,nullptr);actual_cookie=during.cookie;
        EXPECT_TRUE(during.active);EXPECT_TRUE(during.authorizer);
        EXPECT_TRUE(during.progress);EXPECT_TRUE(during.busy);EXPECT_FALSE(during.request);
        EXPECT_FALSE(during.profile);EXPECT_FALSE(during.runtime);
        capture.prepare_read("SELECT value FROM CapturePrerequisite");
        ASSERT_EQ(lattice::detail::store_physical_step(statement),SQLITE_ROW);
        EXPECT_EQ(sqlite3_column_int64(statement,0),71);
        ASSERT_EQ(lattice::detail::store_physical_step(statement),SQLITE_DONE);
    }
    EXPECT_EQ(statement,nullptr);
    const auto after=access::capture_state(db);
    EXPECT_EQ(after.cookie,actual_cookie);EXPECT_TRUE(after.active);
    EXPECT_FALSE(after.authorizer);EXPECT_FALSE(after.progress);EXPECT_FALSE(after.busy);EXPECT_FALSE(after.request);
    EXPECT_FALSE(after.profile);EXPECT_FALSE(after.runtime);
    EXPECT_EQ(std::get<int64_t>(db.query("PRAGMA busy_timeout")[0].begin()->second),43);
    EXPECT_EQ(std::get<int64_t>(db.query("SELECT value FROM CapturePrerequisite")[0].at("value")),71);
}

TEST(DatabaseSqlRequest,RepeatedMemoryCaptureKeepsOneActualDispatcherAndRestoresEachActualTimeout) {
    database db(":memory:");
    ASSERT_EQ(access::capture_state(db).cookie,nullptr);
    const void* actual_cookie=nullptr;
    for(const int timeout:{47,53}) {
        db.execute("PRAGMA busy_timeout="+std::to_string(timeout));
        auto control=std::make_shared<lattice::database_read_control>();
        control->deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        sqlite3_stmt* statement=nullptr;
        {
            lattice::database_projection_capture capture(db,control,statement);
            const auto during=access::capture_state(db);ASSERT_NE(during.cookie,nullptr);
            if(actual_cookie)EXPECT_EQ(during.cookie,actual_cookie);else actual_cookie=during.cookie;
            EXPECT_FALSE(during.profile);EXPECT_FALSE(during.runtime);
            capture.prepare_read("SELECT 79");
            ASSERT_EQ(lattice::detail::store_physical_step(statement),SQLITE_ROW);
            EXPECT_EQ(sqlite3_column_int64(statement,0),79);
            ASSERT_EQ(lattice::detail::store_physical_step(statement),SQLITE_DONE);
        }
        EXPECT_EQ(statement,nullptr);
        const auto after=access::capture_state(db);EXPECT_EQ(after.cookie,actual_cookie);EXPECT_TRUE(after.active);
        EXPECT_FALSE(after.authorizer);EXPECT_FALSE(after.progress);EXPECT_FALSE(after.busy);EXPECT_FALSE(after.request);
        EXPECT_EQ(std::get<int64_t>(db.query("PRAGMA busy_timeout")[0].begin()->second),timeout);
    }
}

TEST(DatabaseSqlRequest,MemoryCaptureDeniedGetterKeepsFirstErrorAndIndependentPolicyWithoutUnknownTimeoutRestore) {
    access::busy_getter_denial denial;database db(":memory:");db.execute("PRAGMA busy_timeout=37");
    ASSERT_EQ(access::bind_busy_denial(db,denial),SQLITE_OK);
    const auto before=access::capture_state(db);ASSERT_NE(before.cookie,nullptr);
    auto control=std::make_shared<lattice::database_read_control>();
    control->deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
    sqlite3_stmt* statement=nullptr;
    for(unsigned attempt=1;attempt<=2;++attempt) {
        try {
            lattice::database_projection_capture capture(db,control,statement);
            FAIL()<<"the real owned denial must refuse the literal timeout getter";
        }catch(const db_error& error) {
            EXPECT_STREQ(error.what(),"memory capture busy policy is unavailable");
        }
        EXPECT_EQ(denial.calls,attempt)<<"failure cleanup must neither erase nor recursively invoke the independent denial";
        EXPECT_EQ(statement,nullptr);
        const auto failed=access::capture_state(db);EXPECT_EQ(failed.cookie,before.cookie);EXPECT_TRUE(failed.active);
        EXPECT_FALSE(failed.authorizer);EXPECT_FALSE(failed.progress);EXPECT_FALSE(failed.busy);EXPECT_FALSE(failed.request);
        EXPECT_FALSE(failed.profile);EXPECT_FALSE(failed.runtime);
    }
    ASSERT_EQ(access::clear_busy_denial(db,denial),SQLITE_OK);
    EXPECT_EQ(std::get<int64_t>(db.query("PRAGMA busy_timeout")[0].begin()->second),37);
    {
        lattice::database_projection_capture capture(db,control,statement);
        capture.prepare_read("SELECT 83");
        ASSERT_EQ(lattice::detail::store_physical_step(statement),SQLITE_ROW);
        EXPECT_EQ(sqlite3_column_int64(statement,0),83);
        ASSERT_EQ(lattice::detail::store_physical_step(statement),SQLITE_DONE);
    }
    EXPECT_EQ(statement,nullptr);EXPECT_EQ(access::capture_state(db).cookie,before.cookie);
    EXPECT_EQ(std::get<int64_t>(db.query("PRAGMA busy_timeout")[0].begin()->second),37);
}
