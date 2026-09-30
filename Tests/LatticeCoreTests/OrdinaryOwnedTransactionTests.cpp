#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"
#include <chrono>

namespace lattice::detail {
struct ordinary_owned_transaction_test_access {
    static recovery_install_result run(lattice_db& owner,
        const std::function<void(database&)>& body,
        const std::function<void()>& tail = {},
        const std::function<void()>& captured = {}) {
        return recovery_writer_access::ordinary_owned_write(owner,body,tail,captured);
    }
};
}
namespace {
using access=lattice::detail::ordinary_owned_transaction_test_access;
using writer_access=lattice::detail::recovery_writer_access;
using state=lattice::detail::recovery_install_state;
struct bounded_case {
    std::mutex mutex;std::condition_variable changed;bool done=false;
    std::thread watcher{[this]{std::unique_lock<std::mutex> lock(mutex);
        if(!changed.wait_for(lock,std::chrono::seconds(15),[&]{return done;}))std::abort();}};
    ~bounded_case(){{std::lock_guard<std::mutex> lock(mutex);done=true;}changed.notify_one();watcher.join();}
};
lattice::configuration config(const std::string& path=":memory:") {
    lattice::configuration result(path);result.audit_retention_seconds=0;result.busy_timeout_ms=100;return result;
}
void insert(lattice::database& db,const std::string& id) {
    db.execute("INSERT INTO TestPerson(globalId,name,age) VALUES(?,?,7)",{id,id});
}
int64_t count(lattice::database& db,const std::string& id) {
    return std::get<int64_t>(db.query("SELECT COUNT(*) AS n FROM TestPerson WHERE globalId=?",{id}).at(0).at("n"));
}
struct observation {
    lattice::lattice_db& owner;lattice::lattice_db::observer_id token;int calls=0;
    explicit observation(lattice::lattice_db& value):owner(value),token(owner.add_table_observer("TestPerson",[this](const auto&){++calls;})){}
    ~observation(){owner.remove_table_observer("TestPerson",token);}
};
struct authorizer_reset {
    sqlite3* handle;
    ~authorizer_reset(){sqlite3_set_authorizer(handle,nullptr,nullptr);}
};
}

TEST(OrdinaryOwnedTransaction, StackOwnerCommitsThenDeliversOutsideWriterLockBeforeReturning) {
    bounded_case bounded;lattice::lattice_db owner{config()};observation observed(owner);
    bool tail=false,observer=false;
    const auto token=owner.add_table_observer("TestPerson",[&](const auto&){
        EXPECT_TRUE(tail);EXPECT_EQ(writer_access::active_writer(owner),nullptr);
        int64_t seen=0;std::thread other([&]{seen=count(owner.db(),"ordinary");});other.join();
        EXPECT_EQ(seen,1);observer=true;
    });
    const auto result=access::run(owner,[&](auto& writer){
        EXPECT_EQ(writer_access::active_writer(owner),&writer);insert(writer,"ordinary");
        EXPECT_FALSE(owner.flush_changes());EXPECT_EQ(observed.calls,0);
    },[&]{EXPECT_EQ(writer_access::active_writer(owner),nullptr);EXPECT_FALSE(owner.db().is_in_transaction());tail=true;});
    owner.remove_table_observer("TestPerson",token);
    EXPECT_EQ(result.state,state::committed);EXPECT_EQ(result.primary_error,nullptr);EXPECT_EQ(result.postcommit_error,nullptr);
    EXPECT_TRUE(observer);EXPECT_EQ(observed.calls,1);EXPECT_EQ(count(owner.db(),"ordinary"),1);
}

TEST(OrdinaryOwnedTransaction, ExplicitAndRawCallerTransactionsStayUntouched) {
    lattice::lattice_db owner{config()};bool body=false;
    owner.begin_transaction();insert(owner.db(),"caller");
    EXPECT_EQ(access::run(owner,[&](auto&){body=true;}).state,state::refused);
    EXPECT_TRUE(owner.db().is_in_transaction());EXPECT_EQ(count(owner.db(),"caller"),1);owner.rollback();
    owner.db().execute("BEGIN IMMEDIATE");insert(owner.db(),"raw");
    EXPECT_EQ(access::run(owner,[&](auto&){body=true;}).state,state::refused);
    EXPECT_TRUE(owner.db().is_in_transaction());EXPECT_EQ(count(owner.db(),"raw"),1);owner.db().rollback();
    EXPECT_FALSE(body);EXPECT_EQ(count(owner.db(),"caller"),0);EXPECT_EQ(count(owner.db(),"raw"),0);
}

TEST(OrdinaryOwnedTransaction, ThrowingBodyRollsBackAndReleasesTheActualNotificationReservation) {
    TempDB file{"ordinary_owned_rollback"};
    for(const auto& path:{std::string(":memory:"),file.str()}) {
        lattice::lattice_db owner{config(path)};observation observed(owner);bool tail=false;
        const auto result=access::run(owner,[](auto& writer){insert(writer,"lost");throw std::runtime_error("body");},[&]{tail=true;});
        EXPECT_EQ(result.state,state::rolled_back);EXPECT_NE(result.primary_error,nullptr);EXPECT_EQ(result.cleanup_error,nullptr);
        EXPECT_FALSE(tail);EXPECT_EQ(observed.calls,0);EXPECT_EQ(count(owner.db(),"lost"),0);
        insert(owner.db(),"next");EXPECT_EQ(observed.calls,1);EXPECT_EQ(count(owner.db(),"next"),1);
    }
}

TEST(OrdinaryOwnedTransaction, PrematureBodyCommitCannotIssueSuccessOrDeliverItsRows) {
    lattice::lattice_db owner{config()};observation observed(owner);bool tail=false;
    const auto result=access::run(owner,[](auto& writer){insert(writer,"premature");writer.commit();},[&]{tail=true;});
    EXPECT_EQ(result.state,state::rolled_back);EXPECT_NE(result.primary_error,nullptr);
    EXPECT_FALSE(tail);EXPECT_EQ(observed.calls,0);EXPECT_EQ(count(owner.db(),"premature"),0);
    EXPECT_FALSE(owner.db().is_in_transaction());
}

TEST(OrdinaryOwnedTransaction, ActualCommitDenialRollsBackPreparedRowsAndRetryCommits) {
    lattice::lattice_db owner{config()};observation observed(owner);auto* handle=owner.db().handle();authorizer_reset reset{handle};int attempts=0;
    ASSERT_EQ(sqlite3_set_authorizer(handle,[](void* raw,int action,const char* first,const char*,const char*,const char*)noexcept{
        if(action==SQLITE_TRANSACTION&&first&&std::strcmp(first,"COMMIT")==0){++*static_cast<int*>(raw);return SQLITE_DENY;}return SQLITE_OK;
    },&attempts),SQLITE_OK);
    const auto failed=access::run(owner,[](auto& writer){insert(writer,"denied");});
    ASSERT_EQ(sqlite3_set_authorizer(handle,nullptr,nullptr),SQLITE_OK);
    EXPECT_EQ(attempts,1);EXPECT_EQ(failed.state,state::rolled_back);EXPECT_NE(failed.primary_error,nullptr);EXPECT_EQ(failed.cleanup_error,nullptr);
    EXPECT_EQ(observed.calls,0);EXPECT_EQ(count(owner.db(),"denied"),0);
    const auto retry=access::run(owner,[](auto& writer){insert(writer,"retry");});
    EXPECT_EQ(retry.state,state::committed);EXPECT_EQ(observed.calls,1);EXPECT_EQ(count(owner.db(),"retry"),1);
}

TEST(OrdinaryOwnedTransaction, CleanupFailureStaysUnsettledAndFencesNewOrdinaryWork) {
    lattice::lattice_db owner{config()};auto* handle=owner.db().handle();authorizer_reset reset{handle};
    ASSERT_EQ(sqlite3_set_authorizer(handle,[](void*,int action,const char* first,const char*,const char*,const char*)noexcept{
        return action==SQLITE_TRANSACTION&&first&&std::strcmp(first,"ROLLBACK")==0?SQLITE_DENY:SQLITE_OK;
    },nullptr),SQLITE_OK);
    const auto result=access::run(owner,[](auto& writer){insert(writer,"unsettled");throw std::runtime_error("primary");});
    EXPECT_EQ(result.state,state::unsettled);EXPECT_NE(result.primary_error,nullptr);EXPECT_NE(result.cleanup_error,nullptr);
    EXPECT_EQ(sqlite3_get_autocommit(handle),0);EXPECT_TRUE(owner.db().is_closed());
    EXPECT_EQ(access::run(owner,[](auto&){}).state,state::refused);
    ASSERT_EQ(sqlite3_set_authorizer(handle,nullptr,nullptr),SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(handle,"ROLLBACK",nullptr,nullptr,nullptr),SQLITE_OK);
}

TEST(OrdinaryOwnedTransaction, CloseBeforeAdmissionRefusesButAdmittedWriterStillSettles) {
    {
        lattice::lattice_db owner{config()};bool body=false;
        const auto result=access::run(owner,[&](auto&){body=true;},{},[&]{owner.close();});
        EXPECT_EQ(result.state,state::refused);EXPECT_NE(result.primary_error,nullptr);EXPECT_FALSE(body);
    }
    TempDB file{"ordinary_owned_close"};lattice::lattice_db owner{config(file.str())};observation observed(owner);
    const auto result=access::run(owner,[&](auto& writer){owner.close();EXPECT_EQ(writer_access::active_writer(owner),&writer);insert(writer,"after-close");});
    EXPECT_EQ(result.state,state::committed);EXPECT_EQ(result.primary_error,nullptr);EXPECT_EQ(observed.calls,0);
    lattice::database verifier{file.str()};EXPECT_EQ(count(verifier,"after-close"),1);
}

TEST(OrdinaryOwnedTransaction, ThrowingObserverCannotRollBackItsCommittedParentOrNewSuccessor) {
    lattice::lattice_db owner{config()};
    const auto token=owner.add_table_observer("TestPerson",[&](const auto&){
        EXPECT_EQ(writer_access::active_writer(owner),nullptr);owner.begin_transaction();insert(owner.db(),"successor");throw std::runtime_error("observer");
    });
    const auto result=access::run(owner,[](auto& writer){insert(writer,"parent");});
    owner.remove_table_observer("TestPerson",token);
    EXPECT_EQ(result.state,state::committed);EXPECT_NE(result.postcommit_error,nullptr);EXPECT_EQ(result.primary_error,nullptr);
    EXPECT_TRUE(owner.db().is_in_transaction());EXPECT_EQ(count(owner.db(),"parent"),1);EXPECT_EQ(count(owner.db(),"successor"),1);
    owner.rollback();EXPECT_EQ(count(owner.db(),"parent"),1);EXPECT_EQ(count(owner.db(),"successor"),0);
}

TEST(OrdinaryOwnedTransaction, CallRetainsPendingDeliveryUntilTheActualObserverReturns) {
    bounded_case bounded;lattice::lattice_db owner{config()};
    std::mutex mutex;std::condition_variable changed;bool entered=false,released=false;std::atomic<bool> returned{false};
    const auto token=owner.add_table_observer("TestPerson",[&](const auto&){
        std::unique_lock<std::mutex> lock(mutex);entered=true;changed.notify_all();changed.wait(lock,[&]{return released;});
    });
    lattice::detail::recovery_install_result result;
    std::thread caller([&]{result=access::run(owner,[](auto& writer){insert(writer,"held-observer");});returned.store(true);});
    bool arrived;
    {std::unique_lock<std::mutex> lock(mutex);arrived=changed.wait_for(lock,std::chrono::seconds(5),[&]{return entered;});}
    EXPECT_TRUE(arrived);EXPECT_FALSE(returned.load());
    {std::lock_guard<std::mutex> lock(mutex);released=true;}changed.notify_all();caller.join();
    owner.remove_table_observer("TestPerson",token);
    EXPECT_TRUE(returned.load());EXPECT_EQ(result.state,state::committed);EXPECT_EQ(result.postcommit_error,nullptr);
    EXPECT_EQ(count(owner.db(),"held-observer"),1);
}


TEST(OrdinaryOwnedTransaction, CapturedTopologyObserverRefusesOwnedWriteBeforeBeginThenLaterSucceeds) {
    bounded_case bounded;
    lattice::lattice_db parent{config()};
    lattice::lattice_db arm{config("file:topology_owned_write_admission_arm?mode=memory&cache=shared")};
    for (auto* owner : {&parent, &arm}) {
        owner->db().execute("CREATE TABLE TopologyFixture(id INTEGER PRIMARY KEY,globalId TEXT NOT NULL,n INTEGER)");
        owner->db().execute("INSERT INTO TopologyFixture VALUES(1,'topology-row',7)");
    }
    int callbacks = 0, begin_attempts = 0;
    bool premature_body = false, callback_had_no_transaction = false;
    lattice::detail::recovery_install_result callback_result;
    const auto token = parent.add_table_observer("TopologyFixture", [&](const auto&) {
        ++callbacks;
        if (callbacks != 1) return;
        callback_had_no_transaction = !parent.db().is_in_transaction();
        callback_result = access::run(parent, [&](auto& writer) {
            premature_body = true;
            writer.execute("UPDATE main.TopologyFixture SET n=999 WHERE id=1");
        });
    });
    parent.db().execute("BEGIN");
    parent.db().execute("UPDATE main.TopologyFixture SET n=n+1 WHERE id=1");
    ASSERT_EQ(sqlite3_exec(parent.db().handle(), "COMMIT", nullptr, nullptr, nullptr), SQLITE_OK);
    ASSERT_EQ(callbacks, 0);
    auto* handle = parent.db().handle();
    authorizer_reset reset{handle};
    ASSERT_EQ(sqlite3_set_authorizer(handle,
        [](void* raw, int action, const char* first, const char*, const char*, const char*) noexcept {
            if (action == SQLITE_TRANSACTION && first && std::strcmp(first, "BEGIN") == 0)
                ++*static_cast<int*>(raw);
            return SQLITE_OK;
        }, &begin_attempts), SQLITE_OK);
    EXPECT_NO_THROW(parent.attach(arm));
    EXPECT_EQ(callbacks, 1);
    EXPECT_TRUE(callback_had_no_transaction);
    EXPECT_FALSE(premature_body);
    EXPECT_EQ(begin_attempts, 0);
    EXPECT_EQ(callback_result.state, state::refused);
    EXPECT_NE(callback_result.primary_error, nullptr);
    EXPECT_EQ(callback_result.cleanup_error, nullptr);
    EXPECT_FALSE(parent.db().is_in_transaction());
    const auto later = access::run(parent, [](auto& writer) {
        writer.execute("UPDATE main.TopologyFixture SET n=n+1 WHERE id=1");
    });
    EXPECT_EQ(later.state, state::committed);
    EXPECT_EQ(later.primary_error, nullptr);
    EXPECT_EQ(later.postcommit_error, nullptr);
    EXPECT_EQ(later.notification_error, nullptr);
    EXPECT_EQ(begin_attempts, 1);
    EXPECT_EQ(callbacks, 2);
    const auto rows = parent.db().query("SELECT n FROM main.TopologyFixture WHERE id=1");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("n")), 9);
    parent.remove_table_observer("TopologyFixture", token);
    EXPECT_NO_THROW(parent.detach(arm));
}
