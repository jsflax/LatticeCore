#include <gtest/gtest.h>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include "../../Sources/LatticeCore/src/store_physical_vfs.hpp"
#include <atomic>
#include <chrono>
#include <cerrno>
#include <condition_variable>
#include <cstdlib>
#include <cstdio>
#include <filesystem>
#include <future>
#include <mutex>
#include <thread>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#include <sys/wait.h>

namespace lattice::detail {
struct store_physical_lifetime_test_access {
    using pin=store_physical_lifetime::pin;
    static pin adopt(int actual){return store_physical_lifetime::adopt_lease_(actual);}
    static auto observe(const pin& actual){return store_physical_lifetime::observe_(actual);}
    static bool seal_native(const pin& actual,std::chrono::steady_clock::time_point end){return store_physical_lifetime::seal_native_and_wait_(actual,end);}
    static bool wait(const pin& actual,std::chrono::steady_clock::time_point end){return store_physical_lifetime::wait_until_(actual,end);}
};
struct store_physical_vfs_test_access {
    using tail_counts=store_physical_api::returned_tail_observation;
    static void tail_observation(tail_counts* actual)noexcept{store_physical_api::returned_tail_observation_=actual;}
    static void returned_tail()noexcept{store_physical_api::returned_tail();}
    static std::string actual_vfs_name(sqlite3* db){
        auto held=store_physical_api::capture_(db);auto* actual=store_physical_api::actual_owner_(held);
        if(!actual)throw std::runtime_error("component actual VFS original missing");return actual->name_;
    }
    static void mutex_returns(std::uint64_t* actual)noexcept{store_physical_api::mutex_return_observation_=actual;}
    static bool bound(const store_physical_open_original& owner,sqlite3* actual){return owner.matches_open_(actual);}
    // Component access to the SAME closed producer adapter. These controls do
    // not mint a managed target/Host or claim target ledger/file retirement.
    static bool issue_exclusive(store_physical_open_original& owner){return owner.issue_exclusive_target_(database_native_process());}
    static int open_exclusive(store_physical_open_original& owner,const char* path,sqlite3** actual,const char* vfs){
#ifdef SQLITE_OPEN_NOFOLLOW
        const int flags=SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX|SQLITE_OPEN_NOFOLLOW;
#else
        const int flags=SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX;
#endif
        return owner.open_exclusive_target_(path,actual,flags,vfs,std::chrono::steady_clock::now()+std::chrono::seconds(2));
    }
    static auto policy(const store_physical_open_original& owner){return store_physical_policy_original::capture_(owner);}
    static bool seal(const store_physical_open_original& owner,std::chrono::steady_clock::time_point end){return owner.seal_and_wait_(end);}
    static int open(const store_physical_lifetime::pin& actual,const std::string& path,const struct stat& expected,sqlite3** db,store_physical_open_original* original=nullptr){
        return store_physical_vfs::open_(actual,sqlite3_vfs_find(nullptr),path,expected,db,SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX,original);
    }
    // Component-only issuer: actual independently-held test lease + actual
    // Native VFS, not production normal-factory/cold-epoch admission evidence.
    static std::string quoted(const std::string& path){std::string out="'";for(char c:path){out+=c;if(c=='\'')out+='\'';}return out+"'";}
    static int attach(sqlite3* db,const store_physical_lifetime::pin& target,const std::string& path,
                      const struct stat& expected,const std::string& alias,std::int64_t token){
        auto prior=store_physical_api::capture_(db);auto* root=store_physical_api::actual_owner_(prior);
        if(!root)throw std::runtime_error("component actual MAIN original unavailable");
        auto* member=root->enroll_(db,target,path,expected,alias,token,sqlite3_limit(db,SQLITE_LIMIT_ATTACHED,-1));
        auto all=store_physical_api::capture_(db);store_physical_api::api_scope call(all);
        if(!call.entered()||!member->member_entered_(false))throw std::runtime_error("component actual ATTACH admission failed");
        const auto sql="ATTACH DATABASE "+quoted(path)+" AS \""+alias+"\"";
        const int rc=store_physical_exec(db,sql.c_str(),nullptr,nullptr,nullptr);
        if(!member->member_returned_(false,true,rc==SQLITE_OK))throw std::runtime_error("component ATTACH ending DATA failed");
        return rc;
    }
    static std::uint64_t actual_super_journal_opens(sqlite3* db){
        auto all=store_physical_api::capture_(db);auto* root=store_physical_api::actual_owner_(all);
        if(!root)throw std::runtime_error("component actual MAIN original unavailable");
        std::lock_guard lock(root->members_leaf_);return root->super_journal_opens_;
    }
    static int detach(sqlite3* db,const std::string& path,const std::string& alias,std::int64_t token){
        auto all=store_physical_api::capture_(db);auto* root=store_physical_api::actual_owner_(all);
        if(!root)throw std::runtime_error("component actual MAIN original unavailable");
        auto* member=root->find_member_(db,path,alias,token);store_physical_api::api_scope call(all);
        if(!call.entered()||!member->member_entered_(true))throw std::runtime_error("component DETACH original unavailable");
        const auto sql="DETACH DATABASE \""+alias+"\"";
        const int rc=store_physical_exec(db,sql.c_str(),nullptr,nullptr,nullptr);
        if(!member->member_returned_(true,true,rc==SQLITE_OK))throw std::runtime_error("component DETACH ending DATA failed");
        return rc;
    }
};
}
namespace {
using Access=lattice::detail::store_physical_lifetime_test_access;
using VFS=lattice::detail::store_physical_vfs_test_access;
using namespace lattice::detail;
struct OwnedStore {
    std::filesystem::path directory,path,lease;
    Access::pin physical;
    sqlite3* db=nullptr;
    store_physical_open_original original;
    struct stat expected{};
    int first=-1,fallback=-1;
    OwnedStore(){
        const char* root=std::getenv("TEST_TMPDIR");if(!root)root=std::getenv("TMPDIR");
        if(!root||!std::filesystem::path(root).is_absolute())throw std::runtime_error("owned explicit test tmp required");
        std::string pattern=(std::filesystem::path(root)/"physical-lifetime-XXXXXX").string();
        if(!::mkdtemp(pattern.data()))throw std::runtime_error("owned physical fixture directory failed");
        directory=pattern;path=directory/"store.sqlite";lease=directory/"physical.lock";
        // This test creates a NEW empty file before any SQLite user exists.
        // Production VFS code itself opens/closes no extra MAIN descriptor.
        int main=::open(path.c_str(),O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600);
        if(main<0||::fstat(main,&expected)!=0){if(main>=0)::close(main);throw std::runtime_error("fixture new MAIN failed");}
        if(::close(main)!=0)throw std::runtime_error("fixture initial MAIN close failed");
        int locked=::open(lease.c_str(),O_RDONLY|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600);
        if(locked<0||::flock(locked,LOCK_SH|LOCK_NB)!=0){if(locked>=0)::close(locked);throw std::runtime_error("fixture SH lease failed");}
        try{physical=Access::adopt(locked);}catch(...){::close(locked);throw;}
    }
    int open(){return VFS::open(physical,path.string(),expected,&db,&original);}
    void close(){auto* actual=std::exchange(db,nullptr);if(!actual)return;
        first=store_physical_close_owned(original,actual);if(first!=SQLITE_OK)fallback=store_physical_close_v2_owned(original,actual);}
    bool exclusive_available(){
        int actual=::open(lease.c_str(),O_RDONLY|O_NOFOLLOW|O_CLOEXEC);
        if(actual<0)throw std::runtime_error("fixture independent lease observation failed");
        errno=0;const int rc=::flock(actual,LOCK_EX|LOCK_NB);const int error=errno;
        const int ended=::close(actual);if(ended!=0)throw std::runtime_error("fixture observation close failed");
        if(rc==0)return true;if(error==EWOULDBLOCK||error==EAGAIN)return false;
        throw std::runtime_error("fixture independent lease observation unknown");
    }
    ~OwnedStore(){
        close();const auto ended=Access::observe(physical);
        // No recursive removal while any actual resource ending is unknown.
        if(!ended.physical_ended||ended.unknown){ADD_FAILURE()<<"physical fixture retains actual ending debt";return;}
        std::error_code error;std::filesystem::remove_all(directory,error);EXPECT_FALSE(error);
    }
};
struct Statement {
    sqlite3_stmt* value=nullptr;
    explicit Statement(sqlite3* db,const char* sql){if(sqlite3_prepare_v2(db,sql,-1,&value,nullptr)!=SQLITE_OK)throw std::runtime_error("real fixture prepare failed");}
    int finish(){return store_physical_finalize(std::exchange(value,nullptr));}
    ~Statement(){if(value)store_physical_finalize(value);}
};
TEST(StorePhysicalLifetime, RealCleanCloseReturnsIndependentLeaseWithoutFacadeDestruction){
    OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);
    ASSERT_EQ(store_physical_exec(store.db,"CREATE TABLE row_value(id INTEGER PRIMARY KEY);INSERT INTO row_value VALUES(1)",nullptr,nullptr,nullptr),SQLITE_OK);
    EXPECT_FALSE(store.exclusive_available());
    store.close();const auto actual=Access::observe(store.physical);
    EXPECT_EQ(store.first,SQLITE_OK);EXPECT_EQ(actual.first_native_close,SQLITE_OK);
    EXPECT_TRUE(actual.physical_ended);EXPECT_FALSE(actual.unknown);EXPECT_EQ(actual.files,0u);
    EXPECT_TRUE(store.exclusive_available()); // same still-live DATA facade
}
TEST(StorePhysicalLifetime, RealBusyAndDeferredCloseKeepFirstDebtUntilLastStatementReturn){
    OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);
    Statement statement(store.db,"SELECT 41");ASSERT_EQ(store_physical_step(statement.value),SQLITE_ROW);
    store.close();const auto pending=Access::observe(store.physical);
    EXPECT_EQ(store.first,SQLITE_BUSY);EXPECT_EQ(store.fallback,SQLITE_OK);
    EXPECT_EQ(pending.first_native_close,SQLITE_BUSY);EXPECT_EQ(pending.fallback_close,SQLITE_OK);
    EXPECT_FALSE(pending.physical_ended);EXPECT_FALSE(store.exclusive_available());
    ASSERT_EQ(statement.finish(),SQLITE_OK);
    const auto ended=Access::observe(store.physical);
    EXPECT_EQ(ended.first_native_close,SQLITE_BUSY);EXPECT_TRUE(ended.physical_ended);
    EXPECT_FALSE(ended.unknown);EXPECT_TRUE(store.exclusive_available());
}
TEST(StorePhysicalLifetime, CapturedOpenOriginalNeverBecomesLegacyAfterActualEnding){
    OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);auto& original=store.original;
    auto* const actual=store.db;ASSERT_TRUE(VFS::bound(original,actual));
    {store_physical_native_scope admitted(original,actual);ASSERT_TRUE(admitted.entered());}
    store.close();ASSERT_TRUE(Access::observe(store.physical).physical_ended);
    // The still-live DATA original cannot turn a stale address into a Native
    // permission merely because its registry row has genuinely retired.
    store_physical_native_scope denied(original,actual);EXPECT_FALSE(denied.entered());
    EXPECT_FALSE(Access::observe(store.physical).unknown);EXPECT_TRUE(store.exclusive_available());
}
TEST(StorePhysicalLifetime, SealedActualOriginalRefusesFreshAdmissionButKeepsCapturedContinuation){
    OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);auto& original=store.original;
    ASSERT_TRUE(VFS::bound(original,store.db));
    store_physical_native_scope admitted(original,store.db);ASSERT_TRUE(admitted.entered());
    bool sealed=false,fresh=true;std::atomic<bool> worker_done{false},offer_done{false};std::thread worker,offered;
    struct Ending {
        std::thread& worker;std::thread& offered;store_physical_native_scope& active;
        std::atomic<bool>& worker_done;std::atomic<bool>& offer_done;
        ~Ending()noexcept{
            active.reset();const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(4);
            while(((worker.joinable()&&!worker_done.load(std::memory_order_acquire))||
                  (offered.joinable()&&!offer_done.load(std::memory_order_acquire)))&&std::chrono::steady_clock::now()<end)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            if((worker.joinable()&&!worker_done.load(std::memory_order_acquire))||
               (offered.joinable()&&!offer_done.load(std::memory_order_acquire))){
                std::fputs("physical-fixture-ending-debt:native-original-thread-unreturned\n",stderr);::_exit(95);}
            try{if(worker.joinable())worker.join();if(offered.joinable())offered.join();}
            catch(...){std::fputs("physical-fixture-ending-debt:native-original-join-unproved\n",stderr);::_exit(95);}
        }
    } ending{worker,offered,admitted,worker_done,offer_done};
    worker=std::thread([&]{sealed=VFS::seal(original,std::chrono::steady_clock::now()+std::chrono::seconds(2));worker_done.store(true,std::memory_order_release);});
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(1);
    while(!Access::observe(store.physical).native_sealed&&std::chrono::steady_clock::now()<end)std::this_thread::yield();
    ASSERT_TRUE(Access::observe(store.physical).native_sealed);
    offered=std::thread([&]{store_physical_native_scope frame(original,store.db);fresh=frame.entered();offer_done.store(true,std::memory_order_release);});
    const auto offer_end=std::chrono::steady_clock::now()+std::chrono::seconds(1);
    while(!offer_done.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<offer_end)std::this_thread::yield();
    ASSERT_TRUE(offer_done.load(std::memory_order_acquire));offered.join();
    EXPECT_FALSE(fresh);
    {store_physical_native_scope nested(original,store.db);EXPECT_TRUE(nested.entered());}
    admitted.reset();const auto worker_end=std::chrono::steady_clock::now()+std::chrono::seconds(3);
    while(!worker_done.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<worker_end)std::this_thread::yield();
    ASSERT_TRUE(worker_done.load(std::memory_order_acquire));worker.join();EXPECT_TRUE(sealed);
    store_physical_native_scope denied(original,store.db);EXPECT_FALSE(denied.entered());
    store.close();EXPECT_EQ(store.first,SQLITE_OK);EXPECT_FALSE(Access::observe(store.physical).unknown);
}
TEST(StorePhysicalLifetime, RefusedMultiOriginalFrameCannotLendPrimaryContinuationAfterActualDetach){
    OwnedStore target;OwnedStore main;ASSERT_EQ(main.open(),SQLITE_OK);
    auto& original=main.original;ASSERT_TRUE(VFS::bound(original,main.db));
    ASSERT_EQ(VFS::attach(main.db,target.physical,target.path.string(),target.expected,"secondary",19),SQLITE_OK);
    ASSERT_TRUE(Access::seal_native(target.physical,std::chrono::steady_clock::now()+std::chrono::seconds(1)));
    // Genuine primary admission and genuine target refusal create a PARTIAL
    // multi-original frame; it is not an admitted calling unit.
    store_physical_native_scope refused(original,main.db);ASSERT_FALSE(refused.entered());
    EXPECT_EQ(Access::observe(main.physical).active_native,1u);
    EXPECT_EQ(Access::observe(target.physical).active_native,0u);
    bool sealed=false;std::atomic<bool> returned{false};std::thread worker;
    struct Ending {
        store_physical_native_scope& frame;std::thread& worker;std::atomic<bool>& returned;
        ~Ending()noexcept{
            frame.reset();const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(4);
            while(worker.joinable()&&!returned.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<end)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            if(worker.joinable()&&!returned.load(std::memory_order_acquire)){
                std::fputs("physical-fixture-ending-debt:partial-frame-seal-unreturned\n",stderr);::_exit(95);}
            try{if(worker.joinable())worker.join();}
            catch(...){std::fputs("physical-fixture-ending-debt:partial-frame-join-unproved\n",stderr);::_exit(95);}
        }
    } ending{refused,worker,returned};
    worker=std::thread([&]{sealed=VFS::seal(original,std::chrono::steady_clock::now()+std::chrono::seconds(2));returned.store(true,std::memory_order_release);});
    const auto sealed_end=std::chrono::steady_clock::now()+std::chrono::seconds(1);
    while(!Access::observe(main.physical).native_sealed&&std::chrono::steady_clock::now()<sealed_end)std::this_thread::yield();
    ASSERT_TRUE(Access::observe(main.physical).native_sealed);
    // This exact private producer performs real cleanup of the typed target.
    // The next capture has MAIN alone; it must not borrow the refused frame.
    ASSERT_EQ(VFS::detach(main.db,target.path.string(),"secondary",19),SQLITE_OK);
    ASSERT_TRUE(Access::observe(target.physical).physical_ended);
    {store_physical_native_scope fresh(original,main.db);EXPECT_FALSE(fresh.entered());}
    refused.reset();const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(3);
    while(!returned.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<end)std::this_thread::yield();
    ASSERT_TRUE(returned.load(std::memory_order_acquire));worker.join();EXPECT_TRUE(sealed);
    main.close();EXPECT_EQ(main.first,SQLITE_OK);EXPECT_TRUE(main.exclusive_available());EXPECT_TRUE(target.exclusive_available());
}
TEST(StorePhysicalLifetime, ActualNativeReentryRefusesOwnSealWithoutStrandingOriginal){
    OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);auto& original=store.original;
    ASSERT_TRUE(VFS::bound(original,store.db));
    {store_physical_native_scope frame(original,store.db);ASSERT_TRUE(frame.entered());
        EXPECT_FALSE(VFS::seal(original,std::chrono::steady_clock::now()+std::chrono::seconds(1)));
        EXPECT_FALSE(Access::observe(store.physical).native_sealed);}
    EXPECT_TRUE(VFS::seal(original,std::chrono::steady_clock::now()+std::chrono::seconds(1)));
    store.close();EXPECT_EQ(store.first,SQLITE_OK);EXPECT_TRUE(Access::observe(store.physical).physical_ended);
}
TEST(StorePhysicalLifetime, MovedOpenOriginalCannotRebindOrAdoptAnotherActualConnection){
    OwnedStore one;OwnedStore two;ASSERT_EQ(one.open(),SQLITE_OK);ASSERT_EQ(two.open(),SQLITE_OK);
    auto& original=one.original;ASSERT_TRUE(VFS::bound(original,one.db));
    store_physical_open_original moved(std::move(original));EXPECT_FALSE(VFS::bound(moved,two.db));
    sqlite3* refused=nullptr;
    EXPECT_EQ(VFS::open(one.physical,one.path.string(),one.expected,&refused,&moved),SQLITE_MISUSE);EXPECT_EQ(refused,nullptr);
    {store_physical_native_scope wrong(moved,two.db);EXPECT_FALSE(wrong.entered());}
    {store_physical_native_scope right(moved,one.db);EXPECT_TRUE(right.entered());}
    // A moved-from record has no actual-open provenance for a nonnull handle.
    {store_physical_native_scope old(original,one.db);EXPECT_FALSE(old.entered());}
    one.first=store_physical_close_owned(moved,std::exchange(one.db,nullptr));two.close();EXPECT_EQ(one.first,SQLITE_OK);EXPECT_TRUE(one.exclusive_available());EXPECT_TRUE(two.exclusive_available());
}
TEST(StorePhysicalLifetime, ObserverPolicyDebtSurvivesRollbackAndMoveWithoutBlockingActualClose){
    OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);auto& original=store.original;
    store_physical_open_original absent;EXPECT_TRUE(absent.observer_policy_unsettled());ASSERT_TRUE(VFS::bound(original,store.db));
    EXPECT_FALSE(original.observer_policy_unsettled());
    ASSERT_EQ(store_physical_exec(store.db,"BEGIN;CREATE TABLE rolled_back(id INTEGER)",nullptr,nullptr,nullptr),SQLITE_OK);
    original.note_observer_policy_unsettled();EXPECT_TRUE(original.observer_policy_unsettled());
    ASSERT_EQ(store_physical_exec(store.db,"ROLLBACK",nullptr,nullptr,nullptr),SQLITE_OK);
    store_physical_open_original moved(std::move(original));EXPECT_TRUE(moved.observer_policy_unsettled());
    EXPECT_TRUE(original.observer_policy_unsettled());store.first=store_physical_close_owned(moved,std::exchange(store.db,nullptr));EXPECT_EQ(store.first,SQLITE_OK);
    EXPECT_TRUE(Access::observe(store.physical).physical_ended);EXPECT_FALSE(Access::observe(store.physical).unknown);
    EXPECT_TRUE(store.exclusive_available());
}
TEST(StorePhysicalLifetime, ActualPartialOpenCleanupUsesProducerOriginalBeforeAnyRegistryLookup){
    OwnedStore store;auto wrong=store.expected;++wrong.st_ino;
    ASSERT_NE(VFS::open(store.physical,store.path.string(),wrong,&store.db,&store.original),SQLITE_OK);
    ASSERT_NE(store.db,nullptr);ASSERT_TRUE(VFS::bound(store.original,store.db));
    store.close();EXPECT_EQ(store.first,SQLITE_OK);EXPECT_TRUE(Access::observe(store.physical).physical_ended);
    EXPECT_FALSE(Access::observe(store.physical).unknown);EXPECT_TRUE(store.exclusive_available());
}
TEST(StorePhysicalLifetime, OwningCleanupKeepsRealBusyThenRejectsRetiredPointerWithoutNativeEntry){
    OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);Statement statement(store.db,"SELECT 41");
    ASSERT_EQ(store_physical_step(statement.value),SQLITE_ROW);auto* actual=store.db;
    EXPECT_EQ(store_physical_close_owned(store.original,actual),SQLITE_BUSY);
    EXPECT_EQ(store_physical_close_v2_owned(store.original,actual),SQLITE_OK);store.db=nullptr;
    EXPECT_EQ(store_physical_close_owned(store.original,actual),SQLITE_MISUSE);
    {store_physical_native_scope denied(store.original,actual);EXPECT_FALSE(denied.entered());}
    ASSERT_EQ(statement.finish(),SQLITE_OK);const auto ended=Access::observe(store.physical);
    EXPECT_EQ(ended.first_native_close,SQLITE_BUSY);EXPECT_EQ(ended.fallback_close,SQLITE_OK);
    EXPECT_TRUE(ended.physical_ended);EXPECT_FALSE(ended.unknown);EXPECT_TRUE(store.exclusive_available());
}
TEST(StorePhysicalLifetime, GenuineMemoryProducerNeverMintsResourceHealthOrRepeatedCleanup){
    store_physical_open_original original;sqlite3* native=nullptr;
    struct Close {store_physical_open_original& original;sqlite3*& native;~Close(){if(native){auto* p=std::exchange(native,nullptr);if(store_physical_close_owned(original,p)!=SQLITE_OK)store_physical_close_v2_owned(original,p);}}} cleanup{original,native};
    ASSERT_EQ(store_physical_open_v2_owned_at(":memory:",&native,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_FULLMUTEX,nullptr,
        std::chrono::steady_clock::now()+std::chrono::seconds(2),original),SQLITE_OK);
    EXPECT_TRUE(original.observer_policy_unsettled());ASSERT_TRUE(VFS::bound(original,native));
    auto* actual=std::exchange(native,nullptr);ASSERT_EQ(store_physical_close_owned(original,actual),SQLITE_OK);
    EXPECT_EQ(store_physical_close_owned(original,actual),SQLITE_MISUSE);
    {store_physical_native_scope denied(original,actual);EXPECT_FALSE(denied.entered());}
}
TEST(StorePhysicalLifetime, CapturedPolicyDataSharesStickyDebtAcrossMoveRollbackAndRealClose){
    OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);auto token=VFS::policy(store.original);
    EXPECT_FALSE(token.observer_policy_unsettled());
    ASSERT_EQ(store_physical_exec(store.db,"BEGIN;CREATE TABLE policy_rollback(globalId TEXT)",nullptr,nullptr,nullptr),SQLITE_OK);
    token.note_observer_policy_unsettled();EXPECT_TRUE(store.original.observer_policy_unsettled());
    ASSERT_EQ(store_physical_exec(store.db,"ROLLBACK",nullptr,nullptr,nullptr),SQLITE_OK);
    store_physical_policy_original moved(std::move(token));EXPECT_TRUE(token.observer_policy_unsettled());
    EXPECT_TRUE(moved.observer_policy_unsettled());store.close();
    EXPECT_EQ(store.first,SQLITE_OK);EXPECT_TRUE(Access::observe(store.physical).physical_ended);
    EXPECT_TRUE(moved.observer_policy_unsettled());EXPECT_TRUE(store.exclusive_available());
    auto too_late=VFS::policy(store.original);EXPECT_TRUE(too_late.observer_policy_unsettled());
}
TEST(StorePhysicalLifetime, ActualSqliteOwnedPolicyDataSurvivesBusyZombieUntilFinalStatementDestructor){
    std::atomic<bool> destroyed{false};OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);
    struct Cookie {
        const std::uint64_t creator=database_native_process();store_physical_policy_original original;std::atomic<bool>* destroyed;
        Cookie(store_physical_policy_original token,std::atomic<bool>& ended):original(std::move(token)),destroyed(&ended){}
        ~Cookie(){if(creator==database_native_process())destroyed->store(true,std::memory_order_release);}
    };
    auto* cookie=new Cookie(VFS::policy(store.original),destroyed);
    ASSERT_EQ(store_physical_create_function_v2(store.db,"policy_data",0,SQLITE_UTF8,cookie,
        [](sqlite3_context* context,int,sqlite3_value**){auto* value=static_cast<Cookie*>(sqlite3_user_data(context));
            if(value->creator!=database_native_process())return; // no additional inherited Native API
            sqlite3_result_int(context,value->original.observer_policy_unsettled()?1:0);},nullptr,nullptr,
        [](void* raw){auto* value=static_cast<Cookie*>(raw);if(value->creator==database_native_process())delete value;}),SQLITE_OK);
    Statement statement(store.db,"SELECT policy_data()");ASSERT_EQ(store_physical_step(statement.value),SQLITE_ROW);
    store.original.note_observer_policy_unsettled();EXPECT_TRUE(cookie->original.observer_policy_unsettled());
    store.close();EXPECT_EQ(store.first,SQLITE_BUSY);EXPECT_EQ(store.fallback,SQLITE_OK);
    // Simulate destruction of the wrapper's DATA field while SQLite keeps its
    // actual zombie statement/UDF context. The cookie owns no wrapper/Native.
    store.original=store_physical_open_original{};EXPECT_FALSE(destroyed.load(std::memory_order_acquire));
    ASSERT_EQ(statement.finish(),SQLITE_OK);EXPECT_TRUE(destroyed.load(std::memory_order_acquire));
    const auto ended=Access::observe(store.physical);EXPECT_EQ(ended.first_native_close,SQLITE_BUSY);
    EXPECT_TRUE(ended.physical_ended);EXPECT_FALSE(ended.unknown);EXPECT_TRUE(store.exclusive_available());
}
TEST(StorePhysicalLifetime, RealV2FailureConsumesActualPolicyCookieExactlyOnce){
    std::atomic<unsigned> destroyed{0};OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);
    struct Cookie {store_physical_policy_original token;std::atomic<unsigned>* destroyed;
        Cookie(store_physical_policy_original value,std::atomic<unsigned>& count):token(std::move(value)),destroyed(&count){}
        ~Cookie(){destroyed->fetch_add(1,std::memory_order_release);}};
    auto* cookie=new Cookie(VFS::policy(store.original),destroyed);
    // Genuine SQLite invalid-arity failure: the adapter must delegate actual
    // entry's consumption to SQLite, never destroy this userdata a second time.
    EXPECT_NE(store_physical_create_function_v2(store.db,"invalid_policy",-2,SQLITE_UTF8,cookie,
        [](sqlite3_context* context,int,sqlite3_value**){sqlite3_result_null(context);},nullptr,nullptr,
        [](void* raw){delete static_cast<Cookie*>(raw);}),SQLITE_OK);
    EXPECT_EQ(destroyed.load(std::memory_order_acquire),1u);store.close();
    EXPECT_EQ(destroyed.load(std::memory_order_acquire),1u);EXPECT_EQ(store.first,SQLITE_OK);
    EXPECT_TRUE(Access::observe(store.physical).physical_ended);EXPECT_TRUE(store.exclusive_available());
}
TEST(StorePhysicalLifetime, SameActualFileHasSeparateOriginalsAndKernelAggregateEnding){
    OwnedStore one;ASSERT_EQ(one.open(),SQLITE_OK);
    int locked=::open(one.lease.c_str(),O_RDONLY|O_NOFOLLOW|O_CLOEXEC);
    struct LeaseFd {int& fd;~LeaseFd(){if(fd>=0)::close(fd);}} lease_ending{locked};
    ASSERT_GE(locked,0);ASSERT_EQ(::flock(locked,LOCK_SH|LOCK_NB),0);
    auto two=Access::adopt(locked);locked=-1;
    sqlite3* second=nullptr;
    struct Close {sqlite3*& p;~Close(){if(p){const int rc=store_physical_close(p);if(rc!=SQLITE_OK)store_physical_close_v2(p);}}} cleanup{second};
    ASSERT_EQ(VFS::open(two,one.path.string(),one.expected,&second),SQLITE_OK);
    one.close();EXPECT_TRUE(Access::observe(one.physical).physical_ended);EXPECT_FALSE(one.exclusive_available());
    ASSERT_EQ(store_physical_close(std::exchange(second,nullptr)),SQLITE_OK);
    EXPECT_TRUE(Access::observe(two).physical_ended);EXPECT_TRUE(one.exclusive_available());
}
TEST(StorePhysicalLifetime, WrongIssuedMainIdentityRefusesAndStillEndsActualPartialOpen){
    OwnedStore store;auto wrong=store.expected;++wrong.st_ino;
    EXPECT_NE(VFS::open(store.physical,store.path.string(),wrong,&store.db,&store.original),SQLITE_OK);
    store.close();const auto actual=Access::observe(store.physical);
    EXPECT_TRUE(actual.physical_ended);EXPECT_FALSE(actual.unknown);EXPECT_TRUE(store.exclusive_available());
}
struct CleanupPause {
    std::mutex mutex;std::condition_variable changed;bool entered=false,released=false,timed_out=false;
    void wait(){std::unique_lock lock(mutex);entered=true;changed.notify_all();if(!changed.wait_for(lock,std::chrono::seconds(5),[&]{return released;}))timed_out=true;}
    bool arrived(){std::unique_lock lock(mutex);return changed.wait_for(lock,std::chrono::seconds(5),[&]{return entered;});}
    void release(){std::lock_guard lock(mutex);released=true;changed.notify_all();}
};
TEST(StorePhysicalLifetime, GenuineSqliteDestructorTailRetainsLeaseUntilActualCallReturns){
    CleanupPause pause;OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);
    ASSERT_EQ(sqlite3_create_function_v2(store.db,"retained_tail",0,SQLITE_UTF8,&pause,
        [](sqlite3_context* c,int,sqlite3_value**){sqlite3_result_null(c);},nullptr,nullptr,
        [](void* p){static_cast<CleanupPause*>(p)->wait();}),SQLITE_OK);
    int actual_rc=SQLITE_MISUSE;auto* actual=std::exchange(store.db,nullptr);std::atomic<bool> returned{false};
    std::thread worker;
    struct Ending {
        CleanupPause& p;std::thread& t;std::atomic<bool>& returned;
        ~Ending()noexcept{
            p.release();if(!t.joinable())return;
            const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
            while(!returned.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<end)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            if(!returned.load(std::memory_order_acquire)){std::fputs("physical-fixture-ending-debt:cleanup-call-unreturned\n",stderr);::_exit(95);}
            try{t.join();}catch(...){std::fputs("physical-fixture-ending-debt:join-unproved\n",stderr);::_exit(95);}
        }
    } ending{pause,worker,returned};
    try{worker=std::thread([&]{actual_rc=store_physical_close(actual);returned.store(true,std::memory_order_release);});}
    catch(...){store.db=actual;throw;}
    ASSERT_TRUE(pause.arrived());EXPECT_FALSE(Access::observe(store.physical).physical_ended);
    EXPECT_FALSE(store.exclusive_available());pause.release();
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
    while(!returned.load(std::memory_order_acquire)&&std::chrono::steady_clock::now()<end)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_TRUE(returned.load(std::memory_order_acquire));worker.join();
    EXPECT_EQ(actual_rc,SQLITE_OK);EXPECT_FALSE(pause.timed_out);
    EXPECT_TRUE(Access::observe(store.physical).physical_ended);EXPECT_TRUE(store.exclusive_available());
}
TEST(StorePhysicalLifetime, ReentrantWaitInsideActualNativeDestructorRefusesOwnTail){
    struct State {const Access::pin* actual=nullptr;bool refused=false;} state;
    OwnedStore store;state.actual=&store.physical;ASSERT_EQ(store.open(),SQLITE_OK);
    ASSERT_EQ(sqlite3_create_function_v2(store.db,"reentrant_tail",0,SQLITE_UTF8,&state,
        [](sqlite3_context* c,int,sqlite3_value**){sqlite3_result_null(c);},nullptr,nullptr,
        [](void* p){auto& s=*static_cast<State*>(p);s.refused=!Access::wait(*s.actual,std::chrono::steady_clock::now()+std::chrono::seconds(1));}),SQLITE_OK);
    store.close();EXPECT_TRUE(state.refused);EXPECT_TRUE(Access::observe(store.physical).physical_ended);
    EXPECT_TRUE(store.exclusive_available());
}
TEST(StorePhysicalLifetime, GenuineWalTypedDetachReleasesOnlyTargetLeaseWhileMainContinues){
    OwnedStore target;OwnedStore main;ASSERT_EQ(main.open(),SQLITE_OK);
    ASSERT_EQ(store_physical_exec(main.db,"PRAGMA main.journal_mode=WAL;CREATE TABLE main.row_value(id INTEGER PRIMARY KEY);INSERT INTO main.row_value VALUES(41)",nullptr,nullptr,nullptr),SQLITE_OK);
    ASSERT_EQ(VFS::attach(main.db,target.physical,target.path.string(),target.expected,"secondary",1),SQLITE_OK);
    ASSERT_EQ(store_physical_exec(main.db,"PRAGMA secondary.journal_mode=WAL;CREATE TABLE secondary.row_value(id INTEGER PRIMARY KEY);INSERT INTO secondary.row_value VALUES(42)",nullptr,nullptr,nullptr),SQLITE_OK);
    {Statement read(main.db,"SELECT id FROM secondary.row_value");ASSERT_EQ(store_physical_step(read.value),SQLITE_ROW);EXPECT_EQ(sqlite3_column_int(read.value,0),42);}
    EXPECT_FALSE(main.exclusive_available());EXPECT_FALSE(target.exclusive_available());
    ASSERT_EQ(VFS::detach(main.db,target.path.string(),"secondary",1),SQLITE_OK);
    const auto detached=Access::observe(target.physical);
    EXPECT_TRUE(detached.attached_original);EXPECT_TRUE(detached.attach_returned);EXPECT_TRUE(detached.detach_returned);
    EXPECT_TRUE(detached.physical_ended);EXPECT_FALSE(detached.unknown);EXPECT_EQ(detached.first_native_close,-1);
    EXPECT_TRUE(target.exclusive_available());EXPECT_FALSE(main.exclusive_available());
    {Statement read(main.db,"SELECT id FROM main.row_value");ASSERT_EQ(store_physical_step(read.value),SQLITE_ROW);EXPECT_EQ(sqlite3_column_int(read.value,0),41);}
    main.close();EXPECT_TRUE(Access::observe(main.physical).physical_ended);
}
TEST(StorePhysicalLifetime, DeferredClosePinsEveryAttachedOriginalUntilActualStatementReturn){
    OwnedStore target;OwnedStore main;ASSERT_EQ(main.open(),SQLITE_OK);
    ASSERT_EQ(VFS::attach(main.db,target.physical,target.path.string(),target.expected,"secondary",2),SQLITE_OK);
    ASSERT_EQ(store_physical_exec(main.db,"PRAGMA secondary.journal_mode=WAL;CREATE TABLE secondary.row_value(id INTEGER PRIMARY KEY);INSERT INTO secondary.row_value VALUES(7)",nullptr,nullptr,nullptr),SQLITE_OK);
    Statement retained(main.db,"SELECT id FROM secondary.row_value");ASSERT_EQ(store_physical_step(retained.value),SQLITE_ROW);
    main.close();EXPECT_EQ(main.first,SQLITE_BUSY);EXPECT_EQ(main.fallback,SQLITE_OK);
    EXPECT_FALSE(main.exclusive_available());EXPECT_FALSE(target.exclusive_available());
    const auto pending=Access::observe(target.physical);EXPECT_EQ(pending.first_native_close,SQLITE_BUSY);EXPECT_EQ(pending.fallback_close,SQLITE_OK);
    ASSERT_EQ(retained.finish(),SQLITE_OK);
    const auto ended=Access::observe(target.physical);EXPECT_EQ(ended.first_native_close,SQLITE_BUSY);
    EXPECT_TRUE(ended.physical_ended);EXPECT_FALSE(ended.unknown);EXPECT_FALSE(ended.detach_returned);
    EXPECT_TRUE(target.exclusive_available());EXPECT_TRUE(main.exclusive_available());
}
TEST(StorePhysicalLifetime, RealLockedDetachKeepsTargetUntilActualSuccessfulRetry){
    OwnedStore target;OwnedStore main;ASSERT_EQ(main.open(),SQLITE_OK);
    ASSERT_EQ(VFS::attach(main.db,target.physical,target.path.string(),target.expected,"secondary",3),SQLITE_OK);
    ASSERT_EQ(store_physical_exec(main.db,"CREATE TABLE secondary.row_value(id INTEGER PRIMARY KEY);INSERT INTO secondary.row_value VALUES(8)",nullptr,nullptr,nullptr),SQLITE_OK);
    Statement retained(main.db,"SELECT id FROM secondary.row_value");ASSERT_EQ(store_physical_step(retained.value),SQLITE_ROW);
    EXPECT_NE(VFS::detach(main.db,target.path.string(),"secondary",3),SQLITE_OK);
    EXPECT_FALSE(Access::observe(target.physical).physical_ended);EXPECT_FALSE(target.exclusive_available());
    ASSERT_EQ(retained.finish(),SQLITE_OK);
    ASSERT_EQ(VFS::detach(main.db,target.path.string(),"secondary",3),SQLITE_OK);
    EXPECT_TRUE(Access::observe(target.physical).physical_ended);EXPECT_FALSE(Access::observe(target.physical).unknown);
    EXPECT_TRUE(target.exclusive_available());EXPECT_FALSE(main.exclusive_available());main.close();
}
TEST(StorePhysicalLifetime, WrongTypedTargetIdentityRefusesBeforeAnyAcceptedTargetAccess){
    OwnedStore target;OwnedStore main;ASSERT_EQ(main.open(),SQLITE_OK);
    auto wrong=target.expected;++wrong.st_ino;
    EXPECT_NE(VFS::attach(main.db,target.physical,target.path.string(),wrong,"secondary",4),SQLITE_OK);
    EXPECT_FALSE(Access::observe(target.physical).physical_ended);EXPECT_FALSE(target.exclusive_available());
    main.close();
    EXPECT_TRUE(Access::observe(target.physical).physical_ended);EXPECT_FALSE(Access::observe(target.physical).unknown);
    EXPECT_TRUE(target.exclusive_available());
}
TEST(StorePhysicalLifetime, RealNonWalMultiStoreCommitRetainsActualSuperJournalInEveryOriginal){
    OwnedStore target;OwnedStore main;ASSERT_EQ(main.open(),SQLITE_OK);
    ASSERT_EQ(VFS::attach(main.db,target.physical,target.path.string(),target.expected,"secondary",5),SQLITE_OK);
    ASSERT_EQ(store_physical_exec(main.db,"PRAGMA main.journal_mode=DELETE;PRAGMA secondary.journal_mode=DELETE;CREATE TABLE main.row_value(id INTEGER PRIMARY KEY);CREATE TABLE secondary.row_value(id INTEGER PRIMARY KEY);BEGIN;INSERT INTO main.row_value VALUES(51);INSERT INTO secondary.row_value VALUES(52);COMMIT",nullptr,nullptr,nullptr),SQLITE_OK);
    EXPECT_GT(VFS::actual_super_journal_opens(main.db),0u);
    EXPECT_FALSE(Access::observe(main.physical).unknown);EXPECT_FALSE(Access::observe(target.physical).unknown);
    {Statement read(main.db,"SELECT (SELECT id FROM main.row_value)+(SELECT id FROM secondary.row_value)");ASSERT_EQ(store_physical_step(read.value),SQLITE_ROW);EXPECT_EQ(sqlite3_column_int(read.value,0),103);}
    ASSERT_EQ(VFS::detach(main.db,target.path.string(),"secondary",5),SQLITE_OK);
    EXPECT_TRUE(target.exclusive_available());EXPECT_FALSE(main.exclusive_available());main.close();
    EXPECT_TRUE(Access::observe(target.physical).physical_ended);EXPECT_TRUE(Access::observe(main.physical).physical_ended);
    EXPECT_FALSE(Access::observe(target.physical).unknown);EXPECT_FALSE(Access::observe(main.physical).unknown);
}
TEST(StorePhysicalLifetime, ActualCloseCannotRetireLeaseBeforeItsLexicalCallerEndingScope){
    OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);
    store_physical_native_scope caller(store.db);ASSERT_TRUE(caller.entered());
    store.close();
    const auto closed=Access::observe(store.physical);
    EXPECT_EQ(closed.first_native_close,SQLITE_OK);EXPECT_EQ(closed.files,0u);
    EXPECT_EQ(closed.active_api,0u);EXPECT_EQ(closed.active_native,1u);
    EXPECT_FALSE(closed.physical_ended);EXPECT_FALSE(store.exclusive_available());
    caller.reset();
    const auto ended=Access::observe(store.physical);EXPECT_EQ(ended.active_native,0u);
    EXPECT_EQ(ended.first_native_close,SQLITE_OK);EXPECT_TRUE(ended.physical_ended);EXPECT_FALSE(ended.unknown);
    EXPECT_TRUE(store.exclusive_available());
}
}
TEST(StorePhysicalLifetime, SealedCleanupPinsActualEndingWithoutLendingFreshSqlAdmission){
    OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);
    ASSERT_TRUE(VFS::seal(store.original,std::chrono::steady_clock::now()+std::chrono::seconds(1)));
    {
        store_physical_cleanup_scope cleanup(store.original,store.db);
        ASSERT_TRUE(cleanup.entered());
        EXPECT_FALSE(VFS::seal(store.original,std::chrono::steady_clock::now()+std::chrono::seconds(1)));
        store_physical_native_scope denied(store.original,store.db);
        EXPECT_FALSE(denied.entered()); // cleanup cannot become public SQL continuation
        store.close();
        const auto pending=Access::observe(store.physical);
        EXPECT_EQ(pending.first_native_close,SQLITE_OK);
        EXPECT_GT(pending.active_native,0u);
        EXPECT_FALSE(pending.physical_ended);
        EXPECT_FALSE(store.exclusive_available());
    }
    const auto ended=Access::observe(store.physical);
    EXPECT_EQ(ended.first_native_close,SQLITE_OK);
    EXPECT_EQ(ended.active_native,0u);
    EXPECT_TRUE(ended.physical_ended);
    EXPECT_FALSE(ended.unknown);
    EXPECT_TRUE(store.exclusive_available());
}
TEST(StorePhysicalLifetime, ActualMutexReturnRejectsForkChildAndParentStillReturnsExactlyOnce){
    OwnedStore store;ASSERT_EQ(store.open(),SQLITE_OK);
    store_physical_native_scope actual(store.original,store.db);ASSERT_TRUE(actual.entered());
    auto* mutex=sqlite3_db_mutex(store.db);ASSERT_NE(mutex,nullptr);
    std::uint64_t returns=0;VFS::mutex_returns(&returns);
    struct ResetObservation {~ResetObservation(){VFS::mutex_returns(nullptr);}} reset;
    sqlite3_mutex_enter(mutex);
    store_physical_mutex_return returned(store.original,mutex);
    const pid_t child=::fork();
    if(child==0){returned.reset();::_exit(returns==0?0:41);}
    returned.reset(); // Actual parent Native return, before child-ending checks.
    if(child<0){ADD_FAILURE()<<"actual fork unavailable";return;}
    int status=0;bool reaped=false;const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(4);
    while(std::chrono::steady_clock::now()<end){
        const auto result=::waitpid(child,&status,WNOHANG);
        if(result==child){reaped=true;break;}
        if(result<0&&errno!=EINTR)break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if(!reaped){std::fputs("physical-fixture-ending-debt:fork-child-unreaped\n",stderr);::_exit(95);}
    ASSERT_TRUE(WIFEXITED(status));EXPECT_EQ(WEXITSTATUS(status),0);
    EXPECT_EQ(returns,1u);
    returned.reset();EXPECT_EQ(returns,1u);
}

namespace {
// Only the real SQLite adapter is under test here. The production target
// constructor supplies its genuine descriptor scope/custom VFS separately.
struct ExclusiveTargetOpenControl {
    std::filesystem::path directory,path;
    store_physical_open_original original;
    sqlite3* db=nullptr;
    int first=-1,fallback=-1;
    ExclusiveTargetOpenControl(){
        const char* root=std::getenv("TEST_TMPDIR");if(!root)root=std::getenv("TMPDIR");
        if(!root||!std::filesystem::path(root).is_absolute())throw std::runtime_error("exclusive-target explicit tmp required");
        std::string pattern=(std::filesystem::path(root)/"exclusive-target-original-XXXXXX").string();
        if(!::mkdtemp(pattern.data()))throw std::runtime_error("exclusive-target control directory failed");
        directory=pattern;path=directory/"target.sqlite";
        // Exclusive birth precedes every SQLite user. No independent MAIN
        // descriptor is closed while a live SQLite POSIX-lock user exists.
        const int fd=::open(path.c_str(),O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600);
        if(fd<0)throw std::runtime_error("exclusive-target control MAIN birth failed");
        if(::close(fd)!=0)throw std::runtime_error("exclusive-target control initial MAIN close failed");
    }
    int open(const char* vfs){return VFS::open_exclusive(original,path.c_str(),&db,vfs);}
    void close(store_physical_open_original& owner){
        auto* actual=std::exchange(db,nullptr);if(!actual)return;
        first=store_physical_close_owned(owner,actual);
        if(first!=SQLITE_OK)fallback=store_physical_close_v2_owned(owner,actual);
    }
    ~ExclusiveTargetOpenControl()noexcept{
        close(original);
        if(first!=-1&&first!=SQLITE_OK&&(fallback!=SQLITE_OK||first==SQLITE_MISUSE)){
            std::fprintf(stderr,"exclusive-target-control-ending-debt:pid=%ld namespace=%s first=%d fallback=%d\n",
                static_cast<long>(::getpid()),directory.c_str(),first,fallback);
            ::_exit(96); // Preserve exact namespace; no unknown Native cleanup or unlink.
        }
        std::error_code error;std::filesystem::remove_all(directory,error);EXPECT_FALSE(error);
    }
};
}
TEST(StorePhysicalLifetime, ExclusiveTargetProducerBindsOnlyItsActualReturnWithoutNormalPolicyGrant){
    ExclusiveTargetOpenControl control;
    auto* default_vfs=sqlite3_vfs_find(nullptr);ASSERT_NE(default_vfs,nullptr);
    EXPECT_EQ(control.open(default_vfs->zName),SQLITE_MISUSE);EXPECT_EQ(control.db,nullptr);
    ASSERT_TRUE(VFS::issue_exclusive(control.original));
    EXPECT_FALSE(VFS::issue_exclusive(control.original));
    ASSERT_EQ(control.open(default_vfs->zName),SQLITE_OK);ASSERT_NE(control.db,nullptr);
    EXPECT_TRUE(VFS::bound(control.original,control.db));
    {store_physical_native_scope exact(control.original,control.db);EXPECT_TRUE(exact.entered());}
    {store_physical_native_scope wrong(control.original,nullptr);EXPECT_FALSE(wrong.entered());}
    EXPECT_TRUE(control.original.observer_policy_unsettled());
    auto policy=VFS::policy(control.original);EXPECT_TRUE(policy.observer_policy_unsettled());
    EXPECT_FALSE(VFS::seal(control.original,std::chrono::steady_clock::now()+std::chrono::seconds(1)));
    auto* const actual=control.db;
    store_physical_open_original moved(std::move(control.original));
    EXPECT_FALSE(VFS::bound(control.original,actual));EXPECT_TRUE(VFS::bound(moved,actual));
    control.close(moved);EXPECT_EQ(control.first,SQLITE_OK);
    {store_physical_native_scope retired(moved,actual);EXPECT_FALSE(retired.entered());}
    EXPECT_EQ(store_physical_close_owned(moved,actual),SQLITE_MISUSE);
}
TEST(StorePhysicalLifetime, ExclusiveTargetActualOpenFailureRetainsPartialHandleForSameOriginalCleanup){
    ExclusiveTargetOpenControl control;ASSERT_TRUE(VFS::issue_exclusive(control.original));
    const int rc=control.open("lattice-exclusive-target-control-no-such-vfs");
    ASSERT_NE(rc,SQLITE_OK);
    // SQLite's actual failed-open handle is owned cleanup evidence, not a
    // registry lookup or a supplied success/failure Boolean.
    ASSERT_NE(control.db,nullptr);EXPECT_TRUE(VFS::bound(control.original,control.db));
    auto* const failed=control.db;
    sqlite3* refused=nullptr;auto* default_vfs=sqlite3_vfs_find(nullptr);ASSERT_NE(default_vfs,nullptr);
    EXPECT_EQ(VFS::open_exclusive(control.original,control.path.c_str(),&refused,default_vfs->zName),SQLITE_MISUSE);
    EXPECT_EQ(refused,nullptr);EXPECT_EQ(control.db,failed);
    control.close(control.original);EXPECT_EQ(control.first,SQLITE_OK);
    EXPECT_EQ(store_physical_close_v2_owned(control.original,failed),SQLITE_MISUSE);
}
TEST(StorePhysicalLifetime, ExclusiveTargetBusyFallbackPreservesFirstResultUntilRealStatementAndUserdataEnd){
    std::atomic<unsigned> destroyed{0}; // Outlives fixture/statement on every assertion path.
    ExclusiveTargetOpenControl control;auto* default_vfs=sqlite3_vfs_find(nullptr);ASSERT_NE(default_vfs,nullptr);
    ASSERT_TRUE(VFS::issue_exclusive(control.original));ASSERT_EQ(control.open(default_vfs->zName),SQLITE_OK);
    ASSERT_EQ(store_physical_create_function_v2(control.db,"target_original_probe",0,SQLITE_UTF8,&destroyed,
        [](sqlite3_context* context,int,sqlite3_value**){sqlite3_result_int(context,17);},nullptr,nullptr,
        [](void* value){static_cast<std::atomic<unsigned>*>(value)->fetch_add(1,std::memory_order_relaxed);}),SQLITE_OK);
    Statement retained(control.db,"SELECT target_original_probe()");
    ASSERT_EQ(store_physical_step(retained.value),SQLITE_ROW);
    auto* const actual=control.db;
    control.close(control.original);
    EXPECT_EQ(control.first,SQLITE_BUSY);EXPECT_EQ(control.fallback,SQLITE_OK);
    EXPECT_EQ(destroyed.load(std::memory_order_relaxed),0u);
    {store_physical_cleanup_scope stale(control.original,actual);EXPECT_FALSE(stale.entered());}
    EXPECT_EQ(store_physical_close_owned(control.original,actual),SQLITE_MISUSE);
    ASSERT_EQ(retained.finish(),SQLITE_OK);EXPECT_EQ(destroyed.load(std::memory_order_relaxed),1u);
    EXPECT_EQ(control.first,SQLITE_BUSY); // A later real zombie ending never rewrites first close.
}

namespace {
struct TailObservation {
    VFS::tail_counts counts;
    TailObservation(){VFS::tail_observation(&counts);}
    ~TailObservation(){VFS::tail_observation(nullptr);}
};
struct CloseOtherFromUdf {
    OwnedStore* other=nullptr;
    const char* name=nullptr;
    bool called=false,physically_ended=false,registered_inside=false;
};
}
TEST(StorePhysicalLifetime, OtherDatabaseCompletionWaitsForActualOuterSqliteReturn){
    TailObservation observation;OwnedStore a;
    ASSERT_EQ(a.open(),SQLITE_OK);const auto name=VFS::actual_vfs_name(a.db);
    CloseOtherFromUdf context{&a,name.c_str()}; // Outlives B and its registration.
    OwnedStore b;ASSERT_EQ(b.open(),SQLITE_OK);
    ASSERT_EQ(store_physical_create_function_v2(b.db,"close_other_original",0,SQLITE_UTF8,&context,
        [](sqlite3_context* result,int,sqlite3_value**){
            auto& data=*static_cast<CloseOtherFromUdf*>(sqlite3_user_data(result));
            data.other->close();data.other->original=store_physical_open_original{};
            data.called=true;data.physically_ended=Access::observe(data.other->physical).physical_ended;
            data.registered_inside=sqlite3_vfs_find(data.name)!=nullptr;
            sqlite3_result_int(result,7);
        },nullptr,nullptr,nullptr),SQLITE_OK);
    ASSERT_EQ(store_physical_exec(b.db,"SELECT close_other_original()",nullptr,nullptr,nullptr),SQLITE_OK);
    EXPECT_TRUE(context.called);EXPECT_TRUE(context.physically_ended);
    EXPECT_TRUE(context.registered_inside); // Actual A VFS must survive B's C call.
    EXPECT_EQ(sqlite3_vfs_find(name.c_str()),nullptr);
    EXPECT_EQ(observation.counts.completed,1u);EXPECT_EQ(observation.counts.destroyed,1u);
    EXPECT_EQ(a.first,SQLITE_OK);EXPECT_FALSE(Access::observe(a.physical).unknown);
}
TEST(StorePhysicalLifetime, OutOfOrderManagedScopeResetKeepsTailUntilLastNativeUnlock){
    TailObservation observation;OwnedStore a,b;
    ASSERT_EQ(a.open(),SQLITE_OK);ASSERT_EQ(b.open(),SQLITE_OK);
    const auto name=VFS::actual_vfs_name(a.db);
    store_physical_native_scope outer(b.original,b.db);ASSERT_TRUE(outer.entered());
    store_physical_native_scope inner(b.original,b.db);ASSERT_TRUE(inner.entered());
    auto* mutex=sqlite3_db_mutex(b.db);ASSERT_NE(mutex,nullptr);sqlite3_mutex_enter(mutex);
    store_physical_mutex_return unlocked(b.original,mutex);
    outer.reset();a.close();a.original=store_physical_open_original{};
    const bool before_unlock=sqlite3_vfs_find(name.c_str())!=nullptr;
    unlocked.reset();const bool before_last_scope=sqlite3_vfs_find(name.c_str())!=nullptr;
    inner.reset();
    EXPECT_TRUE(before_unlock);EXPECT_TRUE(before_last_scope);
    EXPECT_EQ(sqlite3_vfs_find(name.c_str()),nullptr);
    EXPECT_EQ(observation.counts.completed,1u);EXPECT_EQ(observation.counts.destroyed,1u);
}
TEST(StorePhysicalLifetime, ExceptionalManagedUnwindDrainsOnlyAfterRealMutexReturn){
    TailObservation observation;OwnedStore a,b;
    ASSERT_EQ(a.open(),SQLITE_OK);ASSERT_EQ(b.open(),SQLITE_OK);
    const auto name=VFS::actual_vfs_name(a.db);bool retained=false,caught=false;
    struct Canceled{};
    try{
        store_physical_native_scope scope(b.original,b.db);
        if(!scope.entered())throw std::runtime_error("component actual Native refused");
        auto* mutex=sqlite3_db_mutex(b.db);if(!mutex)throw std::runtime_error("component actual mutex absent");
        sqlite3_mutex_enter(mutex);store_physical_mutex_return unlocked(b.original,mutex);
        a.close();a.original=store_physical_open_original{};
        retained=sqlite3_vfs_find(name.c_str())!=nullptr;throw Canceled{};
    }catch(const Canceled&){caught=true;}
    EXPECT_TRUE(caught);EXPECT_TRUE(retained);EXPECT_EQ(sqlite3_vfs_find(name.c_str()),nullptr);
    EXPECT_EQ(observation.counts.completed,1u);EXPECT_EQ(observation.counts.destroyed,1u);
}
TEST(StorePhysicalLifetime, ForeignReturnAbandonsQueuedOriginalAndParentDrainsExactlyOnce){
    TailObservation observation;OwnedStore a,b;
    ASSERT_EQ(a.open(),SQLITE_OK);ASSERT_EQ(b.open(),SQLITE_OK);
    const auto name=VFS::actual_vfs_name(a.db);
    store_physical_native_scope scope(b.original,b.db);ASSERT_TRUE(scope.entered());
    auto* mutex=sqlite3_db_mutex(b.db);ASSERT_NE(mutex,nullptr);
    std::uint64_t mutex_returns=0;VFS::mutex_returns(&mutex_returns);
    struct ResetMutexObservation{~ResetMutexObservation(){VFS::mutex_returns(nullptr);}} reset;
    sqlite3_mutex_enter(mutex);store_physical_mutex_return unlocked(b.original,mutex);
    a.close();a.original=store_physical_open_original{};
    const pid_t child=::fork();
    if(child==0){
        VFS::returned_tail();unlocked.reset();scope.reset();
        ::_exit(observation.counts.completed==0&&observation.counts.destroyed==0&&mutex_returns==0?0:41);
    }
    unlocked.reset();scope.reset(); // Actual parent unlock/drain before any assertion/wait.
    if(child<0){ADD_FAILURE()<<"actual fork unavailable";return;}
    int status=0;bool reaped=false;const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(4);
    while(std::chrono::steady_clock::now()<end){
        const auto result=::waitpid(child,&status,WNOHANG);if(result==child){reaped=true;break;}
        if(result<0&&errno!=EINTR)break;std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if(!reaped){std::fputs("physical-fixture-ending-debt:return-tail-child-unreaped\n",stderr);::_exit(95);}
    ASSERT_TRUE(WIFEXITED(status));EXPECT_EQ(WEXITSTATUS(status),0);
    EXPECT_EQ(mutex_returns,1u);EXPECT_EQ(observation.counts.completed,1u);
    EXPECT_EQ(observation.counts.destroyed,1u);EXPECT_EQ(sqlite3_vfs_find(name.c_str()),nullptr);
    VFS::returned_tail();EXPECT_EQ(observation.counts.completed,1u);EXPECT_EQ(observation.counts.destroyed,1u);
}

TEST(StorePhysicalLifetime, DirectMemoryWrapperCallRetainsOtherOriginalUntilItsActualReturn){
    // No outer Core NativeScope exists here. Empty memory tickets must observe
    // the actual wrapper exec/step call without inventing a physical original.
    for(bool use_exec:{true,false}){
        TailObservation observation;OwnedStore a;
        ASSERT_EQ(a.open(),SQLITE_OK);const auto name=VFS::actual_vfs_name(a.db);
        CloseOtherFromUdf context{&a,name.c_str()}; // Outlives memory registration.
        struct MemoryOwner {
            sqlite3* db=nullptr;
            ~MemoryOwner(){
                if(!db)return;
                const int rc=store_physical_close(std::exchange(db,nullptr));
                if(rc!=SQLITE_OK){std::fputs("physical-fixture-ending-debt:memory-wrapper-close\n",stderr);::_exit(96);}
            }
        } memory;
        ASSERT_EQ(store_physical_open_v2(":memory:",&memory.db,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_FULLMUTEX,nullptr),SQLITE_OK);
        ASSERT_NE(memory.db,nullptr);EXPECT_FALSE(store_physical_is_enrolled(memory.db));
        ASSERT_EQ(store_physical_create_function_v2(memory.db,"close_other_original",0,SQLITE_UTF8,&context,
            [](sqlite3_context* result,int,sqlite3_value**){
                auto& data=*static_cast<CloseOtherFromUdf*>(sqlite3_user_data(result));
                data.other->close();data.other->original=store_physical_open_original{};
                data.called=true;data.physically_ended=Access::observe(data.other->physical).physical_ended;
                data.registered_inside=sqlite3_vfs_find(data.name)!=nullptr;
                sqlite3_result_int(result,11);
            },nullptr,nullptr,nullptr),SQLITE_OK);
        if(use_exec){
            ASSERT_EQ(store_physical_exec(memory.db,"SELECT close_other_original()",nullptr,nullptr,nullptr),SQLITE_OK);
        }else{
            Statement statement(memory.db,"SELECT close_other_original()");
            ASSERT_EQ(store_physical_step(statement.value),SQLITE_ROW);
            EXPECT_EQ(sqlite3_column_int(statement.value,0),11);
            ASSERT_EQ(statement.finish(),SQLITE_OK);
        }
        EXPECT_TRUE(context.called);EXPECT_TRUE(context.physically_ended);EXPECT_TRUE(context.registered_inside);
        EXPECT_EQ(sqlite3_vfs_find(name.c_str()),nullptr);
        EXPECT_EQ(observation.counts.completed,1u);EXPECT_EQ(observation.counts.destroyed,1u);
    }
}
#endif
