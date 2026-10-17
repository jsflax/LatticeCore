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

namespace lattice::detail {
struct store_physical_lifetime_test_access {
    using pin=store_physical_lifetime::pin;
    static pin adopt(int actual){return store_physical_lifetime::adopt_lease_(actual);}
    static auto observe(const pin& actual){return store_physical_lifetime::observe_(actual);}
    static bool wait(const pin& actual,std::chrono::steady_clock::time_point end){return store_physical_lifetime::wait_until_(actual,end);}
};
struct store_physical_vfs_test_access {
    static int open(const store_physical_lifetime::pin& actual,const std::string& path,const struct stat& expected,sqlite3** db){
        return store_physical_vfs::open_(actual,sqlite3_vfs_find(nullptr),path,expected,db,SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX);
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
    int open(){return VFS::open(physical,path.string(),expected,&db);}
    void close(){auto* actual=std::exchange(db,nullptr);if(!actual)return;
        first=store_physical_close(actual);if(first!=SQLITE_OK)fallback=store_physical_close_v2(actual);}
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
    EXPECT_NE(VFS::open(store.physical,store.path.string(),wrong,&store.db),SQLITE_OK);
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
}
#endif
