#include "TestHelpers.hpp"
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include "../../Sources/LatticeCore/src/ordinary_historical_wal_checkpoint.hpp"

namespace lattice::detail::ordinary_installation {
// Tests only the actual raw checkpoint mechanism. No retained generation,
// controller retirement, managed context, GROUP custody or launch is invented.
struct historical_wal_checkpoint_test_access {
    static void checkpoint(const std::string& path,const struct stat& main,const struct stat& wal,const struct stat& shm,
        ordinary_launch::deadline end){historical_wal_checkpoint::checkpoint_one(path,main,wal,shm,end);}
    static void absent(const std::string& path,const struct stat& main,const struct stat& wal,ordinary_launch::deadline end){
        historical_wal_checkpoint::checkpoint_without_shm(path,main,wal,end);}
};
}
namespace {
using checkpoint_access=lattice::detail::ordinary_installation::historical_wal_checkpoint_test_access;
constexpr auto checkpoint_limit=lattice::detail::ordinary_managed_wal_limit_bytes;
struct raw_connection {
    sqlite3* value=nullptr;
    explicit raw_connection(const std::string& path,int flags=SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE){
        if(sqlite3_open_v2(path.c_str(),&value,flags,nullptr)!=SQLITE_OK)throw std::runtime_error("real seed open failed");
    }
    ~raw_connection(){if(value)sqlite3_close(value);}
    void sql(const char* text){if(sqlite3_exec(value,text,nullptr,nullptr,nullptr)!=SQLITE_OK)throw std::runtime_error("real seed SQL failed");}
    void close(){const int rc=sqlite3_close(value);if(rc!=SQLITE_OK)throw std::runtime_error("real seed first close failed");value=nullptr;}
};
struct checkpoint_area {
    TempDB file{"historical-checkpoint"};
    struct stat main{},wal{},shm{};
    checkpoint_area(){file.path=std::filesystem::canonical(file.path.parent_path())/file.path.filename();}
    std::string path()const{return file.str();}
    void facts(){
        if(::lstat(path().c_str(),&main)||::lstat((path()+"-wal").c_str(),&wal)||::lstat((path()+"-shm").c_str(),&shm))
            throw std::runtime_error("real original sidecars required");
    }
    void seed(){
        raw_connection original(path());original.sql("PRAGMA journal_mode=WAL");original.sql("PRAGMA wal_autocheckpoint=0");
        int persist=1,actual=0;
        if(sqlite3_file_control(original.value,"main",SQLITE_FCNTL_PERSIST_WAL,&persist)!=SQLITE_OK||
           sqlite3_db_config(original.value,SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,1,&actual)!=SQLITE_OK||actual!=1)
            throw std::runtime_error("real seed preservation unavailable");
        original.sql("CREATE TABLE historical_row(id INTEGER PRIMARY KEY,payload BLOB NOT NULL)");
        // Actual committed frames exceed the production threshold. No file
        // truncate/sparse fabrication or fake WAL header grants success.
        const std::string insert="INSERT INTO historical_row VALUES(1,zeroblob("+std::to_string(checkpoint_limit+131072)+"))";
        original.sql(insert.c_str());original.close();facts();
        if(wal.st_size<=checkpoint_limit)throw std::runtime_error("real oversized committed WAL required");
    }
    void run(lattice::detail::ordinary_launch::deadline end=std::chrono::steady_clock::now()+std::chrono::seconds(30)){
        checkpoint_access::checkpoint(path(),main,wal,shm,end);
    }
    void original_names()const{
        for(const auto& entry:{std::pair<std::string,struct stat>{path(),main},{path()+"-wal",wal},{path()+"-shm",shm}}){
            struct stat now{};ASSERT_EQ(::lstat(entry.first.c_str(),&now),0);
            EXPECT_EQ(now.st_dev,entry.second.st_dev);EXPECT_EQ(now.st_ino,entry.second.st_ino);
            EXPECT_EQ(now.st_mode,entry.second.st_mode);EXPECT_EQ(now.st_nlink,1);
        }
    }
};
}
TEST(HistoricalWalCheckpoint, ActualOversizedCommittedWalTruncatesWithoutReplacingSidecarsOrRows){
    checkpoint_area files;files.seed();ASSERT_GT(files.wal.st_size,checkpoint_limit);
    EXPECT_NO_THROW(files.run());files.original_names();
    EXPECT_EQ(std::filesystem::file_size(files.path()+"-wal"),0u);
    raw_connection check(files.path(),SQLITE_OPEN_READONLY);sqlite3_stmt* statement=nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(check.value,"SELECT length(payload) FROM historical_row WHERE id=1",-1,&statement,nullptr),SQLITE_OK);
    ASSERT_EQ(sqlite3_step(statement),SQLITE_ROW);EXPECT_EQ(sqlite3_column_int64(statement,0),checkpoint_limit+131072);
    ASSERT_EQ(sqlite3_finalize(statement),SQLITE_OK);check.close();
}
TEST(HistoricalWalCheckpoint, ActualPinnedReaderKeepsFirstBusyOutcomeAndOriginalWal){
    checkpoint_area files;files.seed();raw_connection reader(files.path(),SQLITE_OPEN_READONLY);
    reader.sql("BEGIN");reader.sql("SELECT length(payload) FROM historical_row");
    files.facts();const auto length=files.wal.st_size;
    EXPECT_THROW(files.run(),lattice::db_error);files.original_names();
    EXPECT_EQ(std::filesystem::file_size(files.path()+"-wal"),static_cast<std::uint64_t>(length));
    reader.sql("ROLLBACK");reader.close();
    // A mechanism failure is not retried through the retained-generation
    // route; its actual single-attempt latch is separately source-qualified.
}
TEST(HistoricalWalCheckpoint, RemovedOriginalShmRefusesBeforeOpeningOrChangingWal){
    checkpoint_area files;files.seed();const auto length=files.wal.st_size;
    ASSERT_TRUE(std::filesystem::remove(files.path()+"-shm"));
    EXPECT_THROW(files.run(),lattice::db_error);
    EXPECT_FALSE(std::filesystem::exists(files.path()+"-shm"));
    EXPECT_EQ(std::filesystem::file_size(files.path()+"-wal"),static_cast<std::uint64_t>(length));
}
TEST(HistoricalWalCheckpoint, ReplacedWalAndExpiredOriginalDeadlineCannotObtainDrainSuccess){
    checkpoint_area files;files.seed();const auto retained=files.path()+"-wal.retained";
    std::filesystem::rename(files.path()+"-wal",retained);
    std::filesystem::copy_file(retained,files.path()+"-wal");
    const auto length=std::filesystem::file_size(files.path()+"-wal");
    EXPECT_THROW(files.run(),lattice::db_error);
    EXPECT_EQ(std::filesystem::file_size(files.path()+"-wal"),length);
    files.facts();EXPECT_THROW(files.run(std::chrono::steady_clock::now()),lattice::db_error);
    EXPECT_EQ(std::filesystem::file_size(files.path()+"-wal"),length);
    ASSERT_TRUE(std::filesystem::remove(retained));
}
// Missing-mode mechanism fixtures do not mint original-census or retirement
// authority. The real production selector remains the opaque full census.
namespace {
void remove_seed_shm(checkpoint_area& files){
    ASSERT_TRUE(std::filesystem::remove(files.path()+"-shm"));
    struct stat unexpected{};ASSERT_NE(::lstat((files.path()+"-shm").c_str(),&unexpected),0);ASSERT_EQ(errno,ENOENT);
}
void absent_run(const checkpoint_area& files,lattice::detail::ordinary_launch::deadline end=std::chrono::steady_clock::now()+std::chrono::seconds(30)){
    checkpoint_access::absent(files.path(),files.main,files.wal,end);
}
void absent_originals(const checkpoint_area& files){
    for(const auto& entry:{std::pair<std::string,struct stat>{files.path(),files.main},{files.path()+"-wal",files.wal}}){
        struct stat now{};ASSERT_EQ(::lstat(entry.first.c_str(),&now),0);EXPECT_EQ(now.st_dev,entry.second.st_dev);
        EXPECT_EQ(now.st_ino,entry.second.st_ino);EXPECT_EQ(now.st_mode,entry.second.st_mode);EXPECT_EQ(now.st_nlink,1);
    }
    struct stat unexpected{};EXPECT_NE(::lstat((files.path()+"-shm").c_str(),&unexpected),0);EXPECT_EQ(errno,ENOENT);
}
}
TEST(HistoricalWalCheckpoint, OriginallyAbsentShmUsesExclusiveHeapIndexAndPreservesCommittedRow){
    checkpoint_area files;files.seed();remove_seed_shm(files);
    EXPECT_NO_THROW(absent_run(files));absent_originals(files);
    EXPECT_EQ(std::filesystem::file_size(files.path()+"-wal"),0u);
    raw_connection check(files.path(),SQLITE_OPEN_READONLY);sqlite3_stmt* statement=nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(check.value,"SELECT length(payload) FROM historical_row WHERE id=1",-1,&statement,nullptr),SQLITE_OK);
    ASSERT_EQ(sqlite3_step(statement),SQLITE_ROW);EXPECT_EQ(sqlite3_column_int64(statement,0),checkpoint_limit+131072);
    ASSERT_EQ(sqlite3_finalize(statement),SQLITE_OK);check.close();
}
TEST(HistoricalWalCheckpoint, GenuineExclusivePinnedReaderRefusesAbsentModeWithoutChangingWalOrCreatingShm){
    checkpoint_area files;files.seed();remove_seed_shm(files);
    // A real private-index reader holds an actual read transaction. No active
    // SHM is unlinked, no lock/failure counters are fabricated.
    raw_connection reader(files.path(),SQLITE_OPEN_READWRITE);reader.sql("PRAGMA main.locking_mode=EXCLUSIVE");
    reader.sql("BEGIN");reader.sql("SELECT length(payload) FROM historical_row");
    const auto length=std::filesystem::file_size(files.path()+"-wal");absent_originals(files);
    EXPECT_THROW(absent_run(files),lattice::db_error);absent_originals(files);
    EXPECT_EQ(std::filesystem::file_size(files.path()+"-wal"),length);
    reader.sql("ROLLBACK");reader.close();
}
TEST(HistoricalWalCheckpoint, AppearedShmNeverBecomesAnAbsentModePermit){
    checkpoint_area files;files.seed();remove_seed_shm(files);const auto length=files.wal.st_size;
    {const int fd=::open((files.path()+"-shm").c_str(),O_WRONLY|O_CREAT|O_EXCL,0600);ASSERT_GE(fd,0);ASSERT_EQ(::close(fd),0);}
    struct stat original{};ASSERT_EQ(::lstat((files.path()+"-shm").c_str(),&original),0);
    EXPECT_THROW(absent_run(files),lattice::db_error);
    struct stat after{};ASSERT_EQ(::lstat((files.path()+"-shm").c_str(),&after),0);EXPECT_EQ(after.st_ino,original.st_ino);
    EXPECT_EQ(after.st_size,0);EXPECT_EQ(std::filesystem::file_size(files.path()+"-wal"),static_cast<std::uint64_t>(length));
}
TEST(HistoricalWalCheckpoint, ActualOversizedIndexExtentRefusesBeforeNativeOpenWithoutSidecarCreation){
    checkpoint_area files;files.seed();remove_seed_shm(files);
    // Genuine sparse extension is a negative malformed/original-extent case,
    // not fabricated committed frames or a positive checkpoint authority.
    std::filesystem::resize_file(files.path()+"-wal",700u*1024u*1024u);
    ASSERT_EQ(::lstat((files.path()+"-wal").c_str(),&files.wal),0);const auto length=files.wal.st_size;
    EXPECT_THROW(absent_run(files),lattice::db_error);absent_originals(files);
    EXPECT_EQ(std::filesystem::file_size(files.path()+"-wal"),static_cast<std::uint64_t>(length));
}
TEST(HistoricalWalCheckpoint, AbsentModePreservesExpiredAndReplacedOriginalRefusal){
    checkpoint_area files;files.seed();remove_seed_shm(files);const auto length=files.wal.st_size;
    EXPECT_THROW(absent_run(files,std::chrono::steady_clock::now()),lattice::db_error);absent_originals(files);
    const auto retained=files.path()+"-wal.retained";std::filesystem::rename(files.path()+"-wal",retained);
    std::filesystem::copy_file(retained,files.path()+"-wal");EXPECT_THROW(absent_run(files),lattice::db_error);
    EXPECT_FALSE(std::filesystem::exists(files.path()+"-shm"));
    EXPECT_EQ(std::filesystem::file_size(files.path()+"-wal"),static_cast<std::uint64_t>(length));ASSERT_TRUE(std::filesystem::remove(retained));
}

#endif
