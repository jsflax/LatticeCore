#include "TestHelpers.hpp"
#include <lattice/ordinary_context.hpp>
#include <lattice/projection.hpp>
#include "../../Sources/LatticeCore/src/ordinary_cohort_attachment.hpp"
#include <filesystem>
#include <cstdlib>
#include <unistd.h>

namespace {
using namespace lattice;
// Explicit agent-work root. These source cases have not been executed.
struct consumer_files {
    std::filesystem::path directory;
    consumer_files() {
        const auto* home=std::getenv("HOME");
        if(!home)throw std::runtime_error("fixture home unavailable");
        auto base=std::filesystem::path(home)/"localdev/lattice-managed-consumer-runtime-artifacts";
        std::filesystem::create_directories(base);
        auto pattern=(base/"case-XXXXXX").string();
        std::vector<char> value(pattern.begin(),pattern.end());value.push_back('\0');
        auto* created=::mkdtemp(value.data());
        if(!created)throw std::runtime_error("fixture directory unavailable");
        directory=created;
    }
    ~consumer_files(){std::error_code error;std::filesystem::remove_all(directory,error);}
    std::string path(const char* leaf)const{return (directory/leaf).string();}
};
int64_t scalar(database& db,const std::string& sql) {
    auto result=db.query(sql);if(result.size()!=1||result[0].size()!=1)throw std::runtime_error("fixture scalar shape");
    return std::get<int64_t>(result[0].begin()->second);
}
void probe(lattice_db& owner,int64_t value) {
    owner.db().execute("CREATE TABLE Fixture (id INTEGER PRIMARY KEY, globalId TEXT NOT NULL, n INTEGER)");
    owner.db().execute("INSERT INTO Fixture VALUES(1,'00000000-0000-0000-0000-000000000001',?)",{value});
}
projection_query request() {
    projection_query value;value.table="Fixture";value.columns={"n"};value.order_by="id ASC";
    value.max_rows=10;value.max_copied_bytes=1024;value.timeout_ms=3000;return value;
}
}

TEST(OrdinaryCurrentManagedConsumer, UnmanagedMoveKeepsCommittedRowsAndReadOnlyFlags) {
    consumer_files files;database original(files.path("primary.sqlite"));
    original.execute("CREATE TABLE Probe(n INTEGER)");original.execute("INSERT INTO Probe VALUES(7)");
    database moved(std::move(original));moved.execute("INSERT INTO Probe VALUES(9)");
    database reader(files.path("primary.sqlite"),database::open_mode::read_only);
    EXPECT_EQ(scalar(reader,"SELECT SUM(n) FROM Probe"),16);
    EXPECT_THROW(reader.execute("INSERT INTO Probe VALUES(99)"),db_error);
    EXPECT_EQ(scalar(moved,"SELECT COUNT(*) FROM Probe"),2);
}

TEST(OrdinaryCurrentManagedConsumer, UnmanagedMoveAssignmentRetiresOnlyDisplacedFile) {
    consumer_files files;database one(files.path("one.sqlite")),two(files.path("two.sqlite"));
    one.execute("CREATE TABLE Probe(n INTEGER)");one.execute("INSERT INTO Probe VALUES(1)");
    two.execute("CREATE TABLE Probe(n INTEGER)");two.execute("INSERT INTO Probe VALUES(2)");
    one=std::move(two);one.execute("INSERT INTO Probe VALUES(3)");
    database first(files.path("one.sqlite"),database::open_mode::read_only);
    EXPECT_EQ(scalar(first,"SELECT SUM(n) FROM Probe"),1);
    EXPECT_EQ(scalar(one,"SELECT SUM(n) FROM Probe"),5);
}

TEST(OrdinaryCurrentManagedConsumer, UnmanagedKeeperReuseRetainsSnapshotAndSmallCache) {
    consumer_files files;lattice_db owner(files.path("keeper.sqlite"));probe(owner,1);
    auto first=owner.acquire_read_generation();ASSERT_NE(first,0u);
    auto cached=owner.query_at_generation(first,"PRAGMA cache_size");ASSERT_TRUE(cached);
    ASSERT_EQ(cached->size(),1u);EXPECT_EQ(std::get<int64_t>(cached->at(0).at("cache_size")),2000);
    owner.db().execute("UPDATE Fixture SET n=2");
    auto held=owner.query_at_generation(first,"SELECT n FROM Fixture");ASSERT_TRUE(held);
    EXPECT_EQ(std::get<int64_t>(held->at(0).at("n")),1);
    owner.release_read_generation(first);
    auto next=owner.acquire_read_generation();ASSERT_NE(next,0u);
    auto fresh=owner.query_at_generation(next,"SELECT n FROM Fixture");ASSERT_TRUE(fresh);
    EXPECT_EQ(std::get<int64_t>(fresh->at(0).at("n")),2);owner.release_read_generation(next);
    EXPECT_EQ(scalar(owner.read_db(),"PRAGMA cache_size"),50000);
}

TEST(OrdinaryCurrentManagedConsumer, UnmanagedReopenRetainsActualAttachmentOnWriterAndReader) {
    consumer_files files;lattice_db parent(files.path("parent.sqlite")),child(files.path("child.sqlite"));
    probe(child,13);parent.attach(child);
    const auto aliases=parent.db().query("SELECT name FROM pragma_database_list WHERE seq>1");ASSERT_EQ(aliases.size(),1u);
    const auto alias=std::get<std::string>(aliases[0].at("name"));
    const auto sql="SELECT n FROM \""+alias+"\".Fixture";
    EXPECT_EQ(scalar(parent.db(),sql),13);parent.reopen_read_db();EXPECT_EQ(scalar(parent.read_db(),sql),13);
    parent.reopen_write_db();EXPECT_EQ(scalar(parent.db(),sql),13);
    parent.detach(child);EXPECT_TRUE(parent.db().query("SELECT name FROM pragma_database_list WHERE name=?",{alias}).empty());
}

TEST(OrdinaryCurrentManagedConsumer, FileProjectionPreservesReadonlyRowsAcrossReaderReopen) {
    consumer_files files;lattice_db owner(files.path("projection.sqlite"));probe(owner,21);owner.reopen_read_db();
    auto operation=owner.start_projection(request());auto batch=operation.next_batch(1);
    ASSERT_EQ(batch.status_code(),static_cast<int32_t>(projection_status::batch))<<batch.error_message();
    ASSERT_EQ(batch.row_count(),1);EXPECT_EQ(std::get<int64_t>(batch.value(0,0)),21);
    auto done=operation.next_batch(1);EXPECT_EQ(done.status_code(),static_cast<int32_t>(projection_status::done));
    EXPECT_FALSE(operation.has_resources());owner.db().execute("UPDATE Fixture SET n=22");
    EXPECT_EQ(scalar(owner.db(),"SELECT n FROM Fixture"),22);
}

TEST(OrdinaryCurrentManagedConsumer, BorrowedMemoryRestoreRetainsWriterBusyPolicyAndAllowsLaterCommit) {
    configuration config;config.busy_timeout_ms=17;lattice_db owner(config);probe(owner,31);
    auto operation=owner.start_projection(request());auto batch=operation.next_batch(1);
    ASSERT_EQ(batch.status_code(),static_cast<int32_t>(projection_status::batch))<<batch.error_message();
    EXPECT_EQ(std::get<int64_t>(batch.value(0,0)),31);operation.close();
    EXPECT_EQ(scalar(owner.db(),"PRAGMA busy_timeout"),17);
    owner.db().execute("UPDATE Fixture SET n=32");EXPECT_EQ(scalar(owner.db(),"SELECT n FROM Fixture"),32);
}

TEST(OrdinaryCurrentManagedConsumer, UnissuedCohortActionCannotGrantLiteralOrNullFilename) {
    database owner(":memory:");auto* native=owner.handle();ASSERT_NE(native,nullptr);
    EXPECT_EQ(detail::ordinary_cohort_attachment_access::authorize(native,SQLITE_ATTACH,":memory:",nullptr),SQLITE_DENY);
    EXPECT_EQ(detail::ordinary_cohort_attachment_access::authorize(native,SQLITE_ATTACH,nullptr,nullptr),SQLITE_DENY);
    EXPECT_EQ(detail::ordinary_cohort_attachment_access::authorize(native,SQLITE_DETACH,"claimed",nullptr),SQLITE_DENY);
    owner.execute("CREATE TABLE Probe(n INTEGER)");owner.execute("INSERT INTO Probe VALUES(41)");
    EXPECT_EQ(scalar(owner,"SELECT n FROM Probe"),41);
}

TEST(OrdinaryCurrentManagedConsumer, FailedUnmanagedReadonlyOpenCannotCreateOrPoisonNextConnection) {
    consumer_files files;const auto absent=files.path("missing.sqlite");
    EXPECT_THROW(database refused(absent,database::open_mode::read_only),db_error);
    EXPECT_FALSE(std::filesystem::exists(absent));
    database writer(files.path("next.sqlite"));writer.execute("CREATE TABLE Probe(n INTEGER)");
    writer.execute("INSERT INTO Probe VALUES(51)");EXPECT_EQ(scalar(writer,"SELECT n FROM Probe"),51);
}
