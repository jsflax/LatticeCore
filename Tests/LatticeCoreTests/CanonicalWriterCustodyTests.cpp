#include "TestHelpers.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include "../../Sources/LatticeCore/src/projection_memory.hpp"
#include "../../Sources/LatticeCore/src/recovery_local_producer.hpp"
#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <sstream>

struct CustodyRecord {std::string name;std::string body;};
LATTICE_SCHEMA(CustodyRecord,name,body);

namespace {
using namespace lattice;
using namespace lattice::detail;
configuration custody_config(const std::string& path) {
    configuration result(path);result.audit_retention_seconds=0;result.busy_timeout_ms=100;return result;
}
int64_t count(database& db,const std::string& table) {
    return std::get<int64_t>(db.query("SELECT count(*) AS n FROM "+table).at(0).at("n"));
}
void capture_refused(database& db) {
    auto control=std::make_shared<database_read_control>();
    control->deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
    sqlite3_stmt* statement=nullptr;
    try {database_projection_capture capture(db,control,statement);FAIL()<<"policy borrower must refuse";}
    catch(const projection_capture_failure& error) {
        EXPECT_EQ(error.status,projection_status::unsupported);
        EXPECT_NE(std::string(error.what()).find("recovery writer owns"),std::string::npos);
    }
    EXPECT_EQ(statement,nullptr);EXPECT_EQ(control->target,nullptr);
}
struct scoped_trace_reset {
    sqlite3* connection;
    explicit scoped_trace_reset(sqlite3* connection) noexcept:connection(connection) {}
    scoped_trace_reset(const scoped_trace_reset&)=delete;
    scoped_trace_reset& operator=(const scoped_trace_reset&)=delete;
    ~scoped_trace_reset() {sqlite3_trace_v2(connection,0,nullptr,nullptr);}
};

// A lifecycle callback never waits for SQLite. A separate thread tries the
// physical mutex once, reports the result, and releases it immediately. The
// caller's response wait has a deadline, including when the code under test
// accidentally owns that mutex. No detached thread borrows the test frame.
class callback_mutex_probe {
public:
    struct sample {size_t requests,completed,busy,timeouts;bool failed;};
    void start(sqlite3* connection) {
        connection_mutex_=sqlite3_db_mutex(connection);
        inspector_=std::thread([this] {inspect();});
    }
    ~callback_mutex_probe() {stop();}
    void stop() {
        enabled_.store(false);
        {std::lock_guard<std::mutex> lock(mutex_);stopping_=true;}
        requests_changed_.notify_all();
        if(inspector_.joinable())inspector_.join();
    }
    void observe() noexcept {
        if(!enabled_.load())return;
        try {
            std::unique_lock<std::mutex> lock(mutex_);
            const auto serial=++requests_;
            requests_changed_.notify_one();
            if(!responses_changed_.wait_for(lock,std::chrono::seconds(5),[&]{return completed_>=serial;}))++timeouts_;
        }catch(...){failed_.store(true);}
    }
    sample read() {
        std::lock_guard<std::mutex> lock(mutex_);
        return {requests_,completed_,busy_,timeouts_,failed_.load()};
    }
private:
    void inspect() {
        std::unique_lock<std::mutex> lock(mutex_);
        for(;;) {
            requests_changed_.wait(lock,[&]{return stopping_||completed_<requests_;});
            if(stopping_)return;
            const auto serial=requests_;
            lock.unlock();
            const auto status=sqlite3_mutex_try(connection_mutex_);
            if(status==SQLITE_OK)sqlite3_mutex_leave(connection_mutex_);
            lock.lock();
            if(status!=SQLITE_OK)++busy_;
            completed_=serial;responses_changed_.notify_one();
        }
    }
    sqlite3_mutex* connection_mutex_=nullptr;
    std::mutex mutex_;
    std::condition_variable requests_changed_,responses_changed_;
    size_t requests_=0,completed_=0,busy_=0,timeouts_=0;
    bool stopping_=false;
    std::atomic<bool> enabled_{true},failed_{false};
    std::thread inspector_;
};
struct small_lifecycle_hook {
    callback_mutex_probe* probe;
    int* calls;
    small_lifecycle_hook(callback_mutex_probe& p,int& count) noexcept:probe(&p),calls(&count) {}
    small_lifecycle_hook(const small_lifecycle_hook& other) noexcept:probe(other.probe),calls(other.calls) {probe->observe();}
    ~small_lifecycle_hook() {probe->observe();}
    void operator()() const noexcept {++*calls;}
};
class CanonicalWriterCustody:public ::testing::TestWithParam<bool> {
protected:
    TempDB path{"canonical_custody"};
    std::shared_ptr<lattice_db> owner;
    canonical_writer_profile profile{{"custody-source","custody-epoch","custody-scope","custody-schema"},
        {128,65536,128,65536,32,64,64},{"CustodyRecord"},false};
    std::unique_ptr<canonical_writer_adapter> adapter;
    void SetUp() override {
        owner=std::make_shared<lattice_db>(custody_config(GetParam()?path.str():":memory:"));
        if(GetParam()) {
            auto* notifier=instance_registry::instance().get_or_create_notifier(path.str());
            ASSERT_NE(notifier,nullptr);notifier->stop_listening();
        }
    }
    void attach(){adapter=canonical_writer_adapter::attach(*owner,profile);}
    int64_t rows(){return count(owner->db(),"CustodyRecord");}
    int64_t receipts(){return count(owner->db(),"_lattice_canonical_receipt");}
};
}

TEST_P(CanonicalWriterCustody, BorrowedCaptureCannotRemoveTheCanonicalAuthorizer) {
    attach();owner->add(CustodyRecord{"before","body"});capture_refused(owner->db());
    EXPECT_THROW(owner->db().execute("DROP TABLE _lattice_canonical_touch"),db_error);
    EXPECT_THROW(owner->db().execute("UPDATE _lattice_canonical_store SET head=head+1"),db_error);
    owner->add(CustodyRecord{"after","body"});EXPECT_EQ(rows(),2);EXPECT_EQ(receipts(),2);
}

TEST_P(CanonicalWriterCustody, PublicHookReplacementRefusesBeforeEffectsAndRollbackRemainsOwned) {
    attach();owner->add(CustodyRecord{"kept","body"});int foreign=0;
    owner->begin_transaction();owner->add(CustodyRecord{"rolled back","body"});
    EXPECT_THROW(owner->db().set_txn_hooks([&]{++foreign;},[&]{++foreign;}),db_error);
    EXPECT_THROW(owner->db().set_txn_hooks({},{}),db_error);
    owner->rollback();EXPECT_EQ(rows(),1);EXPECT_EQ(receipts(),1);EXPECT_EQ(foreign,0);
    owner->add(CustodyRecord{"successor","body"});EXPECT_EQ(rows(),2);EXPECT_EQ(receipts(),2);
    EXPECT_EQ(foreign,0);
}

TEST_P(CanonicalWriterCustody, EscapedConnectionCannotBeAdmittedOrSilentlyRepaired) {
    ASSERT_NE(owner->db().handle(),nullptr);EXPECT_THROW(attach(),db_error);
    EXPECT_FALSE(owner->db().table_exists("_lattice_canonical_coverage"));
    EXPECT_FALSE(owner->db().table_exists("_lattice_canonical_store"));
    EXPECT_NO_THROW(owner->add(CustodyRecord{"legacy","body"}));EXPECT_EQ(rows(),1);
}

TEST_P(CanonicalWriterCustody, PublicHookReplacementBeforeAdmissionCannotMasqueradeAsEngineHooks) {
    owner->db().set_txn_hooks([]{},[]{});EXPECT_THROW(attach(),db_error);
    owner->db().set_txn_hooks({},{});EXPECT_THROW(attach(),db_error);
    EXPECT_FALSE(owner->db().table_exists("_lattice_canonical_coverage"));
    EXPECT_NO_THROW(owner->add(CustodyRecord{"legacy","body"}));
}

TEST_P(CanonicalWriterCustody, IdleRawPublicationRevokesAlreadyPreparedWorkAndPreventsReattachment) {
    attach();auto* trusted=canonical_writer_custody_test_access::fault_handle(owner->db());
    sqlite3_stmt* statement=nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(trusted,"INSERT INTO CustodyRecord(name,body) VALUES('cached','body')",-1,&statement,nullptr),SQLITE_OK);
    struct finish {sqlite3_stmt* statement;~finish(){sqlite3_finalize(statement);}} cleanup{statement};
    EXPECT_EQ(owner->db().handle(),trusted);
    EXPECT_NE(sqlite3_step(statement),SQLITE_DONE);EXPECT_EQ(rows(),0);EXPECT_EQ(receipts(),0);
    EXPECT_THROW(owner->add(CustodyRecord{"late","body"}),db_error);
    sqlite3_finalize(cleanup.statement);cleanup.statement=nullptr;
    adapter.reset();EXPECT_THROW(attach(),db_error);EXPECT_EQ(rows(),0);
}

TEST_P(CanonicalWriterCustody, RawRequestInsideTransactionRefusesWithoutRevokingValidWork) {
    attach();owner->begin_transaction();owner->add(CustodyRecord{"first","body"});
    EXPECT_THROW(owner->db().handle(),db_error);
    owner->add(CustodyRecord{"second","body"});owner->commit();
    EXPECT_EQ(rows(),2);EXPECT_EQ(receipts(),2);
}

TEST_P(CanonicalWriterCustody, ReentrantRawRequestDuringActualStatementsRefusesBeforePointerPublication) {
    attach();struct attempt {database* db;int calls=0,refused=0,published=0;} state{&owner->db()};
    auto* trusted=canonical_writer_custody_test_access::fault_handle(owner->db());
    ASSERT_EQ(sqlite3_create_function_v2(trusted,"custody_try_escape",0,SQLITE_UTF8,&state,
        [](sqlite3_context* context,int,sqlite3_value**) {
            auto& value=*static_cast<attempt*>(sqlite3_user_data(context));++value.calls;
            try {if(value.db->handle())++value.published;}catch(const db_error&){++value.refused;}
            catch(...){sqlite3_result_error(context,"unexpected raw request failure",-1);return;}
            sqlite3_result_text(context,"kept",-1,SQLITE_STATIC);
        },nullptr,nullptr,nullptr),SQLITE_OK);
    owner->db().execute("INSERT INTO CustodyRecord(name,body) VALUES(custody_try_escape(),'script')");
    owner->db().execute("INSERT INTO CustodyRecord(name,body) VALUES(custody_try_escape(),?)",{std::string("prepared")});
    EXPECT_EQ(owner->db().query("INSERT INTO CustodyRecord(name,body) VALUES(custody_try_escape(),'returning') RETURNING id").size(),1u);
    EXPECT_EQ(state.calls,3);EXPECT_EQ(state.refused,3);EXPECT_EQ(state.published,0);
    EXPECT_EQ(rows(),3);EXPECT_EQ(receipts(),3);
    ASSERT_EQ(sqlite3_create_function_v2(trusted,"custody_try_escape",0,SQLITE_UTF8,nullptr,nullptr,nullptr,nullptr,nullptr),SQLITE_OK);
    owner->add(CustodyRecord{"successor","body"});EXPECT_EQ(receipts(),4);
}

TEST_P(CanonicalWriterCustody, RetirementDoesNotAllowBorrowerOrPublicHooksToEraseRefusal) {
    attach();adapter.reset();capture_refused(owner->db());
    EXPECT_THROW(owner->db().set_txn_hooks({},{}),db_error);
    EXPECT_THROW(owner->db().execute("DROP TABLE _lattice_canonical_touch"),db_error);
    EXPECT_THROW(owner->add(CustodyRecord{"retired","body"}),db_error);EXPECT_EQ(rows(),0);
    attach();owner->add(CustodyRecord{"reattached","body"});EXPECT_EQ(receipts(),1);
}

TEST_P(CanonicalWriterCustody, ConstructorMovePreservesOwnedHooksAndCanonicalPolicy) {
    attach();auto& original=owner->db();database moved(std::move(original));
    EXPECT_THROW(moved.set_txn_hooks({},{}),db_error);EXPECT_EQ(count(moved,"CustodyRecord"),0);
    original=std::move(moved);
    owner->begin_transaction();owner->add(CustodyRecord{"rolled back","body"});owner->rollback();
    owner->add(CustodyRecord{"after move","body"});EXPECT_EQ(rows(),1);EXPECT_EQ(receipts(),1);
}

TEST_P(CanonicalWriterCustody, AssignmentMovePreservesRawRetirementAcrossPhysicalWrappers) {
    attach();auto& original=owner->db();database moved(":memory:");moved=std::move(original);
    EXPECT_THROW(moved.set_txn_hooks({},{}),db_error);ASSERT_NE(moved.handle(),nullptr);
    original=std::move(moved);EXPECT_THROW(owner->add(CustodyRecord{"retired","body"}),db_error);
    adapter.reset();EXPECT_THROW(attach(),db_error);EXPECT_EQ(rows(),0);
}

TEST_P(CanonicalWriterCustody, BootstrapOwnsPolicyBeforeTheContextIsPublished) {
    struct attempt {database* db;bool visited=false;int refused=0,published=0;bool unexpected=false;} state{&owner->db()};
    auto* trusted=canonical_writer_custody_test_access::fault_handle(owner->db());
    scoped_trace_reset trace_cleanup{trusted};
    ASSERT_EQ(sqlite3_trace_v2(trusted,SQLITE_TRACE_STMT,
        [](unsigned,void* data,void*,void*) -> int {
            auto& value=*static_cast<attempt*>(data);if(value.visited)return 0;value.visited=true;
            try {if(value.db->handle())++value.published;}catch(const db_error&){++value.refused;}catch(...){value.unexpected=true;}
            try {value.db->set_txn_hooks({},{});}catch(const db_error&){++value.refused;}catch(...){value.unexpected=true;}
            return 0;
        },&state),SQLITE_OK);
    // The first traced SQL runs inside real attachment, not a fabricated flag.
    attach();ASSERT_EQ(sqlite3_trace_v2(trusted,0,nullptr,nullptr),SQLITE_OK);
    EXPECT_TRUE(state.visited);EXPECT_EQ(state.refused,2);EXPECT_EQ(state.published,0);EXPECT_FALSE(state.unexpected);
    owner->add(CustodyRecord{"after bootstrap","body"});EXPECT_EQ(rows(),1);EXPECT_EQ(receipts(),1);
}

TEST_P(CanonicalWriterCustody, FailedBootstrapUnregistersTraceBeforeFixtureTeardown) {
    owner->db().execute("CREATE TEMP TABLE custody_forced_failure(value INTEGER)");
    struct attempt {int calls=0;} state;
    auto* trusted=canonical_writer_custody_test_access::fault_handle(owner->db());
    {
        scoped_trace_reset trace_cleanup{trusted};
        ASSERT_EQ(sqlite3_trace_v2(trusted,SQLITE_TRACE_STMT,
            [](unsigned,void* data,void*,void*) -> int {
                ++static_cast<attempt*>(data)->calls;return 0;
            },&state),SQLITE_OK);
        EXPECT_THROW(attach(),db_error);
        EXPECT_GT(state.calls,0);
    }
    // Retain the userdata while testing cleanup, so a missing unregister is
    // observed as a count failure instead of intentionally dereferencing UAF.
    const auto before=state.calls;
    EXPECT_EQ(owner->db().query("SELECT 1 AS value").size(),1u);
    EXPECT_EQ(state.calls,before);
    owner.reset(); // Includes database's destructor SQL, with state still live.
    EXPECT_EQ(state.calls,before);
}

TEST_P(CanonicalWriterCustody, PublicHookLifecycleRunsOutsideTheConnectionMutex) {
    // The probe state outlives the writer even if hook cleanup throws; stopping
    // its inspector before writer destruction removes every physical borrow.
    callback_mutex_probe probe;
    TempDB file{"canonical_callback_lifecycle"};
    database writer(GetParam()?file.str():":memory:");
    writer.execute("CREATE TABLE callback_lifecycle(value INTEGER)");
    auto* trusted=canonical_writer_custody_test_access::fault_handle(writer);
    ASSERT_NE(sqlite3_db_mutex(trusted),nullptr);
    probe.start(trusted);
    struct clear_before_probe {
        database& writer;callback_mutex_probe& probe;
        ~clear_before_probe() {
            try {writer.set_txn_hooks({},{});}catch(...){ADD_FAILURE()<<"hook cleanup failed";}
            probe.stop();
        }
    } cleanup{writer,probe};
    const auto checked=[&](callback_mutex_probe::sample before) {
        const auto after=probe.read();
        EXPECT_GT(after.requests,before.requests);
        EXPECT_EQ(after.completed,after.requests);
        EXPECT_EQ(after.busy,0u);EXPECT_EQ(after.timeouts,0u);EXPECT_FALSE(after.failed);
    };
    int first_settled=0,first_rollback=0,second_settled=0,second_rollback=0;
    auto before=probe.read();
    writer.set_txn_hooks(small_lifecycle_hook{probe,first_settled},small_lifecycle_hook{probe,first_rollback});
    checked(before);
    writer.mark_txn_dirty();writer.execute("SELECT 1");EXPECT_EQ(first_settled,1);
    writer.begin_transaction();writer.execute("INSERT INTO callback_lifecycle VALUES(1)");
    writer.mark_txn_dirty();writer.rollback();EXPECT_EQ(first_rollback,1);
    writer.execute("SELECT 1");EXPECT_EQ(first_settled,1);

    before=probe.read();
    writer.set_txn_hooks(small_lifecycle_hook{probe,second_settled},small_lifecycle_hook{probe,second_rollback});
    checked(before);
    writer.mark_txn_dirty();writer.execute("SELECT 1");EXPECT_EQ(second_settled,1);EXPECT_EQ(first_settled,1);
    writer.begin_transaction();writer.execute("INSERT INTO callback_lifecycle VALUES(2)");
    writer.mark_txn_dirty();writer.rollback();EXPECT_EQ(second_rollback,1);EXPECT_EQ(first_rollback,1);

    before=probe.read();writer.set_txn_hooks({},{});checked(before);
    writer.mark_txn_dirty();writer.execute("SELECT 1");EXPECT_EQ(second_settled,1);
}

TEST_P(CanonicalWriterCustody, PublicHookBundleSurvivesSelfReplacementAndPhysicalMoves) {
    int first=0,successor=0,rolled_back=0;bool retired=false;
    TempDB file{"canonical_callback_moves"};
    database writer(GetParam()?file.str():":memory:");
    writer.execute("CREATE TABLE callback_moves(value INTEGER)");
    struct lifetime {bool& retired;explicit lifetime(bool& flag):retired(flag) {}~lifetime(){retired=true;}};
    auto held=std::make_shared<lifetime>(retired);
    writer.set_txn_hooks([&,held] {
        ++first;EXPECT_FALSE(retired);
        writer.set_txn_hooks([&]{++successor;},[&]{++rolled_back;});
        EXPECT_FALSE(retired) << "the executing callback must survive its replacement";
    },[]{});
    held.reset();writer.mark_txn_dirty();writer.execute("SELECT 1");
    EXPECT_EQ(first,1);EXPECT_TRUE(retired);
    database moved(std::move(writer));
    moved.mark_txn_dirty();moved.execute("SELECT 1");EXPECT_EQ(successor,1);
    database destination(":memory:");destination=std::move(moved);
    destination.begin_transaction();destination.execute("INSERT INTO callback_moves VALUES(1)");
    destination.mark_txn_dirty();destination.rollback();EXPECT_EQ(rolled_back,1);
    destination.execute("SELECT 1");EXPECT_EQ(successor,1);
    destination.mark_txn_dirty();destination.execute("SELECT 1");EXPECT_EQ(successor,2);
    destination.set_txn_hooks({},{});
}

TEST_P(CanonicalWriterCustody, LocalProducerPolicyAlsoSurvivesBorrowerAndPublicHookRefusal) {
    recovery_obligation_producer_discovery_limits caps{{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
    recovery_obligation_profile binding{{"channel","authority","source","epoch","scope","schema"},"grant","namespace"};
    recovery_obligation_address address;
    const auto initialized=recovery_writer_access::install(owner,[&](auto&) {
        receive_install_store receiver(owner,caps.installations);receiver.initialize();receiver.bind(binding.binding);
        recovery_obligation_store journal(owner,caps.obligations,caps.installations);journal.initialize();address=journal.bind(binding).address;
    });
    ASSERT_EQ(initialized.state,recovery_install_state::committed);
    const auto enrolled=recovery_local_producer_adapter::enroll_for_qualification(owner,{address,{"CustodyRecord"},{'c'}},caps);
    ASSERT_EQ(enrolled.state,recovery_install_state::committed);
    capture_refused(owner->db());EXPECT_THROW(owner->db().set_txn_hooks({},{}),db_error);
    EXPECT_THROW(attach(),db_error);
    owner->add(CustodyRecord{"preserved producer","body"});EXPECT_EQ(rows(),1);
    EXPECT_EQ(count(owner->db(),"_lattice_obligation_producer_stamp"),1);
}

INSTANTIATE_TEST_SUITE_P(Storage,CanonicalWriterCustody,::testing::Bool());

#if defined(__APPLE__) || defined(__linux__)
namespace {
using namespace lattice;
using identity_access = lattice::detail::canonical_writer_custody_test_access;

// Actual SQLite mutex ownership on another joined thread. No callback, SQL,
// global registry lock or source owner is manufactured by the observation.
class held_identity_mutex {
    std::mutex mutex;
    std::condition_variable changed;
    bool acquired = false, released = false;
    std::thread worker;
public:
    explicit held_identity_mutex(sqlite3_mutex* target) : worker([this, target] {
        sqlite3_mutex_enter(target);
        {
            std::unique_lock lock(mutex);
            acquired = true; changed.notify_all();
            changed.wait(lock, [&] { return released; });
        }
        sqlite3_mutex_leave(target);
    }) {}
    bool ready() {
        std::unique_lock lock(mutex);
        return changed.wait_for(lock, std::chrono::seconds(2), [&] { return acquired; });
    }
    ~held_identity_mutex() {
        { std::lock_guard lock(mutex); released = true; }
        changed.notify_all(); worker.join();
    }
};

TEST(PhysicalIdentityObservation, ActualFileSuccessHasNoFailureAndMatchesLegacyIdentity) {
    TempDB file("identity_observation_success"); database writer(file.str());
    writer.execute("CREATE TABLE IdentityValue(id INTEGER)");
    const auto actual = identity_access::observe_identity(writer);
    ASSERT_TRUE(actual.identity); EXPECT_EQ(actual.failure, nullptr);
    const auto legacy = writer.physical_identity("main", {}, true);
    ASSERT_TRUE(legacy); EXPECT_EQ(*actual.identity, *legacy);
    EXPECT_EQ(actual.identity->filename, legacy->filename);
}

TEST(PhysicalIdentityObservation, NonFilesystemRefusalRemainsNullWithBoundedReason) {
    database writer(":memory:");
    const auto actual = identity_access::observe_identity(writer);
    EXPECT_FALSE(actual.identity); EXPECT_STREQ(actual.failure, "missing_file_name");
    EXPECT_FALSE(writer.physical_identity("main", {}, true));
}

TEST(PhysicalIdentityObservation, ActualMutexContentionIsDistinctFromMovedFileAndDoesNotBypassValidation) {
    TempDB file("identity_observation_busy");
    database writer(file.str(), database::open_mode::read_write, 20);
    writer.execute("CREATE TABLE IdentityValue(id INTEGER)");
    const auto before = writer.physical_identity("main", {}, true); ASSERT_TRUE(before);
    auto* mutex = sqlite3_db_mutex(identity_access::fault_handle(writer)); ASSERT_NE(mutex, nullptr);
    {
        held_identity_mutex held(mutex); ASSERT_TRUE(held.ready());
        const auto actual = identity_access::observe_identity(writer);
        EXPECT_FALSE(actual.identity); EXPECT_STREQ(actual.failure, "metadata_busy");
        EXPECT_FALSE(writer.physical_identity("main", {}, true));
        // Legacy callers requesting the existing cache keep that exact behavior.
        const auto cached = identity_access::observe_identity(writer, {}, false);
        EXPECT_EQ(cached.identity, before); EXPECT_EQ(cached.failure, nullptr);
    }
    const auto after = identity_access::observe_identity(writer);
    ASSERT_TRUE(after.identity); EXPECT_EQ(after.failure, nullptr);
    EXPECT_EQ(*after.identity, *before);
}

TEST(PhysicalIdentityObservation, ActualCancelledControlStillRefusesCurrentIdentity) {
    TempDB file("identity_observation_cancelled"); database writer(file.str());
    writer.execute("CREATE TABLE IdentityValue(id INTEGER)");
    auto control = std::make_shared<database_read_control>();
    control->deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    control->stop(1);
    const auto actual = identity_access::observe_identity(writer, control);
    EXPECT_FALSE(actual.identity); EXPECT_STREQ(actual.failure, "metadata_cancelled");
    EXPECT_FALSE(writer.physical_identity("main", control, true));
    const auto current = identity_access::observe_identity(writer);
    EXPECT_TRUE(current.identity); EXPECT_EQ(current.failure, nullptr);
}

// Called only by an existing failed assertion's stream. No pathname, raw
// SQLite source ID, error text, SQL, or extra VFS/filesystem operation is used.
std::string identity_failure_details(const identity_access::identity_observation& value,
                                    int original_restore, int successor_restore) noexcept {
    try {
        const auto& d = value.details;
        const char* source = sqlite3_sourceid();
        size_t source_bytes = 0;
        while (source && source_bytes < 128 && source[source_bytes]) ++source_bytes;
        const auto source_hash = source ? picosha2::hash256_hex_string(source, source + source_bytes) : "unavailable";
        std::ostringstream out;
        out << " identityDetailsV1 platform="
#if defined(__APPLE__)
            << "apple"
#else
            << "linux"
#endif
            << " sqliteVersion=" << sqlite3_libversion_number() << " sourceIDPrefixSHA256=" << source_hash
            << " sourceIDBytes=" << source_bytes << " sourceIDAtCap=" << (source_bytes == 128)
            << " failure=" << (value.failure ? value.failure : "none")
            << " stage=" << static_cast<int>(d.last_stage) << " mutexAcquired=" << d.mutex_acquired
            << " vfs=" << static_cast<int>(d.vfs) << " vfsObserved=" << d.vfs_result_observed << " vfsRC=" << d.vfs_rc
            << " firstObserved=" << d.first_move.observed << " firstRC=" << d.first_move.rc
            << " firstMovedValid=" << d.first_move.moved_valid << " firstMoved=" << d.first_move.moved
            << " secondObserved=" << d.second_move.observed << " secondRC=" << d.second_move.rc
            << " secondMovedValid=" << d.second_move.moved_valid << " secondMoved=" << d.second_move.moved
            << " originalRestoreRC=" << original_restore << " successorRestoreRC=" << successor_restore;
        auto text = out.str();
        return text.size() <= 1024 ? text : " identityDetailsV1 unavailable";
    } catch (...) { return {}; }
}

TEST(PhysicalIdentityObservation, ActualFileReplacementRemainsRefusedWithoutLeakingItsPath) {
    TempDB original("identity_observation_original"), successor("identity_observation_successor"), moved("identity_observation_moved");
    database writer(original.str()); writer.execute("CREATE TABLE IdentityValue(id INTEGER)");
    { database other(successor.str()); other.execute("CREATE TABLE IdentityValue(id INTEGER)"); }
    const auto before = writer.physical_identity("main", {}, true); ASSERT_TRUE(before);
    // Rename guards restore the actual paths before any assertion or database
    // destructor can execute SQL. Construction failures unwind the first move.
    std::error_code original_restore, successor_restore;
    struct rename_back {
        std::filesystem::path from, to;
        std::error_code& restore_error;
        rename_back(std::filesystem::path from, std::filesystem::path to, std::error_code& restore_error)
            : from(std::move(from)), to(std::move(to)), restore_error(restore_error) {
            std::filesystem::rename(this->from, this->to);
        }
        ~rename_back() { std::filesystem::rename(to, from, restore_error); }
    };
    identity_access::identity_observation actual{};
    std::shared_ptr<const physical_store_identity> legacy;
    {
        rename_back first(original.path, moved.path, original_restore), second(successor.path, original.path, successor_restore);
        actual = identity_access::observe_identity_details(writer);
        legacy = writer.physical_identity("main", {}, true);
    }
    EXPECT_FALSE(actual.identity) << identity_failure_details(actual, original_restore.value(), successor_restore.value());
#if defined(__APPLE__)
    // Apple's SQLite can permanently refuse this handle after observing the
    // deliberate rename. That exact extension error is also a refusal, never
    // proof that the pathname still identifies the opened file.
    if (actual.details.first_move.observed && actual.details.first_move.rc == SQLITE_IOERR_VNODE) {
        EXPECT_STREQ(actual.failure, "move_check_unavailable");
        EXPECT_FALSE(actual.details.first_move.moved_valid);
    } else
#endif
    EXPECT_STREQ(actual.failure, "file_moved") << identity_failure_details(actual, original_restore.value(), successor_restore.value());
    EXPECT_FALSE(legacy) << identity_failure_details(actual, original_restore.value(), successor_restore.value());
    ASSERT_FALSE(original_restore) << original_restore.message();
    ASSERT_FALSE(successor_restore) << successor_restore.message();
    const auto restored = identity_access::observe_identity_details(writer);
#if defined(__APPLE__)
    // The asynchronous vnode notification may arrive before or after the first
    // observation. Restoring the name does not repair an invalidated handle.
    const bool first_vnode = restored.details.first_move.observed && restored.details.first_move.rc == SQLITE_IOERR_VNODE;
    const bool second_vnode = restored.details.second_move.observed && restored.details.second_move.rc == SQLITE_IOERR_VNODE;
    if (first_vnode || second_vnode) {
        EXPECT_FALSE(restored.identity);
        if (first_vnode) {
            EXPECT_STREQ(restored.failure, "move_check_unavailable");
            EXPECT_FALSE(restored.details.first_move.moved_valid);
            EXPECT_FALSE(restored.details.second_move.observed);
        } else {
            EXPECT_STREQ(restored.failure, "recheck_unavailable");
            EXPECT_TRUE(restored.details.first_move.observed);
            EXPECT_EQ(restored.details.first_move.rc, SQLITE_OK);
            EXPECT_TRUE(restored.details.first_move.moved_valid);
            EXPECT_EQ(restored.details.first_move.moved, 0);
            EXPECT_FALSE(restored.details.second_move.moved_valid);
        }
        database reopened(original.str(), database::open_mode::read_only);
        const auto current = identity_access::observe_identity_details(reopened);
        ASSERT_TRUE(current.identity) << identity_failure_details(current, original_restore.value(), successor_restore.value());
        EXPECT_EQ(current.failure, nullptr);
        EXPECT_EQ(*current.identity, *before);
    } else
#endif
    {
        ASSERT_TRUE(restored.identity) << identity_failure_details(restored, original_restore.value(), successor_restore.value());
        EXPECT_EQ(restored.failure, nullptr) << identity_failure_details(restored, original_restore.value(), successor_restore.value());
        EXPECT_EQ(*restored.identity, *before);
    }
}

TEST(PhysicalIdentityDetail, ActualSuccessReportsBothChecksAndMatchesNilDetailsPath) {
    TempDB file("identity_detail_success"); database writer(file.str());
    writer.execute("CREATE TABLE IdentityValue(id INTEGER)");
    const auto actual = identity_access::observe_identity_details(writer);
    ASSERT_TRUE(actual.identity); EXPECT_EQ(actual.failure, nullptr);
    const auto nil_details = identity_access::observe_identity(writer);
    ASSERT_TRUE(nil_details.identity); EXPECT_EQ(nil_details.failure, nullptr);
    EXPECT_EQ(*actual.identity, *nil_details.identity);
    EXPECT_EQ(actual.identity->filename, nil_details.identity->filename);
    const auto& d = actual.details;
    EXPECT_EQ(d.last_stage, identity_access::identity_details::stage::complete);
    EXPECT_TRUE(d.mutex_acquired); EXPECT_TRUE(d.vfs_result_observed); EXPECT_EQ(d.vfs_rc, SQLITE_OK);
    EXPECT_TRUE(d.vfs == identity_access::identity_details::vfs_kind::unix_vfs ||
                d.vfs == identity_access::identity_details::vfs_kind::unix_excl);
    EXPECT_TRUE(d.first_move.observed); EXPECT_EQ(d.first_move.rc, SQLITE_OK);
    EXPECT_TRUE(d.first_move.moved_valid); EXPECT_EQ(d.first_move.moved, 0);
    EXPECT_TRUE(d.second_move.observed); EXPECT_EQ(d.second_move.rc, SQLITE_OK);
    EXPECT_TRUE(d.second_move.moved_valid); EXPECT_EQ(d.second_move.moved, 0);
}

TEST(PhysicalIdentityDetail, ActualBusyAndExplicitCacheDoNotInventVFSResults) {
    TempDB file("identity_detail_busy");
    database writer(file.str(), database::open_mode::read_write, 20);
    writer.execute("CREATE TABLE IdentityValue(id INTEGER)");
    const auto before = writer.physical_identity("main", {}, true); ASSERT_TRUE(before);
    auto* mutex = sqlite3_db_mutex(identity_access::fault_handle(writer)); ASSERT_NE(mutex, nullptr);
    {
        held_identity_mutex held(mutex); ASSERT_TRUE(held.ready());
        const auto busy = identity_access::observe_identity_details(writer);
        EXPECT_FALSE(busy.identity); EXPECT_STREQ(busy.failure, "metadata_busy");
        EXPECT_EQ(busy.details.last_stage, identity_access::identity_details::stage::connection_mutex);
        EXPECT_FALSE(busy.details.mutex_acquired); EXPECT_FALSE(busy.details.vfs_result_observed);
        EXPECT_FALSE(busy.details.first_move.observed); EXPECT_FALSE(busy.details.second_move.observed);
        EXPECT_FALSE(busy.details.first_move.moved_valid); EXPECT_FALSE(busy.details.second_move.moved_valid);
        const auto cached = identity_access::observe_identity_details(writer, {}, false);
        EXPECT_EQ(cached.identity, before); EXPECT_EQ(cached.failure, nullptr);
        EXPECT_EQ(cached.details.last_stage, identity_access::identity_details::stage::cached_identity);
        EXPECT_FALSE(cached.details.mutex_acquired); EXPECT_FALSE(cached.details.vfs_result_observed);
        EXPECT_FALSE(cached.details.first_move.observed); EXPECT_FALSE(cached.details.second_move.observed);
    }
    const auto current = identity_access::observe_identity_details(writer);
    ASSERT_TRUE(current.identity); EXPECT_EQ(*current.identity, *before);
    EXPECT_TRUE(current.details.first_move.observed); EXPECT_TRUE(current.details.second_move.observed);
}

TEST(PhysicalIdentityDetail, ActualCancelledAndNonFileCaptureDoNotInventFileControls) {
    TempDB file("identity_detail_cancelled"); database writer(file.str());
    writer.execute("CREATE TABLE IdentityValue(id INTEGER)");
    auto control = std::make_shared<database_read_control>();
    control->deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5); control->stop(1);
    const auto canceled = identity_access::observe_identity_details(writer, control);
    EXPECT_FALSE(canceled.identity); EXPECT_STREQ(canceled.failure, "metadata_cancelled");
    EXPECT_TRUE(canceled.details.mutex_acquired); EXPECT_FALSE(canceled.details.vfs_result_observed);
    EXPECT_FALSE(canceled.details.first_move.observed); EXPECT_FALSE(canceled.details.second_move.observed);
    database memory(":memory:");
    const auto nonfile = identity_access::observe_identity_details(memory);
    EXPECT_FALSE(nonfile.identity); EXPECT_STREQ(nonfile.failure, "missing_file_name");
    EXPECT_EQ(nonfile.details.last_stage, identity_access::identity_details::stage::file_name);
    EXPECT_TRUE(nonfile.details.mutex_acquired); EXPECT_FALSE(nonfile.details.vfs_result_observed);
    EXPECT_FALSE(nonfile.details.first_move.observed); EXPECT_FALSE(nonfile.details.second_move.observed);
}

TEST(PhysicalIdentityDetail, MoveOutputValidityRequiresTheActualOKReturnAndClearsPriorValue) {
    identity_access::identity_details::move_result copied;
    EXPECT_FALSE(copied.observed); EXPECT_FALSE(copied.moved_valid);
    copied.record(SQLITE_OK, 1);
    EXPECT_TRUE(copied.observed); EXPECT_EQ(copied.rc, SQLITE_OK);
    EXPECT_TRUE(copied.moved_valid); EXPECT_EQ(copied.moved, 1);
    copied.record(SQLITE_NOTFOUND, 123);
    EXPECT_TRUE(copied.observed); EXPECT_EQ(copied.rc, SQLITE_NOTFOUND);
    EXPECT_FALSE(copied.moved_valid); EXPECT_EQ(copied.moved, 0);
    copied.record(SQLITE_IOERR, 1);
    EXPECT_EQ(copied.rc, SQLITE_IOERR); EXPECT_FALSE(copied.moved_valid); EXPECT_EQ(copied.moved, 0);
    copied.record(SQLITE_OK, 0);
    EXPECT_EQ(copied.rc, SQLITE_OK); EXPECT_TRUE(copied.moved_valid); EXPECT_EQ(copied.moved, 0);
}
}
#endif
