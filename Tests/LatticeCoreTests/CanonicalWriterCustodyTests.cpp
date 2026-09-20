#include "TestHelpers.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include "../../Sources/LatticeCore/src/projection_memory.hpp"
#include "../../Sources/LatticeCore/src/recovery_local_producer.hpp"

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
