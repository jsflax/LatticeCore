#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_obligation_producer.hpp"
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"
#include <future>
#include <iomanip>
#include <map>
#include <sstream>

namespace lattice::detail {
struct recovery_obligation_producer_test_access {
    static recovery_obligation_producer_store store(std::shared_ptr<lattice_db> o,recovery_obligation_limits l,receive_install_limits i,recovery_obligation_producer_limits p){return {std::move(o),l,i,p};}
    static void initialize(recovery_obligation_producer_store& s){s.initialize();}
    static auto enroll(recovery_obligation_producer_store& s,const recovery_obligation_producer_profile& p){return s.enroll(p);}
    static auto compile(const recovery_obligation_producer_profile& p,recovery_obligation_limits o,recovery_obligation_producer_limits l){return recovery_obligation_producer_store::compile(p,o,l);}
    static auto profiles(const recovery_obligation_producer_store& s){return s.profiles();}
    static auto bootstrap(std::shared_ptr<database> p,const recovery_obligation_producer_discovery_limits& l,
        const std::function<void(database&,const recovery_obligation_producer_inventory&)>& validate={}){return recovery_obligation_producer_store::bootstrap_profiles(std::move(p),l,validate);}
    static void retire(recovery_obligation_producer_store& s,const recovery_obligation_address& a,const recovery_obligation_producer_profile& p){s.retire_contribution(a,p);}
};
}
namespace {
using namespace lattice::detail;
using access=recovery_obligation_producer_test_access;
using blob=std::vector<uint8_t>;
using error=recovery_obligation_error_code;
std::string id(int value){std::ostringstream s;s<<"AAAAAAAA-0000-4000-8000-"<<std::hex<<std::setw(12)<<std::setfill('0')<<value;return s.str();}
std::string key(std::string s){for(auto& c:s)if(c>='A'&&c<='F')c+=32;return s;}
blob binary(const std::string& s){return {s.begin(),s.end()};}
int64_t scalar(lattice::database& db,const std::string& sql){return std::get<int64_t>(db.query(sql).at(0).at("n"));}
template<class F>void refuses(error code,F&& f){try{f();FAIL()<<"expected refusal";}catch(const recovery_obligation_error& e){EXPECT_EQ(e.code,code)<<e.what();}}
// The storage fixture intentionally supplies a test-only nonwriting gate. It
// does not qualify real adapter registration/authorizer or local-origin proof.
struct Gate{bool allowed=true;};
void guard(sqlite3_context* c,int argc,sqlite3_value** v)noexcept{
    auto* g=static_cast<Gate*>(sqlite3_user_data(c));sqlite3_result_int(c,argc==6&&g&&g->allowed&&sqlite3_value_int(v[5])==1?1:0);
}
void canonical_uuid(sqlite3_context* c,int argc,sqlite3_value** v)noexcept{
    if(argc!=1||sqlite3_value_type(v[0])!=SQLITE_TEXT||sqlite3_value_bytes(v[0])!=36){sqlite3_result_null(c);return;}
    const auto* in=sqlite3_value_text(v[0]);if(!in){sqlite3_result_error_nomem(c);return;}unsigned char out[36];
    for(int i=0;i<36;++i){auto x=in[i];if(i==8||i==13||i==18||i==23){if(x!='-'){sqlite3_result_null(c);return;}}else if(x>='A'&&x<='F')x+=32;else if(!((x>='a'&&x<='f')||(x>='0'&&x<='9'))){sqlite3_result_null(c);return;}out[i]=x;}
    sqlite3_result_blob(c,out,36,SQLITE_TRANSIENT);
}
lattice::configuration producer_config(const std::string& path){lattice::configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=50;return c;}
class ProducerOwner:public lattice::lattice_db{
public:
    explicit ProducerOwner(const std::string& path):lattice_db(producer_config(path)){add(TestPerson{"seed",0,std::nullopt});}
    void generated(const std::string& tail,bool no_history=false){
        drop_model_table_triggers("TestPerson");
        create_model_table_triggers("TestPerson",{{"name",lattice::column_type::text},{"age",lattice::column_type::integer}},no_history?std::set<std::string>{"name"}:std::set<std::string>{},tail);
    }
    void generated_link(const std::string& tail){
        db().execute("CREATE TABLE _ProducerLinks(globalId TEXT PRIMARY KEY,lhs TEXT NOT NULL,rhs TEXT NOT NULL)");
        create_link_table_triggers("_ProducerLinks",tail);
    }
};
struct Fixture{
    recovery_obligation_limits ol{4,128,256,2*1024*1024};
    receive_install_limits il{4,256,65536};
    recovery_obligation_producer_limits pl{4,128,256,4096,2*1024*1024};
    Gate gate;
    std::shared_ptr<ProducerOwner> owner;
    recovery_obligation_profile contribution{{"channel","authority","source","epoch","scope","schema"},"profile","receipts"};
    recovery_obligation_address address;
    recovery_obligation_producer_profile profile;
    std::optional<recovery_obligation_producer_program> program;
    explicit Fixture(const std::string& path=":memory:",int64_t stamps=128):owner(std::make_shared<ProducerOwner>(path)){
        pl.stamps=stamps;ol.records=std::max<int64_t>(ol.records,stamps);
        if(path!=":memory:"){auto* n=lattice::instance_registry::instance().get_or_create_notifier(path);if(n)n->stop_listening();}
        auto* h=owner->db().handle();
        if(sqlite3_create_function_v2(h,"lattice_recovery_producer_guard_v1",6,SQLITE_UTF8,&gate,guard,nullptr,nullptr,nullptr)!=SQLITE_OK ||
           sqlite3_create_function_v2(h,"lattice_recovery_producer_uuid_v1",1,SQLITE_UTF8|SQLITE_DETERMINISTIC,nullptr,canonical_uuid,nullptr,nullptr,nullptr)!=SQLITE_OK)throw std::runtime_error("test function setup failed");
        committed([&](auto&){receive_install_store receiver(owner,il);receiver.initialize();receiver.bind(contribution.binding);auto s=ordinary();s.initialize();address=s.bind(contribution).address;
            auto p=storage();access::initialize(p);profile={contribution,address.incarnation,1,"program",blob{1,0,2}};program=access::enroll(p,profile);});
    }
    recovery_obligation_store ordinary(){return recovery_obligation_store(owner,ol,il);}
    recovery_obligation_producer_store storage(){return access::store(owner,ol,il,pl);}
    recovery_obligation_producer_discovery_limits discovery()const{return {ol,il,pl};}
    template<class F>void committed(F&& f){auto r=recovery_writer_access::install(owner,std::forward<F>(f));if(r.primary_error)std::rethrow_exception(r.primary_error);if(r.cleanup_error)std::rethrow_exception(r.cleanup_error);if(r.postcommit_error)std::rethrow_exception(r.postcommit_error);if(r.state!=recovery_install_state::committed)throw std::runtime_error("test owned transaction did not commit");}
    void enable(bool no_history=false){owner->generated(program->emit_tail("TestPerson"),no_history);}
    recovery_obligation_record latest(const std::string& target,const std::string& table="TestPerson"){
        const auto r=owner->db().query("SELECT id,globalId,tableName,globalRowId FROM AuditLog WHERE tableName=? AND globalRowId=? ORDER BY id DESC LIMIT 1",{table,target}).at(0);
        return {std::get<int64_t>(r.at("id")),std::get<std::string>(r.at("globalId")),std::get<std::string>(r.at("tableName")),std::get<std::string>(r.at("globalRowId")),recovery_obligation_origin::local_candidate};
    }
    recovery_obligation_record insert(int value){owner->db().execute("INSERT INTO TestPerson(globalId,name,age) VALUES(?,?,?)",{id(value),std::string("value"),int64_t(value)});return latest(id(value));}
    auto stamp(const recovery_obligation_record& r){return recovery_obligation_producer_store::read_stamp(owner,ol,il,pl,address,r.original_id);}
    auto image(){std::map<std::string,std::vector<lattice::database::row_t>> rows;for(const auto* table:{"TestPerson","AuditLog","_lattice_obligation_store","_lattice_obligation_scope","_lattice_obligation_entry","_lattice_obligation_producer_store","_lattice_obligation_producer_profile","_lattice_obligation_producer_stamp"})rows.emplace(table,owner->db().query(std::string("SELECT * FROM ")+table+" ORDER BY 1,2"));return rows;}
    void audit(){committed([&](auto&){ordinary().audit();auto p=storage();EXPECT_EQ(access::profiles(p),std::vector<recovery_obligation_producer_profile>{profile});});}
};
}

TEST(RecoveryObligationProducer, ImplicitGeneratedInsertUpdateDeleteCarryOriginalStamp){
    Fixture f;f.enable();auto inserted=f.insert(1);auto body=f.owner->db().query("SELECT * FROM AuditLog WHERE id=?",{inserted.audit_id});
    f.committed([&](auto&){auto e=f.ordinary().find(f.address,inserted.original_id);ASSERT_TRUE(e);auto stamp=f.stamp(inserted);ASSERT_TRUE(stamp);EXPECT_EQ(stamp->audit_id,inserted.audit_id);EXPECT_EQ(stamp->record_sequence,e->sequence);EXPECT_EQ(e->record,inserted);});
    f.owner->db().execute("UPDATE TestPerson SET name='updated' WHERE globalId=?",{id(1)});const auto updated=f.latest(id(1));
    f.owner->db().execute("DELETE FROM TestPerson WHERE globalId=?",{id(1)});const auto deleted=f.latest(id(1));
    f.committed([&](auto& db){EXPECT_TRUE(f.stamp(updated));EXPECT_TRUE(f.stamp(deleted));EXPECT_EQ(f.ordinary().usage().records,3);EXPECT_EQ(db.query("SELECT * FROM AuditLog WHERE id=?",{inserted.audit_id}),body);});f.audit();
}
TEST(RecoveryObligationProducer, ExplicitTransactionAndModelStatementRollbackRestoreAllCharges){
    Fixture f;f.enable();const auto before=f.image();f.owner->begin_transaction();f.insert(2);f.owner->rollback();EXPECT_EQ(f.image(),before);
    f.gate.allowed=false;EXPECT_THROW(f.insert(2),lattice::db_error);EXPECT_EQ(f.image(),before);f.gate.allowed=true;auto kept=f.insert(2);
    f.committed([&](auto&){EXPECT_TRUE(f.stamp(kept));});f.audit();
}
TEST(RecoveryObligationProducer, MultirowCapacityRefusalRollsBackEarlierRowsOfSameStatement){
    Fixture f(":memory:",1);f.enable();const auto before=f.image();
    EXPECT_THROW(f.owner->db().execute("INSERT INTO TestPerson(globalId,name,age) VALUES(?, 'one', 1),(?, 'two', 2)",{id(3),id(4)}),lattice::db_error);
    EXPECT_EQ(f.image(),before);auto kept=f.insert(3);f.committed([&](auto&){EXPECT_TRUE(f.stamp(kept));});f.audit();
}
TEST(RecoveryObligationProducer, CallerRecordNeverMintsStampOrChangesExistingOrigin){
    Fixture f;const auto actual=f.insert(5);f.committed([&](auto&){auto s=f.ordinary();auto e=s.record(f.address,actual);EXPECT_FALSE(f.stamp(actual));EXPECT_EQ(s.record(f.address,actual),e);
        auto imported=actual;imported.origin=recovery_obligation_origin::imported;refuses(error::conflict,[&]{s.record(f.address,imported);});EXPECT_FALSE(f.stamp(actual));});
    f.enable();auto fresh=f.insert(6);f.committed([&](auto&){EXPECT_FALSE(f.stamp(actual));EXPECT_TRUE(f.stamp(fresh));});f.audit();
}
TEST(RecoveryObligationProducer, FrozenAndInstalledLateWritesUseCurrentDurableGeneration){
    Fixture f;f.enable();auto first=f.insert(7);
    f.committed([&](auto&){f.address=f.ordinary().freeze(f.address,1).address;});auto late=f.insert(8);
    f.committed([&](auto& db){auto s=f.ordinary();auto snapshot=s.snapshot_for_install(f.address,1);ASSERT_EQ(snapshot.entries.size(),2u);
        receive_install_identity i{1,0,{},10,receive_install_mode::full,"Q","E","C","M"};
        receive_install_store receiver(f.owner,f.il);auto applied=receiver.apply_if_new(f.contribution.binding,i,{},[](auto&){});ASSERT_EQ(applied.disposition,receive_install_disposition::installed);
        s.settle_install(f.address,snapshot.scope.revision,i,{{first.original_id,"receipts",5,recovery_obligation_outcome::applied}});
        EXPECT_TRUE(f.stamp(late));EXPECT_EQ(s.usage().records,2);});
    auto after=f.insert(9);f.committed([&](auto&){EXPECT_TRUE(f.stamp(after));EXPECT_EQ(f.ordinary().read("channel")->mode,recovery_obligation_mode::installed);});f.audit();
}
TEST(RecoveryObligationProducer, SettledStampRetainsCapacityAndPendingBodyPin){
    Fixture f(":memory:",1);f.enable();auto r=f.insert(10);f.committed([&](auto&){auto s=f.ordinary();f.address=s.freeze(f.address,1).address;auto snap=s.snapshot_for_install(f.address,1);
        receive_install_identity i{1,0,{},5,receive_install_mode::full,"Q","E","C","M"};receive_install_store(f.owner,f.il).apply_if_new(f.contribution.binding,i,{},[](auto&){});
        s.settle_install(f.address,snap.scope.revision,i,{{r.original_id,"receipts",5,recovery_obligation_outcome::applied}});EXPECT_FALSE(s.pins_audit(r.audit_id,r.original_id));EXPECT_TRUE(f.stamp(r));});
    const auto before=f.image();EXPECT_THROW(f.insert(11),lattice::db_error);EXPECT_EQ(f.image(),before);
}
TEST(RecoveryObligationProducer, RealRegularLinkAuditIDIsPositiveAlthoughModelRowIDIsZero){
    Fixture f;f.owner->generated_link(f.program->emit_tail("_ProducerLinks",true));
    f.owner->db().execute("INSERT INTO _ProducerLinks VALUES(?,?,?)",{id(12),id(13),id(14)});const auto inserted=f.latest(id(12),"_ProducerLinks");
    EXPECT_GT(inserted.audit_id,0);EXPECT_EQ(scalar(f.owner->db(),"SELECT rowId AS n FROM AuditLog WHERE tableName='_ProducerLinks' ORDER BY id DESC LIMIT 1"),0);
    f.owner->db().execute("DELETE FROM _ProducerLinks WHERE globalId=?",{id(12)});const auto deleted=f.latest(id(12),"_ProducerLinks");
    f.committed([&](auto&){EXPECT_TRUE(f.stamp(inserted));EXPECT_TRUE(f.stamp(deleted));});f.audit();
}
TEST(RecoveryObligationProducer, NoHistoryKeepsOriginalNullPlaceholderAndFullCurrentValue){
    Fixture f;f.enable(true);f.insert(15);const std::string text("latest\0owned",12);
    f.owner->db().execute("UPDATE TestPerson SET name=? WHERE globalId=?",{text,id(15)});const auto r=f.latest(id(15));
    auto before=f.owner->db().query("SELECT changedFields,changedFieldsNames FROM AuditLog WHERE id=?",{r.audit_id});
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(f.owner->db().query("SELECT json_extract(changedFields,'$.name') AS value FROM AuditLog WHERE id=?",{r.audit_id}).at(0).at("value")));
    EXPECT_EQ(std::get<std::string>(f.owner->db().query("SELECT name FROM TestPerson WHERE globalId=?",{id(15)}).at(0).at("name")),text);
    f.committed([&](auto& db){EXPECT_TRUE(f.stamp(r));EXPECT_EQ(db.query("SELECT changedFields,changedFieldsNames FROM AuditLog WHERE id=?",{r.audit_id}),before);});f.audit();
}
TEST(RecoveryObligationProducer, StampAndCounterIgnoredWritesRefuseWithNoPartialModelEffect){
    for(const auto* location:{"stamp_insert","base_counter","scope_revision","producer_counter","entry_insert"}){
        SCOPED_TRACE(location);Fixture f;f.enable();const auto before=f.image();std::string statement;
        if(std::string(location)=="stamp_insert")statement="BEFORE INSERT ON _lattice_obligation_producer_stamp";
        else if(std::string(location)=="base_counter")statement="BEFORE UPDATE ON _lattice_obligation_store";
        else if(std::string(location)=="scope_revision")statement="BEFORE UPDATE ON _lattice_obligation_scope";
        else if(std::string(location)=="producer_counter")statement="BEFORE UPDATE ON _lattice_obligation_producer_store";
        else statement="BEFORE INSERT ON _lattice_obligation_entry";
        f.owner->db().execute("CREATE TRIGGER _producer_fault "+statement+" BEGIN SELECT RAISE(IGNORE); END");
        EXPECT_THROW(f.insert(16),lattice::db_error);EXPECT_EQ(f.image(),before);
        f.owner->db().execute("DROP TRIGGER _producer_fault");f.insert(16);f.audit();
    }
}
TEST(RecoveryObligationProducer, ProgramProfileMismatchAndInvalidUUIDRefuseOriginatingStatement){
    Fixture f;f.enable();const auto before=f.image();
    f.owner->db().execute("UPDATE _lattice_obligation_producer_profile SET program_revision=2");const auto changed=f.image();EXPECT_THROW(f.insert(17),lattice::db_error);EXPECT_EQ(f.image(),changed);
    f.owner->db().execute("UPDATE _lattice_obligation_producer_profile SET program_revision=1");EXPECT_EQ(f.image(),before);
    EXPECT_THROW(f.owner->db().execute("INSERT INTO TestPerson(globalId,name,age) VALUES('not-a-uuid','bad',1)"),lattice::db_error);EXPECT_EQ(f.image(),before);f.insert(17);f.audit();
}
TEST(RecoveryObligationProducer, OwnedReadGuardRejectsRawAndOtherThreadTransactions){
    Fixture f;auto p=f.storage();refuses(error::transaction_required,[&]{access::profiles(p);});
    f.owner->db().execute("BEGIN IMMEDIATE");refuses(error::transaction_required,[&]{access::profiles(p);});f.owner->db().rollback();
    f.owner->begin_transaction();auto other=std::async(std::launch::async,[&]{try{access::profiles(p);return false;}catch(const recovery_obligation_error& e){return e.code==error::transaction_required;}});EXPECT_TRUE(other.get());f.owner->rollback();
}
TEST(RecoveryObligationProducer, BootstrapReopensReadOnlyFactsWithIndependentCapsAndNoMutation){
    TempDB path("producer-bootstrap");recovery_obligation_producer_discovery_limits caps;recovery_obligation_producer_profile profile;
    {Fixture f(path.str());f.enable();f.insert(18);caps=f.discovery();profile=f.profile;}
    auto physical=std::make_shared<lattice::database>(path.str(),lattice::database::open_mode::read_only);auto before=sqlite3_total_changes64(physical->handle());
    const auto facts=access::bootstrap(physical,caps);ASSERT_TRUE(facts.initialized);EXPECT_EQ(facts.profiles,std::vector<recovery_obligation_producer_profile>{profile});EXPECT_FALSE(physical->is_in_transaction());EXPECT_EQ(sqlite3_total_changes64(physical->handle()),before);
    caps.producers.manifest_bytes=1;refuses(error::capacity,[&]{access::bootstrap(physical,caps);});EXPECT_FALSE(physical->is_in_transaction());EXPECT_EQ(sqlite3_total_changes64(physical->handle()),before);
}
TEST(RecoveryObligationProducer, BootstrapAbsentPartialUnknownAndBusyStatesRefuseWithoutJoining){
    auto physical=std::make_shared<lattice::database>(":memory:");Fixture f;
    EXPECT_FALSE(access::bootstrap(physical,f.discovery()).initialized);
    physical->execute("BEGIN");refuses(error::transaction_required,[&]{access::bootstrap(physical,f.discovery());});EXPECT_TRUE(physical->is_in_transaction());physical->rollback();
    physical->execute("CREATE TABLE _lattice_obligation_producer_profile(fake BLOB)");refuses(error::corrupt_state,[&]{access::bootstrap(physical,f.discovery());});EXPECT_FALSE(physical->is_in_transaction());
    auto* mutex=sqlite3_db_mutex(physical->handle());std::promise<void> held,release;auto wait=release.get_future();std::thread blocker([&]{sqlite3_mutex_enter(mutex);held.set_value();wait.wait();sqlite3_mutex_leave(mutex);});held.get_future().wait();
    auto attempt=std::async(std::launch::async,[&]{try{access::bootstrap(physical,f.discovery());return false;}catch(const recovery_obligation_error& e){return e.code==error::transaction_required;}});
    EXPECT_EQ(attempt.wait_for(std::chrono::seconds(5)),std::future_status::ready); // watchdog, not a timing/performance oracle
    release.set_value();blocker.join();EXPECT_TRUE(attempt.get());
}
TEST(RecoveryObligationProducer, AddressedStampCorruptionAndOriginalMutationAreNotProof){
    Fixture f;f.enable();auto r=f.insert(19);
    f.owner->db().execute("UPDATE _lattice_obligation_producer_stamp SET audit_id=audit_id+1");
    f.committed([&](auto&){refuses(error::corrupt_state,[&]{f.stamp(r);});});
    f.owner->db().execute("UPDATE _lattice_obligation_producer_stamp SET audit_id=audit_id-1");
    f.owner->db().execute("UPDATE AuditLog SET globalRowId=? WHERE id=?",{id(20),r.audit_id});
    f.committed([&](auto&){refuses(error::audit_mismatch,[&]{f.stamp(r);});});
}
TEST(RecoveryObligationProducer, ExplicitWholeContributionRetirementFencesOldProgramAndRebind){
    Fixture f;f.enable();f.committed([&](auto&){auto s=f.ordinary();refuses(error::wrong_mode,[&]{s.retire(f.address);});});
    const auto old=f.address;const auto before_retire=f.image();
    const auto rolled=recovery_writer_access::install(f.owner,[&](auto&){auto p=f.storage();access::retire(p,f.address,f.profile);throw std::runtime_error("abort retirement");});
    EXPECT_EQ(rolled.state,recovery_install_state::rolled_back);EXPECT_EQ(f.image(),before_retire);
    f.committed([&](auto&){auto p=f.storage();access::retire(p,f.address,f.profile);});
    auto before=f.image();EXPECT_THROW(f.insert(21),lattice::db_error);EXPECT_EQ(f.image(),before);
    f.committed([&](auto&){auto s=f.ordinary();f.address=s.bind(f.contribution).address;EXPECT_GT(f.address.incarnation,old.incarnation);auto p=f.storage();refuses(error::binding_mismatch,[&]{access::enroll(p,f.profile);});});
    EXPECT_THROW(f.insert(21),lattice::db_error);
}
TEST(RecoveryObligationProducer, ManifestProfileAndGeneratedProgramBudgetsAreExplicit){
    Fixture f;f.committed([&](auto&){auto p=f.storage();auto changed=f.profile;changed.program_digest="other";refuses(error::conflict,[&]{access::enroll(p,changed);});changed=f.profile;changed.grant_manifest.resize(4097);refuses(error::invalid_argument,[&]{access::enroll(p,changed);});});
    refuses(error::capacity,[&]{f.program->emit_tail("TestPerson",false,1024);});
    EXPECT_FALSE(f.program->emit_tail("TestPerson",false,2*1024*1024).empty());
}

TEST(RecoveryObligationProducer, RepeatedTailCannotStampOrChargeSameOriginTwice){
    Fixture f;const auto tail=f.program->emit_tail("TestPerson");f.owner->generated(tail+tail);const auto before=f.image();
    EXPECT_THROW(f.insert(22),lattice::db_error);EXPECT_EQ(f.image(),before);f.enable();f.insert(22);f.audit();
}
TEST(RecoveryObligationProducer, RetainedOwnerAndIndependentReadOnlyBootstrapLifetime){
    Fixture f;auto storage=f.storage();std::weak_ptr<lattice::lattice_db> weak=f.owner;auto actual=f.owner;f.owner.reset();
    auto result=recovery_writer_access::install(actual,[&](auto&){actual.reset();EXPECT_FALSE(weak.expired());EXPECT_EQ(access::profiles(storage).size(),1u);});
    EXPECT_EQ(result.state,recovery_install_state::committed);EXPECT_FALSE(weak.expired());
}
TEST(RecoveryObligationProducer, CounterDriftDuringTailIsDetectedBeforeStatementCommit){
    Fixture f;f.enable();const auto before=f.image();
    f.owner->db().execute("CREATE TRIGGER _producer_drift AFTER INSERT ON _lattice_obligation_entry BEGIN UPDATE _lattice_obligation_store SET export_sequence=export_sequence+1; END");
    EXPECT_THROW(f.insert(23),lattice::db_error);EXPECT_EQ(f.image(),before);f.owner->db().execute("DROP TRIGGER _producer_drift");f.insert(23);f.audit();
}

TEST(RecoveryObligationProducer, LargeRetainedPrefixKeepsNewStampAndAddressedReadWithinVMWorkCap){
    Fixture f(":memory:",4096);f.enable();
    f.owner->begin_transaction();for(int n=100;n<2148;++n)f.insert(n);f.owner->commit();
    struct Budget{int steps=0;int maximum=30000;};Budget budget;
    auto* handle=f.owner->db().handle();
    auto progress=[](void* raw)noexcept->int{auto& b=*static_cast<Budget*>(raw);return ++b.steps>b.maximum;};
    sqlite3_progress_handler(handle,1,progress,&budget);
    struct Remove{sqlite3* h;~Remove(){sqlite3_progress_handler(h,0,nullptr,nullptr);}} remove{handle};
    f.owner->db().execute("INSERT INTO TestPerson(globalId,name,age) VALUES(?, 'final', 2148)",{id(2148)});EXPECT_LT(budget.steps,budget.maximum);
    sqlite3_progress_handler(handle,0,nullptr,nullptr);
    const auto r=f.latest(id(2148)); // fixture identity lookup is outside either VM oracle
    budget.steps=0;sqlite3_progress_handler(handle,1,progress,&budget);
    f.committed([&](auto&){auto stamp=f.stamp(r);ASSERT_TRUE(stamp);EXPECT_EQ(stamp->audit_id,r.audit_id);});
    EXPECT_LT(budget.steps,budget.maximum);
}

TEST(RecoveryObligationProducer, StampPostimageFaultCannotCommitIncorrectOriginIdentity){
    for(const auto* column:{"incarnation","program_revision","audit_id","bytes","generation","base_export"}){
        SCOPED_TRACE(column);Fixture f;f.enable();const auto before=f.image();
        f.owner->db().execute("CREATE TRIGGER _producer_stamp_drift AFTER INSERT ON _lattice_obligation_producer_stamp BEGIN UPDATE _lattice_obligation_producer_stamp SET "+std::string(column)+"="+column+"+1; END");
        EXPECT_THROW(f.insert(24),lattice::db_error);EXPECT_EQ(f.image(),before);
        f.owner->db().execute("DROP TRIGGER _producer_stamp_drift");f.insert(24);f.audit();
    }
}
TEST(RecoveryObligationProducer, BootstrapValidatorSharesSnapshotAndFailureLeavesNoTransaction){
    TempDB path("producer-one-snapshot");recovery_obligation_producer_discovery_limits caps;
    {Fixture f(path.str());caps=f.discovery();}
    auto physical=std::make_shared<lattice::database>(path.str());bool called=false;
    auto inventory=access::bootstrap(physical,caps,[&](auto& db,const auto& facts){called=true;EXPECT_TRUE(facts.initialized);EXPECT_TRUE(db.is_in_transaction());EXPECT_EQ(sqlite3_txn_state(db.handle(),"main"),SQLITE_TXN_READ);
        EXPECT_EQ(scalar(db,"SELECT COUNT(*) AS n FROM _lattice_obligation_producer_profile"),static_cast<int64_t>(facts.profiles.size()));});
    EXPECT_TRUE(called);EXPECT_TRUE(inventory.initialized);EXPECT_FALSE(physical->is_in_transaction());
    EXPECT_THROW(access::bootstrap(physical,caps,[](auto&,const auto&){throw std::runtime_error("descriptor validation refused");}),std::runtime_error);
    EXPECT_FALSE(physical->is_in_transaction());
    const auto before=scalar(*physical,"SELECT stamps AS n FROM _lattice_obligation_producer_store");
    refuses(error::transaction_required,[&]{access::bootstrap(physical,caps,[](auto& db,const auto&){db.execute("UPDATE _lattice_obligation_producer_store SET stamps=stamps+1");});});
    EXPECT_FALSE(physical->is_in_transaction());EXPECT_EQ(scalar(*physical,"SELECT stamps AS n FROM _lattice_obligation_producer_store"),before);
}

TEST(RecoveryObligationProducer, HotProgramOmitsManifestContentAndFitsUnchangedProgramCeiling){
    Fixture f;auto limits=f.pl;limits.manifest_bytes=65536;
    auto small=f.profile;small.grant_manifest.assign(4096,0x93);
    auto changed=small;changed.grant_manifest.assign(4096,0x27);
    auto large=small;large.grant_manifest.assign(65536,0x51);
    constexpr int64_t unchanged_program_ceiling=256*1024;
    // Compiler-only descriptors intentionally retain the same digest to
    // isolate SQL expansion from authentication. These descriptors are never
    // enrolled. The real adapter must reject a digest/manifest disagreement.
    const auto a=access::compile(small,f.ol,limits).emit_tail("TestPerson",false,unchanged_program_ceiling);
    const auto b=access::compile(changed,f.ol,limits).emit_tail("TestPerson",false,unchanged_program_ceiling);
    const auto c=access::compile(large,f.ol,limits).emit_tail("TestPerson",false,unchanged_program_ceiling);
    EXPECT_EQ(a,b);EXPECT_LT(a.size(),static_cast<size_t>(unchanged_program_ceiling));EXPECT_LT(c.size(),static_cast<size_t>(unchanged_program_ceiling));
    EXPECT_LE(c.size()>a.size()?c.size()-a.size():a.size()-c.size(),32u); // decimal length/charge fields only
    EXPECT_EQ(a.find("p.manifest=X'"),std::string::npos);EXPECT_NE(a.find("length(p.manifest)=4096"),std::string::npos);
    EXPECT_NE(c.find("length(p.manifest)=65536"),std::string::npos);
}
TEST(RecoveryObligationProducer, AddressedReadStillChecksFullManifestChargeAndEnrollmentIdentity){
    Fixture f;f.enable();const auto r=f.insert(25);const auto before=f.image();
    f.owner->db().execute("UPDATE _lattice_obligation_producer_profile SET manifest=?",{blob{1,0,2,3}});
    f.committed([&](auto&){refuses(error::corrupt_state,[&]{f.stamp(r);});});
    f.owner->db().execute("UPDATE _lattice_obligation_producer_profile SET manifest=?",{f.profile.grant_manifest});EXPECT_EQ(f.image(),before);
    f.committed([&](auto&){auto storage=f.storage();auto changed=f.profile;changed.grant_manifest[0]^=1;refuses(error::conflict,[&]{access::enroll(storage,changed);});EXPECT_TRUE(f.stamp(r));});
}
