#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_local_producer.hpp"
#include "../../Sources/LatticeCore/src/scoped_recovery_install.hpp"
#include <lattice.hpp>
#include <array>
#include <cstring>
#include <functional>
#include <iomanip>
#include <sstream>

struct LocalOriginalStream {std::string name;std::string body;};
LATTICE_SCHEMA(LocalOriginalStream,name,body);
struct LocalOriginalRoot {std::string name;};
LATTICE_SCHEMA(LocalOriginalRoot,name);
struct LocalOriginalLeaf {std::string name;};
LATTICE_SCHEMA(LocalOriginalLeaf,name);
namespace lattice::detail {
struct recovery_local_producer_test_access {
    static std::shared_ptr<database> retain(lattice_db& owner) {return recovery_local_producer_adapter::retained_writer_for_test(owner);}
};
}
namespace {
const bool register_local_original_models=[] {
    auto stream=lattice::managed<LocalOriginalStream>::schema();stream.properties[1].no_history=true;
    lattice::schema_registry::instance().register_model(typeid(LocalOriginalStream),std::move(stream));
    auto root=lattice::managed<LocalOriginalRoot>::schema();
    lattice::property_descriptor link{};link.name="leaf";link.kind=lattice::property_kind::link;
    link.type=lattice::column_type::integer;link.nullable=true;link.target_table="LocalOriginalLeaf";root.properties.push_back(link);
    lattice::schema_registry::instance().register_model(typeid(LocalOriginalRoot),std::move(root));return true;
}();
using namespace lattice::detail;
using blob=std::vector<uint8_t>;
using result_state=recovery_install_state;
constexpr const char* link_table="_LocalOriginalRoot_LocalOriginalLeaf_leaf";
recovery_obligation_producer_discovery_limits limits() {return {{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};}
lattice::configuration configuration(const std::string& path) {
    lattice::configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=100;return c;
}
std::shared_ptr<lattice::lattice_db> open_owner(const std::string& path=":memory:") {
    auto owner=std::make_shared<lattice::lattice_db>(configuration(path));
    if(path!=":memory:") {
        auto* notifier=lattice::instance_registry::instance().get_or_create_notifier(path);
        if(notifier)notifier->stop_listening();
    }
    return owner;
}
void committed(const recovery_install_result& r) {
    if(r.primary_error)try{std::rethrow_exception(r.primary_error);}catch(const std::exception& e){ADD_FAILURE()<<e.what();}
    EXPECT_EQ(r.state,result_state::committed);EXPECT_EQ(r.cleanup_error,nullptr);EXPECT_EQ(r.postcommit_error,nullptr);
}
void refused_at(const std::function<void()>& action,const char* stage) {
    try {action();FAIL()<<"expected refusal at "<<stage;}
    catch(const lattice::db_error& e){EXPECT_NE(std::string(e.what()).find(stage),std::string::npos)<<e.what();}
}
int64_t scalar(lattice::database& db,const std::string& sql) {return std::get<int64_t>(db.query(sql).at(0).at("n"));}
std::string uuid(int n) {std::ostringstream s;s<<"AAAAAAAA-0000-4000-8000-"<<std::hex<<std::setw(12)<<std::setfill('0')<<n;return s.str();}
recovery_obligation_record actual(lattice::database& db,const std::string& table,const std::string& target) {
    auto rows=db.query("SELECT id,globalId,tableName,globalRowId FROM AuditLog WHERE tableName=? AND globalRowId=? ORDER BY id DESC LIMIT 1",{table,target});
    if(rows.size()!=1)throw std::runtime_error("missing genuine generated AuditLog original");const auto& r=rows[0];
    return {std::get<int64_t>(r.at("id")),std::get<std::string>(r.at("globalId")),std::get<std::string>(r.at("tableName")),
        std::get<std::string>(r.at("globalRowId")),recovery_obligation_origin::local_candidate};
}
struct own_transaction {
    lattice::lattice_db& owner;bool done=false;
    explicit own_transaction(lattice::lattice_db& o):owner(o){owner.begin_transaction();}
    ~own_transaction(){if(!done)try{owner.rollback();}catch(...) {}}
    void commit(){owner.commit();done=true;}
};
struct fault_scope {
    const recovery_local_producer_test_hooks::authorizer_fault* previous;
    explicit fault_scope(const recovery_local_producer_test_hooks::authorizer_fault& f):previous(recovery_local_producer_test_hooks::fault){recovery_local_producer_test_hooks::fault=&f;}
    ~fault_scope(){recovery_local_producer_test_hooks::fault=previous;}
};
class RecoveryLocalProducer : public ::testing::Test {
protected:
    recovery_obligation_producer_discovery_limits caps=limits();
    std::shared_ptr<lattice::lattice_db> owner=open_owner();
    recovery_obligation_profile profile{{"channel","authority","source","epoch","scope","schema"},"incoming-grant","receipt-namespace"};
    recovery_obligation_address address;
    recovery_obligation_store obligations() {return {owner,caps.obligations,caps.installations};}
    recovery_local_producer_grant grant() {return {address,{"LocalOriginalStream","LocalOriginalRoot","LocalOriginalLeaf"},{'c','l','a','i','m'}};}
    void bind() {
        committed(recovery_writer_access::install(owner,[&](auto&) {
            receive_install_store receiver(owner,caps.installations);receiver.initialize();receiver.bind(profile.binding);
            auto journal=obligations();journal.initialize();address=journal.bind(profile).address;
        }));
    }
    void SetUp() override {bind();}
    void enroll() {committed(recovery_local_producer_adapter::enroll_for_qualification(owner,grant(),caps));}
    std::optional<recovery_obligation_producer_stamp> stamp(const recovery_obligation_record& r) {
        std::optional<recovery_obligation_producer_stamp> out;
        committed(recovery_writer_access::install(owner,[&](auto&) {
            out=recovery_obligation_producer_store::read_stamp(owner,caps.obligations,caps.installations,caps.producers,address,r.original_id);
        }));return out;
    }
    int64_t stamps() {return scalar(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_obligation_producer_stamp");}
};
}

TEST_F(RecoveryLocalProducer, ImplicitCreateSetDeleteAtomicallyProduceGenuineOriginalStamps) {
    enroll();auto row=owner->add(LocalOriginalStream{"first","seed"});const auto target=row.global_id();
    const auto insert=actual(owner->db(),"LocalOriginalStream",target);row.body="updated";
    const auto update=actual(owner->db(),"LocalOriginalStream",target);owner->remove(row);
    const auto remove=actual(owner->db(),"LocalOriginalStream",target);
    for(const auto& r:{insert,update,remove}) {
        ASSERT_GT(r.audit_id,0);const auto s=stamp(r);ASSERT_TRUE(s);EXPECT_EQ(s->audit_id,r.audit_id);
        EXPECT_EQ(s->contribution_incarnation,address.incarnation);EXPECT_EQ(s->program_revision,1);
    }
    EXPECT_EQ(stamps(),3);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream"),0);
    own_transaction tx(*owner);auto profiles=recovery_local_producer_adapter::profiles_for_owned_write(owner,caps);
    ASSERT_EQ(profiles.size(),1);EXPECT_EQ(profiles[0].contribution,profile);tx.commit();
    EXPECT_FALSE(recovery_local_producer_adapter::all_route_capability);
}

TEST_F(RecoveryLocalProducer, ExplicitTransactionRollbackIncludesModelsAuditObligationsAndStamps) {
    enroll();
    {own_transaction tx(*owner);owner->add(LocalOriginalStream{"rolled","bytes"});EXPECT_EQ(stamps(),1);}
    EXPECT_EQ(stamps(),0);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream"),0);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM AuditLog WHERE tableName='LocalOriginalStream'"),0);
    owner->add(LocalOriginalStream{"successor","bytes"});EXPECT_EQ(stamps(),1);
}

TEST_F(RecoveryLocalProducer, NoHistoryPayloadAndOriginalUuidSpellingArePreserved) {
    enroll();owner->db().execute("INSERT INTO LocalOriginalStream(globalId,name,body) VALUES(?,'upper','seed')",{uuid(1)});
    owner->db().execute("UPDATE LocalOriginalStream SET body=?",{std::string(65536,'x')});
    const auto r=actual(owner->db(),"LocalOriginalStream",uuid(1));const auto s=stamp(r);ASSERT_TRUE(s);
    EXPECT_EQ(r.target_id,uuid(1));EXPECT_EQ(s->audit_id,r.audit_id);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM AuditLog WHERE operation='UPDATE' AND tableName='LocalOriginalStream' AND json_extract(changedFields,'$.body') IS NULL"),1);
    EXPECT_EQ(stamps(),2);
}

TEST_F(RecoveryLocalProducer, RegularLinkRowIdZeroStillUsesPositiveGenuineAuditId) {
    enroll();auto root=owner->add(LocalOriginalRoot{"parent"});auto leaf=owner->add(LocalOriginalLeaf{"child"});
    ASSERT_NO_THROW(owner->ensure_link_table(link_table,"LocalOriginalRoot","LocalOriginalLeaf"));
    owner->db().execute("INSERT INTO "+std::string(link_table)+"(globalId,lhs,rhs) VALUES(?,?,?)",{uuid(2),root.global_id(),leaf.global_id()});
    const auto r=actual(owner->db(),link_table,uuid(2));ASSERT_GT(r.audit_id,0);ASSERT_TRUE(stamp(r));
    EXPECT_EQ(scalar(owner->db(),"SELECT rowId AS n FROM AuditLog WHERE id="+std::to_string(r.audit_id)),0);
    auto other=owner->add(LocalOriginalLeaf{"other"});
    EXPECT_THROW(owner->db().execute("UPDATE "+std::string(link_table)+" SET rhs=?",{other.global_id()}),lattice::db_error);
    committed(recovery_writer_access::install(owner,[&](auto& db) {
        db.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        db.execute("UPDATE "+std::string(link_table)+" SET rhs=?",{other.global_id()});
        db.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
    }));
    EXPECT_EQ(std::get<std::string>(owner->db().query("SELECT rhs FROM "+std::string(link_table))[0].at("rhs")),other.global_id());
    owner->db().execute("DELETE FROM "+std::string(link_table));EXPECT_EQ(stamps(),5);
}

TEST_F(RecoveryLocalProducer, OrdinaryClaimsAndSyntheticAuditsCannotMintStamps) {
    auto prior=owner->add(LocalOriginalStream{"legacy","body"});const auto old=actual(owner->db(),"LocalOriginalStream",prior.global_id());
    committed(recovery_writer_access::install(owner,[&](auto&){obligations().record(address,old);}));enroll();
    EXPECT_FALSE(stamp(old));
    owner->db().execute("INSERT INTO AuditLog(globalId,tableName,operation,rowId,globalRowId,isFromRemote,synthesized) VALUES(?,'LocalOriginalStream','INSERT',0,?,0,0)",{uuid(3),uuid(4)});
    const auto forged=actual(owner->db(),"LocalOriginalStream",uuid(4));
    committed(recovery_writer_access::install(owner,[&](auto&){obligations().record(address,forged);}));
    EXPECT_FALSE(stamp(forged));EXPECT_EQ(stamps(),0);
    EXPECT_THROW(owner->db().execute("DELETE FROM _lattice_obligation_producer_stamp"),lattice::db_error);
}

TEST_F(RecoveryLocalProducer, FrozenLateInsertDeleteRecordsCurrentJournalWithoutChangingFrozenRequest) {
    enroll();recovery_obligation_scope frozen;
    committed(recovery_writer_access::install(owner,[&](auto&){frozen=obligations().freeze(address,1);address=frozen.address;}));
    auto row=owner->add(LocalOriginalStream{"late","body"});const auto i=actual(owner->db(),"LocalOriginalStream",row.global_id());
    owner->remove(row);const auto d=actual(owner->db(),"LocalOriginalStream",i.target_id);
    ASSERT_TRUE(stamp(i));ASSERT_TRUE(stamp(d));
    committed(recovery_writer_access::install(owner,[&](auto&) {
        const auto snapshot=obligations().snapshot_for_install(address,1);EXPECT_EQ(snapshot.entries.size(),2);
        EXPECT_EQ(snapshot.scope.freeze_revision,frozen.freeze_revision);EXPECT_EQ(snapshot.scope.freeze_record_high_water,frozen.freeze_record_high_water);
        EXPECT_GT(snapshot.scope.revision,frozen.revision);EXPECT_EQ(snapshot.scope.address,frozen.address);
    }));
}

TEST_F(RecoveryLocalProducer, CapacitySecondRowRefusalAbortsFirstRowDespiteInsertOrIgnore) {
    caps.producers.stamps=2;enroll();owner->add(LocalOriginalStream{"kept","body"});ASSERT_EQ(stamps(),1);
    const auto before=owner->db().query("SELECT * FROM _lattice_obligation_producer_store");
    EXPECT_THROW(owner->db().execute("INSERT OR IGNORE INTO LocalOriginalStream(name,body) VALUES('first','a'),('second','b')"),lattice::db_error);
    EXPECT_EQ(stamps(),1);EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_obligation_producer_store"),before);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream"),1);
    owner->add(LocalOriginalStream{"retry-one","body"});EXPECT_EQ(stamps(),2);
}

TEST_F(RecoveryLocalProducer, IgnoredCounterColumnRefusesWholeOriginStatementAndAllowsSuccessor) {
    enroll();owner->add(LocalOriginalStream{"kept","body"});ASSERT_EQ(stamps(),1);
    const auto before=owner->db().query("SELECT * FROM _lattice_obligation_producer_store");
    static int restricted_columns=0;restricted_columns=0;
    const recovery_local_producer_test_hooks::authorizer_fault fault{owner.get(),
        [](int op,const char* table,const char* column,const char*) noexcept {
            if(op==SQLITE_UPDATE&&table&&column&&std::strcmp(table,"_lattice_obligation_producer_store")==0&&std::strcmp(column,"stamps")==0) {
                ++restricted_columns;return SQLITE_IGNORE;
            }
            return SQLITE_OK;
        }};
    {fault_scope restricted(fault);EXPECT_THROW(owner->add(LocalOriginalStream{"ignored","body"}),lattice::db_error);}
    EXPECT_GT(restricted_columns,0);EXPECT_EQ(stamps(),1);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_obligation_producer_store"),before);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream"),1);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM AuditLog WHERE tableName='LocalOriginalStream'"),1);
    owner->add(LocalOriginalStream{"successor","body"});EXPECT_EQ(stamps(),2);
}

TEST_F(RecoveryLocalProducer, DisabledOrdinaryAndLegacyRemoteEffectsRefuseButActualInstallCanSuppress) {
    enroll();auto row=owner->add(LocalOriginalStream{"initial","body"});
    owner->db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
    EXPECT_THROW(owner->db().execute("UPDATE LocalOriginalStream SET name='bypass'"),lattice::db_error);
    owner->db().execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
    committed(recovery_writer_access::install(owner,[&](auto& db) {
        db.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");db.execute("UPDATE LocalOriginalStream SET name='installed'");
        db.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
    }));
    EXPECT_EQ(stamps(),1);EXPECT_EQ(std::get<std::string>(owner->db().query("SELECT name FROM LocalOriginalStream")[0].at("name")),"installed");
    auto sender=open_owner();sender->add(LocalOriginalStream{"remote","payload"});
    const auto entries=lattice::query_audit_log(sender->db(),false,std::nullopt);ASSERT_FALSE(entries.empty());
    std::vector<std::string> acknowledgements;
    try {acknowledgements=lattice::apply_remote_changes(*owner,entries);}catch(const lattice::db_error&) {}
    EXPECT_TRUE(acknowledgements.empty());EXPECT_EQ(stamps(),1);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream WHERE name='remote'"),0);
}

TEST_F(RecoveryLocalProducer, RawEscapeRevokesFutureAdmissionWithoutUndoingAdmittedInstall) {
    enroll();owner->add(LocalOriginalStream{"one","body"});
    committed(recovery_writer_access::install(owner,[&](auto& db) {
        ASSERT_NE(db.handle(),nullptr); // Known escape occurs AFTER exact installer admission.
        db.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");db.execute("UPDATE LocalOriginalStream SET name='admitted'");
        db.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
    }));
    EXPECT_THROW(owner->add(LocalOriginalStream{"new","body"}),lattice::db_error);
    bool entered=false;const auto refused=recovery_writer_access::install(owner,[&](auto&){entered=true;});
    EXPECT_EQ(refused.state,result_state::refused);EXPECT_FALSE(entered);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream WHERE name='admitted'"),1);
}

TEST_F(RecoveryLocalProducer, IncompleteSchemaAndPreexistingTriggerRefuseWithoutPartialEnrollment) {
    auto partial=grant();partial.models={"LocalOriginalRoot"};
    EXPECT_NE(recovery_local_producer_adapter::enroll_for_qualification(owner,partial,caps).state,result_state::committed);
    EXPECT_FALSE(owner->db().table_exists("_lattice_obligation_producer_profile"));
    owner->db().execute("CREATE TRIGGER unknown_local_original AFTER INSERT ON LocalOriginalStream BEGIN SELECT 1; END");
    EXPECT_NE(recovery_local_producer_adapter::enroll_for_qualification(owner,grant(),caps).state,result_state::committed);
    EXPECT_FALSE(owner->db().table_exists("_lattice_obligation_producer_profile"));
    owner->db().execute("DROP TRIGGER unknown_local_original");enroll();owner->add(LocalOriginalStream{"retry","body"});EXPECT_EQ(stamps(),1);
}

TEST_F(RecoveryLocalProducer, LateEnrollmentDdlFailureRollsBackAndRestoresOrdinaryRetry) {
    const recovery_local_producer_test_hooks::authorizer_fault fault{owner.get(),
        [](int op,const char* table,const char*,const char*) noexcept {
            return op==SQLITE_CREATE_TRIGGER&&table&&std::strncmp(table,"_lattice_local_producer_",sizeof("_lattice_local_producer_")-1)==0?SQLITE_DENY:SQLITE_OK;
        }};
    {fault_scope restricted(fault);const auto failed=recovery_local_producer_adapter::enroll_for_qualification(owner,grant(),caps);EXPECT_EQ(failed.state,result_state::rolled_back);}
    EXPECT_FALSE(owner->db().table_exists("_lattice_obligation_producer_profile"));
    owner->add(LocalOriginalStream{"ordinary-after-rollback","body"});
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM AuditLog WHERE tableName='LocalOriginalStream'"),1);
    enroll();owner->add(LocalOriginalStream{"enrolled-retry","body"});EXPECT_EQ(stamps(),1);
}

TEST_F(RecoveryLocalProducer, IndependentWholeModelContributionAndUnenrolledRowsRemainSeparate) {
    enroll();auto second=profile;second.binding.channel="second";second.binding.scope="independent";
    recovery_obligation_address other;
    committed(recovery_writer_access::install(owner,[&](auto&) {
        receive_install_store receiver(owner,caps.installations);receiver.bind(second.binding);other=obligations().bind(second).address;
    }));
    committed(recovery_local_producer_adapter::enroll_for_qualification(owner,{other,{"TestPerson"},{'s','e','c','o','n','d'}},caps));
    owner->add(TestPerson{"other",1,std::nullopt});owner->add(LocalOriginalStream{"first","body"});owner->add(TestDog{"local-only",1.0,true});
    EXPECT_EQ(stamps(),2);
    own_transaction tx(*owner);EXPECT_EQ(recovery_local_producer_adapter::profiles_for_owned_write(owner,caps).size(),2);tx.commit();
}

TEST_F(RecoveryLocalProducer, FileReopenRevalidatesProgramsAndKeepsFrozenLateWrites) {
    TempDB file("local-producer-reopen");owner=open_owner(file.str());bind();enroll();
    committed(recovery_writer_access::install(owner,[&](auto&){address=obligations().freeze(address,1).address;}));
    owner->add(LocalOriginalStream{"before","body"});owner->close();owner.reset();owner=open_owner(file.str());
    owner->add(LocalOriginalStream{"after","body"});EXPECT_EQ(stamps(),2);
    committed(recovery_writer_access::install(owner,[&](auto&){EXPECT_EQ(obligations().snapshot_for_install(address,1).entries.size(),2);}));
    owner->reopen_write_db();owner->add(LocalOriginalStream{"replacement-writer","body"});EXPECT_EQ(stamps(),3);
    owner->close();owner.reset();
    {lattice::database raw(file.str());raw.execute("DROP TRIGGER _lattice_local_producer_LocalOriginalStream_INSERT");}
    EXPECT_THROW(open_owner(file.str()),lattice::db_error);
}

TEST_F(RecoveryLocalProducer, SiblingRetirementCannotRemainCurrentProducerFacts) {
    TempDB file("local-producer-retire");owner=open_owner(file.str());bind();enroll();
    auto sibling=open_owner(file.str());
    committed(recovery_local_producer_adapter::retire_for_qualification(sibling,address,caps));
    // Restored ordinary programs deliberately produce no retired stamp. The
    // old context must not present this as continuing current producer custody.
    owner->add(LocalOriginalStream{"after-retire","body"});EXPECT_EQ(stamps(),0);
    {own_transaction tx(*owner);EXPECT_THROW(recovery_local_producer_adapter::profiles_for_owned_write(owner,caps),lattice::db_error);}
    recovery_obligation_address replacement;
    committed(recovery_writer_access::install(sibling,[&](auto&) {
        recovery_obligation_store journal(sibling,caps.obligations,caps.installations);replacement=journal.bind(profile).address;
    }));
    ASSERT_GT(replacement.incarnation,address.incarnation);auto new_grant=grant();new_grant.address=replacement;
    committed(recovery_local_producer_adapter::enroll_for_qualification(sibling,new_grant,caps));
    EXPECT_THROW(owner->add(LocalOriginalStream{"stale-incarnation","body"}),lattice::db_error);
    sibling->add(LocalOriginalStream{"current-incarnation","body"});EXPECT_EQ(stamps(),1);
    sibling->close();owner->close();sibling.reset();owner.reset();
}

TEST_F(RecoveryLocalProducer, RetentionHistoryAndResetRefuseBeforeLosingOriginalEvidence) {
    enroll();auto row=owner->add(LocalOriginalStream{"retained","body"});const auto r=actual(owner->db(),"LocalOriginalStream",row.global_id());
    const auto before=owner->db().query("SELECT * FROM AuditLog WHERE id=?",{r.audit_id});
    EXPECT_THROW(owner->prune_audit_log(1),lattice::db_error);
    EXPECT_THROW(owner->safe_compact_audit_log(1),lattice::db_error);
    EXPECT_THROW(owner->force_compact_audit_log(),lattice::db_error);
    EXPECT_THROW(owner->generate_history(10),lattice::db_error);
    EXPECT_THROW(owner->reset_sync_state(address.channel),lattice::db_error);
    EXPECT_THROW(owner->remove_sync_channel_state(address.channel),lattice::db_error);
    EXPECT_THROW(owner->db().execute("DELETE FROM AuditLog"),lattice::db_error);
    EXPECT_THROW(owner->db().execute("UPDATE AuditLog SET changedFields='{}'"),lattice::db_error);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog WHERE id=?",{r.audit_id}),before);ASSERT_TRUE(stamp(r));
    ASSERT_NO_THROW(owner->db().execute("UPDATE AuditLog SET isSynchronized=1 WHERE id=?",{r.audit_id}));
    row.body="ordinary-successor";EXPECT_EQ(stamps(),2);
}

namespace {
thread_local std::function<void()> bootstrap_sibling_action;
struct bootstrap_action_scope {
    void (*previous)()=recovery_local_producer_test_hooks::after_inventory;
    explicit bootstrap_action_scope(std::function<void()> action) {
        bootstrap_sibling_action=std::move(action);
        recovery_local_producer_test_hooks::after_inventory=[] {bootstrap_sibling_action();};
    }
    ~bootstrap_action_scope(){recovery_local_producer_test_hooks::after_inventory=previous;bootstrap_sibling_action={};}
};
}
TEST_F(RecoveryLocalProducer, BootstrapUsesOneViewButCannotClaimCoveragePastSiblingRetirement) {
    TempDB file("local-producer-bootstrap-race");owner=open_owner(file.str());bind();enroll();
    std::shared_ptr<lattice::lattice_db> opening;bool retired=false;
    {
        bootstrap_action_scope schedule([&] {
            // The opening owner already pinned profile rows. The sibling's
            // real owned transaction commits retirement before descriptor read.
            committed(recovery_local_producer_adapter::retire_for_qualification(owner,address,caps));retired=true;
        });
        ASSERT_NO_THROW(opening=open_owner(file.str()));
    }
    ASSERT_TRUE(retired);ASSERT_TRUE(opening);
    {own_transaction tx(*opening);EXPECT_THROW(recovery_local_producer_adapter::profiles_for_owned_write(opening,caps),lattice::db_error);}
    opening->add(LocalOriginalStream{"ordinary-after-retirement","body"});EXPECT_EQ(stamps(),0);
    opening->close();owner->close();opening.reset();owner.reset();
}

TEST_F(RecoveryLocalProducer, WriterReplacementFencesRetainedOldPhysicalConnection) {
    TempDB file("local-producer-physical");owner=open_owner(file.str());bind();enroll();
    auto old=recovery_local_producer_test_access::retain(*owner);ASSERT_TRUE(old);
    owner->reopen_write_db();
    EXPECT_THROW(old->execute("INSERT INTO LocalOriginalStream(name,body) VALUES('retired','body')"),lattice::db_error);
    owner->add(LocalOriginalStream{"published","body"});EXPECT_EQ(stamps(),1);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream WHERE name='retired'"),0);
    owner->close();owner.reset();old.reset();
}

TEST_F(RecoveryLocalProducer, KnownPriorRawEscapeAndAutomaticRetentionCannotEnroll) {
    ASSERT_NE(owner->db().handle(),nullptr);
    EXPECT_NE(recovery_local_producer_adapter::enroll_for_qualification(owner,grant(),caps).state,result_state::committed);
    EXPECT_FALSE(owner->db().table_exists("_lattice_obligation_producer_profile"));
    auto c=configuration(":memory:");c.audit_retention_seconds=86400;
    owner=std::make_shared<lattice::lattice_db>(c);bind();
    EXPECT_NE(recovery_local_producer_adapter::enroll_for_qualification(owner,grant(),caps).state,result_state::committed);
    EXPECT_FALSE(owner->db().table_exists("_lattice_obligation_producer_profile"));
    owner->add(LocalOriginalStream{"ordinary-still-valid","body"});
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream"),1);
}

TEST_F(RecoveryLocalProducer, PreAdmittedSuppressedInstallMaySettleAfterLogicalClose) {
    TempDB file("local-producer-close");owner=open_owner(file.str());bind();enroll();owner->add(LocalOriginalStream{"before","body"});
    committed(recovery_writer_access::install(owner,[&](auto& db) {
        db.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");owner->close();
        db.execute("UPDATE LocalOriginalStream SET name='after-admission-close'");
        db.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
    }));
    owner.reset();
    {lattice::database raw(file.str());
        EXPECT_EQ(scalar(raw,"SELECT COUNT(*) AS n FROM LocalOriginalStream WHERE name='after-admission-close'"),1);
        EXPECT_EQ(scalar(raw,"SELECT COUNT(*) AS n FROM _lattice_obligation_producer_stamp"),1);
    }
}

TEST_F(RecoveryLocalProducer, ImmutableProfileMutationDeniedAndSameLengthOfflineDriftRefusesBootstrap) {
    TempDB file("local-producer-profile-drift");owner=open_owner(file.str());bind();enroll();
    const auto before=owner->db().query("SELECT * FROM _lattice_obligation_producer_profile");
    EXPECT_THROW(owner->db().execute("UPDATE _lattice_obligation_producer_profile SET manifest=zeroblob(length(manifest))"),lattice::db_error);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_obligation_producer_profile"),before);
    owner->close();owner.reset();
    {lattice::database raw(file.str());
        const auto size=scalar(raw,"SELECT length(manifest) AS n FROM _lattice_obligation_producer_profile");
        // Change one receipt byte while preserving all framing lengths/model
        // names and stored counters. It must fail actual digest reconstruction,
        // not merely a raw byte count or malformed framing check.
        const auto offset=8+std::string("lattice-local-original-v1/uuid-nocase/schema-v1").size()+8;
        raw.execute("UPDATE _lattice_obligation_producer_profile SET manifest=CAST(substr(manifest,1,"+std::to_string(offset)+")||x'7a'||substr(manifest,"+std::to_string(offset+2)+") AS BLOB)");
        EXPECT_EQ(scalar(raw,"SELECT length(manifest) AS n FROM _lattice_obligation_producer_profile"),size);
    }
    try {auto rejected=open_owner(file.str());FAIL()<<"same-length changed grant must refuse bootstrap";}
    catch(const lattice::db_error& e){EXPECT_NE(std::string(e.what()).find("bootstrap descriptor/program revision mismatch"),std::string::npos)<<e.what();}
}

TEST_F(RecoveryLocalProducer, LargeGrantClaimDoesNotExpandEveryGeneratedRowProgram) {
    enroll();const auto small=scalar(owner->db(),"SELECT MAX(length(sql)) AS n FROM sqlite_master WHERE type='trigger' AND tbl_name='LocalOriginalStream'");
    owner=open_owner();bind();auto big=grant();big.incoming_grant_receipt=blob(65536,'g');
    committed(recovery_local_producer_adapter::enroll_for_qualification(owner,big,caps));
    const auto large=scalar(owner->db(),"SELECT MAX(length(sql)) AS n FROM sqlite_master WHERE type='trigger' AND tbl_name='LocalOriginalStream'");
    // Both profiles carry fixed-length digests. Decimal manifest length/charge
    // may change a few bytes; the 64 KiB claim must not be repeated per row.
    EXPECT_LT(std::abs(large-small),1024);EXPECT_LT(large,262144);
    owner->add(LocalOriginalStream{"positive","body"});EXPECT_EQ(stamps(),1);
}

TEST_F(RecoveryLocalProducer, OwnerOpenedBeforeEnrollmentCannotReportFalseProfileAbsence) {
    TempDB file("local-producer-older-owner");owner=open_owner(file.str());bind();auto older=open_owner(file.str());enroll();
    {own_transaction tx(*older);EXPECT_THROW(recovery_local_producer_adapter::profiles_for_owned_write(older,caps),lattice::db_error);}
    EXPECT_THROW(older->add(LocalOriginalStream{"not-admitted","body"}),lattice::db_error);
    owner->add(LocalOriginalStream{"admitted","body"});EXPECT_EQ(stamps(),1);
    older->close();owner->close();older.reset();owner.reset();
}

TEST_F(RecoveryLocalProducer, UnchangedUpdateAndIgnoredInsertDoNotInventOriginals) {
    enroll();auto row=owner->add(LocalOriginalStream{"kept","body"});const auto r=actual(owner->db(),"LocalOriginalStream",row.global_id());
    const auto before=owner->db().query("SELECT * FROM _lattice_obligation_producer_store");
    owner->db().execute("UPDATE LocalOriginalStream SET body=body");
    owner->db().execute("INSERT OR IGNORE INTO LocalOriginalStream(globalId,name,body) VALUES(?,'ignored','other')",{row.global_id()});
    EXPECT_EQ(stamps(),1);EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_obligation_producer_store"),before);
    EXPECT_EQ(actual(owner->db(),"LocalOriginalStream",row.global_id()),r);ASSERT_TRUE(stamp(r));
    row.body="successor";EXPECT_EQ(stamps(),2);
}

TEST_F(RecoveryLocalProducer, ReplaceRecordsActualConflictDeleteAndInsertionWithoutRewritingUuid) {
    owner->db().execute("CREATE UNIQUE INDEX local_original_unique_name ON LocalOriginalStream(name)");enroll();
    auto old=owner->add(LocalOriginalStream{"same-name","old"});const auto previous=old.global_id();
    owner->db().execute("INSERT OR REPLACE INTO LocalOriginalStream(globalId,name,body) VALUES(?,'same-name','replacement')",{uuid(20)});
    EXPECT_EQ(stamps(),3);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream"),1);
    EXPECT_EQ(std::get<std::string>(owner->db().query("SELECT globalId FROM LocalOriginalStream")[0].at("globalId")),uuid(20));
    const auto deleted=actual(owner->db(),"LocalOriginalStream",previous);ASSERT_TRUE(stamp(deleted));
    const auto inserted=actual(owner->db(),"LocalOriginalStream",uuid(20));ASSERT_TRUE(stamp(inserted));
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM AuditLog WHERE id="+std::to_string(deleted.audit_id)+" AND operation='DELETE'"),1);
    EXPECT_THROW(owner->db().execute("PRAGMA recursive_triggers=OFF"),lattice::db_error);
}

TEST_F(RecoveryLocalProducer, PreopenedSiblingCannotPruneOrRewriteDurablyPinnedOriginals) {
    TempDB file("local-producer-durable-retention");owner=open_owner(file.str());bind();auto older=open_owner(file.str());
    enroll();auto row=owner->add(LocalOriginalStream{"retained","body"});
    const auto original=actual(owner->db(),"LocalOriginalStream",row.global_id());
    const auto before=older->db().query("SELECT * FROM AuditLog WHERE id=?",{original.audit_id});
    refused_at([&]{older->db().execute("DELETE FROM AuditLog");},"durable audit retention fence");
    refused_at([&]{older->db().execute("UPDATE AuditLog SET changedFields='{}'");},"durable audit retention fence");
    refused_at([&]{older->db().execute("UPDATE AuditLog SET globalRowId='other'");},"durable audit retention fence");
    refused_at([&]{older->db().execute("UPDATE AuditLog SET _rowid_=_rowid_+1000");},"durable audit retention fence");
    older->db().execute("PRAGMA recursive_triggers=OFF");
    EXPECT_EQ(std::get<int64_t>(older->db().query("PRAGMA recursive_triggers").at(0).at("recursive_triggers")),0);
    refused_at([&]{older->db().execute("INSERT OR REPLACE INTO AuditLog(id,tableName) VALUES(?,'replaced')",{original.audit_id});},"durable audit retention fence");
    refused_at([&]{older->db().execute("INSERT OR REPLACE INTO AuditLog(globalId,tableName) VALUES(?,'replaced')",{original.original_id});},"durable audit retention fence");
    refused_at([&]{older->safe_compact_audit_log(1);},"retention/history/reset integration is not active");
    refused_at([&]{older->prune_audit_log(1);},"retention/history/reset integration is not active");
    refused_at([&]{older->force_compact_audit_log();},"retention/history/reset integration is not active");
    refused_at([&]{older->generate_history(10);},"retention/history/reset integration is not active");
    refused_at([&]{older->reset_sync_state(address.channel);},"retention/history/reset integration is not active");
    refused_at([&]{older->remove_sync_channel_state(address.channel);},"retention/history/reset integration is not active");
    EXPECT_EQ(older->db().query("SELECT * FROM AuditLog WHERE id=?",{original.audit_id}),before);ASSERT_TRUE(stamp(original));
    ASSERT_NO_THROW(older->db().execute("UPDATE AuditLog SET isSynchronized=1 WHERE id=?",{original.audit_id}));
    EXPECT_EQ(scalar(older->db(),"SELECT isSynchronized AS n FROM AuditLog WHERE id="+std::to_string(original.audit_id)),1);
    row.body="successor";EXPECT_EQ(stamps(),2);
    older->close();owner->close();older.reset();owner.reset();
}

TEST_F(RecoveryLocalProducer, LateProgramFailureRollsBackNewDurableRetentionFences) {
    owner->add(LocalOriginalStream{"ordinary","body"});static int fences=0;fences=0;
    const recovery_local_producer_test_hooks::authorizer_fault fault{owner.get(),
        [](int op,const char* name,const char*,const char*) noexcept {
            if(op!=SQLITE_CREATE_TRIGGER||!name)return SQLITE_OK;
            if(std::strncmp(name,"_lattice_local_producer_AuditLog_",sizeof("_lattice_local_producer_AuditLog_")-1)==0)++fences;
            return std::strcmp(name,"_lattice_local_producer_LocalOriginalStream_INSERT")==0?SQLITE_DENY:SQLITE_OK;
        }};
    {fault_scope restricted(fault);const auto failed=recovery_local_producer_adapter::enroll_for_qualification(owner,grant(),caps);EXPECT_EQ(failed.state,result_state::rolled_back);}
    EXPECT_EQ(fences,3);EXPECT_FALSE(owner->db().table_exists("_lattice_obligation_producer_profile"));
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM sqlite_master WHERE type='trigger' AND tbl_name='AuditLog'"),0);
    ASSERT_NO_THROW(owner->db().execute("DELETE FROM AuditLog"));
    enroll();owner->add(LocalOriginalStream{"retry","body"});EXPECT_EQ(stamps(),1);
}

TEST_F(RecoveryLocalProducer, RetirementRollbackPreservesFencesAndFinalRetirementLeavesExactDormantBundle) {
    TempDB file("local-producer-dormant-fences");owner=open_owner(file.str());bind();
    owner->add(LocalOriginalStream{"pre-enrollment","body"});auto older=open_owner(file.str());enroll();
    const recovery_local_producer_test_hooks::authorizer_fault fault{owner.get(),
        [](int op,const char* name,const char*,const char*) noexcept {
            return op==SQLITE_DROP_TRIGGER&&name&&std::strcmp(name,"_lattice_local_producer_LocalOriginalStream_INSERT")==0?SQLITE_DENY:SQLITE_OK;
        }};
    {fault_scope restricted(fault);const auto failed=recovery_local_producer_adapter::retire_for_qualification(owner,address,caps);EXPECT_EQ(failed.state,result_state::rolled_back);}
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_obligation_producer_profile"),1);
    refused_at([&]{older->db().execute("DELETE FROM AuditLog");},"durable audit retention fence");
    committed(recovery_local_producer_adapter::retire_for_qualification(owner,address,caps));
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_obligation_producer_profile"),0);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM sqlite_master WHERE type='trigger' AND tbl_name='AuditLog'"),3);
    ASSERT_NO_THROW(older->db().execute("UPDATE AuditLog SET changedFields='{}'"));
    ASSERT_NO_THROW(older->db().execute("DELETE FROM AuditLog"));
    older->close();owner->close();older.reset();owner.reset();
    ASSERT_NO_THROW(owner=open_owner(file.str()));owner->add(LocalOriginalStream{"ordinary-reopen","body"});
    EXPECT_EQ(stamps(),0);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM AuditLog"),1);
    owner->close();owner.reset();
    {lattice::database raw(file.str());raw.execute("DROP TRIGGER _lattice_local_producer_AuditLog_UPDATE");}
    refused_at([&]{auto rejected=open_owner(file.str());},"durable audit retention program mismatch");
}

TEST_F(RecoveryLocalProducer, UnknownOrMissingAuditRetentionProgramsRefuseAdmission) {
    owner->db().execute("CREATE TRIGGER unknown_audit_guard BEFORE DELETE ON AuditLog BEGIN SELECT 1; END");
    const auto failed=recovery_local_producer_adapter::enroll_for_qualification(owner,grant(),caps);
    EXPECT_NE(failed.state,result_state::committed);EXPECT_FALSE(owner->db().table_exists("_lattice_obligation_producer_profile"));
    owner->db().execute("DROP TRIGGER unknown_audit_guard");
    TempDB file("local-producer-missing-fence");owner=open_owner(file.str());bind();enroll();owner->close();owner.reset();
    {lattice::database raw(file.str());raw.execute("DROP TRIGGER _lattice_local_producer_AuditLog_DELETE");}
    refused_at([&]{auto rejected=open_owner(file.str());},"durable audit retention program mismatch");
}

TEST_F(RecoveryLocalProducer, LegitimateGrowthBeyondInitialInventoryLimitReopensWithoutNewRowScanCap) {
    TempDB file("local-producer-growth");caps.obligations.records=5000;caps.obligations.encoded_bytes=16*1024*1024;
    caps.producers.stamps=5000;caps.producers.encoded_bytes=16*1024*1024;
    owner=open_owner(file.str());bind();enroll();
    owner->db().execute("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<4097) INSERT INTO LocalOriginalStream(name,body) SELECT 'row-'||i,'body' FROM n");
    ASSERT_EQ(stamps(),4097);ASSERT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream"),4097);
    owner->close();owner.reset();ASSERT_NO_THROW(owner=open_owner(file.str()));
    owner->add(LocalOriginalStream{"after-reopen","body"});EXPECT_EQ(stamps(),4098);
    owner->reopen_write_db();owner->add(LocalOriginalStream{"after-replacement","body"});EXPECT_EQ(stamps(),4099);
    owner->close();owner.reset();
}

TEST_F(RecoveryLocalProducer, RealScopedInstallAndOutboxCapturePreserveEngineHandleAdmission) {
    owner=open_owner(); // Admit initial installed-state before contribution bind.
    scoped_recovery_limits install_limits{caps.installations,{512,512,32,64,512,1000000,65536,16000000},4,1024,1048576,512,512,1000000,65536,16000000};
    scoped_recovery_request request;request.binding=profile.binding;
    request.identity={1,0,{},10,receive_install_mode::full,"q1","e1","c1","m1"};
    request.model_tables={"LocalOriginalStream"};request.identity_mode=recovery_identity_mode::uuid;
    const auto target=uuid(40);
    request.full_rows={{{"LocalOriginalStream",target},{{"globalId",target},{"name",std::string("initial")},{"body",std::string("body")}}}};
    // First installation admits its durable scope/witness schema before the
    // producer freezes exact schema custody. New post-enrollment DDL is refused.
    auto initial=install_scoped_recovery(owner,request,install_limits);committed(initial.transaction);ASSERT_TRUE(initial.installation);bind();
    enroll();owner->db().execute("UPDATE LocalOriginalStream SET body='local' WHERE globalId=?",{target});
    const auto original=actual(owner->db(),"LocalOriginalStream",target);ASSERT_EQ(stamps(),1);
    committed(recovery_writer_access::install(owner,[&](auto& writer) {
        EXPECT_EQ(recovery_writer_access::active_handle(*owner,writer),recovery_writer_access::active_handle(*owner,*recovery_writer_access::active_writer(*owner)));
        auto capture=capture_pending_outbox(*owner,address.channel,install_limits.capture);EXPECT_EQ(capture.audit.size(),1);
        auto current=capture_recovery_rows(*owner,{{"LocalOriginalStream",{target}}},install_limits.capture);ASSERT_EQ(current.current_rows.size(),1);EXPECT_TRUE(current.current_rows[0].present);
    }));
    auto previous=request.identity;request.supersede=previous;request.identity={2,1,{receive_frontier_kind::position,10},11,receive_install_mode::full,"q2","e2","c2","m2"};
    request.full_rows[0].values["name"]=std::string("canonical");request.full_rows[0].values["body"]=std::string("accepted");
    request.pending={{original.original_id,{"LocalOriginalStream",target},recovery_pending_outcome::committed_effect}};
    auto installed=install_scoped_recovery(owner,request,install_limits);committed(installed.transaction);ASSERT_TRUE(installed.installation);
    EXPECT_EQ(stamps(),1);ASSERT_TRUE(stamp(original));
    owner->db().execute("UPDATE LocalOriginalStream SET body='ordinary-successor' WHERE globalId=?",{target});EXPECT_EQ(stamps(),2);
    bool entered=false;committed(recovery_writer_access::install(owner,[&](auto&){entered=true;}));EXPECT_TRUE(entered);
    EXPECT_THROW(recovery_writer_access::active_handle(*owner,owner->db()),lattice::db_error);
}

namespace {
void seed_reset(lattice::lattice_db& owner,const std::string& channel) {
    owner.add(LocalOriginalStream{"reset-seed","body"});
    owner.db().execute("INSERT INTO _lattice_sync_state VALUES(1,?,0)",{channel});
    owner.db().execute("INSERT INTO _lattice_sync_set VALUES(?,'LocalOriginalStream','member')",{channel});
    lattice::register_replication_slot(owner.db(),channel);
    owner.db().execute("UPDATE _lattice_replication_slots SET confirmed_audit_id=9,upload_floor=7 WHERE sync_id=?",{channel});
}
auto reset_rows(lattice::lattice_db& owner) {
    return std::array<std::vector<lattice::database::row_t>,3>{owner.db().query("SELECT * FROM _lattice_sync_state ORDER BY sync_id"),
        owner.db().query("SELECT * FROM _lattice_sync_set ORDER BY sync_id"),owner.db().query("SELECT * FROM _lattice_replication_slots ORDER BY sync_id")};
}
struct reset_sql_fault {
    sqlite3* connection;int action;const char* verb;int refusals=0;
    reset_sql_fault(lattice::database& db,int action,const char* verb):connection(db.handle()),action(action),verb(verb) {
        if(sqlite3_set_authorizer(connection,[](void* raw,int action,const char* one,const char*,const char*,const char*) noexcept {
            auto& f=*static_cast<reset_sql_fault*>(raw);
            if(action==f.action&&one&&std::strcmp(one,f.verb)==0){++f.refusals;return SQLITE_DENY;}return SQLITE_OK;
        },this)!=SQLITE_OK)throw std::runtime_error("reset fixture authorizer registration failed");
    }
    ~reset_sql_fault(){sqlite3_set_authorizer(connection,nullptr,nullptr);}
};
thread_local std::function<void()> reset_action;
struct reset_action_scope {
    bool before;void (*previous)();
    reset_action_scope(bool before,std::function<void()> action):before(before),previous(before?recovery_channel_reset_test_hooks::after_writer_capture:recovery_channel_reset_test_hooks::after_write_admission) {
        reset_action=std::move(action);
        auto& hook=before?recovery_channel_reset_test_hooks::after_writer_capture:recovery_channel_reset_test_hooks::after_write_admission;
        hook=[] {reset_action();};
    }
    ~reset_action_scope(){(before?recovery_channel_reset_test_hooks::after_writer_capture:recovery_channel_reset_test_hooks::after_write_admission)=previous;reset_action={};}
};
void primary_and_cleanup(const recovery_channel_reset_error& error) {
    ASSERT_TRUE(error.primary_error);ASSERT_TRUE(error.cleanup_error);
    try{std::rethrow_exception(error.primary_error);}catch(const lattice::db_error& e){EXPECT_NE(std::string(e.what()).find("reset second stage"),std::string::npos)<<e.what();}
    try{std::rethrow_exception(error.cleanup_error);}catch(const lattice::db_error& e){EXPECT_NE(std::string(e.what()).find("not authorized"),std::string::npos)<<e.what();}
}
}

TEST_F(RecoveryLocalProducer, ResetAndRemoveSavepointsPreserveCallerTransactionOnSuccessAndAbort) {
    for(const bool retire:{false,true}) {
        owner=open_owner();bind();seed_reset(*owner,"reset");const auto before=reset_rows(*owner);
        owner->db().execute("CREATE TRIGGER reset_fail_second BEFORE DELETE ON _lattice_sync_set BEGIN SELECT RAISE(ABORT,'reset second stage'); END");
        const auto reset=[&]{if(retire)owner->remove_sync_channel_state("reset");else owner->reset_sync_state("reset");};
        {own_transaction transaction(*owner);
            owner->db().execute("UPDATE LocalOriginalStream SET body='caller-work'");
            refused_at(reset,"reset second stage");EXPECT_EQ(recovery_writer_access::active_writer(*owner),&owner->db());EXPECT_EQ(reset_rows(*owner),before);
            EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream WHERE body='caller-work'"),1);
            owner->db().execute("DROP TRIGGER reset_fail_second");ASSERT_NO_THROW(reset());EXPECT_EQ(recovery_writer_access::active_writer(*owner),&owner->db());
            EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE sync_id='reset'"),0);
            // Destructor rolls back only the caller's still-owned transaction.
        }
        EXPECT_EQ(reset_rows(*owner),before);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream WHERE body='caller-work'"),0);
        owner->db().execute("DROP TRIGGER reset_fail_second");ASSERT_NO_THROW(reset());
        EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE sync_id='reset'"),0);
        EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_replication_slots WHERE sync_id='reset'"),retire?0:1);
    }
}

TEST_F(RecoveryLocalProducer, FailedSavepointCleanupFencesExecuteQueryAndPreparedWritesUntilExplicitRollback) {
    for(const char* denied:{"ROLLBACK","RELEASE"}) {
        owner=open_owner();bind();seed_reset(*owner,"reset");const auto before=reset_rows(*owner);
        owner->db().execute("CREATE TRIGGER reset_fail_second BEFORE DELETE ON _lattice_sync_set BEGIN SELECT RAISE(ABORT,'reset second stage'); END");
        owner->begin_transaction();
        {reset_sql_fault fault(owner->db(),SQLITE_SAVEPOINT,denied);
            try{owner->reset_sync_state("reset");FAIL()<<"cleanup denial must be reported";}
            catch(const recovery_channel_reset_error& error){primary_and_cleanup(error);}
            EXPECT_GT(fault.refusals,0);
        }
        EXPECT_TRUE(owner->db().is_in_transaction());
        EXPECT_THROW(owner->commit(),lattice::db_error);
        EXPECT_THROW(owner->db().execute("COMMIT"),lattice::db_error);
        EXPECT_THROW(owner->db().query("COMMIT"),lattice::db_error);
        EXPECT_THROW(owner->db().execute("UPDATE LocalOriginalStream SET body='blocked'"),lattice::db_error);
        EXPECT_THROW(owner->db().query("UPDATE LocalOriginalStream SET body='blocked' RETURNING id"),lattice::db_error);
        EXPECT_THROW(owner->db().insert("LocalOriginalStream",{{"name",std::string("blocked")},{"body",std::string("blocked")}}),lattice::db_error);
        EXPECT_THROW(owner->db().update("LocalOriginalStream",1,{{"body",std::string("blocked")}}),lattice::db_error);
        EXPECT_THROW(owner->db().remove("LocalOriginalStream",1),lattice::db_error);
        EXPECT_EQ(recovery_writer_access::active_writer(*owner),nullptr);
        EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream WHERE body='blocked'"),0);
        EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream"),1);
        ASSERT_NO_THROW(owner->rollback());EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_EQ(reset_rows(*owner),before);
        owner->db().execute("DROP TRIGGER reset_fail_second");ASSERT_NO_THROW(owner->reset_sync_state("reset"));
        owner->add(LocalOriginalStream{"successor","body"});EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM LocalOriginalStream"),2);
    }
}

TEST_F(RecoveryLocalProducer, FailedTopLevelRollbackRetainsBothErrorsAndRequiresExplicitRollback) {
    seed_reset(*owner,"reset");const auto before=reset_rows(*owner);
    owner->db().execute("CREATE TRIGGER reset_fail_second BEFORE DELETE ON _lattice_sync_set BEGIN SELECT RAISE(ABORT,'reset second stage'); END");
    {reset_sql_fault fault(owner->db(),SQLITE_TRANSACTION,"ROLLBACK");
        try{owner->remove_sync_channel_state("reset");FAIL()<<"whole rollback denial must be reported";}
        catch(const recovery_channel_reset_error& error){primary_and_cleanup(error);}
        EXPECT_EQ(fault.refusals,1);
    }
    EXPECT_TRUE(owner->db().is_in_transaction());EXPECT_THROW(owner->db().commit(),lattice::db_error);
    EXPECT_THROW(owner->db().query("DELETE FROM LocalOriginalStream RETURNING id"),lattice::db_error);
    ASSERT_NO_THROW(owner->rollback());EXPECT_EQ(reset_rows(*owner),before);
    owner->db().execute("DROP TRIGGER reset_fail_second");ASSERT_NO_THROW(owner->remove_sync_channel_state("reset"));
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_replication_slots WHERE sync_id='reset'"),0);
}

TEST_F(RecoveryLocalProducer, EnrollmentBetweenCaptureAndWriteAdmissionCannotResetDurableState) {
    TempDB file("local-producer-reset-interleave");owner=open_owner(file.str());bind();seed_reset(*owner,address.channel);
    auto older=open_owner(file.str());const auto before=reset_rows(*older);bool enrolled=false;
    {reset_action_scope interleave(true,[&]{enroll();enrolled=true;});
        refused_at([&]{older->reset_sync_state(address.channel);},"retention/history/reset integration is not active");
    }
    EXPECT_TRUE(enrolled);EXPECT_EQ(reset_rows(*older),before);EXPECT_FALSE(older->db().is_in_transaction());
    owner->add(LocalOriginalStream{"enrolled-successor","body"});EXPECT_EQ(stamps(),1);
    older->close();owner->close();older.reset();owner.reset();
}

TEST_F(RecoveryLocalProducer, ResetFirstWriterAdmissionExcludesSiblingEnrollmentAndRefusesRawTransaction) {
    TempDB file("local-producer-reset-first");owner=open_owner(file.str());bind();seed_reset(*owner,address.channel);
    auto sibling=open_owner(file.str());bool checked=false;
    {reset_action_scope interleave(false,[&]{checked=true;EXPECT_TRUE(owner->db().is_in_transaction());EXPECT_FALSE(sibling->db().try_begin_immediate());});
        ASSERT_NO_THROW(owner->reset_sync_state(address.channel));
    }
    EXPECT_TRUE(checked);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_sync_state"),0);
    EXPECT_TRUE(sibling->db().try_begin_immediate());sibling->db().rollback();
    owner->db().execute("BEGIN IMMEDIATE");
    EXPECT_THROW(owner->remove_sync_channel_state(address.channel),lattice::db_error);
    EXPECT_TRUE(owner->db().is_in_transaction());owner->db().execute("ROLLBACK");
    enroll();owner->add(LocalOriginalStream{"after-serialization","body"});EXPECT_EQ(stamps(),1);
    sibling->close();owner->close();sibling.reset();owner.reset();
}

TEST_F(RecoveryLocalProducer, AdmittedOrdinaryCallerResetRunsAfterLogicalCloseWithoutSettlingCaller) {
    TempDB file("local-producer-reset-close");owner=open_owner(file.str());bind();seed_reset(*owner,"reset");
    auto retained=recovery_local_producer_test_access::retain(*owner);auto* raw=retained->handle();ASSERT_NE(raw,nullptr);
    owner->begin_transaction();bool closed=false;
    {reset_action_scope close_after_admission(false,[&]{owner->close();closed=true;});
        ASSERT_NO_THROW(owner->reset_sync_state("reset"));
    }
    EXPECT_TRUE(closed);EXPECT_EQ(sqlite3_get_autocommit(raw),0); // caller transaction is not committed
    sqlite3_stmt* statement=nullptr;ASSERT_EQ(sqlite3_prepare_v2(raw,"SELECT COUNT(*) FROM _lattice_sync_state WHERE sync_id='reset'",-1,&statement,nullptr),SQLITE_OK);
    ASSERT_NE(statement,nullptr);std::unique_ptr<sqlite3_stmt,decltype(&sqlite3_finalize)> held(statement,&sqlite3_finalize);
    ASSERT_EQ(sqlite3_step(statement),SQLITE_ROW);EXPECT_EQ(sqlite3_column_int64(statement,0),0);held.reset();
    // The public owner is closed; the explicit raw fixture retains/settles its
    // own transaction, without claiming public post-close COMMIT support.
    ASSERT_EQ(sqlite3_exec(raw,"ROLLBACK",nullptr,nullptr,nullptr),SQLITE_OK);
    owner.reset();retained.reset();owner=open_owner(file.str());
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE sync_id='reset'"),1);
    owner->close();owner.reset();
}

TEST_F(RecoveryLocalProducer, IgnoredResetWriteRefusesPartialSuccessAndRetriesWithoutCreatingAbsentSlot) {
    for(const bool slot_update:{false,true}) {
        owner=open_owner();bind();seed_reset(*owner,"reset");const auto before=reset_rows(*owner);
        owner->db().execute(slot_update?
            "CREATE TRIGGER reset_ignore BEFORE UPDATE OF upload_floor ON _lattice_replication_slots BEGIN SELECT RAISE(IGNORE); END":
            "CREATE TRIGGER reset_ignore BEFORE DELETE ON _lattice_sync_set BEGIN SELECT RAISE(IGNORE); END");
        refused_at([&]{owner->reset_sync_state("reset");},"channel reset postimage mismatch");
        EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_EQ(reset_rows(*owner),before);
        owner->db().execute("DROP TRIGGER reset_ignore");ASSERT_NO_THROW(owner->reset_sync_state("reset"));
        EXPECT_EQ(scalar(owner->db(),"SELECT upload_floor AS n FROM _lattice_replication_slots WHERE sync_id='reset'"),0);
        EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_sync_set WHERE sync_id='reset'"),0);
        owner->db().execute("DELETE FROM _lattice_replication_slots WHERE sync_id='reset'");
        owner->db().execute("INSERT INTO _lattice_sync_state VALUES(1,'reset',0)");
        ASSERT_NO_THROW(owner->reset_sync_state("reset"));
        EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_sync_state WHERE sync_id='reset'"),0);
        EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM _lattice_replication_slots WHERE sync_id='reset'"),0);
    }
}
