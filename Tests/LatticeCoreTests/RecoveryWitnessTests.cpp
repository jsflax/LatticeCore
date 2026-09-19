#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_witness.hpp"
#include "../../Sources/LatticeCore/src/scoped_recovery_install.hpp"
#include <deque>
#include <limits>
#include <set>

namespace {
using namespace lattice::detail;
class queued_refresh_scheduler final : public lattice::scheduler {
public:
    std::deque<std::function<void()>> work;
    bool accepting = true;
    void invoke(std::function<void()>&& fn) override { if (accepting) work.push_back(std::move(fn)); }
    bool is_on_thread() const noexcept override { return true; }
    bool is_same_as(const scheduler* other) const noexcept override { return this == other; }
    bool can_invoke() const noexcept override { return accepting; }
    void shutdown() override { accepting = false; work.clear(); }
    void run_one() {
        if (work.empty()) throw std::runtime_error("expected queued refresh work");
        auto fn = std::move(work.front()); work.pop_front(); fn();
    }
    void drain() { while (!work.empty()) run_one(); }
};
class RecoveryWitness : public ::testing::Test {
protected:
    TempDB path{"recovery-witness"};
    std::shared_ptr<queued_refresh_scheduler> scheduler = std::make_shared<queued_refresh_scheduler>();
    std::shared_ptr<lattice::lattice_db> owner;
    scoped_recovery_limits limits{{8,4096,65536},{512,512,32,64,512,1000000,65536,16000000},8,1024,1048576,512,512,1000000,65536,16000000};
    void SetUp() override {
        lattice::configuration c(path.str()); c.audit_retention_seconds=0;c.busy_timeout_ms=50;c.sched=scheduler;
        owner=std::make_shared<lattice::lattice_db>(c);
        owner->add(TestPerson{"seed",1,std::nullopt});
        owner->db().execute("DELETE FROM AuditLog");scheduler->drain();
        if(auto* notifier=lattice::instance_registry::instance().get_or_create_notifier(path.str()))notifier->stop_listening();
    }
    scoped_recovery_request request() {
        scoped_recovery_request r;r.binding={"channel","authority","source","epoch","scope","schema"};
        r.identity={1,0,{},10,receive_install_mode::full,"Q","E","C","M"};r.model_tables={"TestPerson"};return r;
    }
    scoped_recovery_request next(scoped_recovery_request r) {
        r.supersede=r.identity;++r.identity.sequence;++r.identity.expected_revision;
        r.identity.base={receive_frontier_kind::position,r.identity.head};r.identity.manifest_digest+="n";return r;
    }
    void row(scoped_recovery_request& r,const std::string& name) {
        r.full_rows={{{"TestPerson","remote"},{{"globalId",std::string("remote")},{"name",name},{"age",int64_t(9)},{"email",nullptr}}}};
    }
    scoped_recovery_result install(const scoped_recovery_request& r) { return install_scoped_recovery(owner,r,limits); }
    void committed(const scoped_recovery_result& r) {
        if(r.transaction.primary_error)try{std::rethrow_exception(r.transaction.primary_error);}catch(const std::exception& e){ADD_FAILURE()<<e.what();}
        ASSERT_EQ(r.transaction.state,recovery_install_state::committed);ASSERT_TRUE(r.installation);
    }
    std::optional<recovery_witness> read() {
        lattice::database reader(path.str(),lattice::database::open_mode::read_only);
        return read_recovery_witness(reader);
    }
    void raw_suppression(lattice::database& raw) {
        // A bare connection intentionally has no lattice_db registry/fanout.
        // Generated model triggers still require this connection-local UDF.
        if(sqlite3_create_function_v2(raw.handle(),"sync_disabled",0,SQLITE_UTF8,nullptr,
            [](sqlite3_context* context,int,sqlite3_value**){sqlite3_result_int(context,1);},
            nullptr,nullptr,nullptr)!=SQLITE_OK)throw std::runtime_error("raw suppression registration failed");
    }
    void raw_bump(const std::string& name="external") {
        // No Core instance registry/observer delivery: model and existing
        // witness settle through one independent raw connection transaction.
        lattice::database raw(path.str());raw_suppression(raw);raw.execute("BEGIN IMMEDIATE");
        raw.execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
        raw.execute("UPDATE TestPerson SET name=? WHERE globalId='remote'",{name});
        raw.execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
        raw.execute("UPDATE _lattice_recovery_witness SET generation=generation+1 WHERE id=1");raw.execute("COMMIT");
    }
    void drain_refresh() { owner->request_recovery_refresh();scheduler->drain(); }
    std::string current_name(lattice::database& db) {
        return std::get<std::string>(db.query("SELECT name FROM TestPerson WHERE globalId='remote'").at(0).at("name"));
    }
};
}

TEST_F(RecoveryWitness, GenericWriterReadsAndLegacyObserversDoNotCreateWitness) {
    EXPECT_FALSE(read());
    auto result=recovery_writer_access::install(owner,[&](auto& db){EXPECT_FALSE(read_recovery_witness(db));});
    EXPECT_EQ(result.state,recovery_install_state::committed);EXPECT_FALSE(read());
    int calls=0;owner->add_recovery_refresh_observer([&]{++calls;});drain_refresh();
    EXPECT_EQ(calls,0);EXPECT_FALSE(read());
    EXPECT_THROW(bump_recovery_witness(*owner),lattice::db_error);
}

TEST_F(RecoveryWitness, NewEmptySameHeadInstallBumpsButExactRetryAndReadDoNot) {
    auto r=request();committed(install(r));const auto first=read();ASSERT_TRUE(first);EXPECT_EQ(first->generation,1);
    committed(install(r));EXPECT_EQ(read(),first);
    auto capture=recovery_writer_access::install(owner,[&](auto&){receive_install_store state(owner,limits.installations);const auto snapshot=state.read("channel");ASSERT_TRUE(snapshot);EXPECT_EQ(snapshot->revision,1);});
    EXPECT_EQ(capture.state,recovery_install_state::committed);EXPECT_EQ(read(),first);
    auto n=next(r);committed(install(n));const auto second=read();ASSERT_TRUE(second);
    EXPECT_EQ(second->incarnation,first->incarnation);EXPECT_EQ(second->generation,2);
    EXPECT_TRUE(owner->db().query("SELECT * FROM AuditLog").empty());
}

TEST_F(RecoveryWitness, CommitDenialRollsBackWitnessCreationModelAndInstalledState) {
    auto r=request();row(r,"new");
    auto* db=owner->db().handle();
    sqlite3_set_authorizer(db,[](void*,int action,const char* arg,const char*,const char*,const char*){
        return action==SQLITE_TRANSACTION&&arg&&std::string_view(arg)=="COMMIT"?SQLITE_DENY:SQLITE_OK;
    },nullptr);
    const auto failed=install(r);sqlite3_set_authorizer(db,nullptr,nullptr);
    EXPECT_EQ(failed.transaction.state,recovery_install_state::rolled_back);EXPECT_TRUE(failed.transaction.primary_error);
    EXPECT_FALSE(read());EXPECT_TRUE(owner->db().query("SELECT * FROM TestPerson WHERE globalId='remote'").empty());
    EXPECT_TRUE(owner->db().query("SELECT * FROM sqlite_schema WHERE name='_lattice_install_channel'").empty());
    committed(install(r));ASSERT_TRUE(read());EXPECT_EQ(read()->generation,1);
}

TEST_F(RecoveryWitness, OuterRollbackDiscardsExistingBumpAndModelTogether) {
    auto r=request();row(r,"before");committed(install(r));const auto before=read();
    auto failed=recovery_writer_access::install(owner,[&](auto& db){
        db.execute("UPDATE TestPerson SET name='rolled-back' WHERE globalId='remote'");
        EXPECT_EQ(bump_recovery_witness(*owner).generation,before->generation+1);throw std::runtime_error("after bump");
    });
    EXPECT_EQ(failed.state,recovery_install_state::rolled_back);EXPECT_EQ(read(),before);EXPECT_EQ(current_name(owner->db()),"before");
}

TEST_F(RecoveryWitness, ExhaustionAndMalformedIncarnationRefuseInsteadOfResetting) {
    auto r=request();committed(install(r));
    owner->db().execute("UPDATE _lattice_recovery_witness SET generation=?",{std::numeric_limits<int64_t>::max()});
    const auto max=read();auto n=next(r);row(n,"must-not-install");
    EXPECT_NE(install(n).transaction.state,recovery_install_state::committed);EXPECT_EQ(read(),max);
    owner->db().execute("UPDATE _lattice_recovery_witness SET generation=1,incarnation=zeroblob(15)");
    EXPECT_THROW(read(),lattice::db_error);EXPECT_NE(install(n).transaction.state,recovery_install_state::committed);
    EXPECT_TRUE(owner->db().query("SELECT * FROM TestPerson WHERE globalId='remote'").empty());
}

TEST_F(RecoveryWitness, MissingSingletonAndUnexpectedTriggerRefuse) {
    auto r=request();committed(install(r));owner->db().execute("DELETE FROM _lattice_recovery_witness");
    EXPECT_THROW(read(),lattice::db_error);EXPECT_NE(install(next(r)).transaction.state,recovery_install_state::committed);
    owner->db().execute("INSERT INTO _lattice_recovery_witness VALUES(1,1,zeroblob(16),1)");
    owner->db().execute("CREATE TRIGGER ignore_witness BEFORE UPDATE ON _lattice_recovery_witness BEGIN SELECT RAISE(IGNORE); END");
    EXPECT_THROW(read(),lattice::db_error);EXPECT_NE(install(next(r)).transaction.state,recovery_install_state::committed);
}

TEST_F(RecoveryWitness, MissedRawCommitsCoalesceWithoutModelOrAuditPayloadEvents) {
    auto r=request();row(r,"before");committed(install(r));scheduler->drain();
    int refresh=0,audit=0,model=0,dirty=0;
    owner->add_recovery_refresh_observer([&]{++refresh;});drain_refresh();ASSERT_EQ(refresh,1);
    owner->add_table_observer("AuditLog",[&](const auto&){++audit;});
    owner->add_table_observer("TestPerson",[&](const auto&){++model;});
    owner->add_invalidation_hook_detailed([&](const auto& tables,auto reason){if(reason==lattice::lattice_db::invalidation_reason::recovery){++dirty;EXPECT_TRUE(tables.empty());}});
    raw_bump("one");raw_bump("two");raw_bump("three");EXPECT_EQ(refresh,1);
    drain_refresh();EXPECT_EQ(refresh,2);EXPECT_EQ(dirty,1);EXPECT_EQ(audit,0);EXPECT_EQ(model,0);
    EXPECT_EQ(current_name(owner->read_db()),"three");drain_refresh();EXPECT_EQ(refresh,2);
    EXPECT_TRUE(owner->db().query("SELECT * FROM AuditLog").empty());
}

TEST_F(RecoveryWitness, NewerWitnessDuringCallbackAndLaterRegistrationRemainDue) {
    auto r=request();row(r,"before");committed(install(r));scheduler->drain();
    int calls=0,late=0;bool inject=false;
    owner->add_recovery_refresh_observer([&]{++calls;if(inject){inject=false;raw_bump("during");owner->add_recovery_refresh_observer([&]{++late;});}});
    drain_refresh();ASSERT_EQ(calls,1);inject=true;raw_bump("first");drain_refresh();
    EXPECT_EQ(calls,2);EXPECT_EQ(late,0);drain_refresh();EXPECT_EQ(calls,3);EXPECT_EQ(late,1);
    EXPECT_EQ(current_name(owner->read_db()),"during");drain_refresh();EXPECT_EQ(calls,3);
}

TEST_F(RecoveryWitness, CallbackThrowAndSchedulerDiscardDoNotAcknowledge) {
    auto r=request();committed(install(r));scheduler->drain();
    int attempts=0,other=0;
    owner->add_recovery_refresh_observer([&]{if(++attempts==1)throw std::runtime_error("retry me");});
    owner->add_recovery_refresh_observer([&]{++other;});scheduler->drain();EXPECT_EQ(attempts,1);EXPECT_EQ(other,1);
    drain_refresh();EXPECT_EQ(attempts,2);EXPECT_EQ(other,2);drain_refresh();EXPECT_EQ(attempts,2);
    raw_bump();owner->request_recovery_refresh();ASSERT_EQ(scheduler->work.size(),1u);scheduler->work.clear();
    drain_refresh();EXPECT_EQ(attempts,3);scheduler->accepting=false;raw_bump();owner->request_recovery_refresh();
    scheduler->accepting=true;drain_refresh();EXPECT_EQ(attempts,4);
}

TEST_F(RecoveryWitness, CancellationAndCloseBeforeQueuedDeliverySuppressCallback) {
    auto r=request();committed(install(r));scheduler->drain();int calls=0;
    auto token=owner->add_recovery_refresh_observer([&]{++calls;});owner->remove_recovery_refresh_observer(token);scheduler->drain();EXPECT_EQ(calls,0);
    owner->add_recovery_refresh_observer([&]{++calls;});ASSERT_FALSE(scheduler->work.empty());owner->close();scheduler->drain();EXPECT_EQ(calls,0);
}

TEST_F(RecoveryWitness, RawPinnedWriterViewIsPreservedUntilReleaseThenRefreshes) {
    auto r=request();row(r,"before");committed(install(r));scheduler->drain();int calls=0;
    owner->add_recovery_refresh_observer([&]{++calls;});drain_refresh();ASSERT_EQ(calls,1);
    owner->db().execute("BEGIN");EXPECT_EQ(current_name(owner->db()),"before");
    raw_bump("after");drain_refresh();EXPECT_EQ(calls,1);EXPECT_EQ(current_name(owner->db()),"before");
    owner->db().execute("COMMIT");drain_refresh();EXPECT_EQ(calls,2);EXPECT_EQ(current_name(owner->db()),"after");
}

TEST_F(RecoveryWitness, OldReadBorrowerRetainsItsViewWhileCurrentRouteAdvances) {
    auto r=request();row(r,"before");committed(install(r));scheduler->drain();int calls=0;
    owner->add_recovery_refresh_observer([&]{++calls;});drain_refresh();
    auto old=owner->borrow_read_connection();old->execute("BEGIN");EXPECT_EQ(current_name(*old),"before");
    raw_bump("after");drain_refresh();EXPECT_EQ(calls,2);
    EXPECT_EQ(current_name(*old),"before");EXPECT_EQ(current_name(owner->read_db()),"after");old->execute("ROLLBACK");
}

TEST_F(RecoveryWitness, ReadOnlyFacadeReopenSeesDurableWitnessWithoutSourceOwner) {
    auto r=request();row(r,"before");committed(install(r));scheduler->drain();owner->close();owner.reset();
    lattice::configuration c(path.str());c.read_only=true;c.sched=scheduler=std::make_shared<queued_refresh_scheduler>();
    owner=std::make_shared<lattice::lattice_db>(c);
    if(auto* notifier=lattice::instance_registry::instance().get_or_create_notifier(path.str()))notifier->stop_listening();
    int calls=0;owner->add_recovery_refresh_observer([&]{++calls;});drain_refresh();EXPECT_EQ(calls,1);
    raw_bump("after-exit");drain_refresh();EXPECT_EQ(calls,2);EXPECT_EQ(current_name(owner->read_db()),"after-exit");
}

TEST_F(RecoveryWitness, HeldObjectGetsAllPhysicalAndLogicalGeoFieldsWithoutPayloadRow) {
    owner->add(TestPlace{"park",lattice::geo_bounds(1,2,3,4)});scheduler->drain();
    auto r=request();committed(install(r));scheduler->drain();int calls=0;std::set<std::string> fields;
    owner->add_recovery_refresh_observer([]{});drain_refresh();
    owner->add_object_observer("TestPlace",1,[&](const std::string& json){++calls;for(const auto& x:nlohmann::json::parse(json))fields.insert(x.get<std::string>());});
    scheduler->drain();EXPECT_EQ(calls,1);EXPECT_TRUE(fields.contains("name"));EXPECT_TRUE(fields.contains("location"));
    EXPECT_TRUE(fields.contains("location_minLat"));EXPECT_TRUE(fields.contains("location_maxLat"));
    EXPECT_TRUE(fields.contains("location_minLon"));EXPECT_TRUE(fields.contains("location_maxLon"));
}

TEST_F(RecoveryWitness, IncarnationChangeOrRegressedGenerationStillRefreshes) {
    auto r=request();committed(install(r));scheduler->drain();int calls=0;
    owner->add_recovery_refresh_observer([&]{++calls;});drain_refresh();ASSERT_EQ(calls,1);
    raw_bump();drain_refresh();ASSERT_EQ(calls,2);
    { lattice::database raw(path.str());raw.execute("UPDATE _lattice_recovery_witness SET generation=1"); }
    drain_refresh();EXPECT_EQ(calls,3);
    const auto old=read();
    { lattice::database raw(path.str());raw.execute("UPDATE _lattice_recovery_witness SET incarnation=randomblob(16)"); }
    ASSERT_NE(read()->incarnation,old->incarnation);drain_refresh();EXPECT_EQ(calls,4);
}

TEST_F(RecoveryWitness, ReopenRequestsCatchUpWithoutReseedingWitness) {
    auto r=request();row(r,"before");committed(install(r));scheduler->drain();int calls=0;
    owner->add_recovery_refresh_observer([&]{++calls;});drain_refresh();ASSERT_EQ(calls,1);
    owner->close_read_db();raw_bump("after");owner->reopen_read_db();scheduler->drain();
    EXPECT_EQ(calls,2);EXPECT_EQ(current_name(owner->read_db()),"after");drain_refresh();EXPECT_EQ(calls,2);
}

TEST_F(RecoveryWitness, BusyImplicitStatementDefersUntilItsOldViewIsReleased) {
    auto r=request();row(r,"before");committed(install(r));scheduler->drain();int calls=0;
    owner->add_recovery_refresh_observer([&]{++calls;});drain_refresh();ASSERT_EQ(calls,1);
    sqlite3_stmt* raw=nullptr;auto* db=owner->db().handle();
    ASSERT_EQ(sqlite3_prepare_v2(db,"SELECT name FROM TestPerson WHERE globalId='remote'",-1,&raw,nullptr),SQLITE_OK);
    std::unique_ptr<sqlite3_stmt,decltype(&sqlite3_finalize)> statement(raw,sqlite3_finalize);
    ASSERT_EQ(sqlite3_step(raw),SQLITE_ROW);ASSERT_EQ(sqlite3_get_autocommit(db),1);ASSERT_NE(sqlite3_stmt_busy(raw),0);
    raw_bump("after");drain_refresh();EXPECT_EQ(calls,1);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(sqlite3_column_text(raw,0))),"before");
    statement.reset();drain_refresh();EXPECT_EQ(calls,2);EXPECT_EQ(current_name(owner->db()),"after");
}

TEST_F(RecoveryWitness, LogicalRelationNameComesFromRegisteredInternalTableMap) {
    auto parent=owner->add(TestOwner{"owner",nullptr});
    owner->ensure_link_table("_TestOwner_TestPet_pets","TestOwner:pets","TestPet");scheduler->drain();
    ASSERT_TRUE(owner->db().query("SELECT name FROM pragma_table_info('TestOwner') WHERE name='pets'").empty());
    ASSERT_EQ(std::get<std::string>(owner->db().query("SELECT value FROM _lattice_meta WHERE key='internal_table:_TestOwner_TestPet_pets'").at(0).at("value")),"TestOwner:pets");
    auto r=request();committed(install(r));scheduler->drain();int calls=0;std::set<std::string> fields;
    owner->add_recovery_refresh_observer([]{});drain_refresh();
    owner->add_object_observer("TestOwner",parent.id(),[&](const auto& value){++calls;for(const auto& name:nlohmann::json::parse(value))fields.insert(name.template get<std::string>());});
    scheduler->drain();EXPECT_EQ(calls,1);EXPECT_TRUE(fields.contains("pets"));EXPECT_TRUE(fields.contains("name"));
}

TEST_F(RecoveryWitness, CallbackCanReleaseFinalFacadeAndRemainingCallbacksAreRevoked) {
    auto r=request();committed(install(r));scheduler->drain();int first=0,second=0;
    owner->add_recovery_refresh_observer([&]{++first;owner.reset();});
    owner->add_recovery_refresh_observer([&]{++second;});scheduler->run_one();
    EXPECT_FALSE(owner);EXPECT_EQ(first,1);EXPECT_EQ(second,0);
}

TEST_F(RecoveryWitness, DisappearedWitnessStaysPendingAndRestoringSameTupleRefreshes) {
    auto r=request();row(r,"before");committed(install(r));scheduler->drain();int calls=0;
    owner->add_recovery_refresh_observer([&]{++calls;});drain_refresh();ASSERT_EQ(calls,1);
    const auto old=read();ASSERT_TRUE(old);
    const auto schema=std::get<std::string>(owner->db().query("SELECT sql FROM sqlite_schema WHERE name='_lattice_recovery_witness'").at(0).at("sql"));
    { lattice::database raw(path.str());raw_suppression(raw);raw.execute("BEGIN IMMEDIATE");
      raw.execute("UPDATE TestPerson SET name='after-disappearance' WHERE globalId='remote'");
      raw.execute("DROP TABLE _lattice_recovery_witness");raw.execute("COMMIT"); }
    EXPECT_FALSE(read());drain_refresh();EXPECT_EQ(calls,1);drain_refresh();EXPECT_EQ(calls,1);
    { lattice::database raw(path.str());raw.execute("BEGIN IMMEDIATE");raw.execute(schema);
      raw.execute("INSERT INTO _lattice_recovery_witness VALUES(1,1,?,?)",{std::vector<uint8_t>(old->incarnation.begin(),old->incarnation.end()),old->generation});raw.execute("COMMIT"); }
    ASSERT_EQ(read(),old);drain_refresh();EXPECT_EQ(calls,2);EXPECT_EQ(current_name(owner->read_db()),"after-disappearance");
    drain_refresh();EXPECT_EQ(calls,2);
}
