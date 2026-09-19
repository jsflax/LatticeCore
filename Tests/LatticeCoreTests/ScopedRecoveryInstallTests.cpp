#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/scoped_recovery_install.hpp"
#include <set>
#include <string_view>

namespace {
using namespace lattice::detail;
using state=recovery_install_state;
using outcome=recovery_pending_outcome;
using values=lattice::detail::sync_recovery::row_values;
lattice::configuration config(const std::string& path) {
    lattice::configuration c(path);c.audit_retention_seconds=0;return c;
}
class ScopedOwner:public lattice::lattice_db {
public:
    explicit ScopedOwner(const std::string& path=":memory:"):lattice_db(config(path)) {}
    void document_schema() {
        lattice::model_schema s;s.table_name="RecoveryDocument";
        lattice::property_descriptor body;body.name="body";body.type=lattice::column_type::text;body.no_history=true;
        lattice::property_descriptor title;title.name="title";title.type=lattice::column_type::text;
        lattice::property_descriptor bytes;bytes.name="bytes";bytes.type=lattice::column_type::blob;
        s.properties={body,title,bytes};create_model_table_public(s);
    }
};
class ScopedRecoveryInstall:public ::testing::Test {
protected:
    std::shared_ptr<ScopedOwner> owner=std::make_shared<ScopedOwner>();
    scoped_recovery_limits limits{{8,4096,65536},{512,512,32,64,512,1000000,65536,16000000},8,1024,1048576,512,512,1000000,65536,16000000};
    scoped_recovery_request request() {
        scoped_recovery_request r;r.binding={"channel","authority","source","epoch","scope","schema"};
        r.identity={1,0,{},10,receive_install_mode::full,"request1","receipt1","content1","manifest1"};
        r.model_tables={"TestPerson"};return r;
    }
    scoped_recovery_request next(const scoped_recovery_request& old) {
        auto r=old;r.supersede=old.identity;r.identity.sequence++;
        r.identity.expected_revision++;r.identity.base={receive_frontier_kind::position,old.identity.head};
        r.identity.request_digest+="n";r.identity.receipt_digest+="n";r.identity.content_digest+="n";r.identity.manifest_digest+="n";
        r.initial_row_grants.clear();r.pending.clear();return r;
    }
    recovery_full_row person(const std::string& id,const std::string& name="remote",int64_t age=20) {
        return {{"TestPerson",id},{{"globalId",id},{"name",name},{"age",age},{"email",nullptr}}};
    }
    void local(const std::string& id,const std::string& name="local",int64_t age=1) {
        owner->db().execute("INSERT INTO TestPerson(globalId,name,age,email) VALUES(?,?,?,NULL)",{id,name,age});
    }
    void clear_history() {owner->db().execute("DELETE FROM _lattice_sync_state");owner->db().execute("DELETE FROM AuditLog");}
    std::vector<recovery_pending_grant> grants(const recovery_row_key& target,outcome accepted=outcome::not_committed) {
        std::vector<recovery_pending_grant> result;
        for(const auto& a:owner->db().query("SELECT globalId FROM AuditLog WHERE tableName=? AND globalRowId=? ORDER BY id",{target.table,target.global_id}))
            result.push_back({std::get<std::string>(a.at("globalId")),target,accepted});
        return result;
    }
    void add_grants(scoped_recovery_request& r,const recovery_row_key& k,outcome accepted=outcome::not_committed) {
        auto g=grants(k,accepted);r.pending.insert(r.pending.end(),g.begin(),g.end());
    }
    scoped_recovery_result run(const scoped_recovery_request& r) {return install_scoped_recovery(owner,r,limits);}
    void committed(const scoped_recovery_result& r) {
        if(r.transaction.primary_error)try{std::rethrow_exception(r.transaction.primary_error);}catch(const std::exception& e){ADD_FAILURE()<<e.what();}
        EXPECT_EQ(r.transaction.state,state::committed);EXPECT_FALSE(r.transaction.cleanup_error);ASSERT_TRUE(r.installation);
    }
    void refused(const scoped_recovery_result& r) {
        EXPECT_NE(r.transaction.state,state::committed);EXPECT_TRUE(r.transaction.primary_error);EXPECT_FALSE(r.installation);
    }
    int64_t number(const std::string& sql) {return std::get<int64_t>(owner->db().query(sql).at(0).begin()->second);}
    std::string name(const std::string& id) {
        return std::get<std::string>(owner->db().query("SELECT name FROM TestPerson WHERE globalId=?",{id}).at(0).at("name"));
    }
    int64_t rows(const std::string& id) {
        return std::get<int64_t>(owner->db().query("SELECT COUNT(*) AS n FROM TestPerson WHERE globalId=?",{id}).at(0).at("n"));
    }
    receive_install_snapshot snapshot(const scoped_recovery_request& r) {
        std::optional<receive_install_snapshot> result;
        auto done=recovery_writer_access::install(owner,[&](auto&){receive_install_store store(owner,limits.installations);result=store.read(r.binding.channel);});
        EXPECT_EQ(done.state,state::committed);return result.value();
    }
};
}

TEST_F(ScopedRecoveryInstall, StableHeldModelAndFinalObserverValuesOnFileAndMemory) {
    for(const bool file:{false,true}) {
        TempDB path("scoped-install-held");owner=std::make_shared<ScopedOwner>(file?path.str():":memory:");
        auto held=owner->add(TestPerson{"before",1,std::nullopt});const auto pk=held.id();const auto gid=held.global_id();clear_history();
        auto r=request();r.full_rows={person(gid,"canonical",77)};r.initial_row_grants={{"TestPerson",gid}};
        int callbacks=0;std::set<std::string> fields;
        auto token=held.observe([&](lattice::object_change<TestPerson>& change){
            ++callbacks;for(const auto& p:change.property_changes)fields.insert(p.name);
            EXPECT_EQ(name(gid),"canonical");
            EXPECT_EQ(std::get<int64_t>(owner->db().query("SELECT age FROM TestPerson WHERE globalId=?",{gid}).at(0).at("age")),77);
        });
        committed(run(r));EXPECT_EQ(held.id(),pk);EXPECT_EQ(std::get<std::string>(held.get_value("name")),"canonical");
        EXPECT_GT(callbacks,0);EXPECT_TRUE(fields.count("name"));EXPECT_TRUE(fields.count("age"));
        EXPECT_EQ(number("SELECT COUNT(*) FROM AuditLog"),0);token.invalidate();owner.reset();
    }
}

TEST_F(ScopedRecoveryInstall, AuditOrderPreservesRemoteUnchangedFieldsAndOriginalIds) {
    local("row","old",1);clear_history();owner->db().execute("UPDATE TestPerson SET age=2 WHERE globalId='row'");
    owner->db().execute("UPDATE TestPerson SET age=3 WHERE globalId='row'");
    const auto original=owner->db().query("SELECT * FROM AuditLog ORDER BY id");
    auto r=request();r.full_rows={person("row","remote-name",50)};add_grants(r,{"TestPerson","row"});
    committed(run(r));EXPECT_EQ(name("row"),"remote-name");EXPECT_EQ(number("SELECT age FROM TestPerson WHERE globalId='row'"),3);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog ORDER BY id"),original);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state"),0);
}

TEST_F(ScopedRecoveryInstall, CoveredLostAckInsertThenRemoteDeleteSettlesOnlyThisChannel) {
    local("lost","stale-insert");const auto original=owner->db().query("SELECT * FROM AuditLog");
    owner->db().execute("INSERT INTO _lattice_sync_state SELECT id,'other-channel',0 FROM AuditLog");
    auto r=request();add_grants(r,{"TestPerson","lost"},outcome::committed_effect);
    committed(run(r));EXPECT_EQ(rows("lost"),0);EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),original);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state WHERE sync_id='channel' AND is_synchronized=1"),1);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state WHERE sync_id='other-channel' AND is_synchronized=0"),1);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_recovery_member"),0);
}

TEST_F(ScopedRecoveryInstall, NeverSentInsertAbsentFromFullOverlaysWithoutChangingIdentity) {
    local("new","unsent",7);const auto original=owner->db().query("SELECT * FROM AuditLog");
    const auto pk=number("SELECT id FROM TestPerson WHERE globalId='new'");
    auto r=request();add_grants(r,{"TestPerson","new"});committed(run(r));
    EXPECT_EQ(name("new"),"unsent");EXPECT_EQ(number("SELECT id FROM TestPerson WHERE globalId='new'"),pk);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),original);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_recovery_member WHERE global_id='new'"),1);
}

TEST_F(ScopedRecoveryInstall, CoveredOldFieldDoesNotResurrectOverLaterCanonicalValue) {
    local("row","old");auto r=request();r.full_rows={person("row","newer-remote",9)};
    add_grants(r,{"TestPerson","row"},outcome::committed_noop);committed(run(r));
    EXPECT_EQ(name("row"),"newer-remote");EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state WHERE is_synchronized=1"),1);
}

TEST_F(ScopedRecoveryInstall, LocalOnlyAndSecondScopeRowsAndPendingIntentsRemainUntouched) {
    auto other=request();other.binding.channel="other";other.binding.scope="other-scope";other.full_rows={person("second","second")};committed(run(other));
    local("local-only","local-only");owner->db().execute("UPDATE TestPerson SET age=88 WHERE globalId='second'");
    const auto untouched=owner->db().query("SELECT * FROM AuditLog ORDER BY id");
    auto r=request();r.full_rows={person("own","own")};committed(run(r));
    auto replace=next(r);replace.full_rows.clear();committed(run(replace));
    EXPECT_EQ(rows("own"),0);EXPECT_EQ(name("second"),"second");EXPECT_EQ(name("local-only"),"local-only");
    EXPECT_EQ(number("SELECT age FROM TestPerson WHERE globalId='second'"),88);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog ORDER BY id"),untouched);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state"),0);
}

TEST_F(ScopedRecoveryInstall, UnknownAndMissingCoverageRefuseBeforeAnyScopedEffects) {
    local("row");auto r=request();r.full_rows={person("row","remote")};r.initial_row_grants={{"TestPerson","row"}};
    const auto before=owner->db().query("SELECT * FROM TestPerson");const auto audit=owner->db().query("SELECT * FROM AuditLog");
    refused(run(r));EXPECT_EQ(owner->db().query("SELECT * FROM TestPerson"),before);
    add_grants(r,{"TestPerson","row"},outcome::unknown);refused(run(r));
    r.pending[0].outcome=outcome::policy_only;refused(run(r));
    EXPECT_EQ(owner->db().query("SELECT * FROM TestPerson"),before);EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),audit);
    EXPECT_EQ(number("SELECT disabled FROM _SyncControl WHERE id=1"),0);
}

TEST_F(ScopedRecoveryInstall, UnownedCollisionAndCrossScopeOverlapRefuse) {
    local("collision");clear_history();auto r=request();r.full_rows={person("collision","remote")};refused(run(r));
    r.initial_row_grants={{"TestPerson","collision"}};committed(run(r));
    auto alias=request();alias.binding.channel="other";alias.binding.scope="other";alias.full_rows=r.full_rows;alias.initial_row_grants=r.initial_row_grants;
    refused(run(alias));EXPECT_EQ(name("collision"),"remote");
}

TEST_F(ScopedRecoveryInstall, SameHeadRefreshAndExactRetryDoNotEraseANewLocalEdit) {
    auto one=request();one.full_rows={person("row","one")};committed(run(one));
    auto two=next(one);two.full_rows={person("row","two")};committed(run(two));
    EXPECT_EQ(snapshot(two).revision,2);EXPECT_EQ(snapshot(two).frontier.position,10);
    owner->db().execute("UPDATE TestPerson SET name='after-retry' WHERE globalId='row'");
    const auto audit=owner->db().query("SELECT * FROM AuditLog");auto result=run(two);committed(result);
    ASSERT_TRUE(result.installation);EXPECT_EQ(result.installation->disposition,receive_install_disposition::already_installed);
    EXPECT_EQ(name("row"),"after-retry");EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),audit);
    refused(run(one));EXPECT_EQ(name("row"),"after-retry");
}

TEST_F(ScopedRecoveryInstall, LatestNoHistoryOnlyDoesNotRestoreUnrelatedCurrentColumns) {
    owner->document_schema();owner->db().execute("INSERT INTO RecoveryDocument(globalId,body,title,bytes) VALUES('doc','old','old-title',X'0001')");clear_history();
    owner->db().execute("UPDATE RecoveryDocument SET body='latest-owned' WHERE globalId='doc'");
    auto r=request();r.model_tables={"RecoveryDocument"};r.full_rows={{{"RecoveryDocument","doc"},
        {{"globalId",std::string("doc")},{"body",std::string("remote-body")},{"title",std::string("remote-title")},{"bytes",std::vector<uint8_t>{9,0,8}}}};
    add_grants(r,{"RecoveryDocument","doc"});committed(run(r));
    const auto row=owner->db().query("SELECT body,title,bytes FROM RecoveryDocument").at(0);
    EXPECT_EQ(std::get<std::string>(row.at("body")),"latest-owned");EXPECT_EQ(std::get<std::string>(row.at("title")),"remote-title");
    EXPECT_EQ(std::get<std::vector<uint8_t>>(row.at("bytes")),(std::vector<uint8_t>{9,0,8}));
}

TEST_F(ScopedRecoveryInstall, DeleteInsertSequenceAndSynthesizedInsertKeepExistingPolicy) {
    local("row","old");clear_history();owner->db().execute("DELETE FROM TestPerson WHERE globalId='row'");local("row","reborn",11);
    auto r=request();r.full_rows={person("row","canonical",2)};add_grants(r,{"TestPerson","row"});committed(run(r));
    EXPECT_EQ(name("row"),"reborn");EXPECT_EQ(number("SELECT age FROM TestPerson WHERE globalId='row'"),11);
    owner=std::make_shared<ScopedOwner>();local("synthetic","old");owner->db().execute("UPDATE AuditLog SET synthesized=1");
    auto s=request();s.full_rows={person("synthetic","canonical",22)};add_grants(s,{"TestPerson","synthetic"});committed(run(s));
    EXPECT_EQ(name("synthetic"),"canonical");
}

TEST_F(ScopedRecoveryInstall, ActualFilterRemovalMarkerIsNotADeleteOverlay) {
    local("filtered");clear_history();ASSERT_EQ(owner->delete_rows_no_relay("TestPerson",{"filtered"}),1);
    auto r=request();r.full_rows={person("filtered","still-on-source")};add_grants(r,{"TestPerson","filtered"});
    const auto before=owner->db().query("SELECT * FROM AuditLog");committed(run(r));EXPECT_EQ(name("filtered"),"still-on-source");
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),before);EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state"),0);
}

TEST_F(ScopedRecoveryInstall, ExactNulTextAndBlobAreBoundAndVerifiedWithoutCStringTruncation) {
    owner->document_schema();auto r=request();r.model_tables={"RecoveryDocument"};
    const std::string text("a\0b",3);const std::vector<uint8_t> bytes{0,4,0,8};
    r.full_rows={{{"RecoveryDocument","doc"},{{"globalId",std::string("doc")},{"body",text},{"title",std::string("title")},{"bytes",bytes}}}};
    committed(run(r));
    auto checked=recovery_writer_access::install(owner,[&](auto&){
        const auto captured=capture_recovery_rows(*owner,{{"RecoveryDocument",{"doc"}}},limits.capture);
        ASSERT_EQ(captured.current_rows.size(),1u);const auto& row=captured.current_rows[0];const auto& table=captured.tables[0];
        for(size_t i=0;i<table.columns.size();++i){if(table.columns[i].name=="body")EXPECT_EQ(std::get<std::string>(row.values[i]),text);
            if(table.columns[i].name=="bytes")EXPECT_EQ(std::get<std::vector<uint8_t>>(row.values[i]),bytes);}
    });EXPECT_EQ(checked.state,state::committed);
}

TEST_F(ScopedRecoveryInstall, RealOrdinaryLinksInstallAndDisappearWithTheirScopedEndpoint) {
    owner->ensure_link_table("RecoveryLinks","TestPerson","TestDog");
    auto r=request();r.model_tables.push_back("TestDog");r.relations={{"RecoveryLinks","TestPerson","TestDog"}};r.scoped_link_tables={"RecoveryLinks"};
    r.full_rows={person("person","parent"),{{"TestDog","dog"},{{"globalId",std::string("dog")},{"name",std::string("pet")},{"weight",4.5},{"is_good_boy",int64_t{1}}}},
        {{"RecoveryLinks","link"},{{"globalId",std::string("link")},{"lhs",std::string("person")},{"rhs",std::string("dog")}}}};
    committed(run(r));EXPECT_EQ(number("SELECT COUNT(*) FROM RecoveryLinks WHERE lhs='person' AND rhs='dog'"),1);
    auto empty=next(r);empty.full_rows.clear();committed(run(empty));
    EXPECT_EQ(number("SELECT COUNT(*) FROM RecoveryLinks"),0);EXPECT_EQ(number("SELECT COUNT(*) FROM TestDog"),0);
    EXPECT_EQ(rows("person"),0);EXPECT_EQ(number("SELECT COUNT(*) FROM AuditLog"),0);
}

TEST_F(ScopedRecoveryInstall, OutsideScopeReferenceRefusesDeletionWithoutTouchingItsIntent) {
    owner->ensure_link_table("RecoveryLinks","TestPerson","TestDog");auto r=request();r.model_tables.push_back("TestDog");
    r.relations={{"RecoveryLinks","TestPerson","TestDog"}};r.scoped_link_tables={"RecoveryLinks"};
    r.full_rows={person("person"),{{"TestDog","dog"},{{"globalId",std::string("dog")},{"name",std::string("pet")},{"weight",1.0},{"is_good_boy",int64_t{1}}}}};
    committed(run(r));owner->db().execute("INSERT INTO RecoveryLinks(lhs,rhs,globalId) VALUES('person','dog','local-link')");
    const auto audit=owner->db().query("SELECT * FROM AuditLog");auto remove=next(r);remove.full_rows.clear();refused(run(remove));
    EXPECT_EQ(rows("person"),1);EXPECT_EQ(number("SELECT COUNT(*) FROM RecoveryLinks"),1);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),audit);EXPECT_EQ(snapshot(r).revision,1);
}

TEST_F(ScopedRecoveryInstall, MissingRelationDescriptorAndDanglingFinalEndpointRefuse) {
    owner->ensure_link_table("RecoveryLinks","TestPerson","TestDog");auto r=request();r.full_rows={person("person")};refused(run(r));
    r.relations={{"RecoveryLinks","TestPerson","TestDog"}};r.scoped_link_tables={"RecoveryLinks"};
    r.full_rows.push_back({{"RecoveryLinks","link"},{{"globalId",std::string("link")},{"lhs",std::string("person")},{"rhs",std::string("absent")}}});
    refused(run(r));EXPECT_EQ(rows("person"),0);EXPECT_EQ(number("SELECT COUNT(*) FROM RecoveryLinks"),0);
}

TEST_F(ScopedRecoveryInstall, IgnoredModelAndReceiptWritesRollbackAllStateThenRetryWorks) {
    auto r=request();r.full_rows={person("row")};
    owner->db().execute("CREATE TRIGGER ignore_recovery BEFORE INSERT ON TestPerson BEGIN SELECT RAISE(IGNORE); END");
    refused(run(r));EXPECT_EQ(rows("row"),0);EXPECT_EQ(number("SELECT disabled FROM _SyncControl WHERE id=1"),0);
    owner->db().execute("DROP TRIGGER ignore_recovery");committed(run(r));
    owner->db().execute("UPDATE TestPerson SET name='pending' WHERE globalId='row'");
    auto next_r=next(r);next_r.full_rows={person("row","canonical-after")};add_grants(next_r,{"TestPerson","row"},outcome::committed_effect);
    owner->db().execute("CREATE TRIGGER ignore_receipt BEFORE INSERT ON _lattice_sync_state BEGIN SELECT RAISE(IGNORE); END");
    refused(run(next_r));EXPECT_EQ(name("row"),"pending");EXPECT_EQ(snapshot(r).revision,1);
    owner->db().execute("DROP TRIGGER ignore_receipt");committed(run(next_r));EXPECT_EQ(name("row"),"canonical-after");
}

TEST_F(ScopedRecoveryInstall, IgnoredMembershipAndCounterWritesCannotPublishPartialInstall) {
    auto r=request();r.full_rows={person("row")};committed(run(r));
    auto n=next(r);n.full_rows.push_back(person("second"));
    owner->db().execute("CREATE TRIGGER ignore_member BEFORE INSERT ON _lattice_recovery_member BEGIN SELECT RAISE(IGNORE); END");
    refused(run(n));EXPECT_EQ(rows("second"),0);EXPECT_EQ(snapshot(r).revision,1);owner->db().execute("DROP TRIGGER ignore_member");
    owner->db().execute("CREATE TRIGGER ignore_counter BEFORE UPDATE ON _lattice_recovery_scope_config BEGIN SELECT RAISE(IGNORE); END");
    refused(run(n));EXPECT_EQ(rows("second"),0);EXPECT_EQ(snapshot(r).revision,1);owner->db().execute("DROP TRIGGER ignore_counter");
    committed(run(n));EXPECT_EQ(rows("second"),1);
}

TEST_F(ScopedRecoveryInstall, FiniteBudgetsAndCorruptCountersRefuseWithoutModelEffects) {
    auto r=request();r.full_rows={person("one"),person("two")};auto original=limits;
    limits.targets=1;refused(run(r));EXPECT_EQ(rows("one"),0);limits=original;
    limits.members=1;refused(run(r));EXPECT_EQ(rows("one"),0);limits=original;
    limits.field_bytes=3;refused(run(r));EXPECT_EQ(rows("one"),0);limits=original;
    r.full_rows.resize(1);committed(run(r));owner->db().execute("UPDATE _lattice_recovery_scope_config SET members=members+1");
    auto n=next(r);n.full_rows={person("two")};refused(run(n));EXPECT_EQ(rows("one"),1);EXPECT_EQ(rows("two"),0);
}

TEST_F(ScopedRecoveryInstall, ExistingExplicitOrRawTransactionIsNeverJoined) {
    auto r=request();r.full_rows={person("row")};owner->begin_transaction();refused(run(r));owner->rollback();
    owner->db().begin_transaction();refused(run(r));owner->db().rollback();EXPECT_EQ(rows("row"),0);committed(run(r));
}

TEST_F(ScopedRecoveryInstall, PostCommitObserverErrorRetainsInstalledIdentityAndExactRetry) {
    auto r=request();r.full_rows={person("row")};
    auto token=owner->add_table_observer("TestPerson",[](const auto&){throw std::runtime_error("observer failed");});
    auto result=run(r);EXPECT_EQ(result.transaction.state,state::committed);EXPECT_TRUE(result.transaction.postcommit_error);
    ASSERT_TRUE(result.installation);EXPECT_EQ(result.installation->revision,1);EXPECT_EQ(rows("row"),1);
    owner->remove_table_observer("TestPerson",token);auto retry=run(r);committed(retry);
    ASSERT_TRUE(retry.installation);EXPECT_EQ(retry.installation->disposition,receive_install_disposition::already_installed);
}

TEST_F(ScopedRecoveryInstall, ReopenRetainsScopeMembershipAndSameHeadIdentity) {
    TempDB path("recovery-scope-reopen");owner=std::make_shared<ScopedOwner>(path.str());
    auto r=request();r.full_rows={person("row","before-close")};committed(run(r));owner->close();owner.reset();
    owner=std::make_shared<ScopedOwner>(path.str());auto n=next(r);n.full_rows={person("row","after-reopen")};committed(run(n));
    EXPECT_EQ(name("row"),"after-reopen");EXPECT_EQ(snapshot(n).revision,2);owner->close();owner.reset();
}

TEST_F(ScopedRecoveryInstall, TargetFilteredCaptureSkipsUnrelatedUnsupportedPendingSchema) {
    owner->db().execute("INSERT INTO AuditLog(globalId,tableName,operation,rowId,globalRowId,changedFields,changedFieldsNames,isSynchronized) "
        "VALUES('outside-intent','OutsideUnsupported','INSERT',0,'outside','{}','[]',0)");
    const auto unchanged=owner->db().query("SELECT * FROM AuditLog");auto r=request();r.full_rows={person("own")};committed(run(r));
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),unchanged);
    auto tx=recovery_writer_access::install(owner,[&](auto&){
        EXPECT_THROW(capture_pending_outbox(*owner,"channel",limits.capture),recovery_outbox_error);
        EXPECT_TRUE(capture_pending_outbox_for_targets(*owner,"channel",{{"TestPerson","own"}},limits.capture).audit.empty());
    });EXPECT_EQ(tx.state,state::committed);
}

TEST_F(ScopedRecoveryInstall, ExplicitPendingRemoteRelayedIdentityUsesSameBoundReceiptRules) {
    local("relayed","old");owner->db().execute("UPDATE AuditLog SET isFromRemote=1,isSynchronized=1");
    owner->db().execute("INSERT INTO _lattice_sync_state SELECT id,'channel',0 FROM AuditLog");
    owner->db().execute("INSERT INTO _lattice_sync_state SELECT id,'other',0 FROM AuditLog");
    const auto original=owner->db().query("SELECT * FROM AuditLog");auto r=request();r.full_rows={person("relayed","canonical")};
    add_grants(r,{"TestPerson","relayed"},outcome::committed_effect);committed(run(r));
    EXPECT_EQ(name("relayed"),"canonical");EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),original);
    EXPECT_EQ(number("SELECT is_synchronized FROM _lattice_sync_state WHERE sync_id='channel'"),1);
    EXPECT_EQ(number("SELECT is_synchronized FROM _lattice_sync_state WHERE sync_id='other'"),0);
}

TEST_F(ScopedRecoveryInstall, CommitRefusalRollsBackRowsMembershipRevisionAndReceiptTogether) {
    local("row","old");auto r=request();r.full_rows={person("row","canonical")};
    add_grants(r,{"TestPerson","row"},outcome::committed_effect);
    const auto original=owner->db().query("SELECT * FROM AuditLog");
    struct DenyCommit {
        sqlite3* db;
        explicit DenyCommit(sqlite3* d):db(d) {
            sqlite3_set_authorizer(db,[](void*,int action,const char* one,const char*,const char*,const char*) {
                return action==SQLITE_TRANSACTION&&one&&std::string_view(one)=="COMMIT"?SQLITE_DENY:SQLITE_OK;
            },nullptr);
        }
        ~DenyCommit(){sqlite3_set_authorizer(db,nullptr,nullptr);}
    };
    {DenyCommit deny(owner->db().handle());auto result=run(r);refused(result);EXPECT_EQ(result.transaction.state,state::rolled_back);}
    EXPECT_EQ(name("row"),"old");EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),original);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state"),0);EXPECT_EQ(number("SELECT disabled FROM _SyncControl WHERE id=1"),0);
    EXPECT_EQ(number("SELECT COUNT(*) FROM sqlite_schema WHERE name='_lattice_recovery_scope'"),0);
    committed(run(r));EXPECT_EQ(name("row"),"canonical");
}

TEST_F(ScopedRecoveryInstall, RegisteredGeographicClosureRefusesBeforeScopeAdmission) {
    auto r=request();r.model_tables={"TestPlace"};refused(run(r));
    EXPECT_EQ(number("SELECT COUNT(*) FROM sqlite_schema WHERE name='_lattice_recovery_scope'"),0);
}
