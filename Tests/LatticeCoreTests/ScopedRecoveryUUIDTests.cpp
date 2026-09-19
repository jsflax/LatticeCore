#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/scoped_recovery_install.hpp"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"

namespace {
using namespace lattice::detail;
using state=recovery_install_state;
using outcome=recovery_pending_outcome;
const std::string person_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const std::string dog_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const std::string link_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc";
const std::string other_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd";
std::string upper_uuid(std::string value) {for(auto& c:value)if(c>='a'&&c<='f')c-=32;return value;}
lattice::configuration uuid_configuration(const std::string& path) {
    lattice::configuration c(path);c.audit_retention_seconds=0;return c;
}
class UUIDOwner:public lattice::lattice_db {
public:
    explicit UUIDOwner(const std::string& path=":memory:"):lattice_db(uuid_configuration(path)) {}
    void document_schema() {
        lattice::model_schema s;s.table_name="UUIDDocument";
        lattice::property_descriptor body;body.name="body";body.type=lattice::column_type::text;body.no_history=true;
        lattice::property_descriptor title;title.name="title";title.type=lattice::column_type::text;
        s.properties={body,title};create_model_table_public(s);
    }
};
class ScopedRecoveryUUID:public ::testing::Test {
protected:
    std::shared_ptr<UUIDOwner> owner=std::make_shared<UUIDOwner>();
    scoped_recovery_limits limits{{8,4096,65536},{512,512,32,64,512,1000000,65536,16000000},8,1024,1048576,512,512,1000000,65536,16000000};
    scoped_recovery_request request() {
        scoped_recovery_request r;r.binding={"channel","authority","source","epoch","scope","schema"};
        r.identity={1,0,{},10,receive_install_mode::full,"request1","receipt1","content1","manifest1"};
        r.model_tables={"TestPerson"};r.identity_mode=recovery_identity_mode::uuid;return r;
    }
    scoped_recovery_request next(const scoped_recovery_request& old) {
        auto r=old;r.supersede=old.identity;r.identity.sequence++;r.identity.expected_revision++;
        r.identity.base={receive_frontier_kind::position,old.identity.head};
        r.identity.request_digest+="n";r.identity.receipt_digest+="n";r.identity.content_digest+="n";r.identity.manifest_digest+="n";
        r.initial_row_grants.clear();r.pending.clear();return r;
    }
    recovery_full_row person(const std::string& wire,const std::string& actual,const std::string& name="remote") {
        return {{"TestPerson",wire},{{"globalId",actual},{"name",name},{"age",int64_t{20}},{"email",nullptr}}};
    }
    void local(const std::string& id,const std::string& name="local") {
        owner->db().execute("INSERT INTO TestPerson(globalId,name,age,email) VALUES(?,?,1,NULL)",{id,name});
    }
    void clear_history() {owner->db().execute("DELETE FROM _lattice_sync_state");owner->db().execute("DELETE FROM AuditLog");}
    void add_grants(scoped_recovery_request& r,const recovery_row_key& target,outcome value=outcome::not_committed) {
        for(const auto& a:owner->db().query("SELECT globalId FROM AuditLog WHERE tableName=? AND globalRowId=? COLLATE NOCASE ORDER BY id",{target.table,target.global_id}))
            r.pending.push_back({canonical_writer_adapter::uuid_key(std::get<std::string>(a.at("globalId"))),target,value});
    }
    scoped_recovery_result run(const scoped_recovery_request& r) {return install_scoped_recovery(owner,r,limits);}
    void refused(const scoped_recovery_result& r,const char* expected=nullptr) {
        EXPECT_NE(r.transaction.state,state::committed);EXPECT_TRUE(r.transaction.primary_error);EXPECT_FALSE(r.installation);
        if(expected&&r.transaction.primary_error)try {std::rethrow_exception(r.transaction.primary_error);}
        catch(const std::exception& e){EXPECT_NE(std::string(e.what()).find(expected),std::string::npos)<<e.what();}
        catch(...){ADD_FAILURE()<<"non-standard refusal while expecting "<<expected;}
    }
    int64_t number(const std::string& sql) {return std::get<int64_t>(owner->db().query(sql).at(0).begin()->second);}
    std::string text(const std::string& sql) {return std::get<std::string>(owner->db().query(sql).at(0).begin()->second);}
    void links(scoped_recovery_request& r) {
        owner->ensure_link_table("_UUIDLinks","TestPerson","TestDog");r.model_tables.push_back("TestDog");
        r.relations={{"_UUIDLinks","TestPerson","TestDog"}};r.scoped_link_tables={"_UUIDLinks"};
    }
    recovery_full_row dog(const std::string& actual) {
        return {{"TestDog",dog_id},{{"globalId",actual},{"name",std::string("pet")},{"weight",4.5},{"is_good_boy",int64_t{1}}}};
    }
    recovery_full_row link(const std::string& wire,const std::string& actual,const std::string& lhs,const std::string& rhs) {
        return {{"_UUIDLinks",wire},{{"globalId",actual},{"lhs",lhs},{"rhs",rhs}}};
    }
};
}

TEST_F(ScopedRecoveryUUID, ExistingSpellingStablePKAndHeldObserverSurviveOnFileAndMemory) {
    for(const bool file:{false,true}) {
        TempDB path("uuid-held");owner=std::make_shared<UUIDOwner>(file?path.str():":memory:");
        auto held=owner->add(TestPerson{"before",1,std::nullopt});
        const auto actual=held.global_id();const auto canonical=canonical_writer_adapter::uuid_key(actual);const auto pk=held.id();
        clear_history();auto r=request();r.full_rows={person(canonical,actual==canonical?upper_uuid(canonical):canonical,"installed")};
        r.initial_row_grants={{"TestPerson",canonical}};const auto original=r.full_rows[0].values;
        int callbacks=0;auto token=held.observe([&](lattice::object_change<TestPerson>&){++callbacks;EXPECT_EQ(held.name.detach(),"installed");});
        ASSERT_EQ(run(r).transaction.state,state::committed);
        EXPECT_EQ(held.id(),pk);EXPECT_EQ(held.global_id(),actual);EXPECT_EQ(held.name.detach(),"installed");EXPECT_GT(callbacks,0);
        EXPECT_EQ(text("SELECT globalId FROM TestPerson"),actual);EXPECT_EQ(r.full_rows[0].values,original);
        EXPECT_EQ(r.identity.content_digest,"content1");EXPECT_EQ(r.identity.manifest_digest,"manifest1");
        EXPECT_EQ(number("SELECT COUNT(*) FROM AuditLog"),0);token.invalidate();owner->close();owner.reset();
    }
}

TEST_F(ScopedRecoveryUUID, NewRowUsesFullPayloadSpellingWithoutChangingWireValues) {
    auto r=request();r.full_rows={person(person_id,upper_uuid(person_id))};const auto input=r.full_rows[0].values;
    ASSERT_EQ(run(r).transaction.state,state::committed);
    EXPECT_EQ(text("SELECT globalId FROM TestPerson"),upper_uuid(person_id));
    EXPECT_EQ(text("SELECT global_id FROM _lattice_recovery_member"),upper_uuid(person_id));
    EXPECT_EQ(r.full_rows[0].values,input);EXPECT_EQ(r.full_rows[0].key.global_id,person_id);
}

TEST_F(ScopedRecoveryUUID, CanonicalReceiptSettlesActualAuditIdWithoutRewritingOriginalBytes) {
    local(upper_uuid(person_id));owner->db().execute("UPDATE AuditLog SET globalId=?",{upper_uuid(other_id)});
    owner->db().execute("INSERT INTO _lattice_sync_state SELECT id,'other-channel',0 FROM AuditLog");
    const auto original=owner->db().query("SELECT * FROM AuditLog ORDER BY id");
    auto r=request();r.full_rows={person(person_id,person_id,"later-canonical")};add_grants(r,{"TestPerson",person_id},outcome::committed_noop);
    ASSERT_EQ(r.pending.size(),1u);ASSERT_EQ(run(r).transaction.state,state::committed);
    EXPECT_EQ(text("SELECT globalId FROM TestPerson"),upper_uuid(person_id));EXPECT_EQ(text("SELECT name FROM TestPerson"),"later-canonical");
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog ORDER BY id"),original);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state WHERE sync_id='channel' AND is_synchronized=1"),1);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state WHERE sync_id='other-channel' AND is_synchronized=0"),1);
    // A later H-bound receipt may point to the already-settled original using
    // canonical spelling. This exercises the indexed retained-receipt branch.
    auto n=next(r);n.pending=r.pending;ASSERT_EQ(run(n).transaction.state,state::committed);
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog ORDER BY id"),original);
}

TEST_F(ScopedRecoveryUUID, NeverSentInsertMapsDifferentAuditTargetSpellingAndPreservesPK) {
    local(upper_uuid(person_id),"unsent");const auto pk=number("SELECT id FROM TestPerson");
    owner->db().execute("UPDATE AuditLog SET globalId=?,globalRowId=lower(globalRowId)",{upper_uuid(other_id)});
    const auto original=owner->db().query("SELECT * FROM AuditLog");auto r=request();add_grants(r,{"TestPerson",person_id});
    ASSERT_EQ(run(r).transaction.state,state::committed);
    EXPECT_EQ(text("SELECT globalId FROM TestPerson"),upper_uuid(person_id));EXPECT_EQ(text("SELECT name FROM TestPerson"),"unsent");
    EXPECT_EQ(number("SELECT id FROM TestPerson"),pk);EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),original);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state"),0);
}

TEST_F(ScopedRecoveryUUID, CoveredLostAckInsertThenDeleteDoesNotResurrectOrRewriteAudit) {
    local(upper_uuid(person_id));owner->db().execute("UPDATE AuditLog SET globalId=?",{upper_uuid(other_id)});
    const auto original=owner->db().query("SELECT * FROM AuditLog");auto r=request();add_grants(r,{"TestPerson",person_id},outcome::committed_effect);
    ASSERT_EQ(run(r).transaction.state,state::committed);EXPECT_EQ(number("SELECT COUNT(*) FROM TestPerson"),0);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_recovery_member"),0);EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),original);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state WHERE sync_id='channel' AND is_synchronized=1"),1);
}

TEST_F(ScopedRecoveryUUID, LatestNoHistoryOverlayUsesOwnedRowDespiteCanonicalAuditTarget) {
    owner->document_schema();owner->db().execute("INSERT INTO UUIDDocument(globalId,body,title) VALUES(?,'old','old')",{upper_uuid(person_id)});clear_history();
    owner->db().execute("UPDATE UUIDDocument SET body='latest' WHERE globalId=?",{person_id});
    owner->db().execute("UPDATE AuditLog SET globalRowId=lower(globalRowId)");
    const auto audit=owner->db().query("SELECT * FROM AuditLog");auto r=request();r.model_tables={"UUIDDocument"};
    r.full_rows={{{"UUIDDocument",person_id},{{"globalId",person_id},{"body",std::string("remote")},{"title",std::string("canonical-title")}}}};
    add_grants(r,{"UUIDDocument",person_id});ASSERT_EQ(run(r).transaction.state,state::committed);
    EXPECT_EQ(text("SELECT body FROM UUIDDocument"),"latest");EXPECT_EQ(text("SELECT title FROM UUIDDocument"),"canonical-title");
    EXPECT_EQ(text("SELECT globalId FROM UUIDDocument"),upper_uuid(person_id));EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),audit);
}

TEST_F(ScopedRecoveryUUID, LinkIdentityAndEndpointsUseExistingModelSpelling) {
    auto r=request();links(r);local(upper_uuid(person_id));
    owner->db().execute("INSERT INTO TestDog(globalId,name,weight,is_good_boy) VALUES(?,'old-pet',1,1)",{upper_uuid(dog_id)});
    owner->db().execute("INSERT INTO _UUIDLinks(globalId,lhs,rhs) VALUES(?,?,?)",{upper_uuid(link_id),upper_uuid(person_id),upper_uuid(dog_id)});clear_history();
    const auto pk=number("SELECT id FROM TestPerson");r.initial_row_grants={{"TestPerson",person_id},{"TestDog",dog_id},{"_UUIDLinks",link_id}};
    r.full_rows={person(person_id,person_id),dog(dog_id),link(link_id,link_id,person_id,dog_id)};const auto original=r.full_rows.back().values;
    ASSERT_EQ(run(r).transaction.state,state::committed);
    EXPECT_EQ(text("SELECT globalId FROM _UUIDLinks"),upper_uuid(link_id));EXPECT_EQ(text("SELECT lhs FROM _UUIDLinks"),upper_uuid(person_id));
    EXPECT_EQ(text("SELECT rhs FROM _UUIDLinks"),upper_uuid(dog_id));EXPECT_EQ(number("SELECT id FROM TestPerson"),pk);
    EXPECT_EQ(r.full_rows.back().values,original);EXPECT_EQ(number("SELECT COUNT(*) FROM AuditLog"),0);
}

TEST_F(ScopedRecoveryUUID, CaseDifferentOutsideLinkPairCollisionRefusesBeforeEffects) {
    auto r=request();links(r);local(upper_uuid(person_id));
    owner->db().execute("INSERT INTO TestDog(globalId,name,weight,is_good_boy) VALUES(?,'pet',1,1)",{upper_uuid(dog_id)});
    // The generated pair key is BINARY. This lower-case pair must nevertheless
    // block installing a second UUID-equivalent pair with upper-case endpoints.
    owner->db().execute("INSERT INTO _UUIDLinks(globalId,lhs,rhs) VALUES(?,?,?)",{other_id,person_id,dog_id});clear_history();
    const auto before=owner->db().query("SELECT * FROM TestPerson");const auto preserved=owner->db().query("SELECT * FROM _UUIDLinks");
    r.initial_row_grants={{"TestPerson",person_id},{"TestDog",dog_id}};
    r.full_rows={person(person_id,person_id),dog(dog_id),link(link_id,link_id,person_id,dog_id)};
    refused(run(r),"link pair overlaps a preserved row");EXPECT_EQ(owner->db().query("SELECT * FROM TestPerson"),before);EXPECT_EQ(owner->db().query("SELECT * FROM _UUIDLinks"),preserved);
}

TEST_F(ScopedRecoveryUUID, CaseDifferentOutsideReferencePreventsScopedModelDeletion) {
    auto r=request();links(r);r.full_rows={person(person_id,upper_uuid(person_id)),dog(upper_uuid(dog_id))};
    ASSERT_EQ(run(r).transaction.state,state::committed);
    owner->db().execute("INSERT INTO _UUIDLinks(globalId,lhs,rhs) VALUES(?,?,?)",{upper_uuid(other_id),person_id,dog_id});
    const auto audit=owner->db().query("SELECT * FROM AuditLog");auto n=next(r);n.full_rows.clear();refused(run(n),"orphan a preserved outside-scope link");
    EXPECT_EQ(number("SELECT COUNT(*) FROM TestPerson"),1);EXPECT_EQ(number("SELECT COUNT(*) FROM TestDog"),1);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _UUIDLinks"),1);EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),audit);
}

TEST_F(ScopedRecoveryUUID, OtherScopeAndNonUUIDLocalOnlyRowsRemainUntouched) {
    auto other=request();other.binding.channel="other";other.binding.scope="other-scope";other.full_rows={person(other_id,upper_uuid(other_id),"second")};
    ASSERT_EQ(run(other).transaction.state,state::committed);local("local-only","private");
    owner->db().execute("UPDATE TestPerson SET age=91 WHERE globalId=?",{other_id});const auto audit=owner->db().query("SELECT * FROM AuditLog ORDER BY id");
    auto r=request();r.full_rows={person(person_id,person_id)};ASSERT_EQ(run(r).transaction.state,state::committed);
    auto n=next(r);n.full_rows.clear();ASSERT_EQ(run(n).transaction.state,state::committed);
    EXPECT_EQ(number("SELECT COUNT(*) FROM TestPerson"),2);EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog ORDER BY id"),audit);
    auto overlap=next(n);overlap.full_rows={person(other_id,other_id,"bad")};refused(run(overlap));
    EXPECT_EQ(text("SELECT name FROM TestPerson WHERE name='second'"),"second");EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state"),0);
}

TEST_F(ScopedRecoveryUUID, DuplicateCaseAliasesInRowsGrantsAndReceiptsRefuse) {
    auto r=request();r.full_rows={person(person_id,person_id),person(upper_uuid(person_id),upper_uuid(person_id))};refused(run(r));
    r.full_rows.resize(1);r.initial_row_grants={{"TestPerson",person_id},{"TestPerson",upper_uuid(person_id)}};refused(run(r));
    r.initial_row_grants.clear();local(upper_uuid(person_id));add_grants(r,{"TestPerson",person_id},outcome::committed_effect);
    ASSERT_EQ(r.pending.size(),1u);auto duplicate=r.pending.front();duplicate.audit_global_id=upper_uuid(duplicate.audit_global_id);r.pending.push_back(duplicate);
    const auto before=owner->db().query("SELECT * FROM TestPerson");refused(run(r));EXPECT_EQ(owner->db().query("SELECT * FROM TestPerson"),before);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state"),0);
}

TEST_F(ScopedRecoveryUUID, MalformedUUIDAndPayloadDisagreementRefuseWithoutRows) {
    for(const auto& bad:std::vector<std::string>{"row","aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa","gaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",person_id+std::string(1,'\0')}) {
        auto r=request();r.full_rows={person(bad,bad)};refused(run(r));EXPECT_EQ(number("SELECT COUNT(*) FROM TestPerson"),0);
    }
    auto r=request();r.full_rows={person(person_id,other_id)};refused(run(r));
    r.full_rows={person(person_id,"not-a-uuid")};refused(run(r));
    r.full_rows={person(person_id,person_id)};r.pending={{"not-a-uuid",{"TestPerson",person_id},outcome::committed_effect}};refused(run(r));
    EXPECT_EQ(number("SELECT COUNT(*) FROM TestPerson"),0);
}

TEST_F(ScopedRecoveryUUID, PartialExpressionMulticolumnAndBinaryUniqueKeysCannotAuthorizeUUIDMode) {
    for(const auto& extra:std::vector<std::string>{"", "CREATE UNIQUE INDEX _uuid_partial ON _UUIDShape(globalId COLLATE NOCASE) WHERE name='admitted'",
        "CREATE UNIQUE INDEX _uuid_expression ON _UUIDShape(lower(globalId) COLLATE NOCASE)",
        "CREATE UNIQUE INDEX _uuid_multi ON _UUIDShape(globalId COLLATE NOCASE,name)"}) {
        owner=std::make_shared<UUIDOwner>();owner->db().execute("CREATE TABLE _UUIDShape(id INTEGER PRIMARY KEY,globalId TEXT UNIQUE,name TEXT)");
        owner->db().execute("INSERT INTO _lattice_meta(key,value) VALUES('trigger_flags:_UUIDShape','')");
        if(!extra.empty())owner->db().execute(extra);
        owner->db().execute("INSERT INTO _UUIDShape VALUES(7,?,'old')",{upper_uuid(person_id)});
        if(extra.empty())owner->db().execute("INSERT INTO _UUIDShape VALUES(8,?,'collision')",{person_id});
        auto r=request();r.model_tables={"_UUIDShape"};r.initial_row_grants={{"_UUIDShape",person_id}};
        r.full_rows={{{"_UUIDShape",person_id},{{"globalId",person_id},{"name",std::string("new")}}}};
        const auto before=owner->db().query("SELECT * FROM _UUIDShape");refused(run(r),"complete NOCASE unique key");EXPECT_EQ(owner->db().query("SELECT * FROM _UUIDShape"),before);
    }
}

TEST_F(ScopedRecoveryUUID, CompleteNOCASEIndexMapsBinaryColumnThroughActualSpelling) {
    owner->db().execute("CREATE TABLE _UUIDShape(id INTEGER PRIMARY KEY,globalId TEXT UNIQUE,name TEXT)");
    owner->db().execute("INSERT INTO _lattice_meta(key,value) VALUES('trigger_flags:_UUIDShape','')");
    owner->db().execute("CREATE UNIQUE INDEX _uuid_complete ON _UUIDShape(globalId COLLATE NOCASE)");
    owner->db().execute("INSERT INTO _UUIDShape VALUES(17,?,'old')",{upper_uuid(person_id)});
    auto r=request();r.model_tables={"_UUIDShape"};r.initial_row_grants={{"_UUIDShape",person_id}};
    r.full_rows={{{"_UUIDShape",person_id},{{"globalId",person_id},{"name",std::string("new")}}}};
    ASSERT_EQ(run(r).transaction.state,state::committed);EXPECT_EQ(number("SELECT id FROM _UUIDShape"),17);
    EXPECT_EQ(text("SELECT globalId FROM _UUIDShape"),upper_uuid(person_id));EXPECT_EQ(text("SELECT name FROM _UUIDShape"),"new");
}

TEST_F(ScopedRecoveryUUID, ExactModeRetainsCaseRefusalAndModeCannotMigrateAcrossRevisions) {
    local(upper_uuid(person_id));clear_history();auto exact=request();exact.identity_mode=recovery_identity_mode::exact_string;
    exact.initial_row_grants={{"TestPerson",person_id}};exact.full_rows={person(person_id,person_id)};refused(run(exact));
    exact.initial_row_grants={{"TestPerson",upper_uuid(person_id)}};exact.full_rows={person(upper_uuid(person_id),upper_uuid(person_id))};
    ASSERT_EQ(run(exact).transaction.state,state::committed);auto uuid=next(exact);uuid.identity_mode=recovery_identity_mode::uuid;refused(run(uuid),"scope declaration changed");
    owner=std::make_shared<UUIDOwner>();auto first=request();first.full_rows={person(person_id,upper_uuid(person_id))};
    ASSERT_EQ(run(first).transaction.state,state::committed);auto opposite=next(first);opposite.identity_mode=recovery_identity_mode::exact_string;refused(run(opposite),"scope declaration changed");
}

TEST_F(ScopedRecoveryUUID, SameHeadRefreshAndExactRetryPreserveLaterLocalEdit) {
    auto one=request();one.full_rows={person(person_id,upper_uuid(person_id),"one")};ASSERT_EQ(run(one).transaction.state,state::committed);
    auto two=next(one);two.full_rows={person(person_id,person_id,"two")};ASSERT_EQ(run(two).transaction.state,state::committed);
    owner->db().execute("UPDATE TestPerson SET name='after' WHERE globalId=?",{person_id});const auto audit=owner->db().query("SELECT * FROM AuditLog");
    auto retry=run(two);ASSERT_EQ(retry.transaction.state,state::committed);ASSERT_TRUE(retry.installation);
    EXPECT_EQ(retry.installation->disposition,receive_install_disposition::already_installed);EXPECT_EQ(retry.installation->revision,2);
    EXPECT_EQ(text("SELECT name FROM TestPerson"),"after");EXPECT_EQ(text("SELECT globalId FROM TestPerson"),upper_uuid(person_id));
    EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),audit);refused(run(one));
}

TEST_F(ScopedRecoveryUUID, IgnoredReceiptWriteRollsBackMappedModelsMembershipAndInstallState) {
    local(upper_uuid(person_id));const auto before=owner->db().query("SELECT * FROM TestPerson");const auto audit=owner->db().query("SELECT * FROM AuditLog");
    owner->db().execute("CREATE TRIGGER _uuid_ignore_receipt BEFORE INSERT ON _lattice_sync_state WHEN NEW.sync_id='channel' BEGIN SELECT RAISE(IGNORE); END");
    auto r=request();r.full_rows={person(person_id,person_id,"canonical")};add_grants(r,{"TestPerson",person_id},outcome::committed_effect);
    refused(run(r));EXPECT_EQ(owner->db().query("SELECT * FROM TestPerson"),before);EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),audit);
    EXPECT_EQ(number("SELECT disabled FROM _SyncControl WHERE id=1"),0);EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state"),0);
    owner->db().execute("DROP TRIGGER _uuid_ignore_receipt");auto result=run(r);ASSERT_EQ(result.transaction.state,state::committed);ASSERT_TRUE(result.installation);
    EXPECT_EQ(result.installation->revision,1);EXPECT_EQ(text("SELECT globalId FROM TestPerson"),upper_uuid(person_id));
    EXPECT_EQ(text("SELECT name FROM TestPerson"),"canonical");EXPECT_EQ(owner->db().query("SELECT * FROM AuditLog"),audit);
}

TEST_F(ScopedRecoveryUUID, ReopenRetainsUUIDModeAndFiniteUnionRefusalLeavesPriorInstall) {
    TempDB path("uuid-reopen");owner=std::make_shared<UUIDOwner>(path.str());auto one=request();one.full_rows={person(person_id,upper_uuid(person_id))};
    ASSERT_EQ(run(one).transaction.state,state::committed);const auto pk=number("SELECT id FROM TestPerson");owner->close();owner.reset();
    owner=std::make_shared<UUIDOwner>(path.str());auto two=next(one);two.full_rows={person(person_id,person_id,"updated"),person(other_id,other_id)};
    limits.targets=1;refused(run(two),"target union limit exceeded");EXPECT_EQ(number("SELECT COUNT(*) FROM TestPerson"),1);EXPECT_EQ(number("SELECT id FROM TestPerson"),pk);
    limits.targets=512;two.full_rows.resize(1);ASSERT_EQ(run(two).transaction.state,state::committed);
    EXPECT_EQ(number("SELECT id FROM TestPerson"),pk);EXPECT_EQ(text("SELECT globalId FROM TestPerson"),upper_uuid(person_id));
    EXPECT_EQ(text("SELECT name FROM TestPerson"),"updated");owner->close();owner.reset();
}
