#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/scoped_recovery_install.hpp"
#include "../../Sources/LatticeCore/src/recovery_refresh.hpp"

namespace {
using namespace lattice::detail;
using state=recovery_install_state;
using outcome=recovery_pending_outcome;
using rows=std::vector<lattice::database::row_t>;
const std::string a="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const std::string b="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const std::string dog_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc";
const std::string link_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd";
std::string upper(std::string text) {for(auto& c:text)if(c>='a'&&c<='f')c-=32;return text;}
lattice::configuration delta_configuration(const std::string& path) {
    lattice::configuration c(path);c.audit_retention_seconds=0;return c;
}
class DeltaOwner:public lattice::lattice_db {
public:
    explicit DeltaOwner(const std::string& path=":memory:"):lattice_db(delta_configuration(path)) {}
    void document_schema() {
        lattice::model_schema s;s.table_name="DeltaDocument";
        lattice::property_descriptor body;body.name="body";body.type=lattice::column_type::text;body.no_history=true;
        lattice::property_descriptor title;title.name="title";title.type=lattice::column_type::text;
        s.properties={body,title};create_model_table_public(s);
    }
};
class ScopedRecoveryDelta:public ::testing::Test {
protected:
    std::shared_ptr<DeltaOwner> owner=std::make_shared<DeltaOwner>();
    scoped_recovery_limits limits{{8,4096,65536},{512,512,32,64,512,1000000,65536,16000000},8,1024,1048576,512,512,1000000,65536,16000000};
    scoped_recovery_request initial() {
        scoped_recovery_request r;r.binding={"channel","authority","source","epoch","scope","schema"};
        r.identity={1,0,{},10,receive_install_mode::full,"Q1","E1","C1","M1"};
        r.model_tables={"TestPerson"};r.identity_mode=recovery_identity_mode::uuid;return r;
    }
    scoped_recovery_request next(scoped_recovery_request r,receive_install_mode mode=receive_install_mode::delta) {
        r.supersede=r.identity;++r.identity.sequence;++r.identity.expected_revision;
        r.identity.base={receive_frontier_kind::position,r.identity.head};r.identity.mode=mode;
        r.identity.request_digest+="n";r.identity.receipt_digest+="n";
        r.identity.content_digest+="n";r.identity.manifest_digest+="n";
        r.full_rows.clear();r.pending.clear();r.initial_row_grants.clear();return r;
    }
    recovery_row_image person(const std::string& id,const std::string& name="canonical",int64_t age=20) {
        return {{"TestPerson",id},sync_recovery::row_values{{"globalId",id},{"name",name},{"age",age},{"email",nullptr}}};
    }
    recovery_row_image gone(const std::string& table,const std::string& id) {return {{table,id},std::nullopt};}
    scoped_recovery_result run(const scoped_recovery_request& r,const std::vector<recovery_row_image>& images={}) {
        return install_scoped_recovery_images(owner,r,images,limits);
    }
    bool committed(const scoped_recovery_result& result) {
        if(result.transaction.primary_error)try{std::rethrow_exception(result.transaction.primary_error);}
        catch(const std::exception& e){ADD_FAILURE()<<e.what();}
        EXPECT_EQ(result.transaction.state,state::committed);EXPECT_FALSE(result.transaction.cleanup_error);
        EXPECT_TRUE(result.installation);return result.transaction.state==state::committed && result.installation.has_value();
    }
    void refused(const scoped_recovery_result& result,const std::string& why) {
        EXPECT_NE(result.transaction.state,state::committed);EXPECT_FALSE(result.installation);
        ASSERT_TRUE(result.transaction.primary_error);
        try{std::rethrow_exception(result.transaction.primary_error);}
        catch(const std::exception& e){EXPECT_NE(std::string(e.what()).find(why),std::string::npos)<<e.what();}
        catch(...){ADD_FAILURE()<<"unexpected non-standard error";}
    }
    rows query(const std::string& sql) {return owner->db().query(sql);}
    rows people() {return query("SELECT * FROM TestPerson ORDER BY id");}
    rows members() {return query("SELECT * FROM _lattice_recovery_member ORDER BY table_name,global_id");}
    rows audit() {return query("SELECT * FROM AuditLog ORDER BY id");}
    std::vector<rows> snapshot() {
        return {people(),members(),audit(),query("SELECT * FROM _lattice_sync_state ORDER BY audit_entry_id,sync_id"),
            query("SELECT * FROM _lattice_install_channel ORDER BY channel"),query("SELECT * FROM _lattice_recovery_witness"),
            query("SELECT * FROM _lattice_recovery_scope_config"),query("SELECT * FROM _SyncControl")};
    }
    int64_t number(const std::string& sql) {return std::get<int64_t>(query(sql).at(0).begin()->second);}
    void grants(scoped_recovery_request& r,const recovery_row_key& target,outcome value=outcome::not_committed) {
        for(const auto& row:owner->db().query("SELECT globalId FROM AuditLog WHERE tableName=? AND globalRowId=? COLLATE NOCASE ORDER BY id",{target.table,target.global_id}))
            r.pending.push_back({std::get<std::string>(row.at("globalId")),target,value});
    }
    void links(scoped_recovery_request& r) {
        owner->ensure_link_table("_DeltaLinks","TestPerson","TestDog");r.model_tables.push_back("TestDog");
        r.relations={{"_DeltaLinks","TestPerson","TestDog"}};r.scoped_link_tables={"_DeltaLinks"};
    }
    std::vector<recovery_row_image> linked_rows() {
        return {person(upper(a)),{{"TestDog",upper(dog_id)},sync_recovery::row_values{
            {"globalId",upper(dog_id)},{"name",std::string("pet")},{"weight",4.5},{"is_good_boy",int64_t{1}}}},
            {{"_DeltaLinks",upper(link_id)},sync_recovery::row_values{
            {"globalId",upper(link_id)},{"lhs",a},{"rhs",dog_id}}}};
    }
};
}

TEST_F(ScopedRecoveryDelta, DeltaOmissionPreservesRowsPKsAndMembershipWhileFullOmissionDeletes) {
    for(const bool file:{false,true}) {
        TempDB path("delta-omission");owner=std::make_shared<DeltaOwner>(file?path.str():":memory:");
        auto first=initial();ASSERT_TRUE(committed(run(first,{person(a,"A"),person(b,"B")})));
        const auto before=people();const auto before_members=members();auto delta=next(first);
        ASSERT_TRUE(committed(run(delta,{person(a,"changed")})));
        const auto after=people();ASSERT_EQ(after.size(),2u);EXPECT_EQ(after[1],before[1]);
        EXPECT_EQ(after[0].at("id"),before[0].at("id"));EXPECT_EQ(members(),before_members);EXPECT_TRUE(audit().empty());
        auto full=next(delta,receive_install_mode::full);ASSERT_TRUE(committed(run(full,{person(a,"full")})));
        EXPECT_EQ(people().size(),1u);EXPECT_EQ(members().size(),1u);owner->close();owner.reset();
    }
}

TEST_F(ScopedRecoveryDelta, OmittedLocallyAbsentMemberAndPendingDeleteRemainOwned) {
    auto first=initial();ASSERT_TRUE(committed(run(first,{person(upper(a))})));
    const auto membership=members();owner->db().execute("DELETE FROM TestPerson WHERE globalId=?",{a});
    const auto original=audit();ASSERT_EQ(original.size(),1u);auto delta=next(first);grants(delta,{"TestPerson",a});
    ASSERT_TRUE(committed(run(delta)));EXPECT_TRUE(people().empty());EXPECT_EQ(members(),membership);EXPECT_EQ(audit(),original);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state"),0);
    // A later replacement base still overlays the pending DELETE, rather than
    // treating a canonical present row as cancellation of the original intent.
    auto rebase=next(delta);grants(rebase,{"TestPerson",a});ASSERT_TRUE(committed(run(rebase,{person(a,"remote")})));
    EXPECT_TRUE(people().empty());EXPECT_EQ(members().size(),1u);EXPECT_EQ(audit(),original);
}

TEST_F(ScopedRecoveryDelta, OmittedPendingHistoryDoesNotReplayOverTheCurrentVisibleRow) {
    auto first=initial();ASSERT_TRUE(committed(run(first,{person(a,"initial")})));
    owner->db().execute("UPDATE TestPerson SET name='older-pending' WHERE globalId=?",{a});
    const auto original=audit();ASSERT_EQ(original.size(),1u);
    // A later received/current-state update uses the real audit suppression
    // path. Replaying the older pending UPDATE here would visibly regress it.
    owner->db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
    owner->db().execute("UPDATE TestPerson SET name='already-visible' WHERE globalId=?",{a});
    owner->db().execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
    ASSERT_EQ(audit(),original);const auto current=people();auto delta=next(first);grants(delta,{"TestPerson",a});
    ASSERT_TRUE(committed(run(delta)));EXPECT_EQ(people(),current);EXPECT_EQ(audit(),original);
}

TEST_F(ScopedRecoveryDelta, OmittedAbsentMemberSurvivesWithoutAnyPendingOverlay) {
    auto first=initial();ASSERT_TRUE(committed(run(first,{person(upper(a))})));
    const auto membership=members();owner->db().execute("DELETE FROM TestPerson WHERE globalId=?",{a});
    owner->db().execute("INSERT INTO _lattice_sync_state SELECT id,'channel',1 FROM AuditLog");
    const auto original=audit();auto delta=next(first);ASSERT_TRUE(delta.pending.empty());
    ASSERT_TRUE(committed(run(delta)));EXPECT_TRUE(people().empty());EXPECT_EQ(members(),membership);EXPECT_EQ(audit(),original);
    auto full=next(delta,receive_install_mode::full);ASSERT_TRUE(committed(run(full)));EXPECT_TRUE(members().empty());
}

TEST_F(ScopedRecoveryDelta, CommittedOutcomeRequiresAnExplicitDeltaRebaseImage) {
    auto first=initial();ASSERT_TRUE(committed(run(first,{person(a)})));
    owner->db().execute("UPDATE TestPerson SET name='old-pending' WHERE globalId=?",{a});
    owner->db().execute("INSERT INTO _lattice_sync_state SELECT id,'other-channel',0 FROM AuditLog");
    auto delta=next(first);grants(delta,{"TestPerson",a},outcome::committed_noop);const auto before=snapshot();
    refused(run(delta),"requires explicit rebase image");EXPECT_EQ(snapshot(),before);
    ASSERT_TRUE(committed(run(delta,{person(a,"newer-canonical")})));
    EXPECT_EQ(std::get<std::string>(people()[0].at("name")),"newer-canonical");EXPECT_EQ(audit(),before[2]);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state WHERE sync_id='channel' AND is_synchronized=1"),1);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state WHERE sync_id='other-channel' AND is_synchronized=0"),1);
}

TEST_F(ScopedRecoveryDelta, ReplacedBasePreservesOnlyOriginalPendingFields) {
    auto first=initial();ASSERT_TRUE(committed(run(first,{person(a,"old",1)})));
    owner->db().execute("UPDATE TestPerson SET age=2 WHERE globalId=?",{a});
    owner->db().execute("UPDATE TestPerson SET age=3 WHERE globalId=?",{a});
    const auto original=audit();auto delta=next(first);grants(delta,{"TestPerson",a});
    ASSERT_TRUE(committed(run(delta,{person(a,"source-name",50)})));
    EXPECT_EQ(std::get<std::string>(people()[0].at("name")),"source-name");
    EXPECT_EQ(std::get<int64_t>(people()[0].at("age")),3);EXPECT_EQ(audit(),original);
}

TEST_F(ScopedRecoveryDelta, NoHistoryOverlayReadsLatestNULBytesWithoutChangingSourcePayload) {
    owner->document_schema();auto first=initial();first.model_tables={"DeltaDocument"};
    auto document=[&](std::string body,std::string title) {return recovery_row_image{{"DeltaDocument",a},
        sync_recovery::row_values{{"globalId",a},{"body",std::move(body)},{"title",std::move(title)}}};};
    ASSERT_TRUE(committed(run(first,{document("before","old")})));
    const std::string latest("left\0right",10);owner->db().execute("UPDATE DeltaDocument SET body=? WHERE globalId=?",{latest,a});
    const auto original=audit();auto delta=next(first);grants(delta,{"DeltaDocument",a});
    const std::vector<recovery_row_image> images{document("source-body","source-title")};const auto input=*images[0].present;
    ASSERT_TRUE(committed(run(delta,images)));const auto actual=query("SELECT body,title FROM DeltaDocument").at(0);
    EXPECT_EQ(std::get<std::string>(actual.at("body")),latest);EXPECT_EQ(std::get<std::string>(actual.at("title")),"source-title");
    EXPECT_EQ(*images[0].present,input);EXPECT_EQ(delta.identity.content_digest,"C1n");EXPECT_EQ(audit(),original);
}

TEST_F(ScopedRecoveryDelta, TrustedPendingGrantPreservesNewOmittedTargetWithoutClaimingJournalProof) {
    auto first=initial();ASSERT_TRUE(committed(run(first,{person(a)})));
    owner->db().execute("INSERT INTO TestPerson(globalId,name,age,email) VALUES(?,'new-local',7,NULL)",{upper(b)});
    const auto before=people();const auto original=audit();auto delta=next(first);grants(delta,{"TestPerson",b});
    ASSERT_TRUE(committed(run(delta)));EXPECT_EQ(people(),before);EXPECT_EQ(audit(),original);EXPECT_EQ(members().size(),2u);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state"),0);
}

TEST_F(ScopedRecoveryDelta, OmittedLinkBytesRemainExactAndDeletionRequiresLinkClosure) {
    auto first=initial();links(first);ASSERT_TRUE(committed(run(first,linked_rows())));
    // This is an equivalent existing UUID spelling, not a new canonical image.
    // Delta omission must retain these exact bytes while checking closure.
    owner->db().execute("UPDATE _SyncControl SET disabled=1 WHERE id=1");
    owner->db().execute("UPDATE _DeltaLinks SET lhs=?,rhs=?",{a,dog_id});
    owner->db().execute("UPDATE _SyncControl SET disabled=0 WHERE id=1");
    const auto links_before=query("SELECT rowid,* FROM _DeltaLinks");const auto member_before=members();auto delta=next(first);
    ASSERT_TRUE(committed(run(delta,{person(a,"changed")})));
    EXPECT_EQ(query("SELECT rowid,* FROM _DeltaLinks"),links_before);EXPECT_EQ(members(),member_before);
    const auto before=snapshot();auto remove=next(delta);
    refused(run(remove,{gone("TestPerson",a)}),"endpoint is absent");EXPECT_EQ(snapshot(),before);
    ASSERT_TRUE(committed(run(remove,{gone("TestPerson",a),gone("_DeltaLinks",link_id)})));
    EXPECT_TRUE(people().empty());EXPECT_TRUE(query("SELECT * FROM _DeltaLinks").empty());
    EXPECT_EQ(number("SELECT COUNT(*) FROM TestDog"),1);EXPECT_EQ(members().size(),1u);
}

TEST_F(ScopedRecoveryDelta, UUIDAliasesAndTombstonePresentCollisionRefuseWithoutInputMutation) {
    auto first=initial();ASSERT_TRUE(committed(run(first,{person(upper(a))})));
    auto delta=next(first);const auto before=snapshot();auto image=person(a,"changed");const auto payload=*image.present;
    refused(run(delta,{image,gone("TestPerson",upper(a))}),"duplicate row image");EXPECT_EQ(snapshot(),before);
    ASSERT_TRUE(committed(run(delta,{image})));EXPECT_EQ(*image.present,payload);
    EXPECT_EQ(std::get<std::string>(people()[0].at("globalId")),upper(a));
    EXPECT_EQ(people()[0].at("id"),before[0][0].at("id"));EXPECT_EQ(delta.identity.manifest_digest,"M1n");
}

TEST_F(ScopedRecoveryDelta, OtherScopeAndLocalOnlyRowsAndAuditObligationsStayUntouched) {
    auto other=initial();other.binding.channel="other";other.binding.scope="other-scope";
    ASSERT_TRUE(committed(run(other,{person(b,"other")})));
    owner->db().execute("INSERT INTO TestPerson(globalId,name,age,email) VALUES('local-only','private',9,NULL)");
    owner->db().execute("UPDATE TestPerson SET age=99 WHERE globalId=?",{b});
    const auto unrelated=audit();auto first=initial();ASSERT_TRUE(committed(run(first,{person(a)})));
    auto delta=next(first);ASSERT_TRUE(committed(run(delta,{gone("TestPerson",a)})));
    EXPECT_EQ(people().size(),2u);EXPECT_EQ(audit(),unrelated);EXPECT_EQ(members().size(),1u);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state"),0);
}

TEST_F(ScopedRecoveryDelta, SameHeadAndReopenExactRetryPreserveNewerLocalWorkAndWitness) {
    TempDB path("delta-retry");owner=std::make_shared<DeltaOwner>(path.str());
    recovery_refresh_test_access::use_manual_preparation(*owner);
    auto first=initial();ASSERT_TRUE(committed(run(first,{person(a)})));auto delta=next(first);
    ASSERT_EQ(delta.identity.head,first.identity.head);ASSERT_TRUE(committed(run(delta)));
    EXPECT_EQ(number("SELECT revision FROM _lattice_install_channel"),2);
    EXPECT_EQ(number("SELECT generation FROM _lattice_recovery_witness"),2);
    owner->db().execute("UPDATE TestPerson SET age=88 WHERE globalId=?",{a});const auto before=snapshot();
    owner->close();owner.reset();owner=std::make_shared<DeltaOwner>(path.str());
    recovery_refresh_test_access::use_manual_preparation(*owner);
    const auto retry=run(delta);ASSERT_TRUE(committed(retry));
    EXPECT_EQ(retry.installation->disposition,receive_install_disposition::already_installed);EXPECT_EQ(snapshot(),before);
    owner->close();owner.reset();
}

TEST_F(ScopedRecoveryDelta, CompletionFailureRollsBackRowsMembersReceiptsAndWitnessThenSameAttemptRetries) {
    auto first=initial();ASSERT_TRUE(committed(run(first,{person(a),person(b)})));
    owner->db().execute("UPDATE TestPerson SET age=3 WHERE globalId=?",{a});
    auto delta=next(first);grants(delta,{"TestPerson",a},outcome::committed_effect);const auto before=snapshot();
    // begin(active) retains revision 1. Only completion after validated model
    // effects reaches revision 2, making this a post-effect rollback oracle.
    owner->db().execute("CREATE TRIGGER _delta_completion_failure BEFORE UPDATE ON _lattice_install_channel WHEN NEW.revision=2 BEGIN SELECT RAISE(ABORT,'delta completion denied'); END");
    const std::vector<recovery_row_image> images{person(a,"after",70),gone("TestPerson",b)};
    refused(run(delta,images),"delta completion denied");EXPECT_EQ(snapshot(),before);
    owner->db().execute("DROP TRIGGER _delta_completion_failure");ASSERT_TRUE(committed(run(delta,images)));
    EXPECT_EQ(people().size(),1u);EXPECT_EQ(members().size(),1u);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_sync_state WHERE sync_id='channel' AND is_synchronized=1"),1);
    EXPECT_EQ(number("SELECT generation FROM _lattice_recovery_witness"),2);EXPECT_EQ(audit(),before[2]);
}

TEST_F(ScopedRecoveryDelta, ImageCountLimitRefusesWholePlanAndExactBoundarySucceeds) {
    auto first=initial();ASSERT_TRUE(committed(run(first,{person(a),person(b)})));
    auto delta=next(first);const auto before=snapshot();limits.targets=1;
    const std::vector<recovery_row_image> images{person(a,"A"),person(b,"B")};
    refused(run(delta,images),"input collection limit");EXPECT_EQ(snapshot(),before);
    limits.targets=2;ASSERT_TRUE(committed(run(delta,images)));EXPECT_EQ(people().size(),2u);
}

TEST_F(ScopedRecoveryDelta, LegacyFullWrapperStillRejectsDeltaAndImagesCannotMixWithFullRows) {
    auto first=initial();const auto p=person(a);first.full_rows={{p.key,*p.present}};
    ASSERT_TRUE(committed(install_scoped_recovery(owner,first,limits)));
    auto delta=next(first);const auto before=snapshot();
    refused(install_scoped_recovery(owner,delta,limits),"full scope only");EXPECT_EQ(snapshot(),before);
    delta.full_rows=first.full_rows;refused(run(delta,{gone("TestPerson",a)}),"cannot mix");EXPECT_EQ(snapshot(),before);
    const auto full_retry=install_scoped_recovery(owner,first,limits);ASSERT_TRUE(committed(full_retry));
    EXPECT_EQ(full_retry.installation->disposition,receive_install_disposition::already_installed);EXPECT_EQ(snapshot(),before);
}
