#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/recovery_obligation_store.hpp"
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"
#include <iomanip>
#include <limits>
#include <sstream>

namespace {
using namespace lattice::detail;
using error=recovery_obligation_error_code;
using mode=recovery_obligation_mode;
using stage=recovery_obligation_stage;
using origin=recovery_obligation_origin;
using outcome=recovery_install_state;
using blob=std::vector<uint8_t>;
blob encoded(const std::string& s) { return {s.begin(),s.end()}; }
std::string gid(int n) {
    std::ostringstream s; s<<"AAAAAAAA-0000-4000-8000-"<<std::hex<<std::setw(12)<<std::setfill('0')<<n; return s.str();
}
std::string lower(std::string s) { for (auto& c:s) if (c>='A' && c<='Z') c+=32; return s; }
int64_t scalar(lattice::database& db,const std::string& sql) { return std::get<int64_t>(db.query(sql).at(0).at("n")); }
std::shared_ptr<lattice::lattice_db> owner_at(const std::string& path=":memory:") {
    lattice::configuration c(path); c.audit_retention_seconds=0; c.busy_timeout_ms=100;
    auto owner=std::make_shared<lattice::lattice_db>(c); owner->add(TestPerson{"seed",1,std::nullopt});
    if (!c.is_in_memory()) {
        auto* notifier=lattice::instance_registry::instance().get_or_create_notifier(path);
        if (notifier) notifier->stop_listening();
    }
    return owner;
}
recovery_obligation_record original(lattice::database& db,const std::string& target,origin claim=origin::local_candidate) {
    const auto rows=db.query("SELECT id,globalId,tableName,globalRowId FROM AuditLog WHERE tableName='TestPerson' AND globalRowId=? ORDER BY id DESC LIMIT 1",{target});
    if (rows.size()!=1) throw std::runtime_error("missing actual generated test AuditLog row");
    const auto& r=rows[0]; return {std::get<int64_t>(r.at("id")),std::get<std::string>(r.at("globalId")),
        std::get<std::string>(r.at("tableName")),std::get<std::string>(r.at("globalRowId")),claim};
}
recovery_obligation_record insert(lattice::database& db,int n,origin claim=origin::local_candidate) {
    db.execute("INSERT INTO TestPerson(globalId,name,age) VALUES(?,?,?)",{gid(n),std::string("name"),int64_t(n)});
    return original(db,gid(n),claim);
}
template<class F> void expect(error wanted,F&& f) {
    try { f(); FAIL()<<"expected obligation refusal"; }
    catch (const recovery_obligation_error& e) { EXPECT_EQ(e.code,wanted)<<e.what(); }
}
receive_install_identity first(int64_t head=9) {
    return {1,0,{},head,receive_install_mode::full,"Q1","E1","C1","M1"};
}
receive_install_identity next_install(const receive_install_identity& i) {
    return {i.sequence+1,i.expected_revision+1,{receive_frontier_kind::position,i.head},i.head,
        receive_install_mode::delta,"Q2","E2","C2","M2"};
}
class RecoveryObligationStore : public ::testing::Test {
protected:
    // Explicit test budgets only; these do not establish production limits.
    recovery_obligation_limits limits{4,4096,128,2*1024*1024};
    receive_install_limits install_limits{4,128,8192};
    std::shared_ptr<lattice::lattice_db> owner=owner_at();
    recovery_obligation_profile profile{{"channel","authority","source","epoch","scope","schema"},"profile","receipts"};
    recovery_obligation_address address;
    recovery_obligation_store storage() { return {owner,limits,install_limits}; }
    receive_install_store installs() { return {owner,install_limits}; }
    template<class F> void committed(F&& f) {
        const auto result=recovery_writer_access::install(owner,std::forward<F>(f));
        if (result.primary_error) {
            try { std::rethrow_exception(result.primary_error); }
            catch (const std::exception& e) { ADD_FAILURE()<<e.what(); }
        }
        EXPECT_EQ(result.state,outcome::committed); EXPECT_EQ(result.cleanup_error,nullptr); EXPECT_EQ(result.postcommit_error,nullptr);
    }
    void SetUp() override {
        committed([&](auto&) {
            auto receiver=installs(); receiver.initialize(); receiver.bind(profile.binding);
            auto s=storage(); s.initialize(); address=s.bind(profile).address;
        });
    }
    recovery_obligation_entry add(int n,origin o=origin::local_candidate) {
        std::optional<recovery_obligation_entry> e;
        committed([&](auto& db) { e=storage().record(address,insert(db,n,o)); });
        if (!e) throw std::runtime_error("obligation insertion did not commit"); return *e;
    }
    recovery_obligation_receipt_claim positive(const recovery_obligation_entry& e,int64_t position=5) {
        return {e.record.original_id,profile.receipt_namespace,position,recovery_obligation_outcome::applied};
    }
    recovery_obligation_scope scope() {
        std::optional<recovery_obligation_scope> s;
        committed([&](auto&) { s=storage().read(profile.binding.channel); });
        if (!s) throw std::runtime_error("missing scope"); return *s;
    }
    void freeze(int64_t attempt=1) { committed([&](auto&) { address=storage().freeze(address,attempt).address; }); }
    void install(const receive_install_identity& i,const std::vector<recovery_obligation_receipt_claim>& positives,
        std::optional<receive_install_identity> prior={}) {
        committed([&](auto&) {
            auto s=storage(); const auto snap=s.snapshot_for_install(address,i.sequence);
            auto receipt=installs().apply_if_new(profile.binding,i,prior,[](auto&) {});
            ASSERT_EQ(receipt.disposition,receive_install_disposition::installed);
            s.settle_install(address,snap.scope.revision,i,positives);
        });
    }
};
}

TEST_F(RecoveryObligationStore, ActualOwnedTransactionAndRetainedOwnerRequired) {
    auto s=storage(); expect(error::transaction_required,[&]{s.read(profile.binding.channel);});
    owner->db().execute("BEGIN IMMEDIATE"); expect(error::transaction_required,[&]{s.audit();}); owner->db().rollback();
    owner->begin_transaction();
    std::optional<error> rejected;
    std::thread other([&]{try{s.read(profile.binding.channel);}catch(const recovery_obligation_error& e){rejected=e.code;}});
    other.join(); ASSERT_TRUE(rejected); EXPECT_EQ(*rejected,error::transaction_required); owner->commit();
    std::weak_ptr<lattice::lattice_db> weak=owner;
    auto actual=owner; owner.reset();
    EXPECT_EQ(recovery_writer_access::install(actual,[&](auto&){actual.reset();EXPECT_FALSE(weak.expired());s.audit();}).state,outcome::committed);
    EXPECT_FALSE(weak.expired());
}

TEST_F(RecoveryObligationStore, RecordSharesModelAuditAndOuterRollback) {
    auto s=storage(); recovery_obligation_usage before;
    committed([&](auto&){before=s.usage();});
    auto failed=recovery_writer_access::install(owner,[&](auto& db){s.record(address,insert(db,1));throw std::runtime_error("abort original statement transaction");});
    EXPECT_EQ(failed.state,outcome::rolled_back);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM TestPerson WHERE age=1 AND name='name'"),0);
    committed([&](auto&){EXPECT_EQ(s.usage(),before);s.audit();});
    const auto kept=add(1);
    committed([&](auto&){EXPECT_TRUE(s.pins_audit(kept.record.audit_id,kept.record.original_id));});
}

TEST_F(RecoveryObligationStore, DuplicateUUIDSpellingReusesChargeAndPreservesOriginalBytes) {
    const auto e=add(2); auto s=storage();
    committed([&](auto& db){
        const auto before=s.usage(); const auto body=db.query("SELECT * FROM AuditLog WHERE id=?",{e.record.audit_id});
        auto input=e.record; input.original_id=lower(input.original_id); input.target_id=lower(input.target_id);
        EXPECT_EQ(s.record(address,input),e); EXPECT_EQ(s.usage(),before);
        EXPECT_EQ(db.query("SELECT * FROM AuditLog WHERE id=?",{e.record.audit_id}),body);
        input.origin=origin::legacy_unknown; expect(error::conflict,[&]{s.record(address,input);});
        input=e.record; input.target_id=gid(3); expect(error::audit_mismatch,[&]{s.record(address,input);});
        EXPECT_EQ(s.usage(),before);
    });
}

TEST_F(RecoveryObligationStore, ScopeAliasesAndBindingChangesRefuseWithoutAdoption) {
    committed([&](auto&){
        auto s=storage(); auto alias=profile; alias.binding.channel="other-channel";
        expect(error::alias,[&]{s.bind(alias);});
        alias=profile; alias.binding.epoch="replacement"; expect(error::binding_mismatch,[&]{s.bind(alias);});
        alias=profile; alias.profile_digest="different-selection"; expect(error::binding_mismatch,[&]{s.bind(alias);});
        alias=profile; alias.binding.channel="independent"; alias.binding.scope="other-scope";
        installs().bind(alias.binding);
        EXPECT_NE(s.bind(alias).address.incarnation,address.incarnation); EXPECT_EQ(s.usage().scopes,2);
    });
}

TEST_F(RecoveryObligationStore, StickyExportClaimsSurviveFailureAndRepeatedExports) {
    const auto e=add(4); auto s=storage(); recovery_obligation_export_ticket first_claim;
    committed([&](auto&){first_claim=s.claim_export(address,{e.record.original_id});});
    // Transport did not run. A committed pre-send claim still means possibly sent.
    committed([&](auto&){
        const auto second=s.claim_export(address,{e.record.original_id}); EXPECT_GT(second.sequence,first_claim.sequence);
        const auto stored=s.find(address,e.record.original_id); ASSERT_TRUE(stored);
        EXPECT_EQ(stored->first_export_claim,first_claim.sequence);
        EXPECT_EQ(stored->stage,stage::open); EXPECT_TRUE(s.pins_audit(e.record.audit_id,e.record.original_id));
    });
    auto failed=recovery_writer_access::install(owner,[&](auto&){s.claim_export(address,{e.record.original_id});throw std::runtime_error("unpublished claim");});
    EXPECT_EQ(failed.state,outcome::rolled_back);
    committed([&](auto&){const auto stored=s.find(address,e.record.original_id);ASSERT_TRUE(stored);EXPECT_EQ(stored->first_export_claim,first_claim.sequence);});
}

TEST_F(RecoveryObligationStore, ExportReserveIsAllOrNoneBeforeAnyPhysicalSend) {
    const auto one=add(5),two=add(6); auto s=storage();
    committed([&](auto&){
        const auto before=s.read(address.channel);
        expect(error::conflict,[&]{s.claim_export(address,{one.record.original_id,gid(999)});});
        expect(error::invalid_argument,[&]{s.claim_export(address,{one.record.original_id,lower(one.record.original_id)});});
        EXPECT_EQ(s.read(address.channel),before);
        const auto stored=s.find(address,one.record.original_id); ASSERT_TRUE(stored); EXPECT_FALSE(stored->first_export_claim);
        EXPECT_EQ(s.claim_export(address,{one.record.original_id,two.record.original_id}).canonical_original_ids.size(),2u);
    });
}

TEST_F(RecoveryObligationStore, FreezeFencesOldExportsAndACKsButLocalRecordsAdvanceCurrentSnapshot) {
    const auto sent=add(7); auto s=storage();
    committed([&](auto&){s.claim_export(address,{sent.record.original_id});});
    const auto old=address; freeze(); const auto at_freeze=scope();
    const auto late=add(8);
    committed([&](auto& db){
        expect(error::stale,[&]{s.claim_export(old,{sent.record.original_id});});
        expect(error::stale,[&]{s.acknowledge(old,positive(sent));});
        expect(error::wrong_mode,[&]{s.claim_export(address,{sent.record.original_id});});
        db.execute("SAVEPOINT imported_effect"); auto imported=insert(db,9,origin::imported);
        expect(error::wrong_mode,[&]{s.record(address,imported);});
        db.execute("ROLLBACK TO imported_effect"); db.execute("RELEASE imported_effect");
        const auto snap=s.snapshot_for_install(address,1);
        EXPECT_GT(snap.scope.revision,at_freeze.revision); EXPECT_EQ(snap.scope.freeze_revision,at_freeze.freeze_revision);
        ASSERT_EQ(snap.entries.size(),2u); EXPECT_EQ(snap.entries[0].canonical_original_id,sent.canonical_original_id);
        EXPECT_EQ(snap.entries[1].canonical_original_id,late.canonical_original_id); EXPECT_FALSE(snap.entries[1].first_export_claim);
        EXPECT_GT(late.sequence,snap.scope.freeze_record_high_water);
        // local_candidate + no claim is storage, NOT proof that all routes were covered.
    });
}

TEST_F(RecoveryObligationStore, LegacyUnknownAndRemoteRelayRemainExplicitlyUnproved) {
    auto s=storage();
    committed([&](auto& db){
        auto r=insert(db,10); db.execute("UPDATE AuditLog SET isFromRemote=1 WHERE id=?",{r.audit_id});
        expect(error::audit_mismatch,[&]{s.record(address,r);});
        r.origin=origin::imported; const auto remote=s.record(address,r); EXPECT_EQ(remote.record.origin,origin::imported);
        r=insert(db,11,origin::legacy_unknown); const auto legacy=s.record(address,r); EXPECT_EQ(legacy.record.origin,origin::legacy_unknown);
        EXPECT_FALSE(legacy.first_export_claim); EXPECT_TRUE(s.pins_audit(legacy.record.audit_id,legacy.record.original_id));
        expect(error::wrong_mode,[&]{s.retire(address);});
    });
}

TEST_F(RecoveryObligationStore, PositiveACKKeepsRebaseAndRetentionCustodyEvenAfterLegacyFlagSettles) {
    const auto e=add(12); auto s=storage();
    committed([&](auto& db){
        s.claim_export(address,{e.record.original_id}); s.acknowledge(address,positive(e));
        db.execute("UPDATE AuditLog SET isSynchronized=1 WHERE id=?",{e.record.audit_id});
        const auto stored=s.find(address,e.record.original_id); ASSERT_TRUE(stored); EXPECT_EQ(stored->stage,stage::acknowledged_awaiting_install);
        EXPECT_TRUE(s.pins_audit(e.record.audit_id,e.record.original_id));
        const auto before=s.read(address.channel); s.acknowledge(address,positive(e)); EXPECT_EQ(s.read(address.channel),before);
        expect(error::conflict,[&]{s.acknowledge(address,positive(e,6));});
        expect(error::wrong_mode,[&]{s.claim_export(address,{e.record.original_id});});
    });
    freeze(); committed([&](auto&){const auto snap=s.snapshot_for_install(address,1);ASSERT_EQ(snap.entries.size(),1u);EXPECT_EQ(snap.entries[0].stage,stage::acknowledged_awaiting_install);});
}

TEST_F(RecoveryObligationStore, SettlementRequiresActualInstalledIdentityAndCurrentJournalRevision) {
    const auto e=add(13); freeze(); auto s=storage(); const auto i=first();
    committed([&](auto& db){
        const auto snap=s.snapshot_for_install(address,1);
        expect(error::stale,[&]{s.settle_install(address,snap.scope.revision,i,{positive(e)});});
        installs().apply_if_new(profile.binding,i,std::nullopt,[](auto&){});
        auto late=s.record(address,insert(db,14));
        expect(error::stale,[&]{s.settle_install(address,snap.scope.revision,i,{positive(e)});});
        const auto current=s.snapshot_for_install(address,1);
        auto wrong=positive(e); wrong.receipt_namespace="different";
        expect(error::invalid_argument,[&]{s.settle_install(address,current.scope.revision,i,{wrong});});
        expect(error::conflict,[&]{s.settle_install(address,current.scope.revision,i,{positive(e,10)});});
        s.settle_install(address,current.scope.revision,i,{positive(e)});
        EXPECT_FALSE(s.pins_audit(e.record.audit_id,e.record.original_id)); EXPECT_TRUE(s.pins_audit(late.record.audit_id,late.record.original_id));
        // The caller must independently classify EVERY remaining identity before activation.
    });
}

TEST_F(RecoveryObligationStore, SettlementFailureEscapesOuterInstallerAndRollsBackModelReceiverAndJournal) {
    const auto e=add(15); freeze(); auto s=storage(); const auto before=scope();
    auto failed=recovery_writer_access::install(owner,[&](auto& db){
        const auto snap=s.snapshot_for_install(address,1); const auto i=first();
        installs().apply_if_new(profile.binding,i,std::nullopt,[](auto& writer){writer.execute("UPDATE TestPerson SET name='recovered' WHERE age=15");});
        s.settle_install(address,snap.scope.revision,i,{positive(e,10)}); // beyond H; MUST escape
        db.execute("UPDATE TestPerson SET name='unreachable' WHERE age=15");
    });
    EXPECT_EQ(failed.state,outcome::rolled_back); EXPECT_EQ(scope(),before);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) AS n FROM TestPerson WHERE age=15 AND name='name'"),1);
    committed([&](auto&){const auto state=installs().read(address.channel);ASSERT_TRUE(state);EXPECT_FALSE(state->last_installed);EXPECT_TRUE(s.pins_audit(e.record.audit_id,e.record.original_id));});
    install(first(),{positive(e)});
}

TEST_F(RecoveryObligationStore, SameHeadRefreshAndExactRetryDoNotRepeatJournalOrModelEffects) {
    const auto e=add(16); freeze(); const auto one=first(); install(one,{positive(e)}); auto s=storage();
    const auto late=add(17); const auto before=scope();
    committed([&](auto&){
        int effects=0;
        const auto result=installs().apply_if_new(profile.binding,one,std::nullopt,[&](auto&){++effects;});
        ASSERT_EQ(result.disposition,receive_install_disposition::already_installed); EXPECT_EQ(effects,0);
        // Caller bypasses ALL journal work on exact retry.
        EXPECT_EQ(s.read(address.channel),before); EXPECT_TRUE(s.pins_audit(late.record.audit_id,late.record.original_id));
    });
    freeze(2); const auto two=next_install(one); install(two,{},one);
    committed([&](auto&){
        const auto current=s.read(address.channel); ASSERT_TRUE(current); EXPECT_EQ(current->installed_revision,2); EXPECT_EQ(current->installed_head,one.head);
        const auto old=address; address=s.resume(address,two).address; EXPECT_GT(address.generation,old.generation);
        expect(error::stale,[&]{s.claim_export(old,{late.record.original_id});});
        EXPECT_EQ(s.claim_export(address,{late.record.original_id}).canonical_original_ids.size(),1u);
    });
}

TEST_F(RecoveryObligationStore, SettledTombstonesRetainChargesUntilExplicitSafeRetirement) {
    const auto e=add(18); recovery_obligation_usage before; committed([&](auto&){before=storage().usage();});
    freeze(); install(first(),{positive(e)}); auto s=storage();
    committed([&](auto& db){
        EXPECT_EQ(s.usage().records,before.records); EXPECT_GE(s.usage().encoded_bytes,before.encoded_bytes);
        EXPECT_FALSE(s.pins_audit(e.record.audit_id,e.record.original_id));
        // A future coordinated retention integration may now prune the body.
        db.execute("DELETE FROM AuditLog WHERE id=?",{e.record.audit_id}); s.audit();
        const auto old=address; s.retire(address); EXPECT_EQ(s.usage(),(recovery_obligation_usage{}));
        address=s.bind(profile).address; EXPECT_GT(address.incarnation,old.incarnation);
        expect(error::stale,[&]{s.find(old,e.record.original_id);}); EXPECT_FALSE(s.find(address,e.record.original_id));
    });
}

TEST_F(RecoveryObligationStore, MissingOriginalOrWrongRetentionIdentityNeverMeansUnpinned) {
    const auto e=add(19); auto s=storage();
    committed([&](auto& db){
        expect(error::audit_mismatch,[&]{s.pins_audit(e.record.audit_id,gid(88));});
        expect(error::audit_mismatch,[&]{s.pins_audit(e.record.audit_id+1,e.record.original_id);});
        db.execute("SAVEPOINT injected_loss"); db.execute("DELETE FROM AuditLog WHERE id=?",{e.record.audit_id});
        expect(error::audit_mismatch,[&]{s.audit();});
        EXPECT_TRUE(s.pins_audit(e.record.audit_id,e.record.original_id));
        db.execute("ROLLBACK TO injected_loss"); db.execute("RELEASE injected_loss"); s.audit();
    });
}

TEST_F(RecoveryObligationStore, RebindInheritsActualReceiverBaselineAndOldInstallCannotSettleNewWork) {
    const auto old=add(26); freeze(); const auto one=first(); install(one,{positive(old)}); auto s=storage();
    committed([&](auto& db){
        const auto retired=address; s.retire(address); const auto rebound=s.bind(profile); address=rebound.address;
        EXPECT_GT(address.incarnation,retired.incarnation); EXPECT_EQ(rebound.last_attempt,one.sequence);
        EXPECT_EQ(rebound.installed_revision,1); EXPECT_EQ(rebound.installed_manifest,one.manifest_digest);
        const auto fresh=s.record(address,insert(db,27));
        expect(error::stale,[&]{s.freeze(address,one.sequence);});
        address=s.freeze(address,2).address; const auto snap=s.snapshot_for_install(address,2);
        expect(error::stale,[&]{s.settle_install(address,snap.scope.revision,one,{positive(fresh)});});
        EXPECT_TRUE(s.pins_audit(fresh.record.audit_id,fresh.record.original_id));
        const auto two=next_install(one); installs().apply_if_new(profile.binding,two,one,[](auto&){});
        s.settle_install(address,snap.scope.revision,two,{positive(fresh)});
        EXPECT_FALSE(s.pins_audit(fresh.record.audit_id,fresh.record.original_id));
    });
}

TEST_F(RecoveryObligationStore, RebindRefusesActiveReceiverAndOuterRollbackRestoresPriorIncarnation) {
    auto s=storage(); const auto before=scope(); const auto committed_address=address;
    auto failed=recovery_writer_access::install(owner,[&](auto&){
        s.retire(address); const auto unpublished=s.bind(profile).address;
        EXPECT_GT(unpublished.incarnation,address.incarnation);
        throw std::runtime_error("discard unpublished reincarnation");
    });
    EXPECT_EQ(failed.state,outcome::rolled_back); EXPECT_EQ(scope(),before);
    committed([&](auto&){
        s.retire(committed_address); installs().begin(profile.binding,first());
        expect(error::wrong_mode,[&]{s.bind(profile);}); EXPECT_FALSE(s.read(address.channel));
        installs().abandon_active(profile.binding,first()); const auto bound=s.bind(profile); address=bound.address;
        EXPECT_EQ(bound.last_attempt,1); EXPECT_EQ(bound.installed_revision,0);
        expect(error::stale,[&]{s.freeze(address,1);}); EXPECT_EQ(s.freeze(address,2).last_attempt,2);
        // The rolled-back token was never usable; no uniqueness claim is made for it.
    });
}

TEST_F(RecoveryObligationStore, AnotherScopeKeepsTheOriginalPinnedAfterFirstScopeSettlement) {
    const auto e=add(28); auto s=storage(); auto other=profile;
    other.binding.channel="other"; other.binding.authority="other-authority";
    recovery_obligation_address second;
    committed([&](auto&){installs().bind(other.binding);second=s.bind(other).address;s.record(second,e.record);});
    freeze(); install(first(),{positive(e)});
    committed([&](auto&){
        EXPECT_TRUE(s.pins_audit(e.record.audit_id,e.record.original_id));
        const auto untouched=s.find(second,e.record.original_id); ASSERT_TRUE(untouched); EXPECT_EQ(untouched->stage,stage::open);
        EXPECT_FALSE(untouched->first_export_claim); expect(error::wrong_mode,[&]{s.retire(second);});
    });
}

TEST_F(RecoveryObligationStore, IgnoredAndFailedWritesRestoreCountersAndAllowSuccessor) {
    auto s=storage();
    committed([&](auto& db){
        const auto r=insert(db,20); const auto before=s.usage();
        db.execute("CREATE TEMP TRIGGER obligation_ignore BEFORE INSERT ON main._lattice_obligation_entry BEGIN SELECT RAISE(IGNORE); END");
        EXPECT_THROW(s.record(address,r),lattice::db_error); EXPECT_EQ(s.usage(),before); EXPECT_FALSE(s.find(address,r.original_id));
        db.execute("DROP TRIGGER obligation_ignore");
        db.execute("CREATE TEMP TRIGGER obligation_abort BEFORE UPDATE ON main._lattice_obligation_scope BEGIN SELECT RAISE(ABORT,'scope refused'); END");
        EXPECT_THROW(s.record(address,r),lattice::db_error); EXPECT_EQ(s.usage(),before);
        db.execute("DROP TRIGGER obligation_abort"); const auto kept=s.record(address,r); EXPECT_EQ(kept.record,r); s.audit();
    });
}

TEST_F(RecoveryObligationStore, CounterDriftAndUnsupportedVersionRefuseInsteadOfRepair) {
    const auto e=add(21); auto s=storage();
    committed([&](auto& db){
        db.execute("SAVEPOINT drift"); db.execute("UPDATE _lattice_obligation_store SET records=records+1");
        expect(error::corrupt_state,[&]{s.audit();}); db.execute("ROLLBACK TO drift"); db.execute("RELEASE drift");
        db.execute("SAVEPOINT version"); db.execute("UPDATE _lattice_obligation_store SET version=2");
        expect(error::corrupt_state,[&]{s.find(address,e.record.original_id);});
        db.execute("ROLLBACK TO version"); db.execute("RELEASE version"); s.audit();
    });
}

TEST_F(RecoveryObligationStore, InitializationRefusesMissingOrReplacedRequiredIndexAndTableShape) {
    auto s=storage();
    committed([&](auto& db){
        db.execute("SAVEPOINT missing_index"); db.execute("DROP INDEX _lattice_obligation_audit");
        expect(error::corrupt_state,[&]{s.initialize();});
        db.execute("CREATE INDEX _lattice_obligation_audit ON _lattice_obligation_entry(channel,original)");
        expect(error::corrupt_state,[&]{s.audit();});
        db.execute("ROLLBACK TO missing_index"); db.execute("RELEASE missing_index");
        db.execute("SAVEPOINT changed_shape"); db.execute("ALTER TABLE _lattice_obligation_scope ADD COLUMN extra INTEGER");
        expect(error::corrupt_state,[&]{s.initialize();});
        db.execute("ROLLBACK TO changed_shape"); db.execute("RELEASE changed_shape"); s.audit();
    });
}

TEST_F(RecoveryObligationStore, ReinitializationAndLimitsMismatchDoNotResetHighWaters) {
    const auto e=add(22); auto s=storage();
    committed([&](auto&){
        const auto first=s.claim_export(address,{e.record.original_id}); s.initialize();
        EXPECT_GT(s.claim_export(address,{e.record.original_id}).sequence,first.sequence);
        auto different=limits; ++different.records; recovery_obligation_store mismatch(owner,different,install_limits);
        expect(error::limits_mismatch,[&]{mismatch.initialize();}); s.audit();
    });
}

TEST_F(RecoveryObligationStore, OpaqueBindingBytesRemainExactAndOversizedStoredIdentitiesRefuse) {
    const auto e=add(31); auto s=storage();
    committed([&](auto& db){
        auto binary=profile; binary.binding.channel=std::string("c\0two",5); binary.binding.scope=std::string("s\0two",5);
        binary.profile_digest=std::string("p\0digest",8); installs().bind(binary.binding);
        const auto bound=s.bind(binary); EXPECT_EQ(bound.profile,binary); EXPECT_EQ(s.read(binary.binding.channel),bound);
        db.execute("SAVEPOINT oversized");
        db.execute("UPDATE _lattice_obligation_entry SET actual_original=zeroblob(?) WHERE channel=? AND original=?",
            {limits.field_bytes+1,encoded(address.channel),encoded(e.canonical_original_id)});
        expect(error::corrupt_state,[&]{s.find(address,e.record.original_id);});
        db.execute("ROLLBACK TO oversized"); db.execute("RELEASE oversized");
        auto wrong=e.record; wrong.original_id="not-a-uuid"; expect(error::invalid_argument,[&]{s.record(address,wrong);}); s.audit();
    });
}

TEST_F(RecoveryObligationStore, ExhaustedCommittedCountersRefuseWithoutWrappingOrReleasingEvidence) {
    const auto e=add(29); auto s=storage(); const auto max=std::numeric_limits<int64_t>::max();
    committed([&](auto& db){
        db.execute("SAVEPOINT exhausted_export"); db.execute("UPDATE _lattice_obligation_store SET export_sequence=?",{max});
        expect(error::exhausted,[&]{s.claim_export(address,{e.record.original_id});});
        const auto stored=s.find(address,e.record.original_id); ASSERT_TRUE(stored); EXPECT_FALSE(stored->first_export_claim);
        db.execute("ROLLBACK TO exhausted_export"); db.execute("RELEASE exhausted_export");
        const auto new_record=insert(db,30);
        db.execute("SAVEPOINT exhausted_record"); db.execute("UPDATE _lattice_obligation_store SET record_sequence=?",{max});
        expect(error::exhausted,[&]{s.record(address,new_record);});
        db.execute("ROLLBACK TO exhausted_record"); db.execute("RELEASE exhausted_record");
        db.execute("SAVEPOINT exhausted_generation"); db.execute("UPDATE _lattice_obligation_scope SET generation=? WHERE channel=?",{max,encoded(address.channel)});
        auto exhausted=address; exhausted.generation=max; expect(error::exhausted,[&]{s.freeze(exhausted,1);});
        db.execute("ROLLBACK TO exhausted_generation"); db.execute("RELEASE exhausted_generation");
        EXPECT_TRUE(s.pins_audit(e.record.audit_id,e.record.original_id)); EXPECT_EQ(s.record(address,new_record).record,new_record); s.audit();
    });
}

TEST(RecoveryObligationStoreLimits, ExplicitRecordAndByteCapacityNeverEvictsAcceptedTail) {
    auto owner=owner_at(); const recovery_obligation_profile p{{"c","a","s","e","scope","schema"},"profile","receipts"};
    const receive_install_limits il{2,128,8192}; const recovery_obligation_limits l{1,1,128,2048};
    recovery_obligation_store s(owner,l,il);
    EXPECT_EQ(recovery_writer_access::install(owner,[&](auto& db){
        receive_install_store receiver(owner,il); receiver.initialize(); receiver.bind(p.binding);
        s.initialize(); const auto a=s.bind(p).address; const auto one=s.record(a,insert(db,23)); const auto before=s.usage();
        db.execute("SAVEPOINT second_effect"); auto two=insert(db,24); expect(error::capacity,[&]{s.record(a,two);});
        db.execute("ROLLBACK TO second_effect"); db.execute("RELEASE second_effect"); EXPECT_EQ(s.usage(),before);
        EXPECT_EQ(s.record(a,one.record),one); EXPECT_TRUE(s.pins_audit(one.record.audit_id,one.record.original_id));
        auto other=p; other.binding.channel="d"; other.binding.scope="different";
        receiver.bind(other.binding);
        expect(error::capacity,[&]{s.bind(other);});
        const auto frozen=s.freeze(a,1); const auto i=first();
        receiver.apply_if_new(p.binding,i,std::nullopt,[](auto&){});
        s.settle_install(frozen.address,frozen.revision,i,{{one.record.original_id,p.receipt_namespace,5,recovery_obligation_outcome::applied}});
        db.execute("SAVEPOINT settled_tail_effect"); two=insert(db,24);
        expect(error::capacity,[&]{s.record(frozen.address,two);});
        db.execute("ROLLBACK TO settled_tail_effect"); db.execute("RELEASE settled_tail_effect");
        EXPECT_EQ(s.usage().records,1); EXPECT_FALSE(s.pins_audit(one.record.audit_id,one.record.original_id)); s.audit();
    }).state,outcome::committed);
    auto owner2=owner_at(); recovery_obligation_store tiny(owner2,{1,10,128,130},il);
    EXPECT_EQ(recovery_writer_access::install(owner2,[&](auto&){receive_install_store receiver(owner2,il);receiver.initialize();receiver.bind(p.binding);tiny.initialize();expect(error::capacity,[&]{tiny.bind(p);});EXPECT_EQ(tiny.usage(),(recovery_obligation_usage{}));}).state,outcome::committed);
}

TEST(RecoveryObligationStoreFile, ReopenRetainsStickyClaimFreezeAndACKAwaitingRebase) {
    TempDB file{"recovery_obligation_reopen"}; const recovery_obligation_limits l{2,32,128,16384}; const receive_install_limits il{2,128,4096};
    const recovery_obligation_profile p{{"c","a","s","e","scope","schema"},"profile","receipts"};
    recovery_obligation_address address; recovery_obligation_entry e; int64_t claim=0;
    {
        auto owner=owner_at(file.str()); recovery_obligation_store s(owner,l,il);
        EXPECT_EQ(recovery_writer_access::install(owner,[&](auto& db){
            receive_install_store receiver(owner,il); receiver.initialize(); receiver.bind(p.binding);
            s.initialize(); address=s.bind(p).address; e=s.record(address,insert(db,25));
            claim=s.claim_export(address,{e.record.original_id}).sequence;
            s.acknowledge(address,{e.record.original_id,p.receipt_namespace,8,recovery_obligation_outcome::no_op});
            address=s.freeze(address,1).address;
        }).state,outcome::committed);
    }
    {
        auto owner=owner_at(file.str()); recovery_obligation_store s(owner,l,il);
        EXPECT_EQ(recovery_writer_access::install(owner,[&](auto&){
            s.initialize(); const auto snap=s.snapshot_for_install(address,1); ASSERT_EQ(snap.entries.size(),1u);
            EXPECT_EQ(snap.entries[0].first_export_claim,claim); EXPECT_EQ(snap.entries[0].stage,stage::acknowledged_awaiting_install);
            EXPECT_TRUE(s.pins_audit(e.record.audit_id,e.record.original_id));
            expect(error::wrong_mode,[&]{s.claim_export(address,{e.record.original_id});});
        }).state,outcome::committed);
    }
}

TEST(RecoveryObligationStoreLimits, AuditRefusesOrphanCountBeyondExplicitCapWithoutAdoptingOrPruning) {
    auto owner=owner_at(); const recovery_obligation_limits l{1,1,128,4096}; const receive_install_limits il{1,128,4096};
    const recovery_obligation_profile p{{"c","a","s","e","scope","schema"},"profile","receipts"};
    recovery_obligation_store s(owner,l,il);
    EXPECT_EQ(recovery_writer_access::install(owner,[&](auto& db){
        receive_install_store receiver(owner,il); receiver.initialize(); receiver.bind(p.binding); s.initialize(); s.bind(p);
        // Simulate corrupt private metadata. These rows have no channel and
        // must be caught by the capped orphan count, not silently omitted.
        for(int n=1;n<=2;++n) db.execute("INSERT INTO _lattice_obligation_entry VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            {encoded("orphan"),encoded(lower(gid(n))),int64_t(n),encoded(gid(n)),encoded("TestPerson"),encoded(lower(gid(n))),encoded(gid(n)),
             int64_t(2),int64_t(n),nullptr,int64_t(0),nullptr,nullptr,int64_t(0),int64_t(200)});
        expect(error::corrupt_state,[&]{s.audit();});
        EXPECT_EQ(scalar(db,"SELECT COUNT(*) AS n FROM _lattice_obligation_entry"),2);
        EXPECT_EQ(s.usage().records,0); // no silent counter repair or evidence deletion
    }).state,outcome::committed);
}

TEST_F(RecoveryObligationStore, AddressedPointUpdateWorkDoesNotScanRetainedLedger) {
    auto s=storage(); recovery_obligation_entry last;
    committed([&](auto& db){for(int n=100;n<1124;++n) last=s.record(address,insert(db,n));});
    committed([&](auto& db){
        struct vm_budget {
            sqlite3* handle; int steps=0;
            explicit vm_budget(sqlite3* db):handle(db) { sqlite3_progress_handler(handle,1,[](void* p) -> int {auto& b=*static_cast<vm_budget*>(p);return ++b.steps>20000;},this); }
            ~vm_budget(){sqlite3_progress_handler(handle,0,nullptr,nullptr);}
        };
        { vm_budget bound(db.handle()); EXPECT_EQ(s.claim_export(address,{last.record.original_id}).canonical_original_ids.size(),1u); EXPECT_LE(bound.steps,20000); }
        { vm_budget bound(db.handle()); EXPECT_EQ(s.acknowledge(address,positive(last)).mode,mode::recording); EXPECT_LE(bound.steps,20000); }
        { vm_budget bound(db.handle()); EXPECT_TRUE(s.pins_audit(last.record.audit_id,last.record.original_id)); EXPECT_LE(bound.steps,20000); }
    });
}

TEST_F(RecoveryObligationStore, CancelFrozenPreservesAllOriginalsClaimsAndHighWaters) {
    const auto open=add(301),claimed=add(302),acked=add(303);
    committed([&](auto&) {
        auto s=storage();
        s.claim_export(address,{claimed.record.original_id,acked.record.original_id});
        s.acknowledge(address,positive(acked));
    });
    freeze();
    const auto frozen=scope();
    committed([&](auto& db) {
        auto s=storage();
        const auto before=s.snapshot_for_install(address,1);
        const auto usage=s.usage();
        const auto audit=db.query("SELECT * FROM AuditLog ORDER BY id");
        const auto canceled=s.cancel_frozen_for_retry(address,1,frozen.revision);
        EXPECT_EQ(canceled.mode,mode::recording);
        EXPECT_EQ(canceled.last_attempt,frozen.last_attempt);
        EXPECT_EQ(canceled.freeze_revision,frozen.freeze_revision);
        EXPECT_EQ(canceled.freeze_record_high_water,frozen.freeze_record_high_water);
        EXPECT_EQ(canceled.freeze_export_high_water,frozen.freeze_export_high_water);
        EXPECT_GT(canceled.address.generation,address.generation);
        EXPECT_GT(canceled.revision,frozen.revision);
        for (const auto& e:before.entries) {
            EXPECT_EQ(s.find(canceled.address,e.record.original_id),e);
            EXPECT_TRUE(s.pins_audit(e.record.audit_id,e.record.original_id));
        }
        EXPECT_EQ(s.usage(),usage);
        EXPECT_EQ(db.query("SELECT * FROM AuditLog ORDER BY id"),audit);
        const auto receiver=installs().read(profile.binding.channel);
        ASSERT_TRUE(receiver);
        EXPECT_EQ(receiver->last_sequence,1);
        EXPECT_EQ(receiver->revision,0);
        EXPECT_FALSE(receiver->active);
        EXPECT_FALSE(receiver->last_installed);
        EXPECT_EQ(receiver->frontier,receive_install_frontier{});
        expect(error::stale,[&]{s.snapshot_for_install(address,1);});
        expect(error::stale,[&]{s.claim_export(address,{open.record.original_id});});
        address=canceled.address;
        expect(error::stale,[&]{s.freeze(address,1);});
    });
}

TEST_F(RecoveryObligationStore, CancelBeforeManifestAllowsNextRealInstallationWithoutSequenceReuse) {
    const auto kept=add(304);
    freeze();
    const auto frozen=scope();
    committed([&](auto&) {address=storage().cancel_frozen_for_retry(address,1,frozen.revision).address;});
    freeze(2);
    auto actual=first();
    actual.sequence=2; // Cancellation consumed sequence 1, not a revision/head.
    install(actual,{positive(kept)});
    committed([&](auto&) {
        const auto receiver=installs().read(profile.binding.channel);
        ASSERT_TRUE(receiver);
        EXPECT_EQ(receiver->last_installed,actual);
        EXPECT_EQ(receiver->revision,1);
        EXPECT_EQ(receiver->last_sequence,2);
        address=storage().resume(address,actual).address;
        EXPECT_EQ(storage().find(address,kept.record.original_id)->stage,stage::settled);
    });
}

TEST_F(RecoveryObligationStore, CancelRequiresCurrentFrozenAddressAttemptAndRevision) {
    const auto recording=scope();
    committed([&](auto&) {
        expect(error::stale,[&]{storage().cancel_frozen_for_retry(address,1,recording.revision);});
    });
    freeze();
    const auto old=scope();
    add(305); // Existing frozen mode still records local candidates.
    const auto current=scope();
    ASSERT_GT(current.revision,old.revision);
    committed([&](auto&) {
        auto s=storage();
        const auto receiver=installs().read(profile.binding.channel);
        expect(error::invalid_argument,[&]{s.cancel_frozen_for_retry(address,0,current.revision);});
        expect(error::stale,[&]{s.cancel_frozen_for_retry(address,2,current.revision);});
        expect(error::stale,[&]{s.cancel_frozen_for_retry(address,1,old.revision);});
        auto stale=address;
        --stale.generation;
        expect(error::stale,[&]{s.cancel_frozen_for_retry(stale,1,current.revision);});
        EXPECT_EQ(s.read(profile.binding.channel),current);
        EXPECT_EQ(installs().read(profile.binding.channel),receiver);
    });
}

TEST_F(RecoveryObligationStore, CancelRefusesActiveReceiverUntilExactExplicitAbandonment) {
    freeze();
    const auto frozen=scope();
    const auto identity=first();
    committed([&](auto&) {
        installs().begin(profile.binding,identity);
        expect(error::stale,[&]{storage().cancel_frozen_for_retry(address,1,frozen.revision);});
        EXPECT_EQ(installs().read(profile.binding.channel)->active,identity);
        EXPECT_EQ(storage().read(profile.binding.channel),frozen);
    });
    committed([&](auto&) {
        installs().abandon_active(profile.binding,identity);
        address=storage().cancel_frozen_for_retry(address,1,frozen.revision).address;
        EXPECT_EQ(installs().read(profile.binding.channel)->last_sequence,1);
        EXPECT_FALSE(installs().read(profile.binding.channel)->last_installed);
    });
}

TEST_F(RecoveryObligationStore, CancelNeverReopensAfterActualInstallEvenWithoutJournalSettlement) {
    const auto kept=add(306);
    freeze();
    const auto frozen=scope();
    const auto identity=first();
    committed([&](auto&) {
        installs().apply_if_new(profile.binding,identity,{},[](auto&){});
        expect(error::stale,[&]{storage().cancel_frozen_for_retry(address,1,frozen.revision);});
        EXPECT_EQ(storage().read(profile.binding.channel),frozen);
        storage().settle_install(address,frozen.revision,identity,{positive(kept)});
    });
    const auto installed_scope=scope();
    committed([&](auto&) {
        expect(error::stale,[&]{storage().cancel_frozen_for_retry(address,1,installed_scope.revision);});
        EXPECT_EQ(storage().read(profile.binding.channel),installed_scope);
        EXPECT_EQ(installs().read(profile.binding.channel)->last_installed,identity);
    });
}

TEST_F(RecoveryObligationStore, CancelAfterPriorInstallPreservesItsExactResultAndFrontier) {
    const auto kept=add(307);
    freeze();
    const auto prior=first();
    install(prior,{positive(kept)});
    committed([&](auto&){address=storage().resume(address,prior).address;});
    freeze(2);
    const auto frozen=scope();
    committed([&](auto&) {
        const auto before=installs().read(profile.binding.channel);
        address=storage().cancel_frozen_for_retry(address,2,frozen.revision).address;
        const auto after=installs().read(profile.binding.channel);
        ASSERT_TRUE(before);
        ASSERT_TRUE(after);
        EXPECT_EQ(after->last_sequence,2);
        EXPECT_EQ(after->last_installed,before->last_installed);
        EXPECT_EQ(after->frontier,before->frontier);
        EXPECT_EQ(after->revision,before->revision);
        EXPECT_EQ(storage().find(address,kept.record.original_id)->stage,stage::settled);
    });
}

TEST_F(RecoveryObligationStore, CancelOuterRollbackRestoresFrozenJournalAndReceiverSequence) {
    add(308);
    freeze();
    const auto frozen=scope();
    std::optional<receive_install_snapshot> before;
    committed([&](auto&){before=installs().read(profile.binding.channel);});
    const auto failed=recovery_writer_access::install(owner,[&](auto&) {
        storage().cancel_frozen_for_retry(address,1,frozen.revision);
        throw std::runtime_error("cancel outer rollback");
    });
    EXPECT_EQ(failed.state,outcome::rolled_back);
    EXPECT_EQ(scope(),frozen);
    committed([&](auto&) {
        EXPECT_EQ(installs().read(profile.binding.channel),before);
        EXPECT_EQ(storage().snapshot_for_install(address,1).entries.size(),1u);
    });
}

TEST_F(RecoveryObligationStore, IgnoredCancelJournalWriteRollsBackItsReceiverRetirement) {
    freeze();
    const auto frozen=scope();
    committed([&](auto& db) {
        db.execute("CREATE TRIGGER reject_cancel BEFORE UPDATE ON _lattice_obligation_scope WHEN NEW.mode=0 BEGIN SELECT RAISE(IGNORE); END");
        EXPECT_THROW(storage().cancel_frozen_for_retry(address,1,frozen.revision),lattice::db_error);
        EXPECT_EQ(storage().read(profile.binding.channel),frozen);
        EXPECT_EQ(installs().read(profile.binding.channel)->last_sequence,0);
        db.execute("DROP TRIGGER reject_cancel");
        address=storage().cancel_frozen_for_retry(address,1,frozen.revision).address;
    });
}

TEST(RecoveryObligationStoreFile, CanceledAttemptAndPinnedOriginalSurvivePhysicalReopen) {
    TempDB file{"recovery_obligation_cancel_reopen"};
    const recovery_obligation_limits l{2,32,128,16384};
    const receive_install_limits il{2,128,4096};
    const recovery_obligation_profile p{{"c","a","s","e","scope","schema"},"profile","receipts"};
    recovery_obligation_address address;
    recovery_obligation_entry kept;
    {
        auto owner=owner_at(file.str());
        EXPECT_EQ(recovery_writer_access::install(owner,[&](auto& db) {
            receive_install_store receiver(owner,il);
            receiver.initialize();
            receiver.bind(p.binding);
            recovery_obligation_store s(owner,l,il);
            s.initialize();
            address=s.bind(p).address;
            kept=s.record(address,insert(db,309));
            s.claim_export(address,{kept.record.original_id});
            kept=*s.find(address,kept.record.original_id);
            const auto frozen=s.freeze(address,1);
            address=s.cancel_frozen_for_retry(frozen.address,1,frozen.revision).address;
        }).state,outcome::committed);
    }
    {
        auto owner=owner_at(file.str());
        EXPECT_EQ(recovery_writer_access::install(owner,[&](auto&) {
            recovery_obligation_store s(owner,l,il);
            s.initialize();
            const auto current=s.read(p.binding.channel);
            ASSERT_TRUE(current);
            EXPECT_EQ(current->address,address);
            EXPECT_EQ(current->mode,mode::recording);
            EXPECT_EQ(current->last_attempt,1);
            EXPECT_EQ(s.find(address,kept.record.original_id),kept);
            EXPECT_TRUE(s.pins_audit(kept.record.audit_id,kept.record.original_id));
            receive_install_store receiver(owner,il);
            receiver.initialize();
            EXPECT_EQ(receiver.read(p.binding.channel)->last_sequence,1);
            EXPECT_FALSE(receiver.read(p.binding.channel)->last_installed);
            EXPECT_EQ(s.freeze(address,2).last_attempt,2);
        }).state,outcome::committed);
    }
}
