#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/canonical_range_staging.hpp"
#include <algorithm>
#include <nlohmann/json.hpp>
#include <set>
#include <thread>

struct CanonicalStageKeptModel { std::string body; };
LATTICE_SCHEMA(CanonicalStageKeptModel,body);
namespace {
using namespace lattice::detail;
namespace cr=lattice::detail::canonical_range;
using blob=std::vector<uint8_t>;
std::string uuid(char last){return std::string("00000000-0000-4000-8000-00000000000")+last;}
cr::limits codec(){return {{16384,4096,2,256,512,1048576,128,256,1048576},16,8192,4096,128,256,32768,65536,60000,{4096,16,256,2048,4096}};}
canonical_staging_limits caps(){return {8,1024,4096,8388608,1024,4096,8388608,33554432};}
receive_install_limits install_caps(){return {8,256,1048576};}
lattice::configuration config(const std::string& path=":memory:"){lattice::configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=100;return c;}
struct Owned {
    lattice::lattice_db& owner;bool done=false;
    explicit Owned(lattice::lattice_db& o):owner(o){owner.begin_transaction();}
    ~Owned(){if(!done)try{owner.rollback();}catch(...) {}}
    void commit(){owner.commit();done=true;}
    void rollback(){owner.rollback();done=true;}
};
template<class F> void refusal(canonical_staging_code expected,F&& f){try{f();FAIL()<<"expected staging refusal";}catch(const canonical_staging_error& e){EXPECT_EQ(e.code,expected);}}
int64_t scalar(lattice::database& db,const std::string& sql){return std::get<int64_t>(db.query(sql).at(0).begin()->second);}
struct Bundle {
    cr::limits b=codec();cr::attempt a{uuid('1'),uuid('2'),"channel-A",1,uuid('5')};cr::request r;cr::manifest m;
    std::vector<cr::content_item> rows{{{"Person","A"},cr::present{R"({"name":{"kind":2,"value":"value"}})"}},{{"Person","B"},cr::tombstone{}}};
    std::vector<cr::receipt_item> receipts{{"op-A",cr::committed{"namespace-A","coverage-A",cr::decision::applied,7,cr::identity{"Person","A"}}}};
    Bundle(){r.source={"authority-A",uuid('3'),uuid('4'),std::string(64,'a'),std::string(64,'b')};r.selection=cr::mode::full;r.expected={0,r.source,{cr::frontier_kind::uninitialized,std::nullopt}};r.budget=b.maximum;r.receipts={{"op-A",std::string("namespace-A"),{{"Person","A"},{"Person","B"}}}};m.head=12;m.protection={"lease-A",30000};seal();}
    void seal(){
        r.request_digest=cr::request_sha256(a,r,b);m.request_digest=r.request_digest;m.source=r.source;m.selection=r.selection;m.base=r.base;m.counts={};
        m.counts.content_pages=rows.empty()?0:1+(rows.size()-1)/r.budget.items_per_page;m.counts.identities=rows.size();
        for(const auto& x:rows){m.counts.content_bytes+=cr::content_record_bytes(x,b);if(std::holds_alternative<cr::present>(x.value))++m.counts.present;else ++m.counts.tombstones;}
        m.counts.receipt_pages=receipts.empty()?0:1+(receipts.size()-1)/r.budget.items_per_page;m.counts.receipts=receipts.size();for(const auto& x:receipts)m.counts.receipt_bytes+=cr::receipt_record_bytes(x,b);
        std::set<std::pair<std::string,std::string>> targets;for(const auto& x:r.receipts)for(const auto& t:x.targets)targets.emplace(t.table,t.id);
        m.counts.rebase_identities=targets.size();for(const auto& [t,id]:targets)m.counts.rebase_bytes+=16+t.size()+id.size();
        m.content_digest=m.receipt_digest=m.rebase_digest=std::string(64,'0');m.rebase_digest=cr::rebase_sha256(a,r,b);m.content_digest=cr::content_sha256(m,rows,b);m.receipt_digest=cr::receipts_sha256(m,receipts,b);m.manifest_digest=cr::manifest_sha256(m,b);
    }
    receive_install_binding binding() const {const auto& s=r.source;return {a.channel,s.authority,s.source_id,s.epoch,s.scope_digest,s.schema_digest};}
    cr::frame content(uint64_t index=0,uint64_t route=1) const {
        const auto first=index*r.budget.items_per_page;const auto end=std::min<uint64_t>(rows.size(),first+r.budget.items_per_page);
        cr::content_page p;p.manifest_digest=m.manifest_digest;p.index=index;p.items.assign(rows.begin()+first,rows.begin()+end);p.count=p.items.size();for(const auto& x:p.items)p.bytes+=cr::content_record_bytes(x,b);p.digest=cr::page_sha256(p,b);return {a,route,p};
    }
    cr::frame receipt(uint64_t route=1) const {cr::receipt_page p;p.manifest_digest=m.manifest_digest;p.count=receipts.size();p.items=receipts;for(const auto& x:p.items)p.bytes+=cr::receipt_record_bytes(x,b);p.digest=cr::page_sha256(p,b);return {a,route,p};}
    cr::frame ending(uint64_t route=1) const{return {a,route,cr::end{m.manifest_digest}};}
};
class CanonicalRangeStaging : public ::testing::Test {
protected:
    std::shared_ptr<lattice::lattice_db> owner=std::make_shared<lattice::lattice_db>(config());
    Bundle x;canonical_staging_limits limits=caps();receive_install_limits il=install_caps();std::unique_ptr<canonical_range_staging> staged;
    void initialize(){Owned tx(*owner);receive_install_store install(owner,il);install.initialize();install.bind(x.binding());staged=std::make_unique<canonical_range_staging>(owner,il,x.b,limits);staged->initialize();tx.commit();}
    canonical_staging_begin begin(){return staged->begin(x.a,x.r,x.m,1);}
    canonical_staging_snapshot finish(uint64_t route=1){for(uint64_t i=0;i<x.m.counts.content_pages;++i)staged->append(x.content(i,route));if(!x.receipts.empty())staged->append(x.receipt(route));return staged->verify_end(x.ending(route));}
    receive_install_snapshot installed(){return *receive_install_store(owner,il).read(x.a.channel);}
    void next(uint64_t sequence){x.a.sequence=sequence;x.a.attempt_id=uuid('6');x.r.expected={static_cast<uint64_t>(installed().revision),x.r.source,{cr::frontier_kind::position,x.m.head}};x.r.selection=cr::mode::delta;x.r.base=x.m.head;x.seal();}
};
}

TEST_F(CanonicalRangeStaging, OwnedFullPagesVerifyBothStreamsAndReturnOwnedPages) {
    initialize();Owned tx(*owner);const auto accepted=begin();ASSERT_TRUE(accepted.staged);EXPECT_FALSE(accepted.staged->content_verified);
    auto verified=finish();EXPECT_TRUE(verified.content_verified);EXPECT_EQ(verified.state.status,cr::phase::sequence_complete_unverified);
    auto page=staged->read_verified_page(x.a,x.m.manifest_digest,1,cr::stream_kind::content,0);
    EXPECT_EQ(std::get<cr::content_page>(page),std::get<cr::content_page>(x.content().body));
    std::get<cr::content_page>(page).items.clear();EXPECT_EQ(std::get<cr::content_page>(staged->read_verified_page(x.a,x.m.manifest_digest,1,cr::stream_kind::content,0)).items.size(),2);
    EXPECT_EQ(installed().revision,0);EXPECT_TRUE(installed().active);staged->audit();tx.commit();
}

TEST_F(CanonicalRangeStaging, RequiresActualOwnedWriterAndRetainsActualOwner) {
    initialize();refusal(canonical_staging_code::transaction_required,[&]{staged->usage();});
    owner->db().execute("BEGIN IMMEDIATE");refusal(canonical_staging_code::transaction_required,[&]{staged->usage();});owner->db().execute("ROLLBACK");
    {Owned tx(*owner);std::thread other([&]{refusal(canonical_staging_code::transaction_required,[&]{staged->usage();});});other.join();tx.commit();}
    std::weak_ptr<lattice::lattice_db> weak=owner;owner.reset();ASSERT_FALSE(weak.expired());auto retained=weak.lock();
    {Owned tx(*retained);EXPECT_EQ(staged->usage().channels,0);tx.commit();}retained.reset();staged.reset();EXPECT_TRUE(weak.expired());
}

TEST_F(CanonicalRangeStaging, NullExpectedBindingAndUnboundChannelRefuseBeforeReservation) {
    initialize();Owned tx(*owner);const auto before=staged->usage();x.r.expected.binding.reset();x.seal();
    refusal(canonical_staging_code::stale_attempt,[&]{begin();});EXPECT_EQ(staged->usage(),before);EXPECT_FALSE(installed().active);
    x.r.expected.binding=x.r.source;x.a.channel="missing-channel";x.seal();EXPECT_THROW(begin(),receive_install_error);EXPECT_EQ(staged->usage(),before);tx.commit();
}

TEST_F(CanonicalRangeStaging, DeclarationCapFailureRollsBackInstallationBeginAndUsage) {
    limits.identities=1;initialize();Owned tx(*owner);const auto before=staged->usage();
    refusal(canonical_staging_code::capacity,[&]{begin();});EXPECT_EQ(staged->usage(),before);EXPECT_FALSE(installed().active);EXPECT_EQ(installed().last_sequence,0);tx.commit();
}

TEST_F(CanonicalRangeStaging, OuterRollbackRemovesActiveReservationAndAllPageEffects) {
    initialize();canonical_staging_usage before;{Owned tx(*owner);before=staged->usage();tx.commit();}
    {Owned tx(*owner);begin();staged->append(x.content());tx.rollback();}
    Owned tx(*owner);EXPECT_EQ(staged->usage(),before);EXPECT_FALSE(installed().active);EXPECT_EQ(installed().last_sequence,0);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_range_page"),0);begin();EXPECT_TRUE(finish().content_verified);tx.commit();
}

TEST_F(CanonicalRangeStaging, ExactDuplicateDoesNotRechargeAndConflictingPageRefuses) {
    initialize();Owned tx(*owner);begin();const auto first=staged->append(x.content());const auto used=staged->usage();
    EXPECT_EQ(staged->append(x.content()).state,first.state);EXPECT_EQ(staged->usage(),used);
    auto changed=x.content();auto& page=std::get<cr::content_page>(changed.body);std::get<cr::present>(page.items[0].value).payload=R"({"name":{"kind":2,"value":"other"}})";page.digest=cr::page_sha256(page,x.b);
    refusal(canonical_staging_code::conflicting_page,[&]{staged->append(changed);});EXPECT_EQ(staged->usage(),used);tx.commit();
}

TEST_F(CanonicalRangeStaging, RebindKeepsFrozenBytesAndRejectsOldRouteEvenForDuplicate) {
    initialize();Owned tx(*owner);begin();const auto old=staged->append(x.content());const auto used=staged->usage();
    const auto rebound=staged->rebind(x.a,x.m.manifest_digest,1,2);EXPECT_EQ(rebound.state,old.state);EXPECT_EQ(staged->usage(),used);
    refusal(canonical_staging_code::stale_route,[&]{staged->append(x.content());});
    EXPECT_EQ(staged->append(x.content(0,2)).state,old.state);EXPECT_EQ(staged->usage(),used);
    refusal(canonical_staging_code::stale_route,[&]{staged->rebind(x.a,x.m.manifest_digest,1,3);});
    staged->append(x.receipt(2));EXPECT_TRUE(staged->verify_end(x.ending(2)).content_verified);tx.commit();
}

TEST_F(CanonicalRangeStaging, DifferentLogicalAttemptAndManifestNeverReplaceActive) {
    initialize();Owned tx(*owner);const auto first=begin();auto saved=x;const auto usage=staged->usage();x.a.attempt_id=uuid('8');x.seal();
    EXPECT_THROW(begin(),receive_install_error);EXPECT_EQ(staged->usage(),usage);
    refusal(canonical_staging_code::stale_attempt,[&]{staged->append(x.content());});
    x=saved;EXPECT_EQ(begin().staged->state,first.staged->state);tx.commit();
}

TEST_F(CanonicalRangeStaging, CorruptActualPageAndMissingPageRefuseVerifiedResume) {
    initialize();Owned tx(*owner);begin();finish();owner->db().execute("SAVEPOINT test_corruption");
    owner->db().execute("UPDATE _lattice_range_page SET wire=zeroblob(20000) WHERE stream=0");
    refusal(canonical_staging_code::corrupt_state,[&]{staged->resume(x.a,x.m.manifest_digest,1);});owner->db().execute("ROLLBACK TO test_corruption");
    owner->db().execute("DELETE FROM _lattice_range_page WHERE stream=0");
    refusal(canonical_staging_code::corrupt_state,[&]{staged->resume(x.a,x.m.manifest_digest,1);});owner->db().execute("ROLLBACK TO test_corruption");owner->db().execute("RELEASE test_corruption");
    EXPECT_TRUE(staged->resume(x.a,x.m.manifest_digest,1).content_verified);tx.commit();
}

TEST_F(CanonicalRangeStaging, WrongWholeContentWithValidPageHashNeverBecomesVerified) {
    x.m.content_digest=std::string(64,'e');x.m.manifest_digest=cr::manifest_sha256(x.m,x.b);initialize();Owned tx(*owner);begin();staged->append(x.content());staged->append(x.receipt());const auto before=staged->usage();
    refusal(canonical_staging_code::digest_mismatch,[&]{staged->verify_end(x.ending());});EXPECT_EQ(staged->usage(),before);EXPECT_EQ(scalar(owner->db(),"SELECT verified FROM _lattice_range_attempt"),0);
    refusal(canonical_staging_code::not_verified,[&]{staged->read_verified_page(x.a,x.m.manifest_digest,1,cr::stream_kind::content,0);});tx.commit();
}

TEST_F(CanonicalRangeStaging, WrongWholeReceiptDigestRefusesIndependentOfContent) {
    x.m.receipt_digest=std::string(64,'e');x.m.manifest_digest=cr::manifest_sha256(x.m,x.b);initialize();Owned tx(*owner);begin();staged->append(x.content());staged->append(x.receipt());
    refusal(canonical_staging_code::digest_mismatch,[&]{staged->verify_end(x.ending());});EXPECT_EQ(scalar(owner->db(),"SELECT verified FROM _lattice_range_attempt"),0);tx.commit();
}

TEST_F(CanonicalRangeStaging, MissingRebaseCoverageRefusesWithoutProgress) {
    x.rows={{{"Person","B"},cr::present{R"({"name":{"kind":2,"value":"value"}})"}},{{"Person","C"},cr::present{R"({"name":{"kind":2,"value":"value"}})"}}};x.seal();initialize();Owned tx(*owner);begin();const auto before=staged->usage();
    EXPECT_THROW(staged->append(x.content()),cr::protocol_error);EXPECT_EQ(staged->usage(),before);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_range_page"),0);tx.commit();
}

TEST_F(CanonicalRangeStaging, IgnoredCounterWriteRollsBackPageAndProgress) {
    initialize();Owned tx(*owner);const auto first=begin();const auto before=staged->usage();
    owner->db().execute("CREATE TRIGGER _range_ignore BEFORE UPDATE ON _lattice_range_store BEGIN SELECT RAISE(IGNORE); END");
    refusal(canonical_staging_code::corrupt_state,[&]{staged->append(x.content());});EXPECT_EQ(staged->usage(),before);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_range_page"),0);
    owner->db().execute("DROP TRIGGER _range_ignore");EXPECT_EQ(staged->resume(x.a,x.m.manifest_digest,1).state,first.staged->state);EXPECT_TRUE(finish().content_verified);tx.commit();
}

TEST_F(CanonicalRangeStaging, AbandonBeforeFirstInstallReleasesCapsButNeverReusesSequence) {
    limits.channels=1;initialize();Owned tx(*owner);const auto empty=staged->usage();const auto first=begin();staged->append(x.content());
    auto kept=owner->add(CanonicalStageKeptModel{"pending-must-survive"});const auto audits=scalar(owner->db(),"SELECT COUNT(*) FROM AuditLog");
    staged->abandon_active(x.a,x.m.manifest_digest,1);EXPECT_EQ(staged->usage(),empty);auto state=installed();EXPECT_EQ(state.revision,0);EXPECT_EQ(state.last_sequence,1);EXPECT_FALSE(state.active);EXPECT_FALSE(state.last_installed);
    EXPECT_THROW(receive_install_store(owner,il).complete(x.binding(),first.staged->installation_identity),receive_install_error);
    EXPECT_THROW(begin(),receive_install_error);refusal(canonical_staging_code::stale_attempt,[&]{staged->append(x.content());});
    EXPECT_EQ(std::string(kept.body),"pending-must-survive");EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM AuditLog"),audits);
    x.a.sequence=2;x.a.attempt_id=uuid('6');x.seal();EXPECT_TRUE(begin().staged);EXPECT_TRUE(finish().content_verified);tx.commit();
}

TEST_F(CanonicalRangeStaging, AbandonAfterInstalledRetainsExactResultAndHigherWater) {
    initialize();Owned tx(*owner);begin();const auto first=finish();receive_install_store store(owner,il);store.complete(first.installation_binding,first.installation_identity);const auto prior=x;
    staged->release_installed(x.a,x.m.manifest_digest,1);next(2);begin();staged->append(x.content());staged->abandon_active(x.a,x.m.manifest_digest,1);
    const auto state=installed();EXPECT_EQ(state.revision,1);EXPECT_EQ(state.last_sequence,2);EXPECT_EQ(state.last_installed,std::optional<receive_install_identity>{first.installation_identity});EXPECT_FALSE(state.active);
    auto retry=staged->begin(prior.a,prior.r,prior.m,1);EXPECT_EQ(retry.disposition,receive_install_disposition::already_installed);EXPECT_FALSE(retry.staged);
    EXPECT_THROW(begin(),receive_install_error);next(3);begin();const auto third=finish();store.complete(third.installation_binding,third.installation_identity,first.installation_identity);EXPECT_EQ(installed().revision,2);EXPECT_EQ(installed().last_sequence,3);tx.commit();
}

TEST_F(CanonicalRangeStaging, AbandonRollbackRestoresExactPagesActiveAndReservations) {
    initialize();{Owned tx(*owner);begin();staged->append(x.content());tx.commit();}
    canonical_staging_usage before;receive_install_snapshot installed_before;{Owned tx(*owner);before=staged->usage();installed_before=installed();tx.commit();}
    {Owned tx(*owner);staged->abandon_active(x.a,x.m.manifest_digest,1);tx.rollback();}
    Owned tx(*owner);EXPECT_EQ(staged->usage(),before);EXPECT_EQ(installed(),installed_before);EXPECT_EQ(staged->resume(x.a,x.m.manifest_digest,1).state.next_content_page,1);staged->append(x.receipt());EXPECT_TRUE(staged->verify_end(x.ending()).content_verified);tx.commit();
}

TEST_F(CanonicalRangeStaging, FailedCleanupRollsBackReceiverAbandonAndRetainsData) {
    initialize();Owned tx(*owner);begin();staged->append(x.content());const auto before=installed();const auto used=staged->usage();
    owner->db().execute("CREATE TRIGGER _range_delete_ignore BEFORE DELETE ON _lattice_range_page BEGIN SELECT RAISE(IGNORE); END");
    refusal(canonical_staging_code::corrupt_state,[&]{staged->abandon_active(x.a,x.m.manifest_digest,1);});EXPECT_EQ(installed(),before);EXPECT_EQ(staged->usage(),used);
    owner->db().execute("DROP TRIGGER _range_delete_ignore");EXPECT_EQ(staged->resume(x.a,x.m.manifest_digest,1).state.next_content_page,1);tx.commit();
}

TEST_F(CanonicalRangeStaging, InstalledReleaseRequiresExactResultAndPreservesModelAuditState) {
    initialize();Owned tx(*owner);const auto empty=staged->usage();begin();const auto complete=finish();
    refusal(canonical_staging_code::stale_attempt,[&]{staged->release_installed(x.a,x.m.manifest_digest,1);});
    auto row=owner->add(CanonicalStageKeptModel{"not-staging"});const auto audits=scalar(owner->db(),"SELECT COUNT(*) FROM AuditLog");
    receive_install_store(owner,il).complete(complete.installation_binding,complete.installation_identity);const auto state=installed();
    staged->release_installed(x.a,x.m.manifest_digest,1);EXPECT_EQ(staged->usage(),empty);EXPECT_EQ(installed(),state);EXPECT_EQ(std::string(row.body),"not-staging");EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM AuditLog"),audits);
    auto retry=begin();EXPECT_EQ(retry.disposition,receive_install_disposition::already_installed);EXPECT_FALSE(retry.staged);tx.commit();
}

TEST_F(CanonicalRangeStaging, EmptyFullThenSameHeadDeltaStillHaveDistinctInstallIdentity) {
    x.rows.clear();x.receipts.clear();x.r.receipts.clear();x.seal();initialize();Owned tx(*owner);begin();const auto first=finish();
    receive_install_store(owner,il).complete(first.installation_binding,first.installation_identity);staged->release_installed(x.a,x.m.manifest_digest,1);
    next(2);begin();const auto second=finish();EXPECT_NE(first.installation_identity.request_digest,second.installation_identity.request_digest);EXPECT_EQ(first.installation_identity.head,second.installation_identity.head);
    receive_install_store(owner,il).complete(second.installation_binding,second.installation_identity,first.installation_identity);EXPECT_EQ(installed().revision,2);tx.commit();
}

TEST_F(CanonicalRangeStaging, ForgedCompleteStateWithoutPagesIsNotVerification) {
    initialize();Owned tx(*owner);const auto first=begin();auto fake=cr::propose(first.staged->state,x.content(),x.b);fake=cr::propose(fake,x.receipt(),x.b);fake=cr::propose(fake,x.ending(),x.b);const auto image=cr::encode_state(fake,x.b);
    const auto old=scalar(owner->db(),"SELECT length(state) FROM _lattice_range_attempt");
    owner->db().execute("UPDATE _lattice_range_attempt SET state=?,verified=1",{blob(image.begin(),image.end())});
    owner->db().execute("UPDATE _lattice_range_store SET stored_bytes=stored_bytes+?",{static_cast<int64_t>(image.size())-old});
    refusal(canonical_staging_code::corrupt_state,[&]{staged->resume(x.a,x.m.manifest_digest,1);});EXPECT_EQ(installed().revision,0);tx.rollback();
}

TEST_F(CanonicalRangeStaging, ExtraAndOrphanPagesCannotHideBehindCounters) {
    initialize();Owned tx(*owner);begin();finish();owner->db().execute("SAVEPOINT extra_page");
    owner->db().execute("INSERT INTO _lattice_range_page SELECT channel,stream,page_index+10,wire FROM _lattice_range_page WHERE stream=0");
    refusal(canonical_staging_code::corrupt_state,[&]{staged->audit();});owner->db().execute("ROLLBACK TO extra_page");
    owner->db().execute("INSERT INTO _lattice_range_page SELECT X'6f727068616e',stream,page_index,wire FROM _lattice_range_page WHERE stream=0");
    refusal(canonical_staging_code::corrupt_state,[&]{staged->audit();});owner->db().execute("ROLLBACK TO extra_page");owner->db().execute("RELEASE extra_page");EXPECT_TRUE(staged->resume(x.a,x.m.manifest_digest,1).content_verified);tx.commit();
}

TEST_F(CanonicalRangeStaging, ReceiptNamespaceFailureDoesNotPersistReceiptPage) {
    initialize();Owned tx(*owner);begin();staged->append(x.content());const auto used=staged->usage();auto receipt=x.receipt();auto& page=std::get<cr::receipt_page>(receipt.body);std::get<cr::committed>(page.items[0].value).namespace_id="namespace-B";page.digest=cr::page_sha256(page,x.b);
    EXPECT_THROW(staged->append(receipt),cr::protocol_error);EXPECT_EQ(staged->usage(),used);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_range_page WHERE stream=1"),0);staged->append(x.receipt());EXPECT_TRUE(staged->verify_end(x.ending()).content_verified);tx.commit();
}

TEST_F(CanonicalRangeStaging, StoredBudgetChargesEscapedWireAndStateNotOnlyCanonicalValues) {
    x.rows[0].value=cr::present{R"({"name":{"kind":2,"value":"quote:\" slash:\\ newline:\n"}})"};x.seal();initialize();Owned tx(*owner);begin();const auto before=staged->usage();
    const auto old_state=scalar(owner->db(),"SELECT length(state) FROM _lattice_range_attempt");const auto after=staged->append(x.content());const auto used=staged->usage();
    const auto new_state=static_cast<int64_t>(cr::encode_state(after.state,x.b).size());const auto wire=static_cast<int64_t>(cr::encode(x.content(),x.b).size());
    EXPECT_EQ(used.stored_bytes-before.stored_bytes,static_cast<int64_t>(x.a.channel.size())+wire+new_state-old_state);
    EXPECT_GT(wire,static_cast<int64_t>(x.m.counts.content_bytes));staged->audit();tx.commit();
}

TEST_F(CanonicalRangeStaging, SameCountRepartitionCannotReplaceRetainedPageIndex) {
    x.rows.push_back({{"Person","C"},cr::present{R"({"name":{"kind":2,"value":"value"}})"}});x.seal();initialize();Owned tx(*owner);begin();
    auto first=x.content();auto& page=std::get<cr::content_page>(first.body);page.items.resize(1);page.count=1;page.bytes=cr::content_record_bytes(page.items[0],x.b);page.digest=cr::page_sha256(page,x.b);staged->append(first);const auto used=staged->usage();
    refusal(canonical_staging_code::conflicting_page,[&]{staged->append(x.content());});EXPECT_EQ(staged->usage(),used);tx.commit();
}

TEST_F(CanonicalRangeStaging, IndependentReservationStreamsAndConfigurationMustMatch) {
    limits.receipts=0;initialize();Owned tx(*owner);const auto before=staged->usage();refusal(canonical_staging_code::capacity,[&]{begin();});EXPECT_EQ(staged->usage(),before);EXPECT_FALSE(installed().active);
    auto changed=limits;++changed.channels;canonical_range_staging other(owner,il,x.b,changed);refusal(canonical_staging_code::limits_mismatch,[&]{other.initialize();});tx.commit();
}

TEST(CanonicalRangeHash, StreamFacadeMatchesIndependentGoldenAcrossPages) {
    Bundle x;x.r.budget={16384,4096,2,8,16,65536,8,16,65536};x.a.sequence=5;x.r.selection=cr::mode::delta;x.r.base=10;x.r.expected={4,x.r.source,{cr::frontier_kind::position,10}};x.r.receipts[0].targets.resize(1);x.seal();
    cr::stream_hasher content(x.m,cr::stream_kind::content,x.b),receipts(x.m,cr::stream_kind::receipts,x.b);
    for(const auto& row:x.rows)content.append(row);for(const auto& row:x.receipts)receipts.append(row);
    EXPECT_EQ(content.finish(),"748c3b98eec56e4e9cc909654e334a37a6c56e0df20fc8a808ce9e2c1de99a03");
    EXPECT_EQ(receipts.finish(),"56e76613b67f5cd51cb776605c5828c49d20dc740195aa1813b2905ce5d35417");
    EXPECT_THROW(content.finish(),cr::protocol_error);
    EXPECT_THROW(receipts.append(x.receipts[0]),cr::protocol_error);
}

TEST(CanonicalRangeHash, StreamFacadeRefusesPrematureFinishWrongStreamOrderAndTotals) {
    Bundle x;cr::stream_hasher empty(x.m,cr::stream_kind::content,x.b);EXPECT_THROW(empty.finish(),cr::protocol_error);
    cr::stream_hasher wrong(x.m,cr::stream_kind::content,x.b);EXPECT_THROW(wrong.append(x.receipts[0]),cr::protocol_error);
    cr::stream_hasher duplicate(x.m,cr::stream_kind::content,x.b);duplicate.append(x.rows[0]);EXPECT_THROW(duplicate.append(x.rows[0]),cr::protocol_error);
    auto m=x.m;++m.counts.content_bytes;cr::stream_hasher bytes(m,cr::stream_kind::content,x.b);for(const auto& r:x.rows)bytes.append(r);EXPECT_THROW(bytes.finish(),cr::protocol_error);
}

TEST(CanonicalRangeStagingFile, ReopenPartialVerifiedAbandonedAndNextAttempt) {
    TempDB file{"canonical_range_staging"};Bundle x;const auto il=install_caps();
    auto open=[&]{auto owner=std::make_shared<lattice::lattice_db>(config(file.str()));auto* n=lattice::instance_registry::instance().get_or_create_notifier(file.str());if(n)n->stop_listening();return owner;};
    {auto owner=open();Owned tx(*owner);receive_install_store install(owner,il);install.initialize();install.bind(x.binding());canonical_range_staging staged(owner,il,x.b,caps());staged.initialize();staged.begin(x.a,x.r,x.m,1);staged.append(x.content());tx.commit();}
    {auto owner=open();Owned tx(*owner);canonical_range_staging staged(owner,il,x.b,caps());staged.initialize();EXPECT_FALSE(staged.resume(x.a,x.m.manifest_digest,1).content_verified);staged.append(x.receipt());EXPECT_TRUE(staged.verify_end(x.ending()).content_verified);tx.commit();}
    {auto owner=open();Owned tx(*owner);canonical_range_staging staged(owner,il,x.b,caps());staged.initialize();EXPECT_TRUE(staged.resume(x.a,x.m.manifest_digest,1).content_verified);staged.abandon_active(x.a,x.m.manifest_digest,1);tx.commit();}
    {auto owner=open();Owned tx(*owner);canonical_range_staging staged(owner,il,x.b,caps());staged.initialize();const auto prior=receive_install_store(owner,il).read(x.a.channel);ASSERT_TRUE(prior);EXPECT_EQ(prior->last_sequence,1);EXPECT_EQ(prior->revision,0);EXPECT_FALSE(prior->active);EXPECT_EQ(staged.usage().channels,0);
     EXPECT_THROW(staged.begin(x.a,x.r,x.m,1),receive_install_error);x.a.sequence=2;x.a.attempt_id=uuid('6');x.seal();EXPECT_TRUE(staged.begin(x.a,x.r,x.m,1).staged);tx.commit();}
}

TEST_F(CanonicalRangeStaging, IgnoredAttemptInsertRollsBackNewInstallationSequence) {
    initialize();Owned tx(*owner);const auto before=staged->usage();
    owner->db().execute("CREATE TRIGGER _range_ignore_attempt BEFORE INSERT ON _lattice_range_attempt BEGIN SELECT RAISE(IGNORE); END");
    refusal(canonical_staging_code::corrupt_state,[&]{begin();});EXPECT_EQ(staged->usage(),before);EXPECT_EQ(installed().last_sequence,0);EXPECT_FALSE(installed().active);
    owner->db().execute("DROP TRIGGER _range_ignore_attempt");EXPECT_TRUE(begin().staged);tx.commit();
}

TEST_F(CanonicalRangeStaging, ActualStoredByteLimitRefusesEscapedPageAtomically) {
    x.rows[0].value=cr::present{R"({"name":{"kind":2,"value":"quote:\" slash:\\ newline:\n"}})"};x.seal();initialize();
    nlohmann::json configuration;
    {Owned tx(*owner);const auto data=std::get<blob>(owner->db().query("SELECT configuration FROM _lattice_range_store").at(0).at("configuration"));configuration=nlohmann::json::parse(data);tx.commit();}
    const auto initial=cr::begin(x.a,x.r,x.m,x.b);const auto page=x.content();const auto next=cr::propose(initial,page,x.b);
    const auto logical=cr::encode({x.a,1,cr::end{x.m.manifest_digest}},x.b);
    // Exact first-page postimage storage, including JSON escapes/state growth;
    // choose one byte less and recompute the configuration's own decimal size.
    const int64_t rest=2*static_cast<int64_t>(x.a.channel.size())+logical.size()+cr::encode_state(next,x.b).size()+cr::encode(page,x.b).size();
    int64_t cap=rest+static_cast<int64_t>(configuration.dump().size())-1;
    for(int i=0;i<8;++i){configuration["staging"][7]=cap;const auto updated=rest+static_cast<int64_t>(configuration.dump().size())-1;if(updated==cap)break;cap=updated;}
    staged.reset();owner=std::make_shared<lattice::lattice_db>(config());limits.stored_bytes=cap;initialize();
    Owned tx(*owner);const auto first=begin();const auto before=staged->usage();
    refusal(canonical_staging_code::capacity,[&]{staged->append(page);});EXPECT_EQ(staged->usage(),before);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_range_page"),0);
    EXPECT_EQ(staged->resume(x.a,x.m.manifest_digest,1).state,first.staged->state);tx.commit();
}

TEST_F(CanonicalRangeStaging, VerifiedPointReadRefusesMissingPageAndStaleRoute) {
    initialize();Owned tx(*owner);begin();finish();
    refusal(canonical_staging_code::stale_route,[&]{staged->read_verified_page(x.a,x.m.manifest_digest,2,cr::stream_kind::content,0);});
    refusal(canonical_staging_code::invalid_argument,[&]{staged->read_verified_page(x.a,x.m.manifest_digest,1,cr::stream_kind::receipts,1);});
    owner->db().execute("DELETE FROM _lattice_range_page WHERE stream=0");
    refusal(canonical_staging_code::corrupt_state,[&]{staged->read_verified_page(x.a,x.m.manifest_digest,1,cr::stream_kind::content,0);});tx.rollback();
}


TEST_F(CanonicalRangeStaging, FailedProgressWriteRollsBackPageAndAllowsExactRetry) {
    initialize();Owned tx(*owner);const auto first=begin();const auto before=staged->usage();const auto receiver=installed();
    owner->db().execute("CREATE TRIGGER _range_fail_progress BEFORE UPDATE ON _lattice_range_attempt BEGIN SELECT RAISE(ABORT,'range progress fault'); END");
    EXPECT_THROW(staged->append(x.content()),lattice::db_error);
    EXPECT_EQ(staged->usage(),before);EXPECT_EQ(installed(),receiver);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_range_page"),0);
    owner->db().execute("DROP TRIGGER _range_fail_progress");EXPECT_EQ(staged->resume(x.a,x.m.manifest_digest,1).state,first.staged->state);
    EXPECT_TRUE(finish().content_verified);tx.commit();
}

TEST_F(CanonicalRangeStaging, AppendAndPointReadHaveFixedSQLiteWorkDespiteRetainedPages) {
    x.b.maximum.items_per_page=1;x.b.maximum.content_pages=4096;x.b.maximum.content_identities=4096;
    x.r.budget=x.b.maximum;x.r.receipts.clear();x.receipts.clear();x.rows.clear();limits.content_pages=4096;limits.identities=4096;
    for(int i=0;i<2049;++i)x.rows.push_back({{"Person","id"+std::to_string(100000+i)},cr::present{R"({"name":{"kind":2,"value":"value"}})"}});
    x.seal();initialize();Owned tx(*owner);begin();
    for(uint64_t i=0;i<2048;++i)staged->append(x.content(i));
    // Setup and whole verification are deliberately outside the VM ceiling.
    auto* handle=owner->db().handle();int callbacks=0;
    const auto ceiling=[&]{callbacks=0;sqlite3_progress_handler(handle,10,[](void* p) noexcept {return ++*static_cast<int*>(p)>2000?1:0;},&callbacks);};
    ceiling();EXPECT_NO_THROW(staged->append(x.content(2048)));sqlite3_progress_handler(handle,0,nullptr,nullptr);
    EXPECT_LE(callbacks,2000)<<"append must not recount/rehash the retained page set";
    ASSERT_TRUE(staged->verify_end(x.ending()).content_verified);
    for(const uint64_t index:{uint64_t{0},uint64_t{1024},uint64_t{2048}}){
        ceiling();std::optional<cr::message> page;EXPECT_NO_THROW(page=staged->read_verified_page(x.a,x.m.manifest_digest,1,cr::stream_kind::content,index));
        sqlite3_progress_handler(handle,0,nullptr,nullptr);EXPECT_LE(callbacks,2000)<<"each indexed read has its own fixed VM budget";
        ASSERT_TRUE(page);EXPECT_EQ(std::get<cr::content_page>(*page),std::get<cr::content_page>(x.content(index).body));
    }
    tx.commit();
}


TEST(CanonicalRangeStagingFile, ReopenAfterAbandonRetainsPriorInstalledEvidence) {
    TempDB file{"canonical_range_abandon"};Bundle first;Bundle active=first;std::optional<receive_install_identity> installed_identity,abandoned_identity;
    const auto il=install_caps();
    auto open=[&]{auto owner=std::make_shared<lattice::lattice_db>(config(file.str()));auto* n=lattice::instance_registry::instance().get_or_create_notifier(file.str());if(n)n->stop_listening();return owner;};
    {auto owner=open();Owned tx(*owner);receive_install_store install(owner,il);install.initialize();install.bind(first.binding());canonical_range_staging staged(owner,il,first.b,caps());staged.initialize();
     staged.begin(first.a,first.r,first.m,1);staged.append(first.content());staged.append(first.receipt());const auto verified=staged.verify_end(first.ending());installed_identity=verified.installation_identity;
     install.complete(first.binding(),*installed_identity);staged.release_installed(first.a,first.m.manifest_digest,1);
     active.a.sequence=2;active.a.attempt_id=uuid('6');active.r.expected={1,active.r.source,{cr::frontier_kind::position,first.m.head}};active.seal();
     abandoned_identity=staged.begin(active.a,active.r,active.m,1).staged->installation_identity;staged.append(active.content());tx.commit();}
    {auto owner=open();Owned tx(*owner);canonical_range_staging staged(owner,il,first.b,caps());staged.initialize();EXPECT_EQ(staged.resume(active.a,active.m.manifest_digest,1).state.next_content_page,1);
     staged.abandon_active(active.a,active.m.manifest_digest,1);tx.commit();}
    {auto owner=open();Owned tx(*owner);receive_install_store install(owner,il);canonical_range_staging staged(owner,il,first.b,caps());staged.initialize();const auto state=install.read(first.a.channel);ASSERT_TRUE(state);
     EXPECT_EQ(state->revision,1);EXPECT_EQ(state->last_sequence,2);EXPECT_FALSE(state->active);EXPECT_EQ(state->last_installed,installed_identity);EXPECT_EQ(staged.usage().channels,0);
     const auto retry=staged.begin(first.a,first.r,first.m,1);EXPECT_EQ(retry.disposition,receive_install_disposition::already_installed);EXPECT_FALSE(retry.staged);
     EXPECT_THROW(staged.begin(active.a,active.r,active.m,1),receive_install_error);
     EXPECT_THROW(install.complete(active.binding(),*abandoned_identity,installed_identity),receive_install_error);tx.commit();}
}


TEST_F(CanonicalRangeStaging, CorruptNumericSlotsAreRejectedBeforeGenericBlobOrTextCopy) {
    initialize();Owned tx(*owner);begin();
    struct exposed_columns { int rows=0;int largest=0; } exposed;
    struct trace_guard {
        sqlite3* handle;
        trace_guard(sqlite3* h,exposed_columns& seen):handle(h){
            if(sqlite3_trace_v2(handle,SQLITE_TRACE_ROW,[](unsigned,void* context,void* value,void*) noexcept {
                auto* statement=static_cast<sqlite3_stmt*>(value);const auto* sql=sqlite3_sql(statement);
                if(!sql||!std::strstr(sql,"_lattice_range_"))return 0;
                auto& observed=*static_cast<exposed_columns*>(context);++observed.rows;
                // TRACE_ROW runs after sqlite3_step produces each result and
                // before database::query copies its columns. Inspect sizes only.
                for(int i=0;i<sqlite3_column_count(statement);++i){const auto type=sqlite3_column_type(statement,i);
                    if(type==SQLITE_BLOB||type==SQLITE_TEXT)observed.largest=std::max(observed.largest,sqlite3_column_bytes(statement,i));}
                return 0;
            },&seen)!=SQLITE_OK)throw std::runtime_error("column trace admission failed");
        }
        ~trace_guard(){sqlite3_trace_v2(handle,0,nullptr,nullptr);}
    };
    const auto check=[&](const char* table,const char* column,bool stored_text){
        owner->db().execute("SAVEPOINT corrupt_numeric");
        owner->db().execute(std::string("UPDATE ")+table+" SET "+column+"="+(stored_text?"CAST(zeroblob(262144) AS TEXT)":"zeroblob(262144)"));
        exposed={};
        {trace_guard trace(owner->db().handle(),exposed);
         refusal(canonical_staging_code::corrupt_state,[&]{
             if(std::string_view(table)=="_lattice_range_store")staged->usage();else staged->append(x.content());
         });}
        EXPECT_GT(exposed.rows,0)<<column;
        EXPECT_LE(exposed.largest,static_cast<int>(x.b.restart_bytes))<<column<<" exposed corrupt payload before type refusal";
        owner->db().execute("ROLLBACK TO corrupt_numeric");owner->db().execute("RELEASE corrupt_numeric");
    };
    for(const auto* column:{"version","channels","content_pages","identities","content_bytes","receipt_pages","receipts","receipt_bytes","stored_bytes"})
        for(const bool text:{false,true})check("_lattice_range_store",column,text);
    for(const auto* column:{"route","verified","content_pages","identities","content_bytes","receipt_pages","receipts","receipt_bytes","page_bytes"})
        for(const bool text:{false,true})check("_lattice_range_attempt",column,text);
    // Every refused corruption was rolled back, leaving the original attempt
    // usable; the oracle did not depend on a later unrelated setup failure.
    EXPECT_TRUE(finish().content_verified);staged->audit();tx.commit();
}
