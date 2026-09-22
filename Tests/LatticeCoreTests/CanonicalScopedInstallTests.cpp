#include "TestHelpers.hpp"
#include "../../Sources/LatticeCore/src/canonical_scoped_install.hpp"
#include "../../Sources/LatticeCore/src/recovery_refresh.hpp"
#include <algorithm>
#include <set>
#include <tuple>
#include <limits>
#include <type_traits>

namespace lattice::detail {
// Mechanical test mint only. There is no production issuer or assertion that
// these fixtures prove authenticated source, generated coverage or all routes.
struct canonical_scoped_install_test_access {
    static canonical_install_admission mint(std::shared_ptr<lattice_db> owner,
        canonical_range::attempt attempt,uint64_t route,std::string q,std::string m,
        recovery_obligation_profile profile,recovery_obligation_address journal,int64_t revision,
        canonical_scoped_contract contract,canonical_scoped_limits limits,
        std::optional<receive_install_identity> supersede,std::string coverage="coverage",
        std::optional<receive_guard_snapshot> receive_guard={}) {
        canonical_install_admission result;result.owner_=std::move(owner);result.attempt_=std::move(attempt);
        result.route_=route;result.request_digest_=std::move(q);result.manifest_digest_=std::move(m);
        result.coverage_id_=std::move(coverage);result.profile_=std::move(profile);result.journal_=std::move(journal);
        result.journal_revision_=revision;result.contract_=std::move(contract);result.limits_=std::move(limits);
        result.supersede_=std::move(supersede);result.receive_guard_=std::move(receive_guard);return result;
    }
};
}
static_assert(!std::is_default_constructible_v<lattice::detail::canonical_install_admission>);
namespace {
using namespace lattice::detail;
namespace cr=lattice::detail::canonical_range;
using rows=std::vector<lattice::database::row_t>;
using blob=std::vector<uint8_t>;
using state=recovery_install_state;
const std::string A="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",B="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const std::string D="dddddddd-dddd-4ddd-8ddd-dddddddddddd",L="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
std::string uuid(char c){return std::string("00000000-0000-4000-8000-00000000000")+c;}
std::string upper(std::string s){for(auto& c:s)if(c>='a'&&c<='f')c-=32;return s;}
canonical_scoped_limits caps(){return {
    {{8,4096,1048576},{512,512,32,64,512,1000000,65536,16000000},8,1024,1048576,512,512,1000000,65536,16000000},
    {{16384,4096,2,256,512,1048576,256,512,1048576},16,8192,4096,512,512,65536,262144,60000,{4096,16,256,2048,4096}},
    {8,1024,4096,8388608,1024,4096,8388608,33554432},{8,4096,4096,4194304}};}
lattice::configuration fixture_config(const std::string& path){lattice::configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=100;return c;}
class StagedOwner:public lattice::lattice_db {
public:
    explicit StagedOwner(const std::string& path=":memory:"):lattice_db(fixture_config(path)) {}
    void document() {
        lattice::model_schema s;s.table_name="StagedDocument";
        lattice::property_descriptor body;body.name="body";body.type=lattice::column_type::text;body.no_history=true;
        lattice::property_descriptor title;title.name="title";title.type=lattice::column_type::text;
        s.properties={body,title};create_model_table_public(s);
    }
};
struct Bundle {
    cr::attempt a{uuid('1'),uuid('2'),"staged-channel",1,uuid('5')};cr::request q;cr::manifest m;
    std::vector<cr::content_item> content;std::vector<cr::receipt_item> receipts;
    Bundle(){q.source={"authority",uuid('3'),uuid('4'),std::string(64,'a'),std::string(64,'b')};
        q.expected={0,q.source,{cr::frontier_kind::uninitialized,std::nullopt}};q.budget=caps().codec.maximum;
        m.head=10;m.protection={"lease",30000};}
    receive_install_binding binding() const {const auto& b=q.source;return {a.channel,b.authority,b.source_id,b.epoch,b.scope_digest,b.schema_digest};}
    void seal(const cr::limits& limits) {
        std::sort(content.begin(),content.end(),[](const auto& x,const auto& y){return std::tie(x.key.table,x.key.id)<std::tie(y.key.table,y.key.id);});
        std::sort(receipts.begin(),receipts.end(),[](const auto& x,const auto& y){return x.original_id<y.original_id;});
        std::sort(q.receipts.begin(),q.receipts.end(),[](const auto& x,const auto& y){return x.original_id<y.original_id;});
        q.request_digest=cr::request_sha256(a,q,limits);m.request_digest=q.request_digest;m.source=q.source;m.selection=q.selection;m.base=q.base;m.counts={};
        auto pages=[&](size_t n){return n?1+(n-1)/q.budget.items_per_page:0;};
        m.counts.content_pages=pages(content.size());m.counts.identities=content.size();
        for(const auto& x:content){m.counts.content_bytes+=cr::content_record_bytes(x,limits);if(std::holds_alternative<cr::present>(x.value))++m.counts.present;else ++m.counts.tombstones;}
        m.counts.receipt_pages=pages(receipts.size());m.counts.receipts=receipts.size();for(const auto& x:receipts)m.counts.receipt_bytes+=cr::receipt_record_bytes(x,limits);
        std::set<std::pair<std::string,std::string>> rebase;for(const auto& x:q.receipts)for(const auto& k:x.targets)rebase.emplace(k.table,k.id);
        m.counts.rebase_identities=rebase.size();for(const auto& [table,id]:rebase)m.counts.rebase_bytes+=16+table.size()+id.size();
        m.content_digest=m.receipt_digest=m.rebase_digest=std::string(64,'0');m.rebase_digest=cr::rebase_sha256(a,q,limits);
        m.content_digest=cr::content_sha256(m,content,limits);m.receipt_digest=cr::receipts_sha256(m,receipts,limits);m.manifest_digest=cr::manifest_sha256(m,limits);
    }
    cr::frame page(bool receipt,uint64_t index,const cr::limits& limits,uint64_t route=1) const {
        const auto first=index*q.budget.items_per_page;
        if(receipt){cr::receipt_page p;p.manifest_digest=m.manifest_digest;p.index=index;
            const auto end=std::min<uint64_t>(receipts.size(),first+q.budget.items_per_page);p.items.assign(receipts.begin()+first,receipts.begin()+end);p.count=p.items.size();
            for(const auto& x:p.items)p.bytes+=cr::receipt_record_bytes(x,limits);p.digest=cr::page_sha256(p,limits);return {a,route,p};}
        cr::content_page p;p.manifest_digest=m.manifest_digest;p.index=index;
        const auto end=std::min<uint64_t>(content.size(),first+q.budget.items_per_page);p.items.assign(content.begin()+first,content.begin()+end);p.count=p.items.size();
        for(const auto& x:p.items)p.bytes+=cr::content_record_bytes(x,limits);p.digest=cr::page_sha256(p,limits);return {a,route,p};
    }
    cr::frame ending(uint64_t route=1) const{return {a,route,cr::end{m.manifest_digest}};}
};
class CanonicalScopedInstall:public ::testing::Test {
protected:
    std::shared_ptr<StagedOwner> owner;
    canonical_scoped_limits limits=caps();canonical_scoped_contract contract;
    recovery_obligation_profile profile;recovery_obligation_address address;Bundle x;
    recovery_obligation_store journal(){return {owner,limits.obligations,limits.install.installations};}
    receive_install_store receiver(){return {owner,limits.install.installations};}
    canonical_range_staging stage(){return {owner,limits.install.installations,limits.codec,limits.staging};}
    template<class F> void owned(F&& f) {
        auto result=recovery_writer_access::install(owner,std::forward<F>(f));
        if(result.primary_error)try{std::rethrow_exception(result.primary_error);}catch(const std::exception& e){throw std::runtime_error(std::string("fixture owned frame: ")+e.what());}
        if(result.state!=state::committed || result.cleanup_error || result.postcommit_error)throw std::runtime_error("fixture owned frame did not cleanly commit");
    }
    void reset(const std::string& path=":memory:",uint64_t page_items=2) {
        owner=std::make_shared<StagedOwner>(path);limits=caps();contract={{"TestPerson"},{},{},{}};x=Bundle{};
        limits.codec.maximum.items_per_page=x.q.budget.items_per_page=page_items;
        profile={x.binding(),"profile","namespace"};
        owned([&](auto&){receiver().initialize();receiver().bind(profile.binding);auto j=journal();j.initialize();address=j.bind(profile).address;stage().initialize();});
    }
    void SetUp() override {reset();}
    rows query(const std::string& sql){return owner->db().query(sql);}
    int64_t number(const std::string& sql){return std::get<int64_t>(query(sql).at(0).begin()->second);}
    rows table(const std::string& name){if(owner->db().query("SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?",{name}).empty())return {};return query("SELECT * FROM \""+name+"\" ORDER BY 1,2");}
    std::vector<rows> snapshot(){std::vector<rows> result;
        for(const auto* name:{"TestPerson","TestDog","_StagedLink","StagedDocument","AuditLog","_lattice_sync_state","_lattice_install_channel","_lattice_install_store",
            "_lattice_recovery_witness","_lattice_recovery_scope_config","_lattice_recovery_scope","_lattice_recovery_member",
            "_lattice_range_attempt","_lattice_range_page","_lattice_range_store","_lattice_obligation_store","_lattice_obligation_scope","_lattice_obligation_entry","_SyncControl"})result.push_back(table(name));
        result.push_back(query("SELECT name,sql FROM sqlite_schema WHERE type='table' ORDER BY name"));return result;}
    recovery_obligation_scope scope(){std::optional<recovery_obligation_scope> s;owned([&](auto&){s=journal().read(address.channel);});if(!s)throw std::runtime_error("missing fixture scope");return *s;}
    receive_install_snapshot installation(){std::optional<receive_install_snapshot> s;owned([&](auto&){s=receiver().read(address.channel);});if(!s)throw std::runtime_error("missing fixture receiver");return *s;}
    canonical_install_admission mint(std::string coverage="coverage") {
        const auto s=scope();const auto i=installation();return canonical_scoped_install_test_access::mint(owner,x.a,1,x.q.request_digest,x.m.manifest_digest,profile,address,s.revision,contract,limits,i.last_installed,std::move(coverage));
    }
    scoped_recovery_result run(){return install_staged_canonical_range(mint());}
    bool committed(const scoped_recovery_result& result) {
        if(result.transaction.primary_error)try{std::rethrow_exception(result.transaction.primary_error);}catch(const std::exception& e){ADD_FAILURE()<<e.what();}
        EXPECT_EQ(result.transaction.state,state::committed);EXPECT_FALSE(result.transaction.cleanup_error);EXPECT_TRUE(result.installation);
        return result.transaction.state==state::committed && result.installation.has_value();
    }
    void refused(const scoped_recovery_result& result,const std::string& why="") {
        EXPECT_NE(result.transaction.state,state::committed);EXPECT_FALSE(result.installation);ASSERT_TRUE(result.transaction.primary_error);
        try{std::rethrow_exception(result.transaction.primary_error);}catch(const std::exception& e){if(!why.empty())EXPECT_NE(std::string(e.what()).find(why),std::string::npos)<<e.what();}
        catch(...){ADD_FAILURE()<<"non-standard refusal";}
    }
    cr::content_item person(const std::string& id,const std::string& name="source",int64_t age=20) {
        return {{"TestPerson",id},cr::present{sync_recovery::encode_values({{"globalId",id},{"name",name},{"age",age},{"email",nullptr}},limits.codec.values)}};
    }
    cr::content_item tombstone(const std::string& table,const std::string& id){return {{table,id},cr::tombstone{}};}
    recovery_obligation_entry record_latest(const std::string& table,const std::string& id) {
        std::optional<recovery_obligation_entry> entry;
        owned([&](auto& db){const auto a=db.query("SELECT id,globalId,tableName,globalRowId FROM AuditLog WHERE tableName=? AND globalRowId=? COLLATE NOCASE ORDER BY id DESC LIMIT 1",{table,id}).at(0);
            entry=journal().record(address,{std::get<int64_t>(a.at("id")),std::get<std::string>(a.at("globalId")),std::get<std::string>(a.at("tableName")),std::get<std::string>(a.at("globalRowId")),recovery_obligation_origin::local_candidate});});
        if(!entry)throw std::runtime_error("missing generated original");return *entry;
    }
    recovery_obligation_entry add(const std::string& id=A) {
        owner->db().execute("INSERT INTO TestPerson(globalId,name,age) VALUES(?,?,?)",{id,std::string("local"),int64_t{3}});return record_latest("TestPerson",id);
    }
    recovery_obligation_entry edit(const std::string& id,const std::string& name) {
        owner->db().execute("UPDATE TestPerson SET name=? WHERE globalId=? COLLATE NOCASE",{name,id});return record_latest("TestPerson",id);
    }
    void requested(const recovery_obligation_entry& entry,int outcome=-1,uint64_t position=5) {
        const auto& id=entry.record.original_id;const cr::identity target{entry.record.table,entry.canonical_target_id};
        x.q.receipts.push_back({id,profile.receipt_namespace,{target}});
        if(outcome<0)x.receipts.push_back({id,cr::not_committed{profile.receipt_namespace,"coverage"}});
        else x.receipts.push_back({id,cr::committed{profile.receipt_namespace,"coverage",outcome==0?cr::decision::applied:cr::decision::no_op,position,target}});
    }
    void staged(bool verify=true) {
        x.seal(limits.codec);owned([&](auto&){address=journal().freeze(address,x.a.sequence).address;auto s=stage();s.begin(x.a,x.q,x.m,1);
            for(uint64_t i=0;i<x.m.counts.content_pages;++i)s.append(x.page(false,i,limits.codec));
            for(uint64_t i=0;i<x.m.counts.receipt_pages;++i)s.append(x.page(true,i,limits.codec));if(verify)s.verify_end(x.ending());});
    }
    void next(cr::mode mode=cr::mode::delta) {
        const auto installed=installation();owned([&](auto&){stage().release_installed(x.a,x.m.manifest_digest,1);address=journal().resume(address,*installed.last_installed).address;});
        ++x.a.sequence;x.a.attempt_id=uuid('6');x.q.expected={static_cast<uint64_t>(installed.revision),x.q.source,{cr::frontier_kind::position,static_cast<uint64_t>(*installed.frontier.position)}};
        x.q.selection=mode;x.q.base=mode==cr::mode::delta?std::optional<uint64_t>{x.m.head}:std::nullopt;x.content.clear();x.receipts.clear();x.q.receipts.clear();contract.initial_row_grants.clear();
    }
    std::pair<receive_guard_token,receive_guard_snapshot> recovery_guard() {
        receive_guard_token token;receive_guard_snapshot before;
        owned([&](auto& db){token=receive_delivery_guard_access::begin(*owner,db,address.channel);
            before=receive_delivery_guard_access::finish(*owner,db,token,token.admitted,std::string("old-legacy-prefix"),true,true);
            receive_delivery_guard_access::verify_owned(*owner,db,before);});
        return {token,before};
    }
    canonical_install_admission guarded(const receive_guard_snapshot& before) {
        const auto s=scope();const auto i=installation();return canonical_scoped_install_test_access::mint(owner,x.a,1,
            x.q.request_digest,x.m.manifest_digest,profile,address,s.revision,contract,limits,i.last_installed,"coverage",before);
    }
    std::vector<rows> guarded_snapshot() {
        auto result=snapshot();for(const auto* name:{"_lattice_receive_guard_store","_lattice_receive_guard","_lattice_replication_slots"})
            result.push_back(table(name));return result;
    }
    void baseline(){x.content={person(A,"A"),person(B,"B")};staged();if(!committed(run()))throw std::runtime_error("initial staged installation failed");}
};
}

TEST_F(CanonicalScopedInstall, ActualRetainedUnverifiedPagesInstallAndCommitAllStores) {
    auto e=add(upper(A));requested(e,0);x.content={person(A,"canonical",44)};staged(false);
    const auto audit=table("AuditLog"),pages=table("_lattice_range_page");const auto id=number("SELECT id FROM TestPerson");
    auto result=run();ASSERT_TRUE(committed(result));EXPECT_EQ(result.installation->disposition,receive_install_disposition::installed);
    EXPECT_EQ(number("SELECT id FROM TestPerson"),id);EXPECT_EQ(std::get<std::string>(query("SELECT globalId FROM TestPerson")[0].at("globalId")),upper(A));
    EXPECT_EQ(std::get<std::string>(query("SELECT name FROM TestPerson")[0].at("name")),"canonical");
    EXPECT_EQ(table("AuditLog"),audit);EXPECT_EQ(table("_lattice_range_page"),pages);EXPECT_EQ(scope().mode,recovery_obligation_mode::installed);
    owned([&](auto&){EXPECT_EQ(journal().find(address,e.record.original_id)->stage,recovery_obligation_stage::settled);EXPECT_TRUE(stage().resume(x.a,x.m.manifest_digest,1).content_verified);});
    EXPECT_EQ(number("SELECT is_synchronized FROM _lattice_sync_state WHERE sync_id='staged-channel'"),1);
}
TEST_F(CanonicalScopedInstall, DeltaOmissionKeepsRowsPKMembershipAndFullOmissionDeletes) {
    baseline();const auto row_b=owner->db().query("SELECT * FROM TestPerson WHERE globalId=?",{B});next();++x.m.head;x.content={person(A,"changed")};staged();ASSERT_TRUE(committed(run()));
    EXPECT_EQ(owner->db().query("SELECT * FROM TestPerson WHERE globalId=?",{B}),row_b);EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_recovery_member"),2);
    next(cr::mode::full);++x.m.head;x.content={person(A,"full")};staged();ASSERT_TRUE(committed(run()));EXPECT_EQ(number("SELECT COUNT(*) FROM TestPerson"),1);
}
TEST_F(CanonicalScopedInstall, CoveredNegativeReplaysOnlyOriginalFieldsAndPreservesOtherChannel) {
    baseline();next();auto e=edit(A,"local-name");requested(e);x.content={person(A,"source-name",81)};
    owner->db().execute("INSERT INTO _lattice_sync_state VALUES(?,?,0)",{e.record.audit_id,std::string("another-channel")});
    const auto audit=table("AuditLog");staged();ASSERT_TRUE(committed(run()));const auto row=owner->db().query("SELECT name,age FROM TestPerson WHERE globalId=?",{A})[0];
    EXPECT_EQ(std::get<std::string>(row.at("name")),"local-name");EXPECT_EQ(std::get<int64_t>(row.at("age")),81);EXPECT_EQ(table("AuditLog"),audit);
    EXPECT_EQ(number("SELECT is_synchronized FROM _lattice_sync_state WHERE sync_id='another-channel'"),0);
    owned([&](auto&){EXPECT_EQ(journal().find(address,e.record.original_id)->stage,recovery_obligation_stage::open);});
}
TEST_F(CanonicalScopedInstall, LostAckAtOrBeforeBaseRebasesWithoutResurrectingOldIntent) {
    baseline();next();auto e=edit(A,"old-local");requested(e,1,4);x.content={person(A,"newer-source",50)};staged();ASSERT_TRUE(committed(run()));
    EXPECT_EQ(std::get<std::string>(owner->db().query("SELECT name FROM TestPerson WHERE globalId=?",{A})[0].at("name")),"newer-source");
    owned([&](auto&){const auto entry=journal().find(address,e.record.original_id);EXPECT_EQ(entry->stage,recovery_obligation_stage::settled);EXPECT_EQ(entry->acknowledged->outcome,recovery_obligation_outcome::no_op);});
}
TEST_F(CanonicalScopedInstall, ExactRetainedRetrySkipsNewNonQWorkJournalAndWitness) {
    baseline();const auto installed=installation();auto late=edit(A,"after-install");const auto before=snapshot();
    auto result=run();ASSERT_TRUE(committed(result));EXPECT_EQ(result.installation->disposition,receive_install_disposition::already_installed);
    EXPECT_EQ(snapshot(),before);EXPECT_EQ(installation(),installed);
    owned([&](auto&){EXPECT_EQ(journal().find(address,late.record.original_id)->stage,recovery_obligation_stage::open);});
}
TEST_F(CanonicalScopedInstall, SameHeadEmptyDeltaIsANewInstallationNotAnExactRetry) {
    baseline();const auto old=installation();next();staged();auto result=run();ASSERT_TRUE(committed(result));
    EXPECT_EQ(result.installation->disposition,receive_install_disposition::installed);const auto now=installation();EXPECT_EQ(now.frontier,old.frontier);EXPECT_EQ(now.revision,old.revision+1);
    EXPECT_NE(now.last_installed,old.last_installed);EXPECT_EQ(scope().installed_sequence,2);
}
TEST_F(CanonicalScopedInstall, LateNonQOriginalRefusesEvenWithLocalCandidateAndNoExportClaim) {
    baseline();next();++x.m.head;x.content={person(A,"remote")};staged();auto late=add(D);const auto before=snapshot();
    EXPECT_FALSE(late.first_export_claim);EXPECT_EQ(late.record.origin,recovery_obligation_origin::local_candidate);
    refused(run(),"absent from frozen Q");EXPECT_EQ(snapshot(),before);
}
TEST_F(CanonicalScopedInstall, UnknownPolicyAndWrongCoveragePreserveEveryOldPostimage) {
    for(int variant=0;variant<3;++variant){reset();auto e=add();requested(e,0);x.content={person(A)};
        if(variant==0)x.receipts[0].value=cr::unknown{cr::unknown_reason::legacy};
        if(variant==1)std::get<cr::committed>(x.receipts[0].value).outcome=cr::decision::policy;
        if(variant==2)std::get<cr::committed>(x.receipts[0].value).coverage_id="another-coverage";
        staged();const auto before=snapshot();refused(run(),variant==0?"unknown receipt":"positive receipt");EXPECT_EQ(snapshot(),before);}
}
TEST_F(CanonicalScopedInstall, NamespaceActualTargetAndMissingJournalCannotBeCallerGranted) {
    for(int variant=0;variant<3;++variant){reset();auto e=add();requested(e,0);x.content={person(A)};
        if(variant==0){x.q.receipts[0].namespace_id="other";std::get<cr::committed>(x.receipts[0].value).namespace_id="other";}
        if(variant==1){x.q.receipts[0].targets[0].id=B;std::get<cr::committed>(x.receipts[0].value).accepted_target->id=B;x.content.push_back(person(B));}
        if(variant==2){x.q.receipts[0].original_id=uuid('9');x.receipts[0].original_id=uuid('9');}
        staged();const auto before=snapshot();refused(run(),variant==0?"namespace":"journal original/target");EXPECT_EQ(snapshot(),before);}
}
TEST_F(CanonicalScopedInstall, AliasContentRefusesBeforeModelEffects) {
    auto e=add();requested(e,0);x.content={person(A),person(upper(A))};
    staged();const auto before=snapshot();refused(run(),"alias identities");EXPECT_EQ(snapshot(),before);
}
TEST_F(CanonicalScopedInstall, TamperedVerifiedPageCannotInstallOrRetryFromItsManifestAlone) {
    baseline();const auto before=snapshot();owner->db().execute("UPDATE _lattice_range_page SET wire=X'00' WHERE stream=0");const auto corrupt=snapshot();
    refused(run());EXPECT_EQ(snapshot(),corrupt);EXPECT_NE(snapshot(),before);
}
TEST_F(CanonicalScopedInstall, StalePhysicalRouteAndJournalGenerationRefuse) {
    x.content={person(A)};staged();const auto old=mint();owned([&](auto&){stage().rebind(x.a,x.m.manifest_digest,1,2);});const auto before=snapshot();
    refused(install_staged_canonical_range(old));EXPECT_EQ(snapshot(),before);
    reset();x.content={person(A)};staged();const auto token=mint();owned([&](auto&){address=journal().freeze(address,2).address;});const auto newer=snapshot();
    refused(install_staged_canonical_range(token),"journal binding");EXPECT_EQ(snapshot(),newer);
}
TEST_F(CanonicalScopedInstall, ModelTriggerCannotChangeRetainedRouteOrJournalDuringEffects) {
    for(const bool journal_change:{false,true}){reset();baseline();next();++x.m.head;x.content={person(A,"new")};staged();
        owner->db().execute(journal_change?
            "CREATE TRIGGER _staged_mutation AFTER UPDATE ON TestPerson BEGIN UPDATE _lattice_obligation_scope SET revision=revision+1; END":
            "CREATE TRIGGER _staged_mutation AFTER UPDATE ON TestPerson BEGIN UPDATE _lattice_range_attempt SET route=2; END");
        const auto before=snapshot();refused(run());EXPECT_EQ(snapshot(),before);owner->db().execute("DROP TRIGGER _staged_mutation");ASSERT_TRUE(committed(run()));}
}
TEST_F(CanonicalScopedInstall, JournalSettlementTriggerCannotChangeFinalModelOrMembership) {
    for(const bool member_change:{false,true}){reset();baseline();next();++x.m.head;x.content={person(A,"new")};staged();
        owner->db().execute(member_change?
            "CREATE TRIGGER _staged_settle AFTER UPDATE ON _lattice_obligation_scope WHEN NEW.mode=2 BEGIN DELETE FROM _lattice_recovery_member; END":
            "CREATE TRIGGER _staged_settle AFTER UPDATE ON _lattice_obligation_scope WHEN NEW.mode=2 BEGIN UPDATE TestPerson SET name='corrupt'; END");
        const auto before=snapshot();refused(run());EXPECT_EQ(snapshot(),before);owner->db().execute("DROP TRIGGER _staged_settle");ASSERT_TRUE(committed(run()));}
}
TEST_F(CanonicalScopedInstall, IgnoredEffectsReceiverAndJournalRollbackTogetherThenRetry) {
    const std::vector<std::string> faults={
        "BEFORE UPDATE ON TestPerson",
        "BEFORE INSERT ON _lattice_recovery_member",
        "BEFORE INSERT ON _lattice_sync_state",
        "BEFORE UPDATE ON _lattice_obligation_scope WHEN NEW.mode=2",
        "BEFORE UPDATE ON _lattice_install_channel WHEN NEW.revision>OLD.revision"};
    for(const auto& fault:faults){reset();baseline();next();auto e=edit(A,"local");requested(e,0);x.content={person(A,"source-final")};staged();
        owner->db().execute("CREATE TRIGGER _staged_ignore "+fault+" BEGIN SELECT RAISE(IGNORE); END");const auto before=snapshot();
        refused(run());EXPECT_EQ(snapshot(),before);owner->db().execute("DROP TRIGGER _staged_ignore");ASSERT_TRUE(committed(run()));}
}
TEST_F(CanonicalScopedInstall, DecodedAggregateLimitAndExactImageCountHaveAtomicOutcomes) {
    x.content={person(A),person(B)};staged();auto saved=limits.install;limits.install.targets=1;const auto before=snapshot();refused(run(),"aggregate collection");EXPECT_EQ(snapshot(),before);
    limits.install=saved;limits.install.logical_bytes=250;refused(run());EXPECT_EQ(snapshot(),before);limits.install=saved;limits.install.targets=2;ASSERT_TRUE(committed(run()));
    reset();auto one=add(A),two=add(B);requested(one);requested(two);x.content={person(A),person(B)};staged();
    limits.install.receipts=1;const auto pending=snapshot();refused(run(),"aggregate collection");EXPECT_EQ(snapshot(),pending);
    limits.install.receipts=2;ASSERT_TRUE(committed(run()));
}
TEST_F(CanonicalScopedInstall, ReopenRetainedStageThenInstalledRetryKeepsOriginalIdsAndPages) {
    TempDB path("canonical-staged-install");reset(path.str());auto e=add();requested(e,0);x.content={person(A,"file")};staged();const auto audit=table("AuditLog"),pages=table("_lattice_range_page");
    owner->close();owner=std::make_shared<StagedOwner>(path.str());ASSERT_TRUE(committed(run()));EXPECT_EQ(table("AuditLog"),audit);EXPECT_EQ(table("_lattice_range_page"),pages);
    const auto installed=installation();owner->close();owner=std::make_shared<StagedOwner>(path.str());const auto before=snapshot();ASSERT_TRUE(committed(run()));EXPECT_EQ(snapshot(),before);EXPECT_EQ(installation(),installed);owner->close();
}
TEST_F(CanonicalScopedInstall, ExistingOwnedOrRawTransactionIsNotJoined) {
    x.content={person(A)};staged();auto admission=mint();owner->begin_transaction();refused(install_staged_canonical_range(admission));owner->rollback();
    owner->db().execute("BEGIN IMMEDIATE");refused(install_staged_canonical_range(admission));owner->db().execute("ROLLBACK");ASSERT_TRUE(committed(run()));
}

TEST_F(CanonicalScopedInstall, LatestNoHistoryNULValuesAreRecapturedWithoutOverwritingSourceFields) {
    owner->document();contract.model_tables={"StagedDocument"};
    auto document=[&](const std::string& body,const std::string& title){return cr::content_item{{"StagedDocument",A},cr::present{
        sync_recovery::encode_values({{"globalId",A},{"body",body},{"title",title}},limits.codec.values)}};};
    x.content={document("initial","initial")};staged();ASSERT_TRUE(committed(run()));next();
    const std::string body("latest\0body",11);owner->db().execute("UPDATE StagedDocument SET body=? WHERE globalId=?",{body,A});
    auto e=record_latest("StagedDocument",A);requested(e);x.content={document("source-body","new-source-title")};
    const auto audits=table("AuditLog");staged();const auto pages=table("_lattice_range_page");ASSERT_TRUE(committed(run()));
    const auto row=query("SELECT body,title FROM StagedDocument")[0];EXPECT_EQ(std::get<std::string>(row.at("body")),body);
    EXPECT_EQ(std::get<std::string>(row.at("title")),"new-source-title");EXPECT_EQ(table("AuditLog"),audits);EXPECT_EQ(table("_lattice_range_page"),pages);
}
TEST_F(CanonicalScopedInstall, OmittedOrdinaryLinkSurvivesAndExplicitTombstonesRequireClosure) {
    owner->ensure_link_table("_StagedLink","TestPerson","TestDog");contract.model_tables={"TestPerson","TestDog"};
    contract.relations={{"_StagedLink","TestPerson","TestDog"}};contract.scoped_link_tables={"_StagedLink"};
    x.content={person(upper(A)),{{"TestDog",upper(D)},cr::present{sync_recovery::encode_values({{"globalId",upper(D)},{"name",std::string("dog")},{"weight",4.5},{"is_good_boy",int64_t{1}}},limits.codec.values)}},
        {{"_StagedLink",upper(L)},cr::present{sync_recovery::encode_values({{"globalId",upper(L)},{"lhs",A},{"rhs",D}},limits.codec.values)}}};
    staged();ASSERT_TRUE(committed(run()));const auto links=table("_StagedLink");next();++x.m.head;x.content={person(A,"updated")};staged();ASSERT_TRUE(committed(run()));EXPECT_EQ(table("_StagedLink"),links);
    next();++x.m.head;x.content={tombstone("TestPerson",A)};staged();const auto before=snapshot();refused(run());EXPECT_EQ(snapshot(),before);EXPECT_EQ(table("_StagedLink"),links);
    owned([&](auto&){stage().abandon_active(x.a,x.m.manifest_digest,1);});++x.a.sequence;x.content.push_back(tombstone("_StagedLink",L));
    staged();ASSERT_TRUE(committed(run()));EXPECT_EQ(number("SELECT COUNT(*) FROM TestPerson"),0);EXPECT_EQ(number("SELECT COUNT(*) FROM _StagedLink"),0);EXPECT_EQ(number("SELECT COUNT(*) FROM TestDog"),1);
}
TEST_F(CanonicalScopedInstall, WitnessExhaustionAfterModelWorkRollsBackTheWholeInstallation) {
    baseline();next();auto e=edit(A,"local");requested(e,0);x.content={person(A,"remote")};staged();
    const auto generation=number("SELECT generation FROM _lattice_recovery_witness");owner->db().execute("UPDATE _lattice_recovery_witness SET generation=?",{std::numeric_limits<int64_t>::max()});
    const auto before=snapshot();refused(run(),"generation exhausted");EXPECT_EQ(snapshot(),before);
    owner->db().execute("UPDATE _lattice_recovery_witness SET generation=?",{generation});ASSERT_TRUE(committed(run()));
}
TEST_F(CanonicalScopedInstall, OuterCommitFailureKeepsStageModelsJournalAndReceiverForExactRetry) {
    auto e=add();requested(e,0);x.content={person(A,"committed-source")};staged();const auto admission=mint();const auto before=snapshot();
    struct DenyCommit {
        sqlite3* db;
        explicit DenyCommit(sqlite3* value):db(value){sqlite3_set_authorizer(db,[](void*,int action,const char* first,const char*,const char*,const char*) {
            return action==SQLITE_TRANSACTION&&first&&std::string_view(first)=="COMMIT"?SQLITE_DENY:SQLITE_OK;
        },nullptr);}
        ~DenyCommit(){sqlite3_set_authorizer(db,nullptr,nullptr);}
    };
    {DenyCommit deny(owner->db().handle());auto result=install_staged_canonical_range(admission);refused(result);EXPECT_EQ(result.transaction.state,state::rolled_back);}
    EXPECT_EQ(snapshot(),before);ASSERT_TRUE(committed(run()));
}
TEST_F(CanonicalScopedInstall, PostcommitCallbackFailureRetainsReceiptAndDoesNotReapply) {
    x.content={person(A)};staged();std::shared_ptr<lattice::lattice_db> base=owner;
    auto observer=base->add_table_observer("TestPerson",[](const auto&){throw std::runtime_error("mechanical postcommit observer");});
    auto result=run();EXPECT_EQ(result.transaction.state,state::committed);EXPECT_TRUE(result.transaction.postcommit_error);ASSERT_TRUE(result.installation);
    base->remove_table_observer("TestPerson",observer);const auto before=snapshot();auto retry=run();ASSERT_TRUE(committed(retry));
    EXPECT_EQ(retry.installation->disposition,receive_install_disposition::already_installed);EXPECT_EQ(snapshot(),before);
}
TEST_F(CanonicalScopedInstall, ReleasedStageCannotUseThisEntryAsADigestOnlyRetry) {
    baseline();owned([&](auto&){stage().release_installed(x.a,x.m.manifest_digest,1);});const auto before=snapshot();refused(run());EXPECT_EQ(snapshot(),before);
}
TEST_F(CanonicalScopedInstall, MechanicallyMintedWrongSourceOrQCannotReplaceBoundStage) {
    x.content={person(A)};staged();const auto before=snapshot();const auto js=scope();auto altered=profile;altered.binding.authority="another-authority";
    auto wrong=canonical_scoped_install_test_access::mint(owner,x.a,1,x.q.request_digest,x.m.manifest_digest,altered,address,js.revision,contract,limits,{});
    refused(install_staged_canonical_range(wrong),"differs from retained stage");EXPECT_EQ(snapshot(),before);
    auto wrong_q=canonical_scoped_install_test_access::mint(owner,x.a,1,std::string(64,'f'),x.m.manifest_digest,profile,address,js.revision,contract,limits,{});
    refused(install_staged_canonical_range(wrong_q),"differs from retained stage");EXPECT_EQ(snapshot(),before);
}

TEST_F(CanonicalScopedInstall, RetainedFirstAckAndAlreadySettledOriginalStillRequireExactEvidence) {
    for(const bool unknown:{false,true}) {
        reset();auto e=add();owned([&](auto&){journal().acknowledge(address,{e.record.original_id,profile.receipt_namespace,5,recovery_obligation_outcome::applied});});
        owner->db().execute("INSERT INTO _lattice_sync_state(audit_entry_id,sync_id,is_synchronized) VALUES(?,?,1)",{e.record.audit_id,address.channel});
        requested(e,0);x.content={person(A,"first")};staged();ASSERT_TRUE(committed(run()));next();requested(e,0);x.content={person(A,"second")};
        if(unknown)x.receipts[0].value=cr::unknown{cr::unknown_reason::retired_coverage};staged();const auto before=snapshot();
        if(unknown){refused(run(),"unknown receipt");EXPECT_EQ(snapshot(),before);}
        else {ASSERT_TRUE(committed(run()));owned([&](auto&){const auto retained=journal().find(address,e.record.original_id);ASSERT_TRUE(retained);EXPECT_EQ(retained->settled_install_sequence,1);});}
    }
}

TEST_F(CanonicalScopedInstall, EqualWholeHashesCannotHideChangedConsumedPagePartition) {
    // This fixture needs both valid 3+1 and 2+2 partitions of the same stream.
    // Set its matching local/request page cap before durable initialization.
    reset(":memory:",3);baseline();next();++x.m.head;x.content={person(A,"mutate"),person(B),person(D),person(L)};staged();
    ASSERT_EQ(x.m.counts.content_pages,2u);auto first=std::get<cr::content_page>(x.page(false,0,limits.codec).body);
    auto second=std::get<cr::content_page>(x.page(false,1,limits.codec).body);
    second.items.insert(second.items.begin(),first.items.back());first.items.pop_back();
    auto seal_page=[&](cr::content_page& page){page.count=page.items.size();page.bytes=0;for(const auto& item:page.items)page.bytes+=cr::content_record_bytes(item,limits.codec);page.digest=cr::page_sha256(page,limits.codec);};
    seal_page(first);seal_page(second);
    // Both partitions have the same ordered canonical stream and manifest.
    auto effective=limits.codec;effective.maximum=x.q.budget;cr::stream_hasher whole(x.m,cr::stream_kind::content,effective);
    for(const auto& item:first.items)whole.append(item);for(const auto& item:second.items)whole.append(item);EXPECT_EQ(whole.finish(),x.m.content_digest);
    const auto wire0=cr::encode({x.a,1,first},limits.codec),wire1=cr::encode({x.a,1,second},limits.codec);
    const auto old0=cr::encode(x.page(false,0,limits.codec),limits.codec),old1=cr::encode(x.page(false,1,limits.codec),limits.codec);
    const auto change=static_cast<int64_t>(wire0.size()+wire1.size())-static_cast<int64_t>(old0.size()+old1.size());
    auto hex=[](const std::string& bytes){const char* digits="0123456789abcdef";std::string s;for(unsigned char c:bytes){s+=digits[c>>4];s+=digits[c&15];}return s;};
    owner->db().execute("CREATE TRIGGER _staged_repartition AFTER UPDATE ON TestPerson WHEN NEW.name='mutate' BEGIN "
        "UPDATE _lattice_range_page SET wire=X'"+hex(wire0)+"' WHERE stream=0 AND page_index=0; "
        "UPDATE _lattice_range_page SET wire=X'"+hex(wire1)+"' WHERE stream=0 AND page_index=1; "
        "UPDATE _lattice_range_attempt SET page_bytes=page_bytes+("+std::to_string(change)+"); "
        "UPDATE _lattice_range_store SET stored_bytes=stored_bytes+("+std::to_string(change)+"); END");
    const auto before=snapshot();refused(run(),"consumed content page changed");EXPECT_EQ(snapshot(),before);
    owner->db().execute("DROP TRIGGER _staged_repartition");ASSERT_TRUE(committed(run()));
}

TEST_F(CanonicalScopedInstall, NewlySettledScopedReceiptMustSurviveFinalJournalTriggers) {
    for(const bool erase:{false,true}) {
        reset();auto e=add();requested(e,0);x.content={person(A,"source-final")};staged();
        owner->db().execute("INSERT INTO _lattice_sync_state VALUES(?,?,0)",{e.record.audit_id,std::string("other-channel")});
        const auto predicate=" WHERE audit_entry_id="+std::to_string(e.record.audit_id)+" AND sync_id='staged-channel'; ";
        owner->db().execute("CREATE TRIGGER _staged_final_receipt AFTER UPDATE ON _lattice_obligation_scope WHEN NEW.mode=2 BEGIN "+
            std::string(erase?"DELETE FROM _lattice_sync_state":"UPDATE _lattice_sync_state SET is_synchronized=0")+predicate+"END");
        const auto before=snapshot();auto result=run();refused(result,"canonical final scoped receipt changed");
        EXPECT_EQ(result.transaction.state,state::rolled_back);EXPECT_EQ(snapshot(),before);
        owner->db().execute("DROP TRIGGER _staged_final_receipt");ASSERT_TRUE(committed(run()));
        EXPECT_EQ(number("SELECT is_synchronized FROM _lattice_sync_state WHERE audit_entry_id="+std::to_string(e.record.audit_id)+" AND sync_id='staged-channel'"),1);
        EXPECT_EQ(number("SELECT is_synchronized FROM _lattice_sync_state WHERE sync_id='other-channel'"),0);
        owned([&](auto&){const auto entry=journal().find(address,e.record.original_id);ASSERT_TRUE(entry);EXPECT_EQ(entry->stage,recovery_obligation_stage::settled);});
    }
}
TEST_F(CanonicalScopedInstall, AlreadySettledPositiveQStillRequiresItsExactFinalChannelReceipt) {
    for(const bool erase:{false,true}) {
        reset();auto e=add();requested(e,0);x.content={person(A,"first")};staged();ASSERT_TRUE(committed(run()));
        next(cr::mode::full);requested(e,0);x.content={person(A,"second")};staged();
        const auto predicate=" WHERE audit_entry_id="+std::to_string(e.record.audit_id)+" AND sync_id='staged-channel'; ";
        owner->db().execute("CREATE TRIGGER _staged_old_receipt AFTER UPDATE ON _lattice_obligation_scope WHEN NEW.mode=2 BEGIN "+
            std::string(erase?"DELETE FROM _lattice_sync_state":"UPDATE _lattice_sync_state SET is_synchronized=0")+predicate+"END");
        const auto before=snapshot();auto result=run();refused(result,"canonical final scoped receipt changed");
        EXPECT_EQ(result.transaction.state,state::rolled_back);EXPECT_EQ(snapshot(),before);
        owner->db().execute("DROP TRIGGER _staged_old_receipt");ASSERT_TRUE(committed(run()));
        EXPECT_EQ(number("SELECT is_synchronized FROM _lattice_sync_state WHERE audit_entry_id="+std::to_string(e.record.audit_id)+" AND sync_id='staged-channel'"),1);
        owned([&](auto&){const auto entry=journal().find(address,e.record.original_id);ASSERT_TRUE(entry);EXPECT_EQ(entry->settled_install_sequence,1);});
    }
}
TEST_F(CanonicalScopedInstall, OriginalSyncControlSurvivesReceiverAndJournalCompletion) {
    for(const int64_t prior:{int64_t{0},int64_t{1}})for(const bool receiver_trigger:{false,true}) {
        reset();baseline();next();++x.m.head;x.content={person(A,"source-final")};staged();
        owner->db().execute("UPDATE _SyncControl SET disabled=? WHERE id=1",{prior});
        const std::string boundary=receiver_trigger?
            "AFTER UPDATE ON _lattice_install_channel WHEN NEW.revision>OLD.revision":
            "AFTER UPDATE ON _lattice_obligation_scope WHEN NEW.mode=2";
        owner->db().execute("CREATE TRIGGER _staged_final_control "+boundary+
            " BEGIN UPDATE _SyncControl SET disabled="+std::to_string(1-prior)+" WHERE id=1; END");
        const auto before=snapshot();auto result=run();refused(result,"canonical final sync control changed");
        EXPECT_EQ(result.transaction.state,state::rolled_back);EXPECT_EQ(snapshot(),before);
        owner->db().execute("DROP TRIGGER _staged_final_control");ASSERT_TRUE(committed(run()));
        EXPECT_EQ(number("SELECT disabled FROM _SyncControl WHERE id=1"),prior);
    }
}
TEST_F(CanonicalScopedInstall, ExactBumpedWitnessSurvivesFinalJournalTriggers) {
    for(const bool change_incarnation:{false,true}) {
        reset();baseline();next();++x.m.head;x.content={person(A,"source-final")};staged();
        const auto old=table("_lattice_recovery_witness");const auto generation=number("SELECT generation FROM _lattice_recovery_witness");
        const auto incarnation=std::get<blob>(old.at(0).at("incarnation"));ASSERT_EQ(incarnation.size(),16u);
        auto altered=incarnation;altered[0]^=0xff;const char* digits="0123456789abcdef";std::string hex;
        for(const auto byte:altered){hex+=digits[byte>>4];hex+=digits[byte&15];}
        const auto mutation=change_incarnation?"incarnation=X'"+hex+"'":"generation="+std::to_string(generation);
        owner->db().execute("CREATE TRIGGER _staged_final_witness AFTER UPDATE ON _lattice_obligation_scope WHEN NEW.mode=2 "
            "BEGIN UPDATE _lattice_recovery_witness SET "+mutation+" WHERE id=1; END");
        const auto before=snapshot();auto result=run();refused(result,"canonical final recovery witness changed");
        EXPECT_EQ(result.transaction.state,state::rolled_back);EXPECT_EQ(snapshot(),before);
        owner->db().execute("DROP TRIGGER _staged_final_witness");ASSERT_TRUE(committed(run()));
        const auto after=table("_lattice_recovery_witness");EXPECT_EQ(std::get<blob>(after.at(0).at("incarnation")),incarnation);
        EXPECT_EQ(number("SELECT generation FROM _lattice_recovery_witness"),generation+1);
    }
}


TEST_F(CanonicalScopedInstall, GuardedInstallCommitsCanonicalMarkerAndRejectsLegacyCursor) {
    auto e=add();requested(e,0);x.content={person(A,"canonical-guard")};const auto [token,before]=recovery_guard();staged();
    const auto audit=table("AuditLog");const auto admission=guarded(before);const auto result=install_staged_canonical_range(admission);
    ASSERT_TRUE(committed(result));const auto after=receive_delivery_guard_access::read(*owner,address.channel);
    EXPECT_EQ(after.state,receive_guard_state::canonical_installed);EXPECT_EQ(after.reason,receive_guard_reason::none);
    EXPECT_EQ(after.incarnation,before.incarnation);EXPECT_EQ(after.generation,before.generation+1);EXPECT_EQ(after.store_version,2);
    EXPECT_FALSE(after.checkpoint);EXPECT_EQ(table("AuditLog"),audit);EXPECT_EQ(scope().mode,recovery_obligation_mode::installed);
    EXPECT_EQ(std::get<std::string>(query("SELECT name FROM TestPerson")[0].at("name")),"canonical-guard");
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_replication_slots WHERE last_received_event_id IS NULL"),1);
    EXPECT_THROW(receive_delivery_guard_access::legacy_checkpoint(*owner,address.channel),lattice::db_error);
    const auto stable=guarded_snapshot();owned([&](auto& db){
        EXPECT_THROW(receive_delivery_guard_access::begin(*owner,db,address.channel),lattice::db_error);
        EXPECT_THROW(receive_delivery_guard_access::require_current(*owner,db,token),lattice::db_error);
    });EXPECT_EQ(guarded_snapshot(),stable);
}
TEST_F(CanonicalScopedInstall, GuardedDeniedCommitRollsBackVersionRowsReceiptsAndCursor) {
    auto e=add();requested(e,0);x.content={person(A,"source")};const auto before=recovery_guard().second;staged();
    const auto admission=guarded(before);const auto stable=guarded_snapshot();
    struct DenyCommit {
        sqlite3* db;
        explicit DenyCommit(sqlite3* value):db(value){sqlite3_set_authorizer(db,[](void*,int action,const char* first,const char*,const char*,const char*){
            return action==SQLITE_TRANSACTION&&first&&std::string_view(first)=="COMMIT"?SQLITE_DENY:SQLITE_OK;
        },nullptr);}
        ~DenyCommit(){sqlite3_set_authorizer(db,nullptr,nullptr);}
    };
    {DenyCommit deny(owner->db().handle());const auto result=install_staged_canonical_range(admission);refused(result);EXPECT_EQ(result.transaction.state,state::rolled_back);}
    EXPECT_EQ(guarded_snapshot(),stable);EXPECT_EQ(receive_delivery_guard_access::read(*owner,address.channel),before);
    ASSERT_TRUE(committed(install_staged_canonical_range(admission)));
}
TEST_F(CanonicalScopedInstall, GuardedStaleGenerationAndWrongChannelPreserveEveryPostimage) {
    for(const bool wrong_channel:{false,true}) {
        reset();x.content={person(A)};auto before=recovery_guard().second;staged();
        if(wrong_channel)before.channel="another-channel";
        const auto admission=guarded(before);
        if(!wrong_channel)owned([&](auto& db){receive_delivery_guard_access::begin(*owner,db,address.channel);});
        const auto stable=guarded_snapshot();refused(install_staged_canonical_range(admission));EXPECT_EQ(guarded_snapshot(),stable);
    }
}
TEST_F(CanonicalScopedInstall, GuardedModelAndJournalMutationCannotRewriteReceiveGeneration) {
    for(const bool journal_mutation:{false,true}) {
        reset();auto e=add();requested(e,0);x.content={person(A,"new")};const auto before=recovery_guard().second;staged();
        owner->db().execute(journal_mutation?
            "CREATE TRIGGER _guard_mutation AFTER UPDATE ON _lattice_obligation_scope WHEN NEW.mode=2 BEGIN UPDATE _lattice_receive_guard SET generation=generation+1; END":
            "CREATE TRIGGER _guard_mutation AFTER UPDATE ON TestPerson BEGIN UPDATE _lattice_receive_guard SET generation=generation+1; END");
        const auto stable=guarded_snapshot();refused(install_staged_canonical_range(guarded(before)));EXPECT_EQ(guarded_snapshot(),stable);
        owner->db().execute("DROP TRIGGER _guard_mutation");ASSERT_TRUE(committed(install_staged_canonical_range(guarded(before))));
    }
}
TEST_F(CanonicalScopedInstall, GuardedMetadataTriggersRefuseWithoutCommittingModelEffects) {
    for(const auto* table_name:{"_lattice_receive_guard_store","_lattice_receive_guard","_lattice_replication_slots"}) {
        reset();auto e=add();requested(e,0);x.content={person(A,"new")};const auto before=recovery_guard().second;staged();
        owner->db().execute(std::string("CREATE TRIGGER _guard_metadata AFTER UPDATE ON ")+table_name+" BEGIN DELETE FROM _lattice_obligation_entry; END");
        const auto stable=guarded_snapshot();refused(install_staged_canonical_range(guarded(before)),"metadata triggers");EXPECT_EQ(guarded_snapshot(),stable);
        owner->db().execute("DROP TRIGGER _guard_metadata");ASSERT_TRUE(committed(install_staged_canonical_range(guarded(before))));
    }
}
TEST_F(CanonicalScopedInstall, GuardedExactRetryPreservesLateOriginalAndOtherChannelActivity) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();const auto admission=guarded(before);
    ASSERT_TRUE(committed(install_staged_canonical_range(admission)));const auto late=edit(A,"late-local");
    owned([&](auto& db){const auto token=receive_delivery_guard_access::begin(*owner,db,"other-channel");
        const auto done=receive_delivery_guard_access::finish(*owner,db,token,token.admitted,std::string("other-prefix"),false,true);
        receive_delivery_guard_access::verify_owned(*owner,db,done);});
    const auto stable=guarded_snapshot();const auto result=install_staged_canonical_range(admission);ASSERT_TRUE(committed(result));
    EXPECT_EQ(result.installation->disposition,receive_install_disposition::already_installed);EXPECT_EQ(guarded_snapshot(),stable);
    owned([&](auto&){EXPECT_EQ(journal().find(address,late.record.original_id)->stage,recovery_obligation_stage::open);});
    EXPECT_EQ(receive_delivery_guard_access::legacy_checkpoint(*owner,"other-channel"),std::optional<std::string>{"other-prefix"});
}
TEST_F(CanonicalScopedInstall, GuardedRetryRejectsChangedCanonicalGenerationWithoutReapplying) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();const auto admission=guarded(before);
    ASSERT_TRUE(committed(install_staged_canonical_range(admission)));
    owner->db().execute("UPDATE _lattice_receive_guard SET generation=generation+1");const auto stable=guarded_snapshot();
    refused(install_staged_canonical_range(admission),"exact retry");EXPECT_EQ(guarded_snapshot(),stable);
}
TEST_F(CanonicalScopedInstall, GuardedPostcommitNotificationFailureKeepsExactInstalledGuard) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();const auto admission=guarded(before);
    std::shared_ptr<lattice::lattice_db> base=owner;const auto observer=base->add_table_observer("TestPerson",[](const auto&){throw std::runtime_error("guarded observer failure");});
    const auto result=install_staged_canonical_range(admission);EXPECT_EQ(result.transaction.state,state::committed);
    EXPECT_TRUE(result.transaction.postcommit_error);ASSERT_TRUE(result.installation);
    EXPECT_EQ(receive_delivery_guard_access::read(*owner,address.channel).state,receive_guard_state::canonical_installed);
    base->remove_table_observer("TestPerson",observer);const auto stable=guarded_snapshot();const auto retry=install_staged_canonical_range(admission);
    ASSERT_TRUE(committed(retry));EXPECT_EQ(retry.installation->disposition,receive_install_disposition::already_installed);EXPECT_EQ(guarded_snapshot(),stable);
}
TEST_F(CanonicalScopedInstall, GuardedFileReopenRetainsCanonicalMarkerAndExactRetry) {
    TempDB path("canonical-receive-completion");reset(path.str());auto e=add();requested(e,0);x.content={person(A,"reopen")};
    const auto before=recovery_guard().second;staged();const auto audit=table("AuditLog");owner->close();owner=std::make_shared<StagedOwner>(path.str());
    ASSERT_TRUE(committed(install_staged_canonical_range(guarded(before))));owner->close();owner=std::make_shared<StagedOwner>(path.str());
    const auto stable=guarded_snapshot();const auto result=install_staged_canonical_range(guarded(before));ASSERT_TRUE(committed(result));
    EXPECT_EQ(result.installation->disposition,receive_install_disposition::already_installed);EXPECT_EQ(guarded_snapshot(),stable);EXPECT_EQ(table("AuditLog"),audit);
    EXPECT_THROW(receive_delivery_guard_access::legacy_checkpoint(*owner,address.channel),lattice::db_error);owner->close();
}
TEST_F(CanonicalScopedInstall, GuardedSuccessorInstallationAdvancesCanonicalGenerationOnce) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();ASSERT_TRUE(committed(install_staged_canonical_range(guarded(before))));
    const auto first=receive_delivery_guard_access::read(*owner,address.channel);next();++x.m.head;x.content={person(A,"successor")};staged();
    const auto admission=guarded(first);ASSERT_TRUE(committed(install_staged_canonical_range(admission)));
    const auto second=receive_delivery_guard_access::read(*owner,address.channel);EXPECT_EQ(second.generation,first.generation+1);EXPECT_EQ(second.store_version,2);
    const auto stable=guarded_snapshot();ASSERT_TRUE(committed(install_staged_canonical_range(admission)));EXPECT_EQ(guarded_snapshot(),stable);
}
TEST_F(CanonicalScopedInstall, GuardedLegacyOriginCannotBecomeCanonicalByInstallingRows) {
    x.content={person(A)};recovery_guard();owner->db().execute("UPDATE _lattice_receive_guard_store SET legacy_origin=1");
    const auto before=receive_delivery_guard_access::read(*owner,address.channel);ASSERT_TRUE(before.legacy_origin);staged();const auto stable=guarded_snapshot();
    refused(install_staged_canonical_range(guarded(before)),"modern nonretired guard");EXPECT_EQ(guarded_snapshot(),stable);
}

TEST_F(CanonicalScopedInstall, CanonicalChannelCannotOmitGuardOnRetryOrSuccessorInstall) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();ASSERT_TRUE(committed(install_staged_canonical_range(guarded(before))));
    const auto installed=guarded_snapshot();refused(run(),"requires bound receive");EXPECT_EQ(guarded_snapshot(),installed);
    next();++x.m.head;x.content={person(A,"next")};staged();const auto staged_state=guarded_snapshot();
    refused(run(),"requires bound receive");EXPECT_EQ(guarded_snapshot(),staged_state);
    const auto current=receive_delivery_guard_access::read(*owner,address.channel);ASSERT_TRUE(committed(install_staged_canonical_range(guarded(current))));
}

TEST_F(CanonicalScopedInstall, GuardedTempTriggersCannotRewriteOutboundStateDuringCompletion) {
    const std::vector<std::pair<std::string,std::string>> targets={
        {"_LATTICE_RECEIVE_GUARD_STORE","NEW.version=2 AND OLD.version=1"},
        {"_LATTICE_RECEIVE_GUARD","NEW.state=4 AND OLD.state<>4"},
        {"_LATTICE_REPLICATION_SLOTS","NEW.last_received_event_id IS NULL AND OLD.last_received_event_id IS NOT NULL"}};
    for(const auto& [table_name,condition]:targets) {
        reset();auto e=add();requested(e,0);x.content={person(A,"new")};const auto before=recovery_guard().second;staged();
        owner->db().execute("CREATE TEMP TRIGGER _guard_temp_metadata AFTER UPDATE ON main."+table_name+" WHEN "+condition+
            " BEGIN UPDATE _lattice_replication_slots SET upload_floor=COALESCE(upload_floor,0)+7,confirmed_audit_id=COALESCE(confirmed_audit_id,0)+9; END");
        EXPECT_EQ(number("SELECT COUNT(*) FROM main.sqlite_schema WHERE name='_guard_temp_metadata'"),0);
        ASSERT_EQ(number("SELECT COUNT(*) FROM temp.sqlite_schema WHERE name='_guard_temp_metadata'"),1);
        const auto temp_schema=query("SELECT name,sql FROM temp.sqlite_schema ORDER BY name");const auto stable=guarded_snapshot();
        refused(install_staged_canonical_range(guarded(before)),"metadata triggers");EXPECT_EQ(guarded_snapshot(),stable);
        EXPECT_EQ(query("SELECT name,sql FROM temp.sqlite_schema ORDER BY name"),temp_schema);
        owner->db().execute("DROP TRIGGER temp._guard_temp_metadata");ASSERT_TRUE(committed(install_staged_canonical_range(guarded(before))));
    }
}

TEST_F(CanonicalScopedInstall, CommittedInspectionAfterReleasePreservesLateOriginalAndAllRows) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();const auto admission=guarded(before);
    ASSERT_TRUE(committed(install_staged_canonical_range(admission)));const auto identity=*installation().last_installed;
    owned([&](auto&){stage().release_installed(x.a,x.m.manifest_digest,1);});
    const auto late=edit(A,"late-pending");owned([&](auto& db){const auto token=receive_delivery_guard_access::begin(*owner,db,"other-channel");
        receive_delivery_guard_access::finish(*owner,db,token,token.admitted,std::string("other-prefix"),false,true);});
    std::shared_ptr<lattice::lattice_db> base=owner;int notifications=0;
    const auto observer=base->add_table_observer("TestPerson",[&](const auto&){++notifications;});
    const auto stable=guarded_snapshot();const auto result=inspect_committed_canonical_range(admission,identity,x.q,x.m);
    ASSERT_TRUE(committed(result));EXPECT_EQ(result.installation->disposition,receive_install_disposition::already_installed);
    EXPECT_EQ(result.installation->revision,identity.expected_revision+1);EXPECT_EQ(result.installation->head,identity.head);
    EXPECT_EQ(guarded_snapshot(),stable);EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_range_attempt"),0);
    EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_range_page"),0);EXPECT_EQ(notifications,0);
    owned([&](auto&){EXPECT_EQ(journal().find(address,late.record.original_id)->stage,recovery_obligation_stage::open);});
    base->remove_table_observer("TestPerson",observer);
}
TEST_F(CanonicalScopedInstall, CommittedInspectionAfterFileReopenUsesFreshCanonicalGuard) {
    TempDB path("canonical-install-inspection");reset(path.str());x.content={person(A,"committed-before-reopen")};
    const auto before=recovery_guard().second;staged();ASSERT_TRUE(committed(install_staged_canonical_range(guarded(before))));
    const auto identity=*installation().last_installed;owned([&](auto&){stage().release_installed(x.a,x.m.manifest_digest,1);});
    owner->close();owner=std::make_shared<StagedOwner>(path.str());
    const auto fresh=receive_delivery_guard_access::read(*owner,address.channel);ASSERT_EQ(fresh.state,receive_guard_state::canonical_installed);
    const auto admission=guarded(fresh);const auto stable=guarded_snapshot();const auto result=inspect_committed_canonical_range(admission,identity,x.q,x.m);
    ASSERT_TRUE(committed(result));EXPECT_EQ(result.installation->disposition,receive_install_disposition::already_installed);
    EXPECT_EQ(guarded_snapshot(),stable);EXPECT_EQ(number("SELECT COUNT(*) FROM _lattice_range_attempt"),0);
    EXPECT_EQ(std::get<std::string>(query("SELECT name FROM TestPerson")[0].at("name")),"committed-before-reopen");owner->close();
}
TEST_F(CanonicalScopedInstall, CommittedInspectionRequiresEveryIdentityField) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();const auto admission=guarded(before);
    ASSERT_TRUE(committed(install_staged_canonical_range(admission)));const auto identity=*installation().last_installed;
    owned([&](auto&){stage().release_installed(x.a,x.m.manifest_digest,1);});const auto stable=guarded_snapshot();
    for(int field=0;field<10;++field) {
        auto changed=identity;
        switch(field) {
            case 0:++changed.sequence;break;case 1:++changed.expected_revision;break;
            case 2:changed.base.kind=receive_frontier_kind::beginning_null;break;
            case 3:changed.base={receive_frontier_kind::position,int64_t{0}};break;
            case 4:++changed.head;break;case 5:changed.mode=receive_install_mode::delta;break;
            case 6:changed.request_digest=std::string(64,'1');break;
            case 7:changed.receipt_digest=std::string(64,'2');break;
            case 8:changed.content_digest=std::string(64,'3');break;
            case 9:changed.manifest_digest=std::string(64,'4');break;
        }
        refused(inspect_committed_canonical_range(admission,changed,x.q,x.m),"exact retained receiver identity");EXPECT_EQ(guarded_snapshot(),stable);
    }
    ASSERT_TRUE(committed(inspect_committed_canonical_range(admission,identity,x.q,x.m)));EXPECT_EQ(guarded_snapshot(),stable);
}
TEST_F(CanonicalScopedInstall, CommittedInspectionRejectsUninstalledAndDoesNotCreateMissingStores) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();const auto admission=guarded(before);
    const auto identity=*installation().active;const auto stable=guarded_snapshot();
    refused(inspect_committed_canonical_range(admission,identity,x.q,x.m),"exact retained receiver identity");EXPECT_EQ(guarded_snapshot(),stable);
    const auto revision=scope().revision;owner->close();owner=std::make_shared<StagedOwner>();
    const auto missing=canonical_scoped_install_test_access::mint(owner,x.a,1,x.q.request_digest,x.m.manifest_digest,
        profile,address,revision,contract,limits,std::nullopt,"coverage",before);
    const auto pristine=guarded_snapshot();refused(inspect_committed_canonical_range(missing,identity,x.q,x.m));EXPECT_EQ(guarded_snapshot(),pristine);
    EXPECT_EQ(number("SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('_lattice_install_store','_lattice_obligation_store','_lattice_range_store')"),0);
}
TEST_F(CanonicalScopedInstall, CommittedInspectionFencesOldJournalGenerationButFreshAdmissionPreservesPending) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();const auto old=guarded(before);
    ASSERT_TRUE(committed(install_staged_canonical_range(old)));const auto identity=*installation().last_installed;
    owned([&](auto&){stage().release_installed(x.a,x.m.manifest_digest,1);address=journal().resume(address,identity).address;});
    const auto late=edit(A,"pending-after-resume");const auto stable=guarded_snapshot();
    refused(inspect_committed_canonical_range(old,identity,x.q,x.m),"current journal installation");EXPECT_EQ(guarded_snapshot(),stable);
    const auto fresh=guarded(receive_delivery_guard_access::read(*owner,address.channel));
    ASSERT_TRUE(committed(inspect_committed_canonical_range(fresh,identity,x.q,x.m)));EXPECT_EQ(guarded_snapshot(),stable);
    owned([&](auto&){EXPECT_EQ(journal().find(address,late.record.original_id)->stage,recovery_obligation_stage::open);});
}
TEST_F(CanonicalScopedInstall, CommittedInspectionRejectsWrongProfileMissingGuardAndChangedGuard) {
    for(int fault=0;fault<4;++fault) {
        reset();x.content={person(A)};const auto before=recovery_guard().second;staged();const auto original=guarded(before);
        ASSERT_TRUE(committed(install_staged_canonical_range(original)));const auto identity=*installation().last_installed;
        owned([&](auto&){stage().release_installed(x.a,x.m.manifest_digest,1);});
        auto changed=profile;if(fault==0)changed.binding.source="different-source";if(fault==1)changed.receipt_namespace="different-namespace";
        const auto grant=canonical_scoped_install_test_access::mint(owner,x.a,1,x.q.request_digest,x.m.manifest_digest,changed,address,
            scope().revision,contract,limits,identity,"coverage",fault==2?std::nullopt:std::optional<receive_guard_snapshot>{before});
        if(fault==3)owner->db().execute("UPDATE _lattice_receive_guard SET generation=generation+1");
        const auto stable=guarded_snapshot();refused(inspect_committed_canonical_range(grant,identity,x.q,x.m));EXPECT_EQ(guarded_snapshot(),stable);
    }
}
TEST_F(CanonicalScopedInstall, CommittedInspectionDiscardsReceiptWhenItsOwnedCommitFails) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();const auto admission=guarded(before);
    ASSERT_TRUE(committed(install_staged_canonical_range(admission)));const auto identity=*installation().last_installed;
    owned([&](auto&){stage().release_installed(x.a,x.m.manifest_digest,1);});const auto stable=guarded_snapshot();
    struct DenyCommit {
        sqlite3* db;explicit DenyCommit(sqlite3* value):db(value){sqlite3_set_authorizer(db,[](void*,int action,const char* first,const char*,const char*,const char*){
            return action==SQLITE_TRANSACTION&&first&&std::string_view(first)=="COMMIT"?SQLITE_DENY:SQLITE_OK;
        },nullptr);}~DenyCommit(){sqlite3_set_authorizer(db,nullptr,nullptr);}
    };
    {DenyCommit deny(owner->db().handle());const auto result=inspect_committed_canonical_range(admission,identity,x.q,x.m);
        refused(result);EXPECT_EQ(result.transaction.state,state::rolled_back);}
    EXPECT_EQ(guarded_snapshot(),stable);ASSERT_TRUE(committed(inspect_committed_canonical_range(admission,identity,x.q,x.m)));
}
TEST_F(CanonicalScopedInstall, CommittedInspectionRejectsSupersededLastIdentity) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();const auto old=guarded(before);
    ASSERT_TRUE(committed(install_staged_canonical_range(old)));const auto previous=*installation().last_installed;
    const auto previous_request=x.q;const auto previous_manifest=x.m;
    const auto guard=receive_delivery_guard_access::read(*owner,address.channel);next();++x.m.head;x.content={person(A,"successor")};staged();
    ASSERT_TRUE(committed(install_staged_canonical_range(guarded(guard))));const auto stable=guarded_snapshot();
    refused(inspect_committed_canonical_range(old,previous,previous_request,previous_manifest),"exact retained receiver identity");EXPECT_EQ(guarded_snapshot(),stable);
}

TEST_F(CanonicalScopedInstall, CommittedInspectionBindsEveryLogicalAttemptFieldToFrozenRequest) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();ASSERT_TRUE(committed(install_staged_canonical_range(guarded(before))));
    const auto identity=*installation().last_installed;owned([&](auto&){stage().release_installed(x.a,x.m.manifest_digest,1);});
    const auto logical=x.a;const auto current=receive_delivery_guard_access::read(*owner,address.channel);const auto stable=guarded_snapshot();
    for(int field=0;field<3;++field) {
        x.a=logical;if(field==0)x.a.receiver_incarnation=uuid('7');if(field==1)x.a.channel_incarnation=uuid('8');if(field==2)x.a.attempt_id=uuid('9');
        const auto changed=guarded(current);refused(inspect_committed_canonical_range(changed,identity,x.q,x.m));EXPECT_EQ(guarded_snapshot(),stable);
    }
    x.a=logical;const auto exact=guarded(current);ASSERT_TRUE(committed(inspect_committed_canonical_range(exact,identity,x.q,x.m)));EXPECT_EQ(guarded_snapshot(),stable);
}
TEST_F(CanonicalScopedInstall, CommittedInspectionRejectsChangedFrozenRequestOrManifestBytes) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();const auto admission=guarded(before);
    ASSERT_TRUE(committed(install_staged_canonical_range(admission)));const auto identity=*installation().last_installed;
    owned([&](auto&){stage().release_installed(x.a,x.m.manifest_digest,1);});const auto stable=guarded_snapshot();
    for(int field=0;field<4;++field) {
        auto request=x.q;auto manifest=x.m;
        if(field==0)++request.expected.revision;if(field==1)request.source.authority="different-authority";
        if(field==2)++manifest.head;if(field==3)manifest.protection.id="different-lease";
        refused(inspect_committed_canonical_range(admission,identity,request,manifest));EXPECT_EQ(guarded_snapshot(),stable);
    }
    ASSERT_TRUE(committed(inspect_committed_canonical_range(admission,identity,x.q,x.m)));EXPECT_EQ(guarded_snapshot(),stable);
}

TEST_F(CanonicalScopedInstall, CommittedInspectionReadDenialCannotBecomeSuccessOrAbsence) {
    x.content={person(A)};const auto before=recovery_guard().second;staged();const auto admission=guarded(before);
    ASSERT_TRUE(committed(install_staged_canonical_range(admission)));const auto identity=*installation().last_installed;
    owned([&](auto&){stage().release_installed(x.a,x.m.manifest_digest,1);});const auto stable=guarded_snapshot();
    struct DenyRead {
        sqlite3* db;std::string table;
        DenyRead(sqlite3* value,std::string name):db(value),table(std::move(name)) {
            sqlite3_set_authorizer(db,[](void* context,int action,const char* first,const char*,const char*,const char*){
                const auto& self=*static_cast<DenyRead*>(context);
                return action==SQLITE_READ&&first&&self.table==first?SQLITE_DENY:SQLITE_OK;
            },this);
        }
        ~DenyRead(){sqlite3_set_authorizer(db,nullptr,nullptr);}
    };
    for(const auto* table_name:{"_lattice_install_channel","_lattice_obligation_scope"}) {
        {DenyRead deny(owner->db().handle(),table_name);const auto result=inspect_committed_canonical_range(admission,identity,x.q,x.m);
            refused(result);EXPECT_EQ(result.transaction.state,state::rolled_back);}
        EXPECT_EQ(guarded_snapshot(),stable);
    }
    ASSERT_TRUE(committed(inspect_committed_canonical_range(admission,identity,x.q,x.m)));EXPECT_EQ(guarded_snapshot(),stable);
}
