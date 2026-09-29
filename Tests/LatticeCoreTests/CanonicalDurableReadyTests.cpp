#include "TestHelpers.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "CanonicalReadyAdoptionTestAccess.hpp"
#include "../../Sources/LatticeCore/src/canonical_ready_named_profile.hpp"
#include "../../Sources/LatticeCore/src/canonical_ready_sha256.hpp"
#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include <cstdio>
#include <cstring>
#include <thread>
#include <cstdlib>
#include <cerrno>
#include <csignal>
#if defined(__APPLE__) || defined(__linux__)
#include <spawn.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
extern char** environ;
#endif

struct DurableReadyRow {std::string body;};
LATTICE_SCHEMA(DurableReadyRow,body);
namespace lattice::detail {
// Unique TU-owned friend; it schedules the real public operation's stages.
struct canonical_ready_test_access {
    static canonical_ready_result prepare(canonical_writer_adapter& adapter,std::shared_ptr<lattice_db> owner,
        const canonical_namespace_admission& admission,const canonical_range::attempt& attempt,const canonical_range::request& request,
        int64_t duration,uint64_t route,const std::function<void()>& reserved={},const std::function<void(size_t,uint64_t)>& batch={}) {
        return adapter.prepare_ready_impl(std::move(owner),admission,attempt,request,duration,route,reserved,batch);
    }
    static canonical_ready_adoption_result adopt(std::shared_ptr<lattice_db> owner,
        const canonical_namespaced_writer_profile& source,canonical_upstream_limits upstream,canonical_retention_limits retention,
        const canonical_ready_profile& before,const std::string& name,int64_t grace) {
        return canonical_writer_adapter::adopt_authenticated_lifecycle(std::move(owner),source,upstream,retention,before,name,grace);
    }
    static void migrate(std::shared_ptr<lattice_db> owner,const canonical_namespaced_writer_profile& source,
        canonical_upstream_limits upstream,canonical_retention_limits retention,
        const canonical_ready_profile& before,const canonical_ready_profile& after) {
        canonical_writer_adapter::migrate_authenticated_source(std::move(owner),source,upstream,retention,before,after);
    }
};
canonical_ready_adoption_result adopt_ready_lifecycle_for_test(std::shared_ptr<lattice_db> owner,
    const canonical_namespaced_writer_profile& source,canonical_upstream_limits upstream,canonical_retention_limits retention,
    const canonical_ready_profile& before,const std::string& name,int64_t grace) {
    return canonical_ready_test_access::adopt(std::move(owner),source,upstream,retention,before,name,grace);
}
}
#if defined(__APPLE__) || defined(__linux__)
namespace {
using namespace lattice;
using namespace lattice::detail;
namespace cr=lattice::detail::canonical_range;
namespace sr=lattice::detail::sync_recovery;
using phase=recovery_install_state;
const bool registered=[] {
    auto schema=managed<DurableReadyRow>::schema();schema.properties[0].no_history=true;
    schema_registry::instance().register_model(typeid(DurableReadyRow),std::move(schema));return true;
}();
std::string ready_uuid(unsigned n){char out[37];std::snprintf(out,sizeof(out),"00000000-0000-4000-8000-%012u",n);return out;}
std::shared_ptr<lattice_db> ready_owner(const std::string& path,const char* synchronous="FULL") {
    configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=100;
    auto owner=std::make_shared<lattice_db>(c);
    // Existing public SQL configuration path, before protected attachment.
    // This fixture opts in explicitly; product attachment never changes it.
    owner->db().execute("PRAGMA main.synchronous="+std::string(synchronous));
    if(auto* notifier=instance_registry::instance().get_or_create_notifier(path))notifier->stop_listening();return owner;
}
canonical_namespaced_writer_profile writer_profile() {
    return {{{ready_uuid(8001),ready_uuid(8002),std::string(64,'a'),std::string(64,'b')},
        {128,65536,128,65536,32,128,64},{"DurableReadyRow"},true},
        {"source-local",{{"source-local","local-coverage",1},{"application-a","coverage-a",7},{"application-b","coverage-b",11}}}};
}
canonical_upstream_limits upstream(){return {32,4096,65536};}
canonical_retention_limits retention(){return {8,10000};}
canonical_ready_profile ready_profile(const canonical_namespaced_writer_profile& source) {
    canonical_ready_profile p;
    p.authority="qualification-source";p.transfers=4;p.bindings=8;p.charged_bytes=8*1024*1024;p.transfer_bytes=1024*1024;
    p.package={{{16384,4096,2,32,128,262144,32,32,131072},16,4096,4096,32,128,32768,65536,10000,{4096,16,256,2048,4096}},524288,66};
    p.capture={{{65536,16,4096,8192,2,32,64,524288},8,16,16},source.writer.limits,32,64,2};return p;
}
audit_log_entry ready_entry(unsigned original,unsigned target,std::string body="accepted") {
    audit_log_entry e;e.global_id=ready_uuid(original);e.global_row_id=ready_uuid(target);e.table_name="DurableReadyRow";e.operation="INSERT";
    e.changed_fields_names={"body"};e.changed_fields={{"body",any_property(std::move(body))}};e.timestamp="1789819200.0";return e;
}
int64_t scalar(database& db,const std::string& sql) {return std::get<int64_t>(db.query(sql).at(0).begin()->second);}
void committed(const recovery_install_result& result) {
    if(result.primary_error)std::rethrow_exception(result.primary_error);if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);if(result.notification_error)std::rethrow_exception(result.notification_error);
    if(result.state!=phase::committed)throw std::runtime_error("READY fixture expected known COMMIT");
}
void complete(const canonical_ready_result& result) {
    committed(result.preparation);if(result.capture_error)std::rethrow_exception(result.capture_error);committed(result.publication);
    if(!result.transfer || !result.transfer->ready || !result.lease)throw std::runtime_error("READY fixture lacks committed capsule and lease");
}
class CanonicalDurableReady:public ::testing::Test {
protected:
    TempDB file{"canonical-durable-ready"};
    std::shared_ptr<lattice_db> owner=ready_owner(file.str()),sibling=ready_owner(file.str());
    canonical_namespaced_writer_profile p=writer_profile();canonical_ready_profile policy=ready_profile(p);
    std::unique_ptr<canonical_writer_adapter> adapter;
    cr::attempt logical{ready_uuid(8101),ready_uuid(8102),"ready-channel",1,ready_uuid(8103)};
    cr::request request;
    void SetUp()override {request.source={policy.authority,p.writer.binding.source,p.writer.binding.epoch,p.writer.binding.scope,p.writer.binding.schema};
        request.budget=policy.package.codec.maximum;seal();}
    void seal(){request.request_digest=cr::request_sha256(logical,request,policy.package.codec);}
    void attach(){adapter=canonical_writer_adapter::attach_ready_for_qualification(owner,p,upstream(),retention(),policy);}
    canonical_namespace_admission admit(const std::string& ns="application-a",const std::string& replica="replica-one"){
        return adapter->admit_namespace_for_qualification(owner,ns,replica);
    }
    void import_entry(const canonical_namespace_admission& a,const audit_log_entry& e){
        if(adapter->apply_upstream_namespaced_owned(owner,a,{e},"upstream")!=std::vector<std::string>{e.global_id})throw std::runtime_error("READY import failed");
    }
    void ask(const audit_log_entry& e,const std::string& ns="application-a"){
        request.receipts={{e.global_id,ns,{{e.table_name,e.global_row_id}}}};seal();
    }
    canonical_ready_result prepare(const canonical_namespace_admission& a){return adapter->prepare_ready_owned(owner,a,logical,request,10000,7);}
    int64_t head(){return scalar(owner->db(),"SELECT head FROM _lattice_canonical_store");}
    int64_t count(const std::string& table){return scalar(owner->db(),"SELECT COUNT(*) FROM "+table);}
    std::vector<std::vector<database::row_t>> snapshot(){
        std::vector<std::vector<database::row_t>> out;
        for(const auto* table:{"_lattice_canonical_ready_profile","_lattice_canonical_ready_binding","_lattice_canonical_ready_transfer",
            "_lattice_canonical_ready_frame","_lattice_canonical_retention","_lattice_canonical_attempt","_lattice_canonical_store",
            "_lattice_canonical_receipt","_lattice_canonical_touch"})out.push_back(owner->db().query("SELECT * FROM "+std::string(table)+" ORDER BY 1"));return out;
    }
    std::vector<cr::frame> frames(const canonical_namespace_admission& a,const canonical_ready_result& result) {
        std::vector<cr::frame> values;
        for(uint64_t i=0;i<result.transfer->frames;++i){auto f=adapter->read_ready_frame_owned(owner,a,*result.lease,i);committed(f.settlement);
            if(!f.frame)throw std::runtime_error("READY frame absent after COMMIT");values.push_back(cr::decode(*f.frame,policy.package.codec));}
        return values;
    }
    void reopen(){adapter.reset();owner->close();sibling->close();owner=ready_owner(file.str());sibling=ready_owner(file.str());attach();}
};
struct ReadyFault {
    enum kind {charge_ignore,frame_ignore,frame_deny,publish_commit_deny,cleanup_deny,disposal_deny,lease_deadline_ignore};
    static thread_local ReadyFault* current;
    kind value;int hits=0,commits=0,cleanup=0;
    canonical_upstream_test_hooks::authorizer_fault fault;
    const canonical_upstream_test_hooks::authorizer_fault* prior;ReadyFault* previous;
    ReadyFault(database& db,kind k):value(k),fault{canonical_writer_custody_test_access::fault_handle(db),restrict_action},
        prior(canonical_retention_test_hooks::fault),previous(current){current=this;canonical_retention_test_hooks::fault=&fault;}
    ~ReadyFault(){canonical_retention_test_hooks::fault=prior;current=previous;}
    static int restrict_action(int action,const char* table,const char* column,const char* origin)noexcept {
        auto& f=*current;if(origin)return SQLITE_OK;
        const auto same=[](const char* a,const char* b){return a&&std::strcmp(a,b)==0;};
        if(action==SQLITE_TRANSACTION&&same(table,"COMMIT")) {
            ++f.commits;if(f.value==publish_commit_deny&&f.commits==2){++f.hits;return SQLITE_DENY;}
        }
        if(f.value==cleanup_deny&&action==SQLITE_TRANSACTION&&same(table,"ROLLBACK")){++f.cleanup;return SQLITE_DENY;}
        if(f.hits)return SQLITE_OK;
        if(f.value==charge_ignore&&action==SQLITE_UPDATE&&same(table,"_lattice_canonical_ready_profile")&&same(column,"charged")){++f.hits;return SQLITE_IGNORE;}
        if(f.value==lease_deadline_ignore&&action==SQLITE_UPDATE&&same(table,"_lattice_canonical_ready_transfer")&&same(column,"deadline_ms")){++f.hits;return SQLITE_IGNORE;}
        if((f.value==frame_ignore||f.value==frame_deny||f.value==cleanup_deny)&&action==SQLITE_INSERT&&same(table,"_lattice_canonical_ready_frame")) {
            ++f.hits;return f.value==frame_ignore?SQLITE_IGNORE:SQLITE_DENY;
        }
        if(f.value==disposal_deny&&action==SQLITE_DELETE&&same(table,"_lattice_canonical_ready_frame")){++f.hits;return SQLITE_DENY;}
        return SQLITE_OK;
    }
};
thread_local ReadyFault* ReadyFault::current=nullptr;
}

TEST_F(CanonicalDurableReady, RealReceiptSameViewPayloadAndPinSurviveLaterImport) {
    attach();auto admission=admit();auto first=ready_entry(101,1,"captured");import_entry(admission,first);ask(first);
    const auto before=head();bool reserved=false,mutated=false;
    auto result=canonical_ready_test_access::prepare(*adapter,owner,admission,logical,request,10000,7,[&]{
        reserved=true;EXPECT_EQ(count("_lattice_canonical_attempt"),1);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
        EXPECT_EQ(scalar(owner->db(),"SELECT state FROM _lattice_canonical_ready_transfer"),1);EXPECT_EQ(count("_lattice_canonical_ready_frame"),0);
        import_entry(admission,ready_entry(102,2,"after reservation"));EXPECT_NE(adapter->prune_recovery_owned(owner,head()).state,phase::committed);
    },[&](size_t,uint64_t){if(mutated)return;mutated=true;auto later=ready_entry(103,1,"after snapshot");later.operation="UPDATE";import_entry(admission,later);});
    complete(result);EXPECT_TRUE(reserved);EXPECT_TRUE(mutated);EXPECT_GT(result.transfer->protected_base,before);EXPECT_LT(result.transfer->protected_base,head());
    EXPECT_EQ(count("_lattice_canonical_attempt"),0);EXPECT_EQ(scalar(owner->db(),"SELECT pin=head AND state=2 FROM _lattice_canonical_ready_transfer"),1);
    bool payload=false,receipt=false;for(const auto& frame:frames(admission,result)) {
        EXPECT_EQ(frame.route_generation,7u);
        if(const auto* page=std::get_if<cr::content_page>(&frame.body))for(const auto& item:page->items)if(item.key.id==first.global_row_id) {
            ASSERT_TRUE(std::holds_alternative<cr::present>(item.value));const auto values=sr::decode_values(std::get<cr::present>(item.value).payload,policy.package.codec.values);
            EXPECT_EQ(std::get<std::string>(values.at("body")),"captured");payload=true;
        }
        if(const auto* page=std::get_if<cr::receipt_page>(&frame.body))for(const auto& item:page->items) {
            ASSERT_TRUE(std::holds_alternative<cr::committed>(item.value));const auto& fact=std::get<cr::committed>(item.value);
            EXPECT_EQ(item.original_id,first.global_id);EXPECT_EQ(fact.namespace_id,"application-a");EXPECT_EQ(fact.coverage_id,"coverage-a");
            EXPECT_LE(fact.position,result.transfer->manifest->head);receipt=true;
        }
    }
    EXPECT_TRUE(payload);EXPECT_TRUE(receipt);EXPECT_EQ(owner->local_read_generations_outstanding(),0u);
    committed(adapter->prune_recovery_owned(owner,result.transfer->protected_base));EXPECT_NE(adapter->prune_recovery_owned(owner,head()).state,phase::committed);
}
TEST_F(CanonicalDurableReady, MissingReceiptStaysUnknownAndDeleteRemainsAnExplicitTombstone) {
    attach();auto admission=admit();auto original=ready_entry(101,1);import_entry(admission,original);
    auto deleted=ready_entry(102,1);deleted.operation="DELETE";deleted.changed_fields.clear();deleted.changed_fields_names.clear();import_entry(admission,deleted);
    ask(deleted);request.receipts.push_back({ready_uuid(999),"application-a",{{"DurableReadyRow",ready_uuid(2)}}});seal();
    auto result=prepare(admission);complete(result);int tombstones=0,positive=0,unknown=0;
    for(const auto& frame:frames(admission,result)) {
        if(const auto* page=std::get_if<cr::content_page>(&frame.body))for(const auto& item:page->items)if(std::holds_alternative<cr::tombstone>(item.value))++tombstones;
        if(const auto* page=std::get_if<cr::receipt_page>(&frame.body))for(const auto& item:page->items) {
            EXPECT_FALSE(std::holds_alternative<cr::not_committed>(item.value));positive+=std::holds_alternative<cr::committed>(item.value);unknown+=std::holds_alternative<cr::unknown>(item.value);
        }
    }
    EXPECT_EQ(tombstones,2);EXPECT_EQ(positive,1);EXPECT_EQ(unknown,1);
}
TEST_F(CanonicalDurableReady, ActualDeltaCapsulePreservesRequestedBaseAndCanonicalReceipt) {
    attach();auto admission=admit();auto original=ready_entry(101,1,"base");import_entry(admission,original);const auto base=head();
    auto update=ready_entry(102,1,"delta");update.operation="UPDATE";import_entry(admission,update);ask(update);
    request.selection=cr::mode::delta;request.base=base;
    request.expected={static_cast<uint64_t>(base),request.source,{cr::frontier_kind::position,static_cast<uint64_t>(base)}};
    logical.sequence=static_cast<uint64_t>(base+1);seal();auto result=prepare(admission);complete(result);
    EXPECT_EQ(result.transfer->manifest->selection,cr::mode::delta);EXPECT_EQ(result.transfer->manifest->base,std::optional<uint64_t>{static_cast<uint64_t>(base)});
    EXPECT_EQ(result.transfer->manifest->head,static_cast<uint64_t>(head()));int present=0,positive=0;
    for(const auto& frame:frames(admission,result)) {
        if(const auto* page=std::get_if<cr::content_page>(&frame.body))for(const auto& item:page->items) {
            EXPECT_EQ(item.key.id,update.global_row_id);ASSERT_TRUE(std::holds_alternative<cr::present>(item.value));
            const auto values=sr::decode_values(std::get<cr::present>(item.value).payload,policy.package.codec.values);
            EXPECT_EQ(std::get<std::string>(values.at("body")),"delta");++present;
        }
        if(const auto* page=std::get_if<cr::receipt_page>(&frame.body))for(const auto& item:page->items) {
            EXPECT_EQ(item.original_id,update.global_id);EXPECT_TRUE(std::holds_alternative<cr::committed>(item.value));++positive;
        }
    }
    EXPECT_EQ(present,1);EXPECT_EQ(positive,1);
}
TEST_F(CanonicalDurableReady, SeparateAdmittedNamespacesKeepIndependentCapsulesOnTheSameSource) {
    attach();auto a=admit(),b=admit("application-b");auto ea=ready_entry(101,1,"a"),eb=ready_entry(102,2,"b");
    import_entry(a,ea);import_entry(b,eb);ask(ea);auto ra=prepare(a);complete(ra);ask(eb,"application-b");auto rb=prepare(b);complete(rb);
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),2);EXPECT_EQ(count("_lattice_canonical_ready_binding"),2);
    for(const auto& frame:frames(b,rb))if(const auto* page=std::get_if<cr::receipt_page>(&frame.body))for(const auto& item:page->items) {
        ASSERT_TRUE(std::holds_alternative<cr::committed>(item.value));EXPECT_EQ(std::get<cr::committed>(item.value).namespace_id,"application-b");
    }
    EXPECT_NE(adapter->read_ready_frame_owned(owner,a,*rb.lease,0).settlement.state,phase::committed);
    committed(adapter->abandon_ready_owned(owner,ra.transfer->identity));EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
    auto read=adapter->read_ready_frame_owned(owner,b,*rb.lease,0);committed(read.settlement);ASSERT_TRUE(read.frame);
}
TEST_F(CanonicalDurableReady, IgnoredPreparationChargeRollsBackEveryNewRow) {
    attach();auto admission=admit();const auto before=snapshot();ReadyFault fault(owner->db(),ReadyFault::charge_ignore);
    auto result=prepare(admission);EXPECT_EQ(result.preparation.state,phase::rolled_back);EXPECT_EQ(fault.hits,1);
    EXPECT_FALSE(result.transfer);EXPECT_FALSE(result.lease);EXPECT_EQ(snapshot(),before);
}
TEST_F(CanonicalDurableReady, FailedOrIgnoredFrameAndCommitKeepChargedPreparationAndRealPin) {
    for(auto kind:{ReadyFault::frame_ignore,ReadyFault::frame_deny,ReadyFault::publish_commit_deny}) {
        if(!adapter)attach();auto admission=admit();canonical_ready_result result;
        {ReadyFault fault(owner->db(),kind);result=prepare(admission);EXPECT_EQ(fault.hits,1);}
        committed(result.preparation);EXPECT_EQ(result.publication.state,phase::rolled_back);EXPECT_NE(result.publication.primary_error,nullptr);
        ASSERT_TRUE(result.transfer);EXPECT_FALSE(result.transfer->ready);EXPECT_FALSE(result.lease);
        EXPECT_EQ(count("_lattice_canonical_ready_frame"),0);EXPECT_EQ(count("_lattice_canonical_attempt"),1);
        owner->add(DurableReadyRow{"tail"});EXPECT_NE(adapter->prune_recovery_owned(owner,head()).state,phase::committed);
        committed(adapter->abandon_ready_owned(owner,result.transfer->identity));EXPECT_EQ(count("_lattice_canonical_attempt"),0);
        ++logical.sequence;logical.attempt_id=ready_uuid(8200+static_cast<unsigned>(logical.sequence));seal();
    }
}
TEST_F(CanonicalDurableReady, RollbackFailureRemainsUnsettledAndExposesNoReadyLease) {
    attach();auto admission=admit();ReadyFault fault(owner->db(),ReadyFault::cleanup_deny);auto result=prepare(admission);
    committed(result.preparation);EXPECT_EQ(result.publication.state,phase::unsettled);EXPECT_NE(result.publication.primary_error,nullptr);
    EXPECT_NE(result.publication.cleanup_error,nullptr);EXPECT_EQ(fault.cleanup,1);EXPECT_FALSE(result.lease);EXPECT_TRUE(owner->db().is_closed());
}
TEST_F(CanonicalDurableReady, PublicationPostcommitErrorKeepsExactCommittedCapsule) {
    attach();auto admission=admit();int calls=0;const auto hook=owner->add_invalidation_hook([&](const auto&,auto){if(++calls==2)throw std::runtime_error("READY publication observer");});
    auto result=prepare(admission);owner->remove_invalidation_hook(hook);
    EXPECT_EQ(result.preparation.state,phase::committed);EXPECT_EQ(result.publication.state,phase::committed);EXPECT_EQ(result.publication.primary_error,nullptr);
    EXPECT_NE(result.publication.postcommit_error,nullptr);ASSERT_TRUE(result.transfer);EXPECT_TRUE(result.transfer->ready);ASSERT_TRUE(result.lease);
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);EXPECT_EQ(count("_lattice_canonical_attempt"),0);
    auto read=adapter->read_ready_frame_owned(owner,admission,*result.lease,0);committed(read.settlement);ASSERT_TRUE(read.frame);
}
TEST_F(CanonicalDurableReady, EquivalentResumeChangesOnlyPhysicalEnvelopeAndFencesPriorLease) {
    attach();auto admission=admit();auto result=prepare(admission);complete(result);
    const auto stored=owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index");
    auto resumed=adapter->resume_ready_owned(owner,admission,logical,request,10000,999);committed(resumed.settlement);
    ASSERT_TRUE(resumed.lease);ASSERT_TRUE(resumed.transfer);EXPECT_EQ(resumed.transfer->manifest,result.transfer->manifest);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index"),stored);
    EXPECT_NE(adapter->read_ready_frame_owned(owner,admission,*result.lease,0).settlement.state,phase::committed);
    for(uint64_t i=0;i<resumed.transfer->frames;++i) {
        auto frame=adapter->read_ready_frame_owned(owner,admission,*resumed.lease,i);committed(frame.settlement);ASSERT_TRUE(frame.frame);
        auto decoded=cr::decode(*frame.frame,policy.package.codec);EXPECT_EQ(decoded.route_generation,999u);decoded.route_generation=1;
        const auto& raw=std::get<std::vector<uint8_t>>(stored.at(i).at("data"));EXPECT_EQ(cr::encode(decoded,policy.package.codec),std::string(raw.begin(),raw.end()));
    }
    EXPECT_NE(adapter->read_ready_frame_owned(owner,admission,*resumed.lease,resumed.transfer->frames).settlement.state,phase::committed);
}
TEST_F(CanonicalDurableReady, IgnoredResumeDeadlineRollsBackAndKeepsPriorLeaseValid) {
    attach();auto admission=admit();auto result=prepare(admission);complete(result);const auto before=snapshot();
    {ReadyFault fault(owner->db(),ReadyFault::lease_deadline_ignore);auto resumed=adapter->resume_ready_owned(owner,admission,logical,request,1,8);
        EXPECT_EQ(resumed.settlement.state,phase::rolled_back);EXPECT_EQ(fault.hits,1);EXPECT_FALSE(resumed.lease);}
    EXPECT_EQ(snapshot(),before);auto frame=adapter->read_ready_frame_owned(owner,admission,*result.lease,0);committed(frame.settlement);ASSERT_TRUE(frame.frame);
}
TEST_F(CanonicalDurableReady, WrongNamespaceReplicaRequestAndSourceCannotResumeOrRead) {
    attach();auto admission=admit();auto result=prepare(admission);complete(result);const auto before=snapshot();
    for(const auto& foreign:{admit("application-b"),admit("application-a","other-replica")}) {
        EXPECT_NE(adapter->resume_ready_owned(owner,foreign,logical,request,10000,8).settlement.state,phase::committed);
        EXPECT_NE(adapter->read_ready_frame_owned(owner,foreign,*result.lease,0).settlement.state,phase::committed);
    }
    auto changed=request;changed.budget.items_per_page=1;changed.request_digest=cr::request_sha256(logical,changed,policy.package.codec);
    EXPECT_NE(adapter->resume_ready_owned(owner,admission,logical,changed,10000,8).settlement.state,phase::committed);
    changed=request;changed.source.authority="other";changed.request_digest=cr::request_sha256(logical,changed,policy.package.codec);
    EXPECT_NE(adapter->resume_ready_owned(owner,admission,logical,changed,10000,8).settlement.state,phase::committed);
    EXPECT_EQ(snapshot(),before);
}
TEST_F(CanonicalDurableReady, PreopenedSiblingAndOrdinaryOwnerCannotMutateSpoolOrPruneItsPin) {
    attach();auto admission=admit();auto result=prepare(admission);complete(result);const auto before=snapshot();
    for(const auto* sql:{"DELETE FROM _lattice_canonical_ready_transfer","UPDATE _lattice_canonical_ready_transfer SET pin=0",
        "UPDATE _lattice_canonical_ready_profile SET charged=0","DELETE FROM _lattice_canonical_ready_binding",
        "UPDATE _lattice_canonical_ready_frame SET data=X'7b7d'","UPDATE _lattice_canonical_store SET floor=head"}) {
        EXPECT_THROW(owner->db().execute(sql),db_error);
        EXPECT_THROW(sibling->db().execute(sql),db_error);
    }
    EXPECT_NE(adapter->read_ready_frame_owned(sibling,admission,*result.lease,0).settlement.state,phase::committed);
    EXPECT_NE(adapter->prune_recovery_owned(sibling,head()).state,phase::committed);EXPECT_EQ(snapshot(),before);
}
TEST_F(CanonicalDurableReady, CapacityAndTerminalHighWaterDoNotRecreateDisposedAttempt) {
    policy.transfers=1;policy.bindings=1;attach();auto admission=admit();auto result=prepare(admission);complete(result);const auto before=snapshot();
    auto other=logical;other.channel="another-channel";auto q=request;q.request_digest=cr::request_sha256(other,q,policy.package.codec);
    EXPECT_NE(adapter->prepare_ready_owned(owner,admission,other,q,10000,8).preparation.state,phase::committed);
    EXPECT_EQ(snapshot(),before);committed(adapter->abandon_ready_owned(owner,result.transfer->identity));
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);EXPECT_EQ(count("_lattice_canonical_ready_frame"),0);
    EXPECT_EQ(count("_lattice_canonical_ready_binding"),1);EXPECT_GT(scalar(owner->db(),"SELECT charged FROM _lattice_canonical_ready_profile"),0);
    EXPECT_NE(prepare(admission).preparation.state,phase::committed);
    EXPECT_NE(adapter->prepare_ready_owned(owner,admission,other,q,10000,8).preparation.state,phase::committed);
    ++logical.sequence;logical.attempt_id=ready_uuid(8200);seal();auto next=prepare(admission);complete(next);
    EXPECT_EQ(count("_lattice_canonical_ready_binding"),1);EXPECT_EQ(scalar(owner->db(),"SELECT sequence FROM _lattice_canonical_ready_binding"),2);
}
TEST_F(CanonicalDurableReady, FailedDisposalRetainsCapsuleAndSuccessfulDisposalDoesNotSettleReceipts) {
    attach();auto admission=admit();auto e=ready_entry(101,1);import_entry(admission,e);ask(e);auto result=prepare(admission);complete(result);
    const auto receipt=owner->db().query("SELECT * FROM _lattice_canonical_receipt ORDER BY original_id");const auto before=snapshot();
    {ReadyFault fault(owner->db(),ReadyFault::disposal_deny);auto disposed=adapter->abandon_ready_owned(owner,result.transfer->identity);
        EXPECT_EQ(disposed.state,phase::rolled_back);EXPECT_EQ(fault.hits,1);}
    EXPECT_EQ(snapshot(),before);committed(adapter->abandon_ready_owned(owner,result.transfer->identity));
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_receipt ORDER BY original_id"),receipt);
    EXPECT_NE(adapter->read_ready_frame_owned(owner,admission,*result.lease,0).settlement.state,phase::committed);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM DurableReadyRow"),1);
}
TEST_F(CanonicalDurableReady, ExplicitExpiryFencesLeaseAndReleasesOnlySourceStorage) {
    attach();auto admission=admit();auto e=ready_entry(101,1);import_entry(admission,e);ask(e);auto result=prepare(admission);complete(result);
    auto short_lease=adapter->resume_ready_owned(owner,admission,logical,request,1,8);committed(short_lease.settlement);ASSERT_TRUE(short_lease.lease);
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    EXPECT_NE(adapter->read_ready_frame_owned(owner,admission,*short_lease.lease,0).settlement.state,phase::committed);
    EXPECT_NE(adapter->resume_ready_owned(owner,admission,logical,request,10000,9).settlement.state,phase::committed);
    const auto receipts=owner->db().query("SELECT * FROM _lattice_canonical_receipt ORDER BY original_id");
    committed(adapter->expire_ready_owned(owner));EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_receipt ORDER BY original_id"),receipts);
    EXPECT_EQ(count("_lattice_canonical_ready_binding"),1);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM DurableReadyRow"),1);
}
TEST_F(CanonicalDurableReady, ReopenRetainsExpiredOrphanBytesChargeAndPinUntilFreshEquivalentAdmission) {
    attach();auto old_admission=admit();auto result=prepare(old_admission);complete(result);
    auto short_lease=adapter->resume_ready_owned(owner,old_admission,logical,request,1,8);committed(short_lease.settlement);ASSERT_TRUE(short_lease.lease);
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    const auto stored=owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index");
    const auto charged=scalar(owner->db(),"SELECT charged FROM _lattice_canonical_ready_profile");reopen();
    auto inspect=adapter->inspect_ready_owned(owner);committed(inspect.settlement);ASSERT_EQ(inspect.transfers.size(),1u);
    EXPECT_TRUE(inspect.transfers[0].ready);EXPECT_TRUE(inspect.transfers[0].orphan);EXPECT_EQ(inspect.transfers[0].manifest,result.transfer->manifest);
    committed(adapter->expire_ready_owned(owner));EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
    EXPECT_EQ(scalar(owner->db(),"SELECT charged FROM _lattice_canonical_ready_profile"),charged);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index"),stored);
    owner->add(DurableReadyRow{"tail"});EXPECT_NE(adapter->prune_recovery_owned(owner,head()).state,phase::committed);
    auto fresh=admit();EXPECT_NE(adapter->read_ready_frame_owned(owner,fresh,*short_lease.lease,0).settlement.state,phase::committed);
    EXPECT_NE(adapter->resume_ready_owned(owner,old_admission,logical,request,10000,9).settlement.state,phase::committed);
    auto resumed=adapter->resume_ready_owned(owner,fresh,logical,request,10000,9);committed(resumed.settlement);ASSERT_TRUE(resumed.lease);
    EXPECT_EQ(resumed.transfer->manifest,result.transfer->manifest);auto read=adapter->read_ready_frame_owned(owner,fresh,*resumed.lease,0);committed(read.settlement);ASSERT_TRUE(read.frame);
    EXPECT_EQ(cr::decode(*read.frame,policy.package.codec).route_generation,9u);
}
TEST_F(CanonicalDurableReady, CancelledPreparationStaysPinnedAndRestartDisposesItWithoutResettingHighWater) {
    attach();auto admission=admit();auto result=canonical_ready_test_access::prepare(*adapter,owner,admission,logical,request,10000,7,
        []{throw std::runtime_error("cancel before capture");});
    committed(result.preparation);EXPECT_NE(result.capture_error,nullptr);EXPECT_FALSE(result.lease);ASSERT_TRUE(result.transfer);
    EXPECT_EQ(count("_lattice_canonical_attempt"),1);EXPECT_EQ(count("_lattice_canonical_ready_frame"),0);
    owner->add(DurableReadyRow{"after cancellation"});EXPECT_NE(adapter->prune_recovery_owned(owner,head()).state,phase::committed);
    reopen();EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);EXPECT_EQ(count("_lattice_canonical_attempt"),0);EXPECT_EQ(count("_lattice_canonical_ready_binding"),1);
    auto fresh=admit();EXPECT_NE(prepare(fresh).preparation.state,phase::committed);committed(adapter->prune_recovery_owned(owner,head()));
}
TEST_F(CanonicalDurableReady, WrapperRetirementAfterPreparationCannotFollowIntoCaptureOrPublication) {
    attach();auto admission=admit();auto result=canonical_ready_test_access::prepare(*adapter,owner,admission,logical,request,10000,7,[&]{adapter.reset();});
    committed(result.preparation);EXPECT_NE(result.capture_error,nullptr);EXPECT_FALSE(result.lease);EXPECT_FALSE(adapter);
    EXPECT_EQ(owner->local_read_generations_outstanding(),0u);attach();EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);
    EXPECT_EQ(count("_lattice_canonical_ready_binding"),1);
}
TEST_F(CanonicalDurableReady, CloseDuringCaptureReleasesViewAndPostcommitClosePreservesReady) {
    attach();auto admission=admit();owner->add(DurableReadyRow{"copied"});bool closed=false;
    auto cancelled=canonical_ready_test_access::prepare(*adapter,owner,admission,logical,request,10000,7,{},[&](size_t,uint64_t){if(!closed){closed=true;owner->close();adapter.reset();}});
    committed(cancelled.preparation);EXPECT_TRUE(closed);EXPECT_NE(cancelled.capture_error,nullptr);EXPECT_FALSE(cancelled.lease);
    EXPECT_EQ(owner->local_read_generations_outstanding(),0u);reopen();++logical.sequence;logical.attempt_id=ready_uuid(8300);seal();auto fresh=admit();
    int calls=0;auto* physical=owner.get();const auto hook=owner->add_invalidation_hook([&](const auto&,auto){if(++calls==2){adapter.reset();physical->close();}});
    auto published=prepare(fresh);EXPECT_EQ(published.publication.state,phase::committed);ASSERT_TRUE(published.transfer);EXPECT_TRUE(published.transfer->ready);
    owner->remove_invalidation_hook(hook);reopen();auto inspect=adapter->inspect_ready_owned(owner);committed(inspect.settlement);
    ASSERT_EQ(inspect.transfers.size(),1u);EXPECT_TRUE(inspect.transfers[0].ready);EXPECT_TRUE(inspect.transfers[0].orphan);
}
TEST_F(CanonicalDurableReady, RetiredDeltaRequestsRequireANewExplicitFullRequest) {
    attach();auto admission=admit();owner->add(DurableReadyRow{"one"});const auto base=head();owner->add(DurableReadyRow{"two"});committed(adapter->prune_recovery_owned(owner,head()));
    request.selection=cr::mode::delta;request.base=base;request.expected={static_cast<uint64_t>(base),request.source,{cr::frontier_kind::position,static_cast<uint64_t>(base)}};
    logical.sequence=static_cast<uint64_t>(head()+1);seal();const auto frozen=request;const auto before=snapshot();auto result=prepare(admission);
    committed(result.preparation);EXPECT_TRUE(result.requires_full_request);EXPECT_FALSE(result.transfer);EXPECT_FALSE(result.lease);EXPECT_EQ(request,frozen);EXPECT_EQ(snapshot(),before);
    request.selection=cr::mode::full;request.base.reset();++logical.sequence;logical.attempt_id=ready_uuid(8400);seal();auto full=prepare(admission);complete(full);
}
TEST_F(CanonicalDurableReady, LegacyV2AndReadyV3NeverSilentlyAdoptEachOther) {
    adapter=canonical_writer_adapter::attach_namespaced_upstream_for_qualification(owner,p,upstream(),retention());
    auto reserved=adapter->reserve_recovery_owned(owner,{},10000);committed(reserved.settlement);adapter.reset();
    const auto before=owner->db().query("SELECT * FROM _lattice_canonical_retention");
    EXPECT_THROW(attach(),db_error);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_retention"),before);EXPECT_EQ(count("_lattice_canonical_attempt"),1);
    EXPECT_FALSE(owner->db().table_exists("_lattice_canonical_ready_profile"));
}
TEST_F(CanonicalDurableReady, V3CannotReopenThroughUnprotectedV2Attachment) {
    attach();auto admission=admit();auto result=prepare(admission);complete(result);adapter.reset();const auto before=snapshot();
    EXPECT_THROW(canonical_writer_adapter::attach_namespaced_upstream_for_qualification(owner,p,upstream(),retention()),db_error);
    EXPECT_EQ(snapshot(),before);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
}
TEST_F(CanonicalDurableReady, WeakDurabilityRefusesBeforeEnrollmentAndOwnedWriterCannotDowngrade) {
    owner->db().execute("PRAGMA main.synchronous=NORMAL");
    const auto schema=owner->db().query("SELECT type,name,sql FROM sqlite_master ORDER BY type,name");
    EXPECT_THROW(attach(),db_error);
    EXPECT_EQ(owner->db().query("SELECT type,name,sql FROM sqlite_master ORDER BY type,name"),schema);
    EXPECT_FALSE(owner->db().table_exists("_lattice_canonical_ready_transfer"));EXPECT_FALSE(owner->db().table_exists("_lattice_canonical_store"));
    owner->db().execute("PRAGMA main.synchronous=EXTRA");attach();
    EXPECT_EQ(scalar(owner->db(),"PRAGMA main.synchronous"),3);
    EXPECT_THROW(owner->db().execute("PRAGMA main.synchronous=OFF"),db_error);
    EXPECT_THROW(owner->db().execute("PRAGMA main.journal_mode=DELETE"),db_error);
    auto admission=admit();auto result=prepare(admission);complete(result);reopen();
    EXPECT_EQ(scalar(owner->db(),"PRAGMA main.synchronous"),2);auto fresh=admit();auto resumed=adapter->resume_ready_owned(owner,fresh,logical,request,10000,8);
    committed(resumed.settlement);ASSERT_TRUE(resumed.lease);
}
TEST_F(CanonicalDurableReady, WeakReopenPreservesReadyAndConfiguredExtraResumesIt) {
    attach();auto admission=admit();auto result=prepare(admission);complete(result);
    adapter.reset();owner->close();sibling->close();owner=ready_owner(file.str(),"NORMAL");sibling=ready_owner(file.str());
    const auto before=snapshot();
    EXPECT_THROW(attach(),db_error);
    EXPECT_EQ(snapshot(),before);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
    owner->db().execute("PRAGMA main.synchronous=EXTRA");attach();
    EXPECT_EQ(scalar(owner->db(),"PRAGMA main.synchronous"),3);
    auto fresh=admit();auto resumed=adapter->resume_ready_owned(owner,fresh,logical,request,10000,9);committed(resumed.settlement);
    ASSERT_TRUE(resumed.lease);EXPECT_EQ(resumed.transfer->manifest,result.transfer->manifest);
}
TEST_F(CanonicalDurableReady, ChangedPolicyAndMissingGuardRefuseBeforeOrphanCleanup) {
    attach();auto admission=admit();auto result=prepare(admission);complete(result);adapter.reset();
    const auto before=snapshot();++policy.bindings;
    EXPECT_THROW(attach(),db_error);
    EXPECT_EQ(snapshot(),before);--policy.bindings;owner->close();sibling->close();
    {database raw(file.str());raw.execute("DROP TRIGGER _lattice_canonical_ready_frame_guard_DELETE");}
    owner=ready_owner(file.str());sibling=ready_owner(file.str());const auto prior=snapshot();
    EXPECT_THROW(attach(),db_error);
    EXPECT_EQ(snapshot(),prior);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
}
TEST_F(CanonicalDurableReady, AlteredFrameWithRestoredExactGuardRefusesBeforeIncarnationOrCleanup) {
    attach();auto admission=admit();auto result=prepare(admission);complete(result);adapter.reset();owner->close();sibling->close();
    {database raw(file.str());const auto sql=std::get<std::string>(raw.query("SELECT sql FROM sqlite_master WHERE name='_lattice_canonical_ready_frame_guard_UPDATE'").at(0).at("sql"));
        raw.execute("DROP TRIGGER _lattice_canonical_ready_frame_guard_UPDATE");raw.execute("UPDATE _lattice_canonical_ready_frame SET data=X'7b7d' WHERE frame_index=0");raw.execute(sql);}
    owner=ready_owner(file.str());sibling=ready_owner(file.str());const auto before=snapshot();
    EXPECT_THROW(attach(),db_error);
    EXPECT_EQ(snapshot(),before);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
}
TEST(CanonicalDurableReadyRestart, FreshProcessesResumeKnownCommittedCapsuleAfterAbruptSourceExit) {
    constexpr const char* variable="LATTICE_READY_PEER=";
    if(const char* encoded=std::getenv("LATTICE_READY_PEER")) {
        sigset_t signals;sigemptyset(&signals);sigaddset(&signals,SIGALRM);
        ASSERT_NE(std::signal(SIGALRM,SIG_DFL),SIG_ERR);ASSERT_EQ(sigprocmask(SIG_UNBLOCK,&signals,nullptr),0);alarm(30);
        const std::string input(encoded);ASSERT_GT(input.size(),2u);const bool seed=input[0]=='s';
        auto owner=ready_owner(input.substr(2));auto p=writer_profile();auto policy=ready_profile(p);
        auto adapter=canonical_writer_adapter::attach_ready_for_qualification(owner,p,upstream(),retention(),policy);
        auto admission=adapter->admit_namespace_for_qualification(owner,"application-a","replica-one");
        cr::attempt logical{ready_uuid(8101),ready_uuid(8102),"ready-channel",1,ready_uuid(8103)};
        cr::request request;request.source={policy.authority,p.writer.binding.source,p.writer.binding.epoch,p.writer.binding.scope,p.writer.binding.schema};
        request.budget=policy.package.codec.maximum;auto original=ready_entry(101,1,"crash durable");
        request.receipts={{original.global_id,"application-a",{{original.table_name,original.global_row_id}}}};
        request.request_digest=cr::request_sha256(logical,request,policy.package.codec);
        EXPECT_EQ(scalar(owner->db(),"SELECT incarnation FROM _lattice_canonical_retention"),seed?1:2);
        if(seed) {
            ASSERT_EQ(adapter->apply_upstream_namespaced_owned(owner,admission,{original},"upstream"),std::vector<std::string>{original.global_id});
            auto prepared=adapter->prepare_ready_owned(owner,admission,logical,request,10000,7);complete(prepared);
            EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),0);
            if(HasFailure())return;
            // No source/SQLite destructors or checkpoint: the child exits only
            // after the actual publication reports known COMMIT.
            std::fflush(nullptr);::_exit(0);
        }
        auto before=adapter->inspect_ready_owned(owner);committed(before.settlement);ASSERT_EQ(before.transfers.size(),1u);
        EXPECT_TRUE(before.transfers[0].orphan);ASSERT_TRUE(before.transfers[0].manifest);const auto manifest=*before.transfers[0].manifest;
        const auto rows=owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index");
        const auto charge=scalar(owner->db(),"SELECT charged FROM _lattice_canonical_ready_profile");
        committed(adapter->expire_ready_owned(owner));EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_ready_transfer"),1);
        auto resumed=adapter->resume_ready_owned(owner,admission,logical,request,10000,900);committed(resumed.settlement);ASSERT_TRUE(resumed.lease);
        ASSERT_TRUE(resumed.transfer);EXPECT_EQ(resumed.transfer->manifest,std::optional<cr::manifest>{manifest});
        EXPECT_EQ(scalar(owner->db(),"SELECT charged FROM _lattice_canonical_ready_profile"),charge);
        EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index"),rows);
        bool payload=false,receipt=false;
        for(uint64_t i=0;i<resumed.transfer->frames;++i) {
            auto read=adapter->read_ready_frame_owned(owner,admission,*resumed.lease,i);committed(read.settlement);ASSERT_TRUE(read.frame);
            const auto frame=cr::decode(*read.frame,policy.package.codec);EXPECT_EQ(frame.route_generation,900u);
            if(const auto* page=std::get_if<cr::content_page>(&frame.body))for(const auto& item:page->items)if(item.key.id==original.global_row_id) {
                ASSERT_TRUE(std::holds_alternative<cr::present>(item.value));const auto values=sr::decode_values(std::get<cr::present>(item.value).payload,policy.package.codec.values);
                EXPECT_EQ(std::get<std::string>(values.at("body")),"crash durable");payload=true;
            }
            if(const auto* page=std::get_if<cr::receipt_page>(&frame.body))for(const auto& item:page->items) {
                EXPECT_EQ(item.original_id,original.global_id);EXPECT_TRUE(std::holds_alternative<cr::committed>(item.value));receipt=true;
            }
        }
        EXPECT_TRUE(payload);EXPECT_TRUE(receipt);alarm(0);return;
    }
    TempDB file{"ready-crash-restart"};char executable[4096];
#if defined(__APPLE__)
    uint32_t length=sizeof(executable);ASSERT_EQ(_NSGetExecutablePath(executable,&length),0);
#else
    const auto length=readlink("/proc/self/exe",executable,sizeof(executable)-1);ASSERT_GT(length,0);
    ASSERT_LT(length,static_cast<ssize_t>(sizeof(executable)-1));executable[length]=0;
#endif
    for(const auto* phase_name:{"seed","reopen"}) {
        const std::string phase_name_owned(phase_name);const auto* parent_log=std::getenv("LATTICE_TEST_LOG_PATH");
        const auto native=parent_log&&*parent_log?std::string(parent_log)+"."+file.path.filename().string()+"."+phase_name_owned+".native.log":file.str()+"."+phase_name_owned+".native.log";
        std::vector<std::string> values;
        for(char** value=environ;*value;++value)if(std::strncmp(*value,variable,std::strlen(variable))&&std::strncmp(*value,"LATTICE_TEST_LOG_PATH=",22))values.emplace_back(*value);
        values.push_back(std::string(variable)+(phase_name_owned=="seed"?"s:":"r:")+file.str());values.push_back("LATTICE_TEST_LOG_PATH="+native);
        std::vector<char*> environment;for(auto& value:values)environment.push_back(value.data());environment.push_back(nullptr);
        std::string filter="--gtest_filter=CanonicalDurableReadyRestart.FreshProcessesResumeKnownCommittedCapsuleAfterAbruptSourceExit";
        std::string color="--gtest_color=no",repeat="--gtest_repeat=1",output="--gtest_output=";
        char* arguments[]={executable,filter.data(),color.data(),repeat.data(),output.data(),nullptr};
        posix_spawn_file_actions_t actions;ASSERT_EQ(posix_spawn_file_actions_init(&actions),0);
        struct destroy {posix_spawn_file_actions_t& value;~destroy(){posix_spawn_file_actions_destroy(&value);}} cleanup{actions};
        const auto log=file.str()+"."+phase_name_owned+".log";
        ASSERT_EQ(posix_spawn_file_actions_addopen(&actions,STDOUT_FILENO,log.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600),0);
        ASSERT_EQ(posix_spawn_file_actions_adddup2(&actions,STDOUT_FILENO,STDERR_FILENO),0);
        pid_t child=-1;ASSERT_EQ(posix_spawn(&child,executable,&actions,nullptr,arguments,environment.data()),0);
        int status=0;pid_t waited=-1;const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(45);
        do {waited=waitpid(child,&status,WNOHANG);if(waited==child || (waited<0&&errno!=EINTR))break;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));}while(std::chrono::steady_clock::now()<deadline);
        ASSERT_FALSE(waited<0&&errno!=EINTR)<<"owned child observation failed; no unverifiable signal";
        if(waited!=child){kill(child,SIGKILL);do{waited=waitpid(child,&status,0);}while(waited<0&&errno==EINTR);FAIL()<<"READY child deadline: "<<log;}
        ASSERT_TRUE(WIFEXITED(status))<<log;ASSERT_EQ(WEXITSTATUS(status),0)<<log;
    }
}
#endif

#if defined(__APPLE__) || defined(__linux__)
TEST_F(CanonicalDurableReady, RetainedCaptureCannotExceedEitherReadyProfileRequestBudget) {
    attach();auto admission=admit();const auto original=ready_entry(901,901);import_entry(admission,original);
    const std::vector<sr::canonical_capture_request> requested{{original.global_id,{{original.table_name,original.global_row_id}},"application-a"}};
    auto reserved=adapter->reserve_recovery_owned(owner,std::nullopt,10000);committed(reserved.settlement);ASSERT_TRUE(reserved.reservation);
    const auto before=snapshot();
    auto requests_over=policy.capture;++requests_over.requests;
    EXPECT_THROW(adapter->capture_reserved_namespaced_owned(owner,admission,*reserved.reservation,requested,requests_over),db_error);
    EXPECT_EQ(snapshot(),before);EXPECT_EQ(owner->local_read_generations_outstanding(),0u);
    auto targets_over=policy.capture;++targets_over.requested_targets;
    EXPECT_THROW(adapter->capture_reserved_namespaced_owned(owner,admission,*reserved.reservation,requested,targets_over),db_error);
    EXPECT_EQ(snapshot(),before);EXPECT_EQ(owner->local_read_generations_outstanding(),0u);
    auto captured=adapter->capture_reserved_namespaced_owned(owner,admission,*reserved.reservation,requested,policy.capture);
    ASSERT_TRUE(captured.capture);ASSERT_EQ(captured.capture->receipts.size(),1u);
    EXPECT_EQ(captured.capture->receipts.front().original_id,original.global_id);EXPECT_TRUE(captured.capture->receipts.front().stored);
    EXPECT_EQ(snapshot(),before);committed(adapter->release_recovery_owned(owner,*reserved.reservation));
}
TEST_F(CanonicalDurableReady, NamespacedCaptureWithoutReadyRetains4096RequestCeiling) {
    adapter=canonical_writer_adapter::attach_namespaced_upstream_for_qualification(owner,p,upstream(),retention());
    auto admission=admit();const auto original=ready_entry(902,902);import_entry(admission,original);
    EXPECT_FALSE(owner->db().table_exists("_lattice_canonical_ready_profile"));
    const std::vector<sr::canonical_capture_request> requested{{original.global_id,{{original.table_name,original.global_row_id}},"application-a"}};
    auto reserved=adapter->reserve_recovery_owned(owner,std::nullopt,10000);committed(reserved.settlement);ASSERT_TRUE(reserved.reservation);
    const auto before=owner->db().query("SELECT * FROM _lattice_canonical_receipt ORDER BY 1");const auto before_head=head();
    auto limits=policy.capture;limits.requests=4097;
    EXPECT_THROW(adapter->capture_reserved_namespaced_owned(owner,admission,*reserved.reservation,requested,limits),db_error);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_receipt ORDER BY 1"),before);EXPECT_EQ(head(),before_head);
    EXPECT_EQ(owner->local_read_generations_outstanding(),0u);
    limits.requests=4096;
    auto captured=adapter->capture_reserved_namespaced_owned(owner,admission,*reserved.reservation,requested,limits);
    ASSERT_TRUE(captured.capture);ASSERT_EQ(captured.capture->receipts.size(),1u);EXPECT_TRUE(captured.capture->receipts.front().stored);
    EXPECT_EQ(captured.capture->receipts.front().original_id,original.global_id);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_receipt ORDER BY 1"),before);EXPECT_EQ(head(),before_head);
    committed(adapter->release_recovery_owned(owner,*reserved.reservation));
}

TEST_F(CanonicalDurableReady, AddressedReadStillAuditsEveryFrameBeforeAndAfterOwnedBody) {
    attach();auto admission=admit();
    for(unsigned i=1;i<=3;++i)import_entry(admission,ready_entry(i,i));
    auto result=prepare(admission);complete(result);ASSERT_GT(result.transfer->frames,3u);
    const auto before=snapshot();
    using namespace canonical_ready_test_observation;
    observation trace;const auto previous=current;current=&trace;
    struct reset {observation* previous;~reset(){current=previous;}} restore{previous};
    const auto read=adapter->read_ready_frame_owned(owner,admission,*result.lease,0);committed(read.settlement);ASSERT_TRUE(read.frame);
    EXPECT_EQ(trace.visits[static_cast<size_t>(point::audit_begin)],2u);
    EXPECT_EQ(trace.visits[static_cast<size_t>(point::audit_end)],2u);
    EXPECT_EQ(trace.audited_frames,2*result.transfer->frames);
    EXPECT_EQ(snapshot(),before);
}
TEST_F(CanonicalDurableReady, OffPageTailCorruptionStillRefusesReopenBeforeIncarnationOrCleanup) {
    attach();auto admission=admit();import_entry(admission,ready_entry(1,1));
    auto result=prepare(admission);complete(result);ASSERT_GT(result.transfer->frames,2u);
    adapter.reset();owner->close();sibling->close();
    {database raw(file.str());
        const auto sql=std::get<std::string>(raw.query("SELECT sql FROM sqlite_master WHERE name='_lattice_canonical_ready_frame_guard_UPDATE'").at(0).at("sql"));
        raw.execute("DROP TRIGGER _lattice_canonical_ready_frame_guard_UPDATE");
        raw.execute("UPDATE _lattice_canonical_ready_frame SET data=X'7b7d' WHERE frame_index=(SELECT MAX(frame_index) FROM _lattice_canonical_ready_frame)");
        raw.execute(sql);
    }
    owner=ready_owner(file.str());sibling=ready_owner(file.str());const auto before=snapshot();
    EXPECT_THROW(attach(),db_error);
    EXPECT_EQ(snapshot(),before);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
}
#endif

#if defined(__APPLE__) || defined(__linux__)
TEST_F(CanonicalDurableReady, ExplicitLifecycleGraceMustBePositiveBoundedAndPersistExactly) {
    const auto before=owner->db().query("SELECT type,name,sql FROM sqlite_master ORDER BY type,name");
    policy.orphan_resume_grace_ms=0;EXPECT_THROW(attach(),db_error);
    policy.orphan_resume_grace_ms=retention().duration_ms+1;EXPECT_THROW(attach(),db_error);
    policy.orphan_resume_grace_ms=INT64_MAX;EXPECT_THROW(attach(),db_error);
    EXPECT_EQ(owner->db().query("SELECT type,name,sql FROM sqlite_master ORDER BY type,name"),before);
    policy.orphan_resume_grace_ms=1000;attach();auto admission=admit();const auto offered=prepare(admission);complete(offered);
    const auto exact=snapshot();adapter.reset();policy.orphan_resume_grace_ms.reset();EXPECT_THROW(attach(),db_error);EXPECT_EQ(snapshot(),exact);
    policy.orphan_resume_grace_ms=999;EXPECT_THROW(attach(),db_error);EXPECT_EQ(snapshot(),exact);
    policy.orphan_resume_grace_ms=1000;attach();EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
}
TEST_F(CanonicalDurableReady, LifecycleExpiredOrphansFreeAllSlotsPinsAndTransferChargeButKeepReceiptsAndHighWater) {
    policy.orphan_resume_grace_ms=1;attach();auto admission=admit();const auto accepted=ready_entry(111,112);import_entry(admission,accepted);ask(accepted);
    for(int n=0;n<policy.transfers;++n){logical.channel="orphan-"+std::to_string(n);seal();complete(prepare(admission));}
    const auto bindings=owner->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding");
    const auto receipts=owner->db().query("SELECT * FROM _lattice_canonical_receipt ORDER BY original_id");
    const auto payload=owner->db().query("SELECT * FROM DurableReadyRow ORDER BY id");
    reopen();std::this_thread::sleep_for(std::chrono::milliseconds(3));committed(adapter->expire_ready_owned(owner));
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);EXPECT_EQ(count("_lattice_canonical_ready_frame"),0);
    EXPECT_EQ(scalar(owner->db(),"SELECT active FROM _lattice_canonical_ready_profile"),0);
    EXPECT_EQ(scalar(owner->db(),"SELECT charged FROM _lattice_canonical_ready_profile"),scalar(owner->db(),"SELECT SUM(charge) FROM _lattice_canonical_ready_binding"));
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_binding ORDER BY binding"),bindings);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_receipt ORDER BY original_id"),receipts);
    EXPECT_EQ(owner->db().query("SELECT * FROM DurableReadyRow ORDER BY id"),payload);
    auto fresh=admit();EXPECT_NE(prepare(fresh).preparation.state,phase::committed);
    owner->add(DurableReadyRow{"later tail"});committed(adapter->prune_recovery_owned(owner,head()));
    logical.channel="new-binding-after-orphans";seal();complete(prepare(fresh));
}
TEST_F(CanonicalDurableReady, LifecycleEarlyResumeKeepsExactBytesAndWinsOverLaterOrphanMaintenance) {
    policy.orphan_resume_grace_ms=1000;attach();auto old_admission=admit();const auto offered=prepare(old_admission);complete(offered);
    const auto exact_frames=owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index");
    reopen();auto fresh=admit();const auto resumed=adapter->resume_ready_owned(owner,fresh,logical,request,10000,11);
    committed(resumed.settlement);ASSERT_TRUE(resumed.lease);ASSERT_TRUE(resumed.transfer);EXPECT_EQ(resumed.transfer->manifest,offered.transfer->manifest);
    std::this_thread::sleep_for(std::chrono::milliseconds(1002));committed(adapter->expire_ready_owned(owner));
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index"),exact_frames);
    const auto read=adapter->read_ready_frame_owned(owner,fresh,*resumed.lease,0);committed(read.settlement);ASSERT_TRUE(read.frame);
    EXPECT_EQ(cr::decode(*read.frame,policy.package.codec).route_generation,11u);
}
TEST_F(CanonicalDurableReady, LifecycleLateResumeAtomicallyDisposesInsteadOfResurrectingOldCapsule) {
    policy.orphan_resume_grace_ms=1;attach();auto admission=admit();const auto offered=prepare(admission);complete(offered);
    const auto bindings=owner->db().query("SELECT * FROM _lattice_canonical_ready_binding");
    reopen();std::this_thread::sleep_for(std::chrono::milliseconds(3));auto fresh=admit();
    const auto resumed=adapter->resume_ready_owned(owner,fresh,logical,request,10000,11);committed(resumed.settlement);
    EXPECT_FALSE(resumed.lease);EXPECT_FALSE(resumed.transfer);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_binding"),bindings);
    EXPECT_NE(prepare(fresh).preparation.state,phase::committed);
}
TEST_F(CanonicalDurableReady, LifecycleFailedOrphanDisposalKeepsWholeCapsuleForAuditedRetry) {
    policy.orphan_resume_grace_ms=1;attach();auto admission=admit();complete(prepare(admission));reopen();
    std::this_thread::sleep_for(std::chrono::milliseconds(3));const auto before=snapshot();
    {ReadyFault fault(owner->db(),ReadyFault::disposal_deny);const auto failed=adapter->expire_ready_owned(owner);
        EXPECT_EQ(failed.state,phase::rolled_back);EXPECT_EQ(fault.hits,1);}
    EXPECT_EQ(snapshot(),before);committed(adapter->expire_ready_owned(owner));EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);
}
TEST_F(CanonicalDurableReady, LifecycleOffPageCorruptionRefusesReopenBeforeIncarnationAndGraceCleanup) {
    policy.orphan_resume_grace_ms=1;attach();auto admission=admit();import_entry(admission,ready_entry(191,192));complete(prepare(admission));
    adapter.reset();owner->close();sibling->close();
    {database raw(file.str());const auto guard=std::get<std::string>(raw.query("SELECT sql FROM sqlite_master WHERE name='_lattice_canonical_ready_frame_guard_UPDATE'").at(0).at("sql"));
        raw.execute("DROP TRIGGER _lattice_canonical_ready_frame_guard_UPDATE");
        raw.execute("UPDATE _lattice_canonical_ready_frame SET data=X'7b7d' WHERE frame_index=(SELECT MAX(frame_index) FROM _lattice_canonical_ready_frame)");raw.execute(guard);}
    owner=ready_owner(file.str());sibling=ready_owner(file.str());const auto before=snapshot();
    std::this_thread::sleep_for(std::chrono::milliseconds(3));EXPECT_THROW(attach(),db_error);EXPECT_EQ(snapshot(),before);
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);EXPECT_EQ(count("_lattice_canonical_ready_binding"),1);
}
#endif

#if defined(__APPLE__) || defined(__linux__)
namespace {
class CanonicalLifecycleAdoption:public CanonicalDurableReady {
protected:
    std::string name="bounded48MiBV1";
    canonical_retention_limits kept_limits{8,3600000};
    void SetUp()override {CanonicalDurableReady::SetUp();select(name);}
    void select(const std::string& selected) {
        name=selected;policy=canonical_named_ready_profile("qualification-source",p.writer.limits,bool(p.namespaces.coverage),name);
        request.source={policy.authority,p.writer.binding.source,p.writer.binding.epoch,p.writer.binding.scope,p.writer.binding.schema};
        request.budget=policy.package.codec.maximum;seal();
    }
    void attach(){adapter=canonical_writer_adapter::attach_ready_for_qualification(owner,p,upstream(),kept_limits,policy);}
    canonical_ready_adoption_result adopt(int64_t grace) {
        return canonical_ready_test_access::adopt(owner,p,upstream(),kept_limits,policy,name,grace);
    }
    using Snapshot=std::map<std::string,std::vector<database::row_t>>;
    Snapshot preserved() {
        Snapshot rows;
        const auto tables=owner->db().query("SELECT name FROM sqlite_master WHERE type='table' AND (substr(name,1,19)='_lattice_canonical_' OR name IN ('AuditLog','DurableReadyRow')) ORDER BY name LIMIT 33");
        if(tables.size()>32)throw std::runtime_error("adoption fixture inventory exceeds cap");
        for(const auto& table:tables) {
            const auto n=std::get<std::string>(table.at("name"));rows[n]=owner->db().query("SELECT * FROM "+n+" ORDER BY 1");
            if(n=="_lattice_canonical_ready_profile")for(auto& row:rows[n]){row.erase("policy");row.erase("predecessor");}
        }
        return rows;
    }
    auto schema(){return owner->db().query("SELECT type,name,sql FROM sqlite_master ORDER BY type,name");}
    void target(int64_t grace) {
        policy=canonical_named_ready_profile(policy.authority,p.writer.limits,bool(p.namespaces.coverage),
            name=="boundedV1"?"boundedV1OrphanV1":"bounded48MiBOrphanV1",grace);
    }
};
struct AdoptionFault {
    enum class Kind {policy_ignore,record_ignore,alter_deny,commit_deny};
    static thread_local AdoptionFault* current;
    Kind kind;int hits=0;
    canonical_upstream_test_hooks::authorizer_fault fault;
    const canonical_upstream_test_hooks::authorizer_fault* prior;AdoptionFault* previous;
    AdoptionFault(database& db,Kind k):kind(k),fault{canonical_writer_custody_test_access::fault_handle(db),restrict_action},
        prior(canonical_retention_test_hooks::fault),previous(current){current=this;canonical_retention_test_hooks::fault=&fault;}
    ~AdoptionFault(){canonical_retention_test_hooks::fault=prior;current=previous;}
    static int restrict_action(int action,const char* one,const char* two,const char*)noexcept {
        auto& f=*current;const auto same=[](const char* a,const char* b){return a&&std::strcmp(a,b)==0;};if(f.hits)return SQLITE_OK;
        if(action==SQLITE_UPDATE&&same(one,"_lattice_canonical_ready_profile")&&
           (f.kind==Kind::policy_ignore&&same(two,"policy")||f.kind==Kind::record_ignore&&same(two,"predecessor"))){++f.hits;return SQLITE_IGNORE;}
        if(action==SQLITE_ALTER_TABLE&&f.kind==Kind::alter_deny){++f.hits;return SQLITE_DENY;}
        if(action==SQLITE_TRANSACTION&&same(one,"COMMIT")&&f.kind==Kind::commit_deny){++f.hits;return SQLITE_DENY;}
        return SQLITE_OK;
    }
};
thread_local AdoptionFault* AdoptionFault::current=nullptr;
}
TEST_F(CanonicalLifecycleAdoption, PreservesCompletedPreparingOrdinaryReservationsAndAllSourceRows) {
    attach();auto admission=admit();const auto e=ready_entry(401,402);import_entry(admission,e);ask(e);
    const auto completed=prepare(admission);complete(completed);
    logical.channel="adoption-preparing";seal();
    const auto interrupted=canonical_ready_test_access::prepare(*adapter,owner,admission,logical,request,10000,7,
        []{throw std::runtime_error("retain actual PREPARING");});
    committed(interrupted.preparation);ASSERT_NE(interrupted.capture_error,nullptr);
    const auto reserved=adapter->reserve_recovery_owned(owner,head(),10000);committed(reserved.settlement);ASSERT_TRUE(reserved.reservation);
    ASSERT_EQ(count("_lattice_canonical_ready_transfer"),2);ASSERT_EQ(count("_lattice_canonical_attempt"),2);
    const auto before=preserved();const auto incarnation=scalar(owner->db(),"SELECT incarnation FROM _lattice_canonical_retention");
    adapter.reset();const auto result=adopt(10000);committed(result.settlement);ASSERT_TRUE(result.record);EXPECT_LE(result.record->size(),16384u);
    EXPECT_EQ(preserved(),before);EXPECT_EQ(scalar(owner->db(),"SELECT incarnation FROM _lattice_canonical_retention"),incarnation);
    EXPECT_EQ(result.disposition,canonical_ready_adoption_disposition::applied);
    const auto again=adopt(10000);committed(again.settlement);EXPECT_EQ(again.record,result.record);EXPECT_EQ(preserved(),before);
    EXPECT_EQ(again.disposition,canonical_ready_adoption_disposition::verified_existing);
    // Administration has not performed the separately specified first mount.
    target(10000);attach();EXPECT_EQ(scalar(owner->db(),"SELECT incarnation FROM _lattice_canonical_retention"),incarnation+1);
    EXPECT_EQ(count("_lattice_canonical_attempt"),0);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
    EXPECT_EQ(count("_lattice_canonical_ready_binding"),2);EXPECT_EQ(count("_lattice_canonical_receipt"),1);
}
TEST_F(CanonicalLifecycleAdoption, FullSmallSixteenCapsulesPreserveExactEnvelopeAndResume) {
    select("boundedV1");attach();auto admission=admit();const auto e=ready_entry(411,412);import_entry(admission,e);ask(e);
    for(unsigned i=0;i<16;++i){logical.channel="small-adopt-"+std::to_string(i);seal();complete(prepare(admission));}
    ASSERT_EQ(count("_lattice_canonical_ready_transfer"),16);const auto before=preserved();
    const auto stored=owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index");
    adapter.reset();const auto result=adopt(10000);committed(result.settlement);ASSERT_TRUE(result.record);EXPECT_EQ(preserved(),before);
    target(10000);EXPECT_EQ(policy.transfers,16);EXPECT_EQ(policy.transfer_bytes,2097152);EXPECT_EQ(policy.charged_bytes,67108864);
    attach();auto fresh=admit();const auto resumed=adapter->resume_ready_owned(owner,fresh,logical,request,10000,13);
    committed(resumed.settlement);ASSERT_TRUE(resumed.lease);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),16);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_ready_frame ORDER BY binding,frame_index"),stored);
    const auto frame=adapter->read_ready_frame_owned(owner,fresh,*resumed.lease,0);committed(frame.settlement);ASSERT_TRUE(frame.frame);
    EXPECT_EQ(cr::decode(*frame.frame,policy.package.codec).route_generation,13u);
}
TEST_F(CanonicalLifecycleAdoption, LiveAdapterAndDirectoryOwnerRefuseBeforeMutation) {
    attach();auto admission=admit();complete(prepare(admission));const auto before=preserved();const auto ddl=schema();
    const auto same=adopt(10000);EXPECT_NE(same.settlement.state,phase::committed);EXPECT_FALSE(same.record);
    const auto alias=canonical_ready_test_access::adopt(sibling,p,upstream(),kept_limits,policy,name,10000);
    EXPECT_NE(alias.settlement.state,phase::committed);EXPECT_FALSE(alias.record);EXPECT_EQ(preserved(),before);EXPECT_EQ(schema(),ddl);
}
TEST_F(CanonicalLifecycleAdoption, FirstTransitionIsNotImplicitReopenAndChangedGraceOrPredecessorRefuse) {
    attach();complete(prepare(admit()));adapter.reset();const auto before=preserved();const auto ddl=schema();const auto old=policy;
    target(10000);EXPECT_THROW(attach(),db_error);EXPECT_EQ(preserved(),before);EXPECT_EQ(schema(),ddl);policy=old;
    const auto accepted=adopt(10000);committed(accepted.settlement);const auto adopted=preserved();const auto adopted_ddl=schema();
    const auto changed=adopt(10001);EXPECT_NE(changed.settlement.state,phase::committed);EXPECT_FALSE(changed.record);
    auto small=canonical_named_ready_profile(policy.authority,p.writer.limits,false,"boundedV1");
    const auto wrong=canonical_ready_test_access::adopt(owner,p,upstream(),kept_limits,small,"boundedV1",10000);
    EXPECT_NE(wrong.settlement.state,phase::committed);EXPECT_FALSE(wrong.record);EXPECT_EQ(preserved(),adopted);EXPECT_EQ(schema(),adopted_ddl);
    const auto retry=adopt(10000);committed(retry.settlement);EXPECT_EQ(retry.record,accepted.record);
}
TEST_F(CanonicalLifecycleAdoption, IgnoredWritesAlterAndCommitDenialRollbackTheExactSchemaAndRows) {
    attach();complete(prepare(admit()));adapter.reset();const auto before=preserved();const auto ddl=schema();
    const auto old_policy=owner->db().query("SELECT policy FROM _lattice_canonical_ready_profile");
    for(auto kind:{AdoptionFault::Kind::policy_ignore,AdoptionFault::Kind::record_ignore,AdoptionFault::Kind::alter_deny,AdoptionFault::Kind::commit_deny}) {
        AdoptionFault fault(owner->db(),kind);const auto result=adopt(10000);EXPECT_EQ(fault.hits,1);
        EXPECT_EQ(result.settlement.state,phase::rolled_back);EXPECT_NE(result.settlement.primary_error,nullptr);EXPECT_FALSE(result.record);EXPECT_FALSE(result.disposition);
        EXPECT_EQ(preserved(),before);EXPECT_EQ(schema(),ddl);EXPECT_EQ(owner->db().query("SELECT policy FROM _lattice_canonical_ready_profile"),old_policy);
    }
    committed(adopt(10000).settlement);
}
TEST_F(CanonicalLifecycleAdoption, KnownCommitSecondaryErrorKeepsExactPostconditionAndNoSecondTransition) {
    attach();complete(prepare(admit()));adapter.reset();const auto before=preserved();int calls=0;
    const auto hook=owner->add_invalidation_hook([&](const auto&,auto){++calls;throw std::runtime_error("actual adoption commit observer");});
    const auto result=adopt(10000);owner->remove_invalidation_hook(hook);
    EXPECT_EQ(calls,1);EXPECT_EQ(result.settlement.state,phase::committed);EXPECT_NE(result.settlement.postcommit_error,nullptr);
    EXPECT_FALSE(result.settlement.primary_error);ASSERT_TRUE(result.record);EXPECT_EQ(preserved(),before);
    const auto retry=adopt(10000);committed(retry.settlement);EXPECT_EQ(retry.record,result.record);EXPECT_EQ(preserved(),before);
}
TEST_F(CanonicalLifecycleAdoption, AdministrativeAndOrdinarySourceCannotRewriteImmutablePredecessor) {
    attach();complete(prepare(admit()));adapter.reset();const auto adopted=adopt(10000);committed(adopted.settlement);ASSERT_TRUE(adopted.record);
    const auto exact=owner->db().query("SELECT policy,predecessor FROM _lattice_canonical_ready_profile");
    EXPECT_THROW(owner->db().execute("UPDATE _lattice_canonical_ready_profile SET predecessor=X'00'"),db_error);
    EXPECT_THROW(owner->db().execute("UPDATE _lattice_canonical_ready_profile SET policy=X'00'"),db_error);
    target(10000);attach();EXPECT_THROW(owner->write([&]{owner->db().execute("UPDATE _lattice_canonical_ready_profile SET predecessor=X'00'");}),db_error);
    EXPECT_EQ(owner->db().query("SELECT policy,predecessor FROM _lattice_canonical_ready_profile"),exact);
    committed(adapter->inspect_ready_owned(owner).settlement);
}
TEST_F(CanonicalLifecycleAdoption, ExplicitGraceBoundsAndWrongIdentityRefuseWithoutSchemaOrRowChange) {
    attach();complete(prepare(admit()));adapter.reset();const auto before=preserved();const auto ddl=schema();
    for(auto grace:{int64_t(0),int64_t(-1),int64_t(3600001)}){const auto result=adopt(grace);EXPECT_NE(result.settlement.state,phase::committed);EXPECT_FALSE(result.record);}
    auto foreign=p;foreign.writer.binding.epoch=ready_uuid(499);const auto result=canonical_ready_test_access::adopt(owner,foreign,upstream(),kept_limits,policy,name,10000);
    EXPECT_NE(result.settlement.state,phase::committed);EXPECT_FALSE(result.record);EXPECT_EQ(preserved(),before);EXPECT_EQ(schema(),ddl);
}
TEST_F(CanonicalLifecycleAdoption, EscapedHandleCannotAcquireAdministrativeCustody) {
    attach();complete(prepare(admit()));adapter.reset();const auto before=preserved();const auto ddl=schema();(void)owner->db().handle();
    const auto result=adopt(10000);EXPECT_NE(result.settlement.state,phase::committed);EXPECT_FALSE(result.record);EXPECT_EQ(preserved(),before);EXPECT_EQ(schema(),ddl);
}
TEST_F(CanonicalLifecycleAdoption, OffPageCorruptionRefusesBeforePolicySchemaOrPreparingCleanup) {
    attach();complete(prepare(admit()));adapter.reset();owner->close();sibling->close();
    {database raw(file.str());const auto guard=std::get<std::string>(raw.query("SELECT sql FROM sqlite_master WHERE name='_lattice_canonical_ready_frame_guard_UPDATE'").at(0).at("sql"));
        raw.execute("DROP TRIGGER _lattice_canonical_ready_frame_guard_UPDATE");
        raw.execute("UPDATE _lattice_canonical_ready_frame SET data=X'7b7d' WHERE frame_index=(SELECT MAX(frame_index) FROM _lattice_canonical_ready_frame)");raw.execute(guard);}
    owner=ready_owner(file.str());sibling=ready_owner(file.str());const auto before=preserved();const auto ddl=schema();
    const auto result=adopt(10000);EXPECT_NE(result.settlement.state,phase::committed);EXPECT_FALSE(result.record);EXPECT_EQ(preserved(),before);EXPECT_EQ(schema(),ddl);
}
TEST_F(CanonicalLifecycleAdoption, TamperedOrOversizedPredecessorRefusesNormalReopenBeforeCleanup) {
    attach();complete(prepare(admit()));adapter.reset();committed(adopt(10000).settlement);target(10000);
    for(const auto* expression:{"X'00'","zeroblob(16385)","NULL"}) {
        owner->close();sibling->close();
        {database raw(file.str());const auto guard=std::get<std::string>(raw.query("SELECT sql FROM sqlite_master WHERE name='_lattice_canonical_ready_profile_guard_UPDATE'").at(0).at("sql"));
            raw.execute("DROP TRIGGER _lattice_canonical_ready_profile_guard_UPDATE");raw.execute("UPDATE _lattice_canonical_ready_profile SET predecessor="+std::string(expression));raw.execute(guard);}
        owner=ready_owner(file.str());sibling=ready_owner(file.str());const auto before=preserved();const auto ddl=schema();
        EXPECT_THROW(attach(),db_error);EXPECT_EQ(preserved(),before);EXPECT_EQ(schema(),ddl);EXPECT_EQ(count("_lattice_canonical_ready_transfer"),1);
    }
}
TEST_F(CanonicalLifecycleAdoption, LaterReceiptConversionRefusesBeforeDisposingAnyRetainedWork) {
    attach();complete(prepare(admit()));adapter.reset();committed(adopt(10000).settlement);const auto before=preserved();const auto ddl=schema();
    auto registered=p;registered.namespaces.coverage=canonical_coverage_profile{ready_uuid(490),7,{"application-a","application-b"}};
    const auto next=canonical_named_ready_profile(policy.authority,p.writer.limits,true,"bounded48MiBV1");
    EXPECT_THROW(canonical_ready_test_access::migrate(owner,registered,upstream(),kept_limits,policy,next),db_error);
    EXPECT_EQ(preserved(),before);EXPECT_EQ(schema(),ddl);EXPECT_FALSE(owner->db().table_exists("_lattice_canonical_receipt_profile"));
}
#endif

#if defined(__APPLE__) || defined(__linux__)
TEST(CanonicalLifecycleProfileGolden, EveryPreexistingNamedPolicyKeepsExactPersistedBytes) {
    // Fixed SHA256 values transcribed from 4079's pre-refactor policy encoder
    // and numeric recipe, not computed by the new named-profile helper.
    struct Example {const char* name;bool registered;std::optional<int64_t> grace;const char* sha;const char* wire_sha;};
    const Example examples[]={{"boundedV1",false,std::nullopt,"d03779a6979cfc83cb0a02f2641f8e83fd14d9b259a42e5d4489e873c5f2233e","3c0d7d66c2254c4b950b330c36cf5c5a936d3f9b558cad8622f9164101d2ca2f"},
        {"bounded48MiBV1",false,std::nullopt,"98ddf0d97d484bb01eea5e841a0672d746ba77ea3537ac08d10449418088aeb0","ce2570d7ea12f4a0884bd1bfece9fd8b11b782f051750be97f38532aeab6b7d3"},
        {"bounded48MiBV1",true,std::nullopt,"89151c562eef4e63c837f2bbe3ce5576ba2d23b6b9a53ed4475be0021ea8311e","7b6e9d4587bca59ca08d246ef92e48f40c1ae763d768f282c8dc7c8939c31b81"},
        {"bounded48MiBOrphanV1",false,10000,"bf8209f26849b24868045ee8df3aad28175ef7740ade3b413de7fa6f5aa95390","6e83b99e5ce58ed8570d3978dbbe6a03463504bd119bf35fa08ad11af87adf47"},
        {"bounded48MiBOrphanV1",true,10000,"e58f8ff645719154e529896b126575fbe98b51a795120fb7a02cf1c7560a6f61","76cf6ede988161d94d1ca1002f71829a1178b5442cad23e23076f3b22b6c46b3"}};
    for(const auto& example:examples) {
        TempDB file{"canonical-ready-profile-golden"};auto owner=ready_owner(file.str());auto p=writer_profile();
        if(example.registered)p.namespaces.coverage=canonical_coverage_profile{ready_uuid(490),7,{"application-a","application-b"}};
        const auto policy=canonical_named_ready_profile("qualification-source",p.writer.limits,example.registered,example.name,example.grace);
        auto adapter=canonical_writer_adapter::attach_ready_for_qualification(owner,p,upstream(),{8,3600000},policy);
        const auto rows=owner->db().query("SELECT policy FROM _lattice_canonical_ready_profile");ASSERT_EQ(rows.size(),1u);
        const auto& raw=std::get<std::vector<uint8_t>>(rows[0].at("policy"));
        EXPECT_EQ(picosha2::hash256_hex_string(raw),example.sha)<<example.name<<" registered="<<example.registered;
        EXPECT_EQ(picosha2::hash256_hex_string(canonical_ready_profile_description(policy,example.name).dump()),example.wire_sha);
        EXPECT_EQ(owner->db().query("SELECT name FROM pragma_table_xinfo('_lattice_canonical_ready_profile') WHERE name='predecessor'").size(),0u);
        adapter.reset();owner->close();
    }
    EXPECT_THROW(canonical_named_ready_profile("qualification-source",writer_profile().writer.limits,true,"boundedV1OrphanV1",10000),db_error);
}
#endif

#if defined(__APPLE__) || defined(__linux__)
TEST_F(CanonicalLifecycleAdoption, ValidSizedWrongProvenanceAndExtraColumnAreNotAdoptedVariants) {
    attach();complete(prepare(admit()));adapter.reset();const auto result=adopt(10000);committed(result.settlement);ASSERT_TRUE(result.record);
    const auto record=*result.record;target(10000);owner->close();sibling->close();
    {database raw(file.str());const auto guard=std::get<std::string>(raw.query("SELECT sql FROM sqlite_master WHERE name='_lattice_canonical_ready_profile_guard_UPDATE'").at(0).at("sql"));
        raw.execute("DROP TRIGGER _lattice_canonical_ready_profile_guard_UPDATE");auto altered=record;altered.back()='x';
        raw.execute("UPDATE _lattice_canonical_ready_profile SET predecessor=?",{std::vector<uint8_t>(altered.begin(),altered.end())});raw.execute(guard);}
    owner=ready_owner(file.str());sibling=ready_owner(file.str());const auto before=preserved();const auto ddl=schema();
    EXPECT_THROW(attach(),db_error);EXPECT_EQ(preserved(),before);EXPECT_EQ(schema(),ddl);
    owner->close();sibling->close();
    {database raw(file.str());const auto guard=std::get<std::string>(raw.query("SELECT sql FROM sqlite_master WHERE name='_lattice_canonical_ready_profile_guard_UPDATE'").at(0).at("sql"));
        raw.execute("DROP TRIGGER _lattice_canonical_ready_profile_guard_UPDATE");raw.execute("UPDATE _lattice_canonical_ready_profile SET predecessor=?",{std::vector<uint8_t>(record.begin(),record.end())});raw.execute(guard);
        raw.execute("ALTER TABLE _lattice_canonical_ready_profile ADD COLUMN second_predecessor BLOB");}
    owner=ready_owner(file.str());sibling=ready_owner(file.str());const auto extra=preserved();const auto extra_ddl=schema();
    EXPECT_THROW(attach(),db_error);EXPECT_EQ(preserved(),extra);EXPECT_EQ(schema(),extra_ddl);
}
#endif

#if defined(__APPLE__) || defined(__linux__)
TEST_F(CanonicalLifecycleAdoption, BindingOnlyHighWaterAndChargesSurviveWithoutInventingPublishedWork) {
    attach();const auto offered=prepare(admit());complete(offered);committed(adapter->abandon_ready_owned(owner,offered.transfer->identity));
    ASSERT_EQ(count("_lattice_canonical_ready_transfer"),0);ASSERT_EQ(count("_lattice_canonical_ready_binding"),1);
    ASSERT_GT(scalar(owner->db(),"SELECT charged FROM _lattice_canonical_ready_profile"),0);
    ASSERT_GT(scalar(owner->db(),"SELECT next_lease FROM _lattice_canonical_ready_profile"),0);
    const auto before=preserved();adapter.reset();const auto adopted=adopt(10000);committed(adopted.settlement);ASSERT_TRUE(adopted.record);
    EXPECT_EQ(preserved(),before);target(10000);attach();EXPECT_NE(prepare(admit()).preparation.state,phase::committed);
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);EXPECT_EQ(count("_lattice_canonical_ready_binding"),1);
}
TEST_F(CanonicalLifecycleAdoption, EmptyEnrolledSourceHasNoNewCaptureLeaseOrBindingOnExactRetry) {
    attach();adapter.reset();const auto before=preserved();const auto first=adopt(1);committed(first.settlement);ASSERT_TRUE(first.record);
    const auto second=adopt(1);committed(second.settlement);EXPECT_EQ(second.record,first.record);EXPECT_EQ(preserved(),before);
    EXPECT_EQ(first.disposition,canonical_ready_adoption_disposition::applied);EXPECT_EQ(second.disposition,canonical_ready_adoption_disposition::verified_existing);
    EXPECT_EQ(count("_lattice_canonical_ready_transfer"),0);EXPECT_EQ(count("_lattice_canonical_ready_binding"),0);
    EXPECT_EQ(scalar(owner->db(),"SELECT next_lease FROM _lattice_canonical_ready_profile"),0);
}
#endif

#if defined(__APPLE__) || defined(__linux__)
TEST_F(CanonicalLifecycleAdoption, CommitCallbackOwnerRetirementWithholdsRecordWithoutClaimingRollback) {
    attach();complete(prepare(admit()));adapter.reset();const auto before=preserved();
    const auto hook=owner->add_invalidation_hook([&](const auto&,auto){owner->close();});
    const auto retired=adopt(10000);owner->remove_invalidation_hook(hook);
    EXPECT_EQ(retired.settlement.state,phase::committed);EXPECT_NE(retired.settlement.postcommit_error,nullptr);EXPECT_FALSE(retired.record);EXPECT_FALSE(retired.disposition);
    sibling->close();owner=ready_owner(file.str());sibling=ready_owner(file.str());EXPECT_EQ(preserved(),before);
    const auto retry=adopt(10000);committed(retry.settlement);EXPECT_TRUE(retry.record);EXPECT_EQ(preserved(),before);
}
#endif

#if defined(__APPLE__) || defined(__linux__)
TEST_F(CanonicalLifecycleAdoption, FreshLifecyclePolicyCannotManufactureAnUnrecordedPredecessor) {
    const auto old=policy;target(10000);attach();complete(prepare(admit()));adapter.reset();const auto before=preserved();const auto ddl=schema();policy=old;
    const auto refused=adopt(10000);EXPECT_NE(refused.settlement.state,phase::committed);EXPECT_FALSE(refused.record);EXPECT_FALSE(refused.disposition);
    EXPECT_EQ(preserved(),before);EXPECT_EQ(schema(),ddl);
    EXPECT_TRUE(owner->db().query("SELECT name FROM pragma_table_xinfo('_lattice_canonical_ready_profile') WHERE name='predecessor'").empty());
}
#endif

#if defined(__APPLE__) || defined(__linux__)
TEST_F(CanonicalDurableReady, LargerPrivateReceiptPageUsesBounded64IdBatchesInBothFullAudits) {
    // New private fixture only: a real 65-item receipt page exercises internal
    // chunking without changing any named profile or existing workload.
    policy.package.codec.maximum.frame_bytes=65536;policy.package.codec.maximum.items_per_page=128;
    policy.package.codec.maximum.receipts=128;policy.package.codec.request_entries=128;
    policy.capture.requests=128;policy.capture.requested_targets=128;
    policy.capture.rows.wire.rows_per_page=128;policy.capture.rows.wire.total_rows=128;
    request.budget=policy.package.codec.maximum;seal();attach();const auto admission=admit();
    for(unsigned i=0;i<65;++i) {
        const auto e=ready_entry(10000+i,20000+i);import_entry(admission,e);
        request.receipts.push_back({e.global_id,std::string("application-a"),{{e.table_name,e.global_row_id}}});
    }
    seal();const auto offered=prepare(admission);complete(offered);
    size_t receipt_pages=0;
    for(const auto& row:owner->db().query("SELECT data FROM _lattice_canonical_ready_frame ORDER BY frame_index")) {
        const auto& bytes=std::get<std::vector<uint8_t>>(row.at("data"));const auto frame=cr::decode(std::string(bytes.begin(),bytes.end()),policy.package.codec);
        if(const auto* page=std::get_if<cr::receipt_page>(&frame.body)){++receipt_pages;ASSERT_EQ(page->items.size(),65u);}
    }
    ASSERT_EQ(receipt_pages,1u);const auto before=snapshot();
    namespace reads=canonical_ready_read_test_observation;reads::observation trace;
    const auto previous=reads::current;reads::current=&trace;
    struct reset {reads::observation* previous;~reset(){reads::current=previous;}} restore{previous};
    const auto read=adapter->read_ready_frame_owned(owner,admission,*offered.lease,0);committed(read.settlement);ASSERT_TRUE(read.frame);
    EXPECT_EQ(trace.full_audits,2u);EXPECT_EQ(trace.positive_receipt_lookups,130u);
    EXPECT_EQ(trace.cost.receipt_batches,4u);EXPECT_EQ(trace.cost.receipt_batch_ids,130u);
    using cp=canonical_ready_cost_observation::phase;
    EXPECT_EQ(trace.cost.calls[static_cast<size_t>(cp::receipt_batch)],4u);
    EXPECT_EQ(trace.cost.calls[static_cast<size_t>(cp::retention_audit)],2u);
    EXPECT_EQ(trace.cost.calls[static_cast<size_t>(cp::store_audit)],2u);
    // Each full audit validates every frame once through the fused path,
    // including its manifest. No retired standalone stage should be charged.
    EXPECT_EQ(trace.cost.calls[static_cast<size_t>(cp::fused_frame_validation)],2*offered.transfer->frames);
    for(const auto phase:{cp::frame_decode,cp::canonical_encode,cp::sequence_advance})
        EXPECT_EQ(trace.cost.calls[static_cast<size_t>(phase)],0u);
    EXPECT_EQ(snapshot(),before);
}

TEST_F(CanonicalDurableReady, UnknownOnlyReceiptPagePerformsNoPositiveBatchQuery) {
    const auto absent=ready_entry(30000,30001);ask(absent);attach();const auto admission=admit();
    const auto offered=prepare(admission);complete(offered);const auto before=snapshot();
    namespace reads=canonical_ready_read_test_observation;reads::observation trace;
    const auto previous=reads::current;reads::current=&trace;
    struct reset {reads::observation* previous;~reset(){reads::current=previous;}} restore{previous};
    const auto read=adapter->read_ready_frame_owned(owner,admission,*offered.lease,0);committed(read.settlement);ASSERT_TRUE(read.frame);
    EXPECT_EQ(trace.full_audits,2u);EXPECT_EQ(trace.positive_receipt_lookups,0u);
    EXPECT_EQ(trace.cost.receipt_batches,0u);EXPECT_EQ(trace.cost.receipt_batch_ids,0u);
    EXPECT_EQ(trace.cost.calls[static_cast<size_t>(canonical_ready_cost_observation::phase::receipt_batch)],0u);
    EXPECT_EQ(trace.cost.calls[static_cast<size_t>(canonical_ready_cost_observation::phase::receipt_evidence)],2u);
    EXPECT_EQ(snapshot(),before);
}
#endif

#if defined(__APPLE__) || defined(__linux__)
namespace {
std::string byte_sha_pattern(size_t count) {
    std::string value(count,'\0');
    for(size_t i=0;i<count;++i)value[i]=static_cast<char>(i%256);
    return value;
}
std::string byte_sha_digest(const ready_sha256_state& value) {
    return picosha2::bytes_to_hex_string(value.digest_bytes());
}
void byte_sha_check_work(const ready_sha256_work& work,size_t expected) {
    EXPECT_EQ(work.input_bytes,expected);
    EXPECT_EQ(work.input_bytes,work.staged_input_bytes+64*work.direct_blocks);
    EXPECT_LE(work.staged_input_bytes,126u);
}
std::string byte_sha_row_blob(const database::row_t& row,const char* name) {
    const auto& bytes=std::get<std::vector<uint8_t>>(row.at(name));
    return {bytes.begin(),bytes.end()};
}
}

TEST(ReadyByteRangeSHA, KnownAnswersAndBinaryBytesMatchUnchangedVendor) {
    const std::vector<std::pair<std::string,std::string>> known{
        {"","e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
        {"abc","ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
        {"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq","248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"},
        {std::string(1000000,'a'),"cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"}
    };
    for(const auto& [input,expected]:known) {
        SCOPED_TRACE(input.size());ready_sha256_work work;
        EXPECT_EQ(ready_sha256_hex(input,&work),expected);
        EXPECT_EQ(picosha2::hash256_hex_string(input),expected);
        byte_sha_check_work(work,input.size());EXPECT_LE(work.staged_input_bytes,63u);
    }
    const auto binary=byte_sha_pattern(3*256+17);
    EXPECT_EQ(ready_sha256_hex(binary),picosha2::hash256_hex_string(binary));
    ready_sha256_state empty;
    byte_sha_check_work(empty.process(std::string_view{}),0);
    EXPECT_EQ(byte_sha_digest(empty),known.front().second);
}

TEST(ReadyByteRangeSHA, IncrementalBoundariesCarryLengthsAndNeverStageWholeLargeChunks) {
    const size_t maximum_frame=ready_profile(writer_profile()).package.codec.maximum.frame_bytes;
    const std::vector<size_t> lengths{0,1,55,56,63,64,65,127,128,129,65535,65536,65537,maximum_frame};
    for(const auto length:lengths) {
        SCOPED_TRACE(length);const auto input=byte_sha_pattern(length);
        const auto expected=picosha2::hash256_hex_string(input);
        const std::vector<size_t> chunks{1,63,64,65,127,257,std::max(size_t(1),length)};
        for(const auto chunk:chunks) {
            SCOPED_TRACE(chunk);ready_sha256_state state;picosha2::hash256_one_by_one reference;
            byte_sha_check_work(state.process({}),0);
            for(size_t at=0;at<input.size();) {
                const auto count=std::min(chunk,input.size()-at);
                byte_sha_check_work(state.process(std::string_view(input).substr(at,count)),count);
                reference.process(input.begin()+at,input.begin()+at+count);
                byte_sha_check_work(state.process({}),0);at+=count;
            }
            reference.finish();EXPECT_EQ(byte_sha_digest(state),expected);
            EXPECT_EQ(picosha2::get_hash_hex_string(reference),expected);
        }
        if(length<=129)for(size_t split=0;split<=length;++split) {
            ready_sha256_state state;
            byte_sha_check_work(state.process(std::string_view(input).substr(0,split)),split);
            byte_sha_check_work(state.process(std::string_view(input).substr(split)),length-split);
            EXPECT_EQ(byte_sha_digest(state),expected)<<"split="<<split;
        }
    }
}

TEST(ReadyByteRangeSHA, CopiesOwnPartialBytesAndDigestInspectionDoesNotConsumeState) {
    ready_sha256_state original,copy;std::string prefix;
    {
        auto borrowed=byte_sha_pattern(93);prefix=borrowed;
        byte_sha_check_work(original.process(borrowed),borrowed.size());copy=original;
        borrowed.assign(borrowed.size(),'x');
    }
    const auto before=original.digest_bytes();EXPECT_EQ(original.digest_bytes(),before);
    EXPECT_EQ(byte_sha_digest(copy),picosha2::hash256_hex_string(prefix));
    const auto first=byte_sha_pattern(75);const std::string second("\0other\xff",7);
    byte_sha_check_work(original.process(first),first.size());
    byte_sha_check_work(copy.process(second),second.size());
    EXPECT_EQ(byte_sha_digest(original),picosha2::hash256_hex_string(prefix+first));
    EXPECT_EQ(byte_sha_digest(copy),picosha2::hash256_hex_string(prefix+second));
    EXPECT_NE(original.digest_bytes(),copy.digest_bytes());
}

TEST(ReadyByteRangeSHA, OrderedDecimalLengthPackageFramingMatchesOriginalIncrementalHash) {
    const auto compare=[](const std::vector<std::string>& frames,bool wrong_length=false) {
        ready_sha256_state actual;picosha2::hash256_one_by_one reference;
        for(size_t i=0;i<frames.size();++i) {
            const auto& raw=frames[i];
            const auto prefix=std::to_string(raw.size()+(wrong_length&&i==1?1:0))+":";
            byte_sha_check_work(actual.process(prefix),prefix.size());
            byte_sha_check_work(actual.process(raw),raw.size());
            reference.process(prefix.begin(),prefix.end());reference.process(raw.begin(),raw.end());
        }
        reference.finish();const auto expected=picosha2::get_hash_hex_string(reference);
        EXPECT_EQ(byte_sha_digest(actual),expected);return expected;
    };
    // The first frame leaves a 63-byte tail so the next decimal prefix spans
    // a compression block; binary/empty frames also preserve the hash domain.
    std::vector<std::string> frames{byte_sha_pattern(60),byte_sha_pattern(65),"",byte_sha_pattern(129)};
    const auto baseline=compare(frames);
    EXPECT_NE(compare(frames,true),baseline);
    auto changed=frames;std::swap(changed[0],changed[1]);EXPECT_NE(compare(changed),baseline);
    changed=frames;changed.back().back()^=1;EXPECT_NE(compare(changed),baseline);
}

TEST_F(CanonicalDurableReady, StoredFrameAndPackageHashesMatchVendorAndActualReadStillAuditsAllBytes) {
    attach();auto admission=admit();const auto first=ready_entry(98101,98201,std::string(1536,'a'));
    import_entry(admission,first);import_entry(admission,ready_entry(98102,98202,std::string(1536,'b')));
    import_entry(admission,ready_entry(98103,98203,std::string(1536,'c')));ask(first);
    const auto offered=prepare(admission);complete(offered);const auto before=snapshot();
    const auto stored=owner->db().query("SELECT data,sha256 FROM _lattice_canonical_ready_frame ORDER BY frame_index");
    const auto transfers=owner->db().query("SELECT frames_sha FROM _lattice_canonical_ready_transfer");
    ASSERT_GT(stored.size(),3u);ASSERT_EQ(transfers.size(),1u);
    picosha2::hash256_one_by_one original;uint64_t raw_bytes=0,prefix_bytes=0;
    for(const auto& row:stored) {
        const auto raw=byte_sha_row_blob(row,"data");const auto prefix=std::to_string(raw.size())+":";
        EXPECT_EQ(byte_sha_row_blob(row,"sha256"),picosha2::hash256_hex_string(raw));
        original.process(prefix.begin(),prefix.end());original.process(raw.begin(),raw.end());
        raw_bytes+=raw.size();prefix_bytes+=prefix.size();
    }
    original.finish();EXPECT_EQ(byte_sha_row_blob(transfers.front(),"frames_sha"),picosha2::get_hash_hex_string(original));
    namespace reads=canonical_ready_read_test_observation;reads::observation trace;
    const auto previous=reads::current;reads::current=&trace;
    struct reset {reads::observation* previous;~reset(){reads::current=previous;}} restore{previous};
    const auto read=adapter->read_ready_frame_owned(owner,admission,*offered.lease,0);committed(read.settlement);ASSERT_TRUE(read.frame);
    auto expected=cr::decode(byte_sha_row_blob(stored.front(),"data"),policy.package.codec);expected.route_generation=7;
    EXPECT_EQ(*read.frame,cr::encode(expected,policy.package.codec));
    EXPECT_EQ(trace.full_audits,2u);EXPECT_EQ(trace.audited_frames,2*stored.size());EXPECT_EQ(trace.audited_bytes,2*raw_bytes);
    EXPECT_EQ(trace.positive_receipt_lookups,2u);EXPECT_EQ(trace.addressed_frames,1u);
    EXPECT_EQ(trace.cost.hash_input_bytes,4*raw_bytes+2*prefix_bytes+byte_sha_row_blob(stored.front(),"data").size());
    EXPECT_EQ(trace.cost.hash_input_bytes,trace.cost.hash_staged_input_bytes+64*trace.cost.hash_direct_blocks);
    EXPECT_GT(trace.cost.hash_direct_blocks,0u);EXPECT_LT(trace.cost.hash_staged_input_bytes,trace.cost.hash_input_bytes);
    EXPECT_LE(trace.cost.hash_staged_input_bytes,126*(6*stored.size()+1));
    EXPECT_EQ(snapshot(),before);
}

TEST_F(CanonicalDurableReady, PassiveHashWorkCountersSaturateWithoutChangingActualReadOrSettlement) {
    attach();auto admission=admit();import_entry(admission,ready_entry(98111,98211));
    const auto offered=prepare(admission);complete(offered);const auto before=snapshot();
    const auto baseline=adapter->read_ready_frame_owned(owner,admission,*offered.lease,0);committed(baseline.settlement);ASSERT_TRUE(baseline.frame);
    namespace reads=canonical_ready_read_test_observation;reads::observation trace;
    const auto maximum=~uint64_t{0};
    trace.cost.hash_input_bytes=trace.cost.hash_staged_input_bytes=trace.cost.hash_direct_blocks=maximum-1;
    const auto previous=reads::current;reads::current=&trace;
    struct reset {reads::observation* previous;~reset(){reads::current=previous;}} restore{previous};
    const auto read=adapter->read_ready_frame_owned(owner,admission,*offered.lease,0);committed(read.settlement);
    EXPECT_EQ(read.frame,baseline.frame);EXPECT_EQ(trace.full_audits,2u);
    EXPECT_EQ(trace.cost.hash_input_bytes,maximum);EXPECT_EQ(trace.cost.hash_staged_input_bytes,maximum);EXPECT_EQ(trace.cost.hash_direct_blocks,maximum);
    EXPECT_EQ(snapshot(),before);
}
#endif
