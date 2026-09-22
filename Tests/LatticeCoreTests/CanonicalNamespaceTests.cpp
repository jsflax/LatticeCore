#include "TestHelpers.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include "../../Sources/LatticeCore/src/sync_recovery_values.hpp"
#include <cstdio>
#include <cstring>
#include <type_traits>

struct NamespaceRow {std::string body;};
LATTICE_SCHEMA(NamespaceRow,body);
namespace lattice::detail {
// A unique friend definition; the existing retention friend remains unchanged.
struct canonical_namespace_test_access {
    static const std::function<void(lattice_db&)>* before_write(const std::function<void(lattice_db&)>* hook) {
        const auto* prior=canonical_writer_adapter::namespace_before_write_test_hook_;
        canonical_writer_adapter::namespace_before_write_test_hook_=hook;return prior;
    }
    static sync_recovery::owned_canonical_capture capture(canonical_writer_adapter& adapter,
        std::shared_ptr<lattice_db> owner,const canonical_namespace_admission& admission,
        const canonical_retention_ticket& ticket,const std::vector<sync_recovery::canonical_capture_request>& requests,
        const sync_recovery::canonical_capture_limits& limits,const std::function<void(size_t,uint64_t)>& after) {
        return adapter.capture_reserved_impl(std::move(owner),ticket,requests,limits,after,&admission);
    }
};
}
#if defined(__APPLE__) || defined(__linux__)
namespace {
using namespace lattice;
using namespace lattice::detail;
namespace sr=lattice::detail::sync_recovery;
using blob=std::vector<uint8_t>;
const bool registered=[] {
    auto schema=managed<NamespaceRow>::schema();schema.properties[0].no_history=true;
    schema_registry::instance().register_model(typeid(NamespaceRow),std::move(schema));return true;
}();
std::string uuid(unsigned n){char out[37];std::snprintf(out,sizeof(out),"00000000-0000-4000-8000-%012u",n);return out;}
configuration config(const std::string& path){configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=100;return c;}
std::shared_ptr<lattice_db> open_owner(const std::string& path){
    auto owner=std::make_shared<lattice_db>(config(path));
    if(auto* notifier=instance_registry::instance().get_or_create_notifier(path))notifier->stop_listening();
    return owner;
}
canonical_namespaced_writer_profile profile(){
    return {{{"namespaced-source","epoch","scope","schema"},{128,65536,128,65536,32,64,64},{"NamespaceRow"},true},
        {"source-local",{{"source-local","local-coverage",1},{"application-a","coverage-a",7},{"application-b","coverage-b",11}}}};
}
canonical_upstream_limits upstream(){return {32,4096,65536};}
canonical_retention_limits retention(){return {4,10000};}
audit_log_entry entry(unsigned original,unsigned target,const std::string& body="accepted"){
    audit_log_entry e;e.global_id=uuid(original);e.global_row_id=uuid(target);e.table_name="NamespaceRow";e.operation="INSERT";
    e.changed_fields_names={"body"};e.changed_fields={{"body",any_property(body)}};e.timestamp="1789819200.0";return e;
}
int64_t scalar(database& db,const std::string& sql,const std::vector<column_value_t>& args={}){
    const auto rows=db.query(sql,args);if(rows.size()!=1||rows[0].size()!=1)throw std::runtime_error("bad namespace fixture scalar");
    return std::get<int64_t>(rows[0].begin()->second);
}
std::string body(lattice_db& owner,const std::string& id){return std::get<std::string>(owner.db().query("SELECT body FROM NamespaceRow WHERE globalId=?",{id}).at(0).at("body"));}
struct Owned {lattice_db& owner;bool done=false;explicit Owned(lattice_db& db):owner(db){owner.begin_transaction();}
    ~Owned(){if(!done)try{owner.rollback();}catch(...) {}}void finish(){owner.commit();done=true;}};
struct BeforeNamespaceWrite {
    const std::function<void(lattice_db&)> callback;
    const std::function<void(lattice_db&)>* previous;
    explicit BeforeNamespaceWrite(std::function<void(lattice_db&)> value):callback(std::move(value)),
        previous(canonical_namespace_test_access::before_write(&callback)){}
    ~BeforeNamespaceWrite(){canonical_namespace_test_access::before_write(previous);}
    BeforeNamespaceWrite(const BeforeNamespaceWrite&)=delete;
    BeforeNamespaceWrite& operator=(const BeforeNamespaceWrite&)=delete;
};
canonical_store_state state(lattice_db& owner,const canonical_namespaced_writer_profile& p){
    Owned tx(owner);canonical_change_store store(owner,p.writer.binding,p.writer.limits,&p.namespaces);store.audit();auto out=store.state();tx.finish();return out;
}
std::optional<canonical_receipt> receipt(lattice_db& owner,const canonical_namespaced_writer_profile& p,const std::string& original){
    Owned tx(owner);canonical_change_store store(owner,p.writer.binding,p.writer.limits,&p.namespaces);auto out=store.receipt(original);tx.finish();return out;
}
void committed(const recovery_install_result& result){
    if(result.primary_error)std::rethrow_exception(result.primary_error);
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
    if(result.notification_error)std::rethrow_exception(result.notification_error);
    if(result.state!=recovery_install_state::committed)throw std::runtime_error("namespace retention did not commit");
}
class CanonicalNamespace:public ::testing::Test {
protected:
    TempDB file{"canonical-namespace"};
    std::shared_ptr<lattice_db> owner=open_owner(file.str()),sibling=open_owner(file.str());
    canonical_namespaced_writer_profile p=profile();
    sr::canonical_capture_limits limits{{{65536,16,4096,8192,2,32,64,524288},8,16,16},p.writer.limits,32,64,2};
    std::unique_ptr<canonical_writer_adapter> adapter;
    void attach(){adapter=canonical_writer_adapter::attach_namespaced_upstream_for_qualification(owner,p,upstream(),retention());}
    canonical_namespace_admission admit(const std::string& ns="application-a",const std::string& replica="replica-one"){
        return adapter->admit_namespace_for_qualification(owner,ns,replica);
    }
    canonical_retention_ticket reserve(){auto result=adapter->reserve_recovery_owned(owner,0,10000);committed(result.settlement);
        if(!result.reservation)throw std::runtime_error("namespace reserve lacks committed ticket");return *result.reservation;}
    std::vector<std::string> apply(const canonical_namespace_admission& admission,const std::vector<audit_log_entry>& entries){
        return adapter->apply_upstream_namespaced_owned(owner,admission,entries,"receiving");
    }
    sr::canonical_capture_request request(const audit_log_entry& e,const std::string& ns="application-a"){
        return {e.global_id,{{e.table_name,e.global_row_id}},ns};
    }
};
struct Fault {
    enum class Kind {receipt_ignore,receipt_deny,bytes_ignore,commit_deny};
    static thread_local Fault* active;
    Kind kind;int hits=0;
    canonical_upstream_test_hooks::authorizer_fault probe;
    const canonical_upstream_test_hooks::authorizer_fault* previous;
    Fault* prior;
    Fault(database& db,Kind k):kind(k),probe{canonical_writer_custody_test_access::fault_handle(db),restrict_action},
        previous(canonical_upstream_test_hooks::fault),prior(active){active=this;canonical_upstream_test_hooks::fault=&probe;}
    ~Fault(){canonical_upstream_test_hooks::fault=previous;active=prior;}
    static int restrict_action(int action,const char* one,const char* two,const char* origin)noexcept{
        // COMMIT failure retries once; keep denying while this fault is live.
        auto& f=*active;if(origin||(f.hits&&f.kind!=Kind::commit_deny))return SQLITE_OK;
        const auto same=[](const char* a,const char* b){return a&&std::strcmp(a,b)==0;};
        if(f.kind==Kind::commit_deny&&action==SQLITE_TRANSACTION&&same(one,"COMMIT")){++f.hits;return SQLITE_DENY;}
        if(action==SQLITE_INSERT&&same(one,"_lattice_canonical_receipt")&&(f.kind==Kind::receipt_ignore||f.kind==Kind::receipt_deny)){
            ++f.hits;return f.kind==Kind::receipt_ignore?SQLITE_IGNORE:SQLITE_DENY;}
        if(f.kind==Kind::bytes_ignore&&action==SQLITE_UPDATE&&same(one,"_lattice_canonical_store")&&same(two,"receipt_bytes")){
            ++f.hits;return SQLITE_IGNORE;}
        return SQLITE_OK;
    }
};
thread_local Fault* Fault::active=nullptr;
}

TEST_F(CanonicalNamespace, ActualFirstImportBindsNamespaceAndTwoReplicasMayShareIt){
    static_assert(!std::is_default_constructible_v<canonical_namespace_admission>);
    static_assert(!canonical_writer_adapter::serving_capability);
    attach();auto one=admit(),two=admit("application-a","replica-two"),other=admit("application-b");
    auto a=entry(101,1),b=entry(102,2);ASSERT_EQ(apply(one,{a}),std::vector<std::string>{a.global_id});
    const auto saved=receipt(*owner,p,a.global_id);ASSERT_TRUE(saved);
    EXPECT_EQ(saved->original.namespace_id,std::optional<std::string>("application-a"));
    EXPECT_EQ(saved->original.target,(canonical_identity{"NamespaceRow",a.global_row_id}));
    const auto before=state(*owner,p);a.operation="INVALID";a.changed_fields_names={"unknown"};a.changed_fields={{"unknown",any_property("replacement")}};
    EXPECT_EQ(apply(two,{a}),std::vector<std::string>{a.global_id});EXPECT_EQ(state(*owner,p),before);EXPECT_EQ(body(*owner,a.global_row_id),"accepted");
    EXPECT_EQ(apply(other,{b}),std::vector<std::string>{b.global_id});
    EXPECT_EQ(receipt(*owner,p,b.global_id)->original.namespace_id,std::optional<std::string>("application-b"));
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_receipt"),2);
    EXPECT_EQ(scalar(owner->db(),"SELECT receipt_bytes FROM _lattice_canonical_store"),2*(32+36+12+36+13));
}
TEST_F(CanonicalNamespace, GlobalOriginalCollisionAcrossNamespacesAndChangedTargetRefuseBeforeEffects){
    attach();auto a=admit(),b=admit("application-b");auto e=entry(101,1);ASSERT_EQ(apply(a,{e}).size(),1u);
    const auto before=state(*owner,p);auto replacement=e;replacement.global_row_id=uuid(2);replacement.changed_fields["body"]=any_property("replacement");
    EXPECT_TRUE(apply(b,{e}).empty());EXPECT_TRUE(apply(b,{replacement}).empty());EXPECT_TRUE(apply(a,{replacement}).empty());
    EXPECT_EQ(state(*owner,p),before);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM NamespaceRow"),1);EXPECT_EQ(body(*owner,e.global_row_id),"accepted");
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM AuditLog WHERE globalId=?",{e.global_id}),1);
}
TEST_F(CanonicalNamespace, UpdateDeleteAndRealNoopKeepOriginalNamespaceAndNoHistoryRefusal){
    attach();auto admission=admit();auto insert=entry(101,1,"initial");ASSERT_EQ(apply(admission,{insert}).size(),1u);
    auto update=entry(102,1,"current");update.operation="UPDATE";ASSERT_EQ(apply(admission,{update}).size(),1u);
    auto noop=update;noop.global_id=uuid(103);ASSERT_EQ(apply(admission,{noop}).size(),1u);
    ASSERT_TRUE(receipt(*owner,p,noop.global_id));EXPECT_EQ(receipt(*owner,p,noop.global_id)->original.outcome,canonical_receipt_outcome::no_op);
    auto missing=update;missing.global_id=uuid(104);missing.changed_fields["body"]=any_property(nullptr);
    const auto before=state(*owner,p);EXPECT_TRUE(apply(admission,{missing}).empty());EXPECT_EQ(state(*owner,p),before);
    auto deletion=entry(105,1);deletion.operation="DELETE";deletion.changed_fields_names.clear();deletion.changed_fields.clear();
    ASSERT_EQ(apply(admission,{deletion}).size(),1u);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM NamespaceRow"),0);
    for(const auto& e:{insert,update,noop,deletion}){
        const auto r=receipt(*owner,p,e.global_id);ASSERT_TRUE(r);EXPECT_EQ(r->original.namespace_id,std::optional<std::string>("application-a"));
        EXPECT_EQ(r->original.target,(canonical_identity{"NamespaceRow",e.global_row_id}));
    }
    EXPECT_FALSE(receipt(*owner,p,missing.global_id));
}
TEST_F(CanonicalNamespace, GeneratedLocalReceiptsUseEnrolledProvenanceAndNoHistoryStaysCurrent){
    attach();owner->add(NamespaceRow{"local current NoHistory"});
    const auto row=owner->db().query("SELECT globalId,globalRowId FROM AuditLog WHERE tableName='NamespaceRow' AND isFromRemote=0 AND synthesized=0").at(0);
    const auto original=canonical_writer_adapter::uuid_key(std::get<std::string>(row.at("globalId")));
    const auto target=canonical_writer_adapter::uuid_key(std::get<std::string>(row.at("globalRowId")));
    const auto r=receipt(*owner,p,original);ASSERT_TRUE(r);EXPECT_EQ(r->original.namespace_id,std::optional<std::string>(p.namespaces.local_namespace));
    auto local=admit("source-local");auto ticket=reserve();
    auto captured=adapter->capture_reserved_namespaced_owned(owner,local,ticket,{{original,{{"NamespaceRow",target}},"source-local"}},limits);
    ASSERT_TRUE(captured.capture);ASSERT_EQ(captured.capture->receipts.size(),1u);EXPECT_EQ(captured.capture->receipts[0].stored,r);
    ASSERT_EQ(captured.capture->rows.size(),1u);ASSERT_TRUE(captured.capture->rows[0].payload);
    EXPECT_NE(captured.capture->rows[0].payload->find("local current NoHistory"),std::string::npos);
    committed(adapter->release_recovery_owned(owner,ticket));
}
TEST_F(CanonicalNamespace, ReservedCaptureUsesSameViewReceiptImageAndHeadDuringLaterImport){
    attach();auto admission=admit();auto first=entry(101,1,"before"),later=entry(102,1,"after");later.operation="UPDATE";
    ASSERT_EQ(apply(admission,{first}).size(),1u);auto ticket=reserve();const auto before=state(*owner,p);
    bool imported=false;auto capture=canonical_namespace_test_access::capture(*adapter,owner,admission,ticket,{request(first),request(later)},limits,
        [&](size_t batch,uint64_t){if(batch==0&&!imported){imported=true;ASSERT_EQ(apply(admission,{later}).size(),1u);}});
    ASSERT_TRUE(imported);ASSERT_TRUE(capture.capture);EXPECT_EQ(capture.head,before.head);
    ASSERT_EQ(capture.capture->receipts.size(),2u);ASSERT_TRUE(capture.capture->receipts[0].stored);EXPECT_FALSE(capture.capture->receipts[1].stored);
    EXPECT_EQ(capture.capture->receipts[0].stored->original.namespace_id,std::optional<std::string>("application-a"));
    ASSERT_EQ(capture.capture->rows.size(),1u);ASSERT_TRUE(capture.capture->rows[0].payload);
    EXPECT_NE(capture.capture->rows[0].payload->find("before"),std::string::npos);EXPECT_EQ(body(*owner,first.global_row_id),"after");
    EXPECT_GT(state(*owner,p).head,capture.head);EXPECT_NE(adapter->prune_recovery_owned(owner,capture.head).state,recovery_install_state::committed);
    committed(adapter->release_recovery_owned(owner,ticket));committed(adapter->prune_recovery_owned(owner,state(*owner,p).head));
    EXPECT_TRUE(receipt(*owner,p,later.global_id));
}
TEST_F(CanonicalNamespace, RetirementDuringCaptureReleasesViewButKeepsUnadvertisedReservation){
    attach();std::optional<canonical_namespace_admission> admission=admit();auto e=entry(101,1);
    ASSERT_EQ(apply(*admission,{e}).size(),1u);auto ticket=reserve();bool retired=false;
    EXPECT_THROW(canonical_namespace_test_access::capture(*adapter,owner,*admission,ticket,{request(e)},limits,
        [&](size_t batch,uint64_t){if(batch==0&&!retired){retired=true;admission.reset();adapter.reset();}}),db_error);
    EXPECT_TRUE(retired);EXPECT_EQ(owner->local_read_generations_outstanding(),0u);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),1);
    attach();EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),0);
    auto fresh=admit();EXPECT_EQ(apply(fresh,{e}).size(),1u);
}
TEST_F(CanonicalNamespace, CaptureRefusesForeignNamespaceAndLegacyEntryPoints){
    attach();auto a=admit(),b=admit("application-b");auto e=entry(101,1);ASSERT_EQ(apply(a,{e}).size(),1u);auto ticket=reserve();
    EXPECT_THROW(adapter->capture_reserved_namespaced_owned(owner,a,ticket,{request(e,"application-b")},limits),db_error);
    EXPECT_THROW(adapter->capture_reserved_namespaced_owned(owner,b,ticket,{request(e,"application-b")},limits),sr::protocol_error);
    EXPECT_THROW(adapter->capture_reserved_owned(owner,ticket,{request(e)},limits),db_error);
    EXPECT_THROW(adapter->apply_upstream_owned(owner,{e}),db_error);
    EXPECT_THROW(apply_remote_changes(*owner,{entry(102,2)}),db_error);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),1);
    committed(adapter->release_recovery_owned(owner,ticket));
}
TEST_F(CanonicalNamespace, IgnoredOrDeniedFirstReceiptRollsBackItsEffectButNextEntryCanCommit){
    attach();auto admission=admit();unsigned id=101;
    for(auto kind:{Fault::Kind::receipt_ignore,Fault::Kind::receipt_deny,Fault::Kind::bytes_ignore}){
        const auto bad_id=id++;const auto good_id=id++;auto bad=entry(bad_id,bad_id+100),good=entry(good_id,good_id+100);const auto before=state(*owner,p);
        {Fault fault(owner->db(),kind);EXPECT_EQ(apply(admission,{bad,good}),std::vector<std::string>{good.global_id});EXPECT_EQ(fault.hits,1);}
        EXPECT_FALSE(receipt(*owner,p,bad.global_id));ASSERT_TRUE(receipt(*owner,p,good.global_id));
        EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM NamespaceRow WHERE globalId=?",{bad.global_row_id}),0);
        EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM AuditLog WHERE globalId=?",{bad.global_id}),0);
        const auto after=state(*owner,p);EXPECT_EQ(after.receipts,before.receipts+1);EXPECT_EQ(after.head,before.head+2);
    }
}
TEST_F(CanonicalNamespace, CommitFailureLeavesNoNamespaceReceiptAndObserverFailurePreservesCommit){
    attach();auto admission=admit();auto e=entry(101,1);const auto before=state(*owner,p);
    {Fault fault(owner->db(),Fault::Kind::commit_deny);EXPECT_TRUE(apply(admission,{e}).empty());EXPECT_EQ(fault.hits,2);}
    EXPECT_EQ(state(*owner,p),before);EXPECT_FALSE(receipt(*owner,p,e.global_id));EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM NamespaceRow"),0);
    int calls=0;auto token=owner->add_table_observer("NamespaceRow",[&](const auto&){++calls;throw std::runtime_error("postcommit observer");});
    EXPECT_EQ(apply(admission,{e}),std::vector<std::string>{e.global_id});owner->remove_table_observer("NamespaceRow",token);
    EXPECT_EQ(calls,1);ASSERT_TRUE(receipt(*owner,p,e.global_id));EXPECT_EQ(body(*owner,e.global_row_id),"accepted");
    const auto saved=state(*owner,p);EXPECT_EQ(apply(admission,{e}).size(),1u);EXPECT_EQ(state(*owner,p),saved);
}
TEST_F(CanonicalNamespace, PreopenedSiblingCannotRelabelCatalogReceiptsOrPrune){
    attach();auto admission=admit();ASSERT_EQ(apply(admission,{entry(101,1)}).size(),1u);auto ticket=reserve();const auto before=state(*owner,p);
    for(const auto* sql:{"DELETE FROM _lattice_canonical_namespace","UPDATE _lattice_canonical_namespace SET revision=99",
        "UPDATE _lattice_canonical_receipt SET namespace_id=X'62'","UPDATE _lattice_canonical_store SET floor=head","DELETE FROM _lattice_canonical_attempt"}){
        EXPECT_THROW(owner->db().execute(sql),db_error);
        EXPECT_THROW(sibling->db().execute(sql),db_error);
    }
    EXPECT_THROW(adapter->apply_upstream_namespaced_owned(sibling,admission,{entry(102,2)}),db_error);
    EXPECT_THROW(adapter->admit_namespace_for_qualification(sibling,"application-a","replica"),db_error);
    EXPECT_EQ(state(*owner,p),before);committed(adapter->release_recovery_owned(owner,ticket));
}
TEST_F(CanonicalNamespace, LegacyAuditAndReceiptsAreNeverRelabeledDuringNewEnrollment){
    auto e=entry(101,1);ASSERT_EQ(apply_remote_changes(*owner,{e}).size(),1u);
    owner->db().execute("INSERT INTO _lattice_applied_receipts(globalId) VALUES(?)",{uuid(102)});
    attach();auto admission=admit();const auto before=state(*owner,p);
    EXPECT_TRUE(apply(admission,{e,entry(102,2)}).empty());EXPECT_EQ(state(*owner,p),before);
    EXPECT_FALSE(receipt(*owner,p,e.global_id));EXPECT_FALSE(receipt(*owner,p,uuid(102)));EXPECT_EQ(body(*owner,e.global_row_id),"accepted");
}
TEST_F(CanonicalNamespace, InertV2PrimitiveReceiptsCannotBecomeOwnedAcceptanceAtEnrollment){
    auto e=entry(101,1);
    {Owned tx(*owner);canonical_change_store primitive(*owner,p.writer.binding,p.writer.limits,&p.namespaces);primitive.initialize();
        primitive.record({{"NamespaceRow",e.global_row_id}},canonical_receipt_request{e.global_id,canonical_receipt_outcome::applied,
            canonical_identity{"NamespaceRow",e.global_row_id},"application-a"});tx.finish();}
    const auto before=owner->db().query("SELECT * FROM _lattice_canonical_receipt");
    EXPECT_THROW(canonical_writer_adapter::attach_namespaced_upstream_for_qualification(owner,p,upstream(),retention()),db_error);
    EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_receipt"),before);
    EXPECT_FALSE(owner->db().table_exists("_lattice_canonical_coverage"));EXPECT_FALSE(owner->db().table_exists("_lattice_canonical_retention"));
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM NamespaceRow"),0);
}
TEST_F(CanonicalNamespace, SiblingPrimitiveCommitAfterPreflightRefusesWithoutAdoption){
    const auto e=entry(101,1);
    // All queries are fixture-bounded. Include the complete schema/cookie and
    // each primitive row so refusal cannot silently clean up or adopt evidence.
    const auto snapshot=[&] {
        std::vector<std::vector<database::row_t>> rows;
        for(const auto* sql:{"SELECT type,name,tbl_name,sql FROM main.sqlite_master ORDER BY type,name",
            "PRAGMA schema_version","SELECT * FROM _lattice_canonical_store",
            "SELECT * FROM _lattice_canonical_touch ORDER BY position,relation,identity",
            "SELECT * FROM _lattice_canonical_receipt ORDER BY original_id",
            "SELECT * FROM _lattice_canonical_namespace ORDER BY namespace_id",
            "SELECT * FROM NamespaceRow ORDER BY id","SELECT * FROM AuditLog ORDER BY id"})
            rows.push_back(sibling->db().query(sql));
        return rows;
    };
    std::vector<std::vector<database::row_t>> committed_rows;
    int calls=0;
    {
        BeforeNamespaceWrite hook([&](lattice_db& actual) {
            ++calls;EXPECT_EQ(&actual,owner.get());EXPECT_FALSE(actual.db().is_in_transaction());
            EXPECT_FALSE(sibling->db().table_exists("_lattice_canonical_store"));
            {Owned tx(*sibling);canonical_change_store primitive(*sibling,p.writer.binding,p.writer.limits,&p.namespaces);
                primitive.initialize();primitive.record({{"NamespaceRow",e.global_row_id}},
                    canonical_receipt_request{e.global_id,canonical_receipt_outcome::applied,
                        canonical_identity{"NamespaceRow",e.global_row_id},"application-a"});
                primitive.audit();EXPECT_EQ(primitive.state().receipts,1);tx.finish();}
            committed_rows=snapshot();
        });
        try {attach();FAIL()<<"sibling primitive evidence was adopted";}
        catch(const db_error& error) {
            EXPECT_STREQ(error.what(),"canonical namespaced enrollment refuses preexisting unadmitted metadata");
        }
    }
    EXPECT_EQ(calls,1);ASSERT_EQ(committed_rows.size(),8u);EXPECT_EQ(snapshot(),committed_rows);
    EXPECT_FALSE(adapter);EXPECT_FALSE(owner->db().is_in_transaction());EXPECT_FALSE(sibling->db().is_in_transaction());
    EXPECT_FALSE(sibling->db().table_exists("_lattice_canonical_coverage"));
    EXPECT_FALSE(sibling->db().table_exists("_lattice_canonical_retention"));
    EXPECT_FALSE(sibling->db().table_exists("_lattice_canonical_attempt"));
    EXPECT_EQ(scalar(sibling->db(),"SELECT COUNT(*) FROM NamespaceRow"),0);
    EXPECT_EQ(scalar(sibling->db(),"SELECT COUNT(*) FROM AuditLog WHERE globalId=?",{e.global_id}),0);
    const auto saved=receipt(*sibling,p,e.global_id);ASSERT_TRUE(saved);
    EXPECT_EQ(saved->original.namespace_id,std::optional<std::string>("application-a"));
    EXPECT_EQ(saved->original.outcome,canonical_receipt_outcome::applied);
    // The test hook was reset on the exception path. A subsequent real factory
    // call still refuses the now-preexisting primitive without touching it.
    EXPECT_THROW(attach(),db_error);
    EXPECT_EQ(calls,1);EXPECT_EQ(snapshot(),committed_rows);
}
TEST_F(CanonicalNamespace, BoundedEnrollmentAndUnknownNamespaceRefuse){
    auto bad=p;bad.namespaces.entries.push_back(bad.namespaces.entries.front());
    EXPECT_THROW(canonical_writer_adapter::attach_namespaced_upstream_for_qualification(owner,bad,upstream(),retention()),canonical_store_error);
    bad=p;bad.namespaces.local_namespace="absent";EXPECT_THROW(bad.namespaces.validate(),canonical_store_error);
    bad=p;bad.namespaces.entries[0].namespace_id=std::string(257,'x');EXPECT_THROW(bad.namespaces.validate(),canonical_store_error);
    bad=p;bad.namespaces.entries[0].revision=0;EXPECT_THROW(bad.namespaces.validate(),canonical_store_error);
    bad=p;bad.namespaces.entries.resize(65);EXPECT_THROW(bad.namespaces.validate(),canonical_store_error);
    EXPECT_FALSE(owner->db().table_exists("_lattice_canonical_store"));attach();
    EXPECT_THROW(admit("absent"),db_error);
    EXPECT_THROW(admit("application-a",std::string(257,'r')),db_error);
}
TEST_F(CanonicalNamespace, ReattachPreservesReceiptAndRejectsOldPhysicalAdmissionAndTicket){
    attach();auto old=admit();auto e=entry(101,1);ASSERT_EQ(apply(old,{e}).size(),1u);auto ticket=reserve();const auto saved=receipt(*owner,p,e.global_id);
    adapter.reset();attach();EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),0); // unchanged unadvertised v2 restart rule
    EXPECT_THROW(apply(old,{e}),db_error);
    EXPECT_THROW(adapter->capture_reserved_namespaced_owned(owner,old,ticket,{request(e)},limits),db_error);
    auto fresh=admit();e.changed_fields["body"]=any_property("replacement");EXPECT_EQ(apply(fresh,{e}).size(),1u);
    EXPECT_EQ(receipt(*owner,p,e.global_id),saved);EXPECT_EQ(body(*owner,e.global_row_id),"accepted");
}
TEST(CanonicalNamespaceFile, ExactReopenKeepsNamespaceAndOldProfileCannotAdoptIt){
    TempDB file{"canonical-namespace-reopen"};auto p=profile();auto e=entry(101,1);
    {auto owner=open_owner(file.str());auto adapter=canonical_writer_adapter::attach_namespaced_upstream_for_qualification(owner,p,upstream(),retention());
        auto admission=adapter->admit_namespace_for_qualification(owner,"application-a","replica");ASSERT_EQ(adapter->apply_upstream_namespaced_owned(owner,admission,{e}).size(),1u);}
    {auto owner=open_owner(file.str());EXPECT_THROW(canonical_writer_adapter::attach_retained_upstream_for_qualification(owner,p.writer,upstream(),retention()),db_error);}
    {auto owner=open_owner(file.str());auto adapter=canonical_writer_adapter::attach_namespaced_upstream_for_qualification(owner,p,upstream(),retention());
        auto admission=adapter->admit_namespace_for_qualification(owner,"application-b","replica");EXPECT_TRUE(adapter->apply_upstream_namespaced_owned(owner,admission,{e}).empty());
        EXPECT_EQ(receipt(*owner,p,e.global_id)->original.namespace_id,std::optional<std::string>("application-a"));}
}
TEST(CanonicalNamespaceFile, LegacyCanonicalProfileRefusesUpgradeWithoutReceiptOrRegistryCleanup){
    TempDB file{"canonical-namespace-no-upgrade"};auto p=profile();auto e=entry(101,1);
    {auto owner=open_owner(file.str());auto adapter=canonical_writer_adapter::attach_retained_upstream_for_qualification(owner,p.writer,upstream(),retention());
        ASSERT_EQ(adapter->apply_upstream_owned(owner,{e}).size(),1u);auto ticket=adapter->reserve_recovery_owned(owner,0,10000);committed(ticket.settlement);}
    {auto owner=open_owner(file.str());const auto before=owner->db().query("SELECT * FROM _lattice_canonical_store");
        EXPECT_THROW(canonical_writer_adapter::attach_namespaced_upstream_for_qualification(owner,p,upstream(),retention()),db_error);
        EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_store"),before);EXPECT_FALSE(owner->db().table_exists("_lattice_canonical_namespace"));
        EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),1);}
}
TEST(CanonicalNamespaceFile, MissingOrAlteredNamespaceGuardRefusesBeforeIncarnationCleanup){
    for(bool altered:{false,true}){
        TempDB file{"canonical-namespace-guard"};auto p=profile();
        {auto owner=open_owner(file.str());auto adapter=canonical_writer_adapter::attach_namespaced_upstream_for_qualification(owner,p,upstream(),retention());
            owner->add(NamespaceRow{"covered"});auto ticket=adapter->reserve_recovery_owned(owner,0,10000);committed(ticket.settlement);}
        {database raw(file.str());raw.execute("DROP TRIGGER _lattice_canonical_namespace_retention_UPDATE");
            if(altered)raw.execute("CREATE TRIGGER _lattice_canonical_namespace_retention_UPDATE BEFORE UPDATE ON _lattice_canonical_namespace BEGIN SELECT 1; END");}
        {auto owner=open_owner(file.str());const auto before=owner->db().query("SELECT * FROM _lattice_canonical_retention");
            EXPECT_THROW(canonical_writer_adapter::attach_namespaced_upstream_for_qualification(owner,p,upstream(),retention()),db_error);
            EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_retention"),before);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),1);}
    }
}
TEST(CanonicalNamespaceFile, ChangedCatalogBindingRefusesBeforeUnadvertisedAttemptCleanup){
    TempDB file{"canonical-namespace-binding"};auto p=profile();
    {auto owner=open_owner(file.str());auto adapter=canonical_writer_adapter::attach_namespaced_upstream_for_qualification(owner,p,upstream(),retention());
        owner->add(NamespaceRow{"covered"});auto ticket=adapter->reserve_recovery_owned(owner,0,10000);committed(ticket.settlement);}
    {auto owner=open_owner(file.str());const auto before=owner->db().query("SELECT * FROM _lattice_canonical_retention");
        auto changed=p;changed.namespaces.entries[1].revision++;
        EXPECT_THROW(canonical_writer_adapter::attach_namespaced_upstream_for_qualification(owner,changed,upstream(),retention()),canonical_store_error);
        EXPECT_EQ(owner->db().query("SELECT * FROM _lattice_canonical_retention"),before);
        EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_canonical_attempt"),1);}
}
TEST_F(CanonicalNamespace, NamespaceBytesAreChargedBeforeAcceptanceAndRollbackOnCapacity){
    p.writer.limits.receipt_bytes=32+36+12+36; // room for the v1 row, not its actual v2 namespace
    attach();auto admission=admit();const auto before=state(*owner,p);auto e=entry(101,1);
    EXPECT_TRUE(apply(admission,{e}).empty());EXPECT_EQ(state(*owner,p),before);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM NamespaceRow"),0);EXPECT_FALSE(receipt(*owner,p,e.global_id));
}
TEST_F(CanonicalNamespace, CloseFromObserverKeepsFirstReceiptAndFencesFurtherAdmission){
    attach();auto admission=admit();auto e=entry(101,1);auto token=owner->add_table_observer("NamespaceRow",[&](const auto&){owner->close();});
    EXPECT_EQ(apply(admission,{e}),std::vector<std::string>{e.global_id});EXPECT_TRUE(owner->is_closed());
    EXPECT_THROW(apply(admission,{entry(102,2)}),db_error);owner->remove_table_observer("NamespaceRow",token);
    adapter.reset();owner.reset();sibling.reset();auto reopened=open_owner(file.str());
    auto next=canonical_writer_adapter::attach_namespaced_upstream_for_qualification(reopened,p,upstream(),retention());
    EXPECT_TRUE(receipt(*reopened,p,e.global_id));EXPECT_FALSE(receipt(*reopened,p,uuid(102)));
}
#endif
