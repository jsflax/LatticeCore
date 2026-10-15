#include "TestHelpers.hpp"
#include "CanonicalWriterTestAccess.hpp"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include <cstdio>

struct UpstreamNode {std::string name;std::string body;std::vector<uint8_t> data;};
LATTICE_SCHEMA(UpstreamNode,name,body,data);
struct UpstreamLeaf {std::string name;};
LATTICE_SCHEMA(UpstreamLeaf,name);
struct UpstreamRoot {std::string name;};
LATTICE_SCHEMA(UpstreamRoot,name);
namespace {
using namespace lattice;
using namespace lattice::detail;
using blob=std::vector<uint8_t>;
const bool registered=[] {
    auto node=managed<UpstreamNode>::schema();node.properties[1].no_history=true;
    node.properties[2].is_vector=false;
    schema_registry::instance().register_model(typeid(UpstreamNode),std::move(node));
    auto root=managed<UpstreamRoot>::schema();property_descriptor link{};
    link.name="leaf";link.kind=property_kind::link;link.type=column_type::integer;link.nullable=true;link.target_table="UpstreamLeaf";
    root.properties.push_back(link);schema_registry::instance().register_model(typeid(UpstreamRoot),std::move(root));return true;
}();
constexpr const char* relation="_UpstreamRoot_UpstreamLeaf_leaf";
std::string uuid(unsigned n) {char out[37];std::snprintf(out,sizeof(out),"00000000-0000-4000-8000-%012u",n);return out;}
configuration config(const std::string& path) {configuration c(path);c.audit_retention_seconds=0;c.busy_timeout_ms=100;return c;}
canonical_writer_profile profile() {return {{"upstream-source","fixed-epoch","node-root-leaf","schema-v1"},
    {1024,262144,4096,1048576,64,64,64},{"UpstreamNode","UpstreamRoot","UpstreamLeaf"},true};}
canonical_upstream_limits limits(){return {512,65536,1048576};}
audit_log_entry entry(unsigned original,unsigned target,const std::string& name="remote") {
    audit_log_entry e;e.global_id=uuid(original);e.global_row_id=uuid(target);e.table_name="UpstreamNode";e.operation="INSERT";
    e.changed_fields_names={"name","body","data"};e.changed_fields={{"name",any_property(name)},{"body",any_property("body")},{"data",any_property(blob{1,0,2})}};
    e.timestamp="1789819200.0";return e;
}
int64_t scalar(database& db,const std::string& sql,const std::vector<column_value_t>& params={}) {
    const auto rows=db.query(sql,params);if(rows.size()!=1||rows[0].size()!=1)throw std::runtime_error("bad scalar shape");
    return std::get<int64_t>(rows[0].begin()->second);
}
std::string name(lattice_db& db,const std::string& id) {
    return std::get<std::string>(db.db().query("SELECT name FROM UpstreamNode WHERE globalId=?",{id}).at(0).at("name"));
}
struct Owned {lattice_db& db;bool done=false;explicit Owned(lattice_db& d):db(d){db.begin_transaction();}
    ~Owned(){if(!done)try{db.rollback();}catch(...) {}}void finish(){db.commit();done=true;}};
canonical_store_state state(lattice_db& db,const canonical_writer_profile& p) {
    Owned tx(db);canonical_change_store store(db,p.binding,p.limits);store.audit();auto result=store.state();tx.finish();return result;
}
std::optional<canonical_receipt> receipt(lattice_db& db,const canonical_writer_profile& p,const std::string& id) {
    Owned tx(db);canonical_change_store store(db,p.binding,p.limits);auto result=store.receipt(canonical_writer_adapter::uuid_key(id));tx.finish();return result;
}
void stop_notifier(lattice_db& db) {
    if(db.config().is_in_memory())return;
    auto* notifier=instance_registry::instance().get_or_create_notifier(db.config().path);
    ASSERT_NE(notifier,nullptr);notifier->stop_listening();ASSERT_FALSE(notifier->is_listening());
}
class CanonicalUpstream:public ::testing::Test {
protected:
    std::shared_ptr<lattice_db> owner=std::make_shared<lattice_db>(config(":memory:"));
    canonical_writer_profile p=profile();
    std::unique_ptr<canonical_writer_adapter> adapter;
    void attach(){adapter=canonical_writer_adapter::attach_upstream_for_qualification(owner,p,limits());}
    std::vector<std::string> apply(const std::vector<audit_log_entry>& e,const std::optional<std::string>& channel={}) {
        return adapter->apply_upstream_owned(owner,e,channel);
    }
};
struct Fault {
    enum class Kind {receipt_deny,receipt_ignore,head_ignore,count_ignore,bytes_ignore,commit_deny};
    static thread_local Fault* active;
    Kind kind;int hits=0;bool once=true;
    canonical_upstream_test_hooks::authorizer_fault probe;
    const canonical_upstream_test_hooks::authorizer_fault* previous;
    Fault* prior;
    Fault(database& db,Kind k,bool one=true):kind(k),once(one),probe{canonical_writer_custody_test_access::fault_handle(db),restrict_action},
        previous(canonical_upstream_test_hooks::fault),prior(active){active=this;canonical_upstream_test_hooks::fault=&probe;}
    ~Fault(){canonical_upstream_test_hooks::fault=previous;active=prior;}
    static int restrict_action(int action,const char* one,const char* two,const char* origin) noexcept {
        auto& f=*active;if(origin||(f.once&&f.hits))return SQLITE_OK;
        const auto same=[](const char* a,const char* b){return a&&std::strcmp(a,b)==0;};
        if(f.kind==Kind::commit_deny&&action==SQLITE_TRANSACTION&&same(one,"COMMIT")){++f.hits;return SQLITE_DENY;}
        if(action==SQLITE_INSERT&&same(one,"_lattice_canonical_receipt")&&
            (f.kind==Kind::receipt_deny||f.kind==Kind::receipt_ignore)){++f.hits;return f.kind==Kind::receipt_deny?SQLITE_DENY:SQLITE_IGNORE;}
        if(action==SQLITE_UPDATE&&same(one,"_lattice_canonical_store")&&
            ((f.kind==Kind::head_ignore&&same(two,"head"))||(f.kind==Kind::count_ignore&&same(two,"receipts"))||
             (f.kind==Kind::bytes_ignore&&same(two,"receipt_bytes")))){++f.hits;return SQLITE_IGNORE;}
        return SQLITE_OK;
    }
};
thread_local Fault* Fault::active=nullptr;
void fault_case(Fault::Kind kind) {
    auto owner=std::make_shared<lattice_db>(config(":memory:"));auto p=profile();
    auto adapter=canonical_writer_adapter::attach_upstream_for_qualification(owner,p,limits());
    auto first=entry(101,1,"bad"),second=entry(102,2,"good");
    std::vector<std::string> observed;
    auto token=owner->add_table_observer("UpstreamNode",[&](const auto& rows){for(const auto& row:rows)observed.push_back(std::get<3>(row));});
    {Fault fault(owner->db(),kind);EXPECT_EQ(adapter->apply_upstream_owned(owner,{first,second}),std::vector<std::string>{second.global_id});EXPECT_EQ(fault.hits,1);}
    owner->remove_table_observer("UpstreamNode",token);
    EXPECT_EQ(observed,std::vector<std::string>{second.global_row_id});
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM UpstreamNode WHERE globalId=?",{first.global_row_id}),0);
    EXPECT_FALSE(receipt(*owner,p,first.global_id));EXPECT_TRUE(receipt(*owner,p,second.global_id));
    const auto s=state(*owner,p);EXPECT_EQ(s.markers,1);EXPECT_EQ(s.receipts,1);EXPECT_EQ(s.head,2);
    EXPECT_EQ(adapter->apply_upstream_owned(owner,{first}),std::vector<std::string>{first.global_id});
    EXPECT_EQ(state(*owner,p).receipts,2);EXPECT_EQ(name(*owner,first.global_row_id),"bad");
}
}

TEST_F(CanonicalUpstream, GlobalEffectHasOriginalReceiptAfterTouchAndNoLegacyDDL) {
    attach();auto e=entry(101,1);EXPECT_EQ(apply({e}),std::vector<std::string>{e.global_id});
    EXPECT_EQ(name(*owner,e.global_row_id),"remote");const auto r=receipt(*owner,p,e.global_id);ASSERT_TRUE(r);
    EXPECT_EQ(r->original.outcome,canonical_receipt_outcome::applied);
    EXPECT_EQ(r->original.target,(canonical_identity{"UpstreamNode",e.global_row_id}));
    EXPECT_EQ(r->position,2);const auto s=state(*owner,p);EXPECT_EQ(s.head,2);EXPECT_EQ(s.markers,1);EXPECT_EQ(s.receipts,1);
    EXPECT_EQ(scalar(owner->db(),"SELECT position FROM _lattice_canonical_touch"),1);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM AuditLog WHERE globalId=? AND isFromRemote=1 AND isSynchronized=1",{e.global_id}),1);
    EXPECT_FALSE(owner->db().table_exists("_lattice_applied_receipts"));
    EXPECT_FALSE(canonical_writer_adapter::serving_capability);
}
TEST_F(CanonicalUpstream, PerChannelEffectPreservesOtherUploadObligations) {
    attach();auto e=entry(101,1);EXPECT_EQ(apply({e},"receiver"),std::vector<std::string>{e.global_id});
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM AuditLog WHERE globalId=? AND isFromRemote=1 AND isSynchronized=0",{e.global_id}),1);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_sync_state s JOIN AuditLog a ON a.id=s.audit_entry_id WHERE a.globalId=? AND s.sync_id='receiver' AND s.is_synchronized=1",{e.global_id}),1);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM _lattice_sync_state"),1);EXPECT_TRUE(receipt(*owner,p,e.global_id));
}
TEST_F(CanonicalUpstream, DuplicateAfterLocalEditAndAuditRemovalDoesNotInterpretReplacementPayload) {
    attach();auto e=entry(101,1);ASSERT_EQ(apply({e}).size(),1u);
    owner->db().execute("UPDATE UpstreamNode SET name='newer-local' WHERE globalId=?",{e.global_row_id});
    owner->db().execute("DELETE FROM AuditLog WHERE globalId=?",{e.global_id});const auto before=state(*owner,p);
    e.operation="INVALID";e.changed_fields_names={"unknown"};e.changed_fields={{"unknown",any_property("replacement")}};
    EXPECT_EQ(apply({e}),std::vector<std::string>{e.global_id});EXPECT_EQ(name(*owner,e.global_row_id),"newer-local");
    EXPECT_EQ(state(*owner,p),before);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM AuditLog WHERE globalId=?",{e.global_id}),0);
}
TEST_F(CanonicalUpstream, SameOriginalDifferentTargetRefusesAndKeepsFirstReceipt) {
    attach();auto e=entry(101,1);ASSERT_EQ(apply({e}).size(),1u);const auto before=state(*owner,p);auto changed=e;changed.global_row_id=uuid(2);
    EXPECT_TRUE(apply({changed}).empty());EXPECT_EQ(state(*owner,p),before);EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM UpstreamNode"),1);
    const auto retained=receipt(*owner,p,e.global_id);ASSERT_TRUE(retained);
    EXPECT_EQ(retained->original.target,(canonical_identity{"UpstreamNode",e.global_row_id}));
}
TEST_F(CanonicalUpstream, LegacyAuditOnlyAndLegacyNoopOnlyAreNotAdopted) {
    auto a=entry(101,1),b=entry(102,2);
    ASSERT_EQ(apply_remote_changes(*owner,{a}).size(),1u);
    owner->db().execute("INSERT INTO _lattice_applied_receipts(globalId) VALUES(?)",{b.global_id});
    attach();const auto before=state(*owner,p);EXPECT_TRUE(apply({a,b}).empty());
    EXPECT_EQ(state(*owner,p),before);EXPECT_FALSE(receipt(*owner,p,a.global_id));EXPECT_FALSE(receipt(*owner,p,b.global_id));
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM UpstreamNode"),1);
}
TEST_F(CanonicalUpstream, EqualValuePerChannelNoopGetsReceiptWithoutNewAuditOrTouch) {
    attach();auto a=entry(101,1),b=entry(102,1);ASSERT_EQ(apply({a},"receiver").size(),1u);const auto before=state(*owner,p);
    EXPECT_EQ(apply({b},"receiver"),std::vector<std::string>{b.global_id});const auto after=state(*owner,p);
    EXPECT_EQ(after.head,before.head+1);EXPECT_EQ(after.markers,before.markers);EXPECT_EQ(after.receipts,before.receipts+1);
    const auto retained=receipt(*owner,p,b.global_id);ASSERT_TRUE(retained);
    EXPECT_EQ(retained->original.outcome,canonical_receipt_outcome::no_op);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM AuditLog WHERE globalId=?",{b.global_id}),0);
}
TEST_F(CanonicalUpstream, AbsentUpdateDeleteAndEmptyUpdateRetainNoopReceipts) {
    attach();auto update=entry(101,1);update.operation="UPDATE";auto deletion=entry(102,2);deletion.operation="DELETE";
    auto empty=entry(103,3);empty.operation="UPDATE";empty.changed_fields.clear();empty.changed_fields_names.clear();
    EXPECT_EQ(apply({update,deletion,empty}), (std::vector<std::string>{update.global_id,deletion.global_id,empty.global_id}));
    for(const auto& e:{update,deletion,empty}) {
        const auto retained=receipt(*owner,p,e.global_id);ASSERT_TRUE(retained);
        EXPECT_EQ(retained->original.outcome,canonical_receipt_outcome::no_op);
    }
    const auto s=state(*owner,p);EXPECT_EQ(s.head,3);EXPECT_EQ(s.markers,0);EXPECT_EQ(s.receipts,3);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM UpstreamNode"),0);
}
TEST_F(CanonicalUpstream, SynthesizedExistingRowKeepsNewerValuesAndNoopOutcome) {
    attach();auto a=entry(101,1,"newer"),b=entry(102,1,"stale");b.synthesized=true;
    ASSERT_EQ(apply({a}).size(),1u);const auto before=state(*owner,p);EXPECT_EQ(apply({b}).size(),1u);
    EXPECT_EQ(name(*owner,a.global_row_id),"newer");EXPECT_EQ(state(*owner,p).head,before.head+1);
    const auto retained=receipt(*owner,p,b.global_id);ASSERT_TRUE(retained);
    EXPECT_EQ(retained->original.outcome,canonical_receipt_outcome::no_op);
}
TEST_F(CanonicalUpstream, NoHistoryNulAndRawOrHexBlobApplyExactValues) {
    attach();auto e=entry(101,1);e.changed_fields["body"]=any_property(std::string("one\0two",7));
    e.changed_fields["data"]=any_property(blob{0,9,0,8});ASSERT_EQ(apply({e}).size(),1u);
    auto row=owner->db().query("SELECT body,data FROM UpstreamNode").at(0);
    EXPECT_EQ(std::get<std::string>(row.at("body")),std::string("one\0two",7));EXPECT_EQ(std::get<blob>(row.at("data")),(blob{0,9,0,8}));
    auto hex=entry(102,1);hex.operation="UPDATE";hex.changed_fields_names={"data"};hex.changed_fields={{"data",any_property("00ff0080")}};
    hex.changed_fields["data"].kind=any_property_kind::data_kind;ASSERT_EQ(apply({hex}).size(),1u);
    EXPECT_EQ(std::get<blob>(owner->db().query("SELECT data FROM UpstreamNode").at(0).at("data")),(blob{0,255,0,128}));
    EXPECT_EQ(scalar(owner->db(),"SELECT MAX(length(identity)) FROM _lattice_canonical_touch"),36);
    EXPECT_EQ(state(*owner,p).markers,1);
}
TEST_F(CanonicalUpstream, FreshUnknownMalformedNoHistoryAndFilterRemovalRefuse) {
    attach();std::vector<audit_log_entry> bad;
    auto unknown=entry(101,1);unknown.changed_fields_names={"missing"};unknown.changed_fields={{"missing",any_property(1)}};bad.push_back(unknown);
    auto absent=entry(102,2);absent.changed_fields["body"]=any_property(nullptr);bad.push_back(absent);
    auto malformed=entry(103,3);malformed.changed_fields["name"].kind=any_property_kind::int_kind;bad.push_back(malformed);
    auto filter=entry(104,4);filter.operation="DELETE";filter.changed_fields_names={"__lattice_filter_removal"};filter.changed_fields.clear();bad.push_back(filter);
    auto good=entry(105,5);bad.push_back(good);
    EXPECT_EQ(apply(bad),std::vector<std::string>{good.global_id});EXPECT_EQ(state(*owner,p).receipts,1);EXPECT_EQ(state(*owner,p).markers,1);
}
TEST_F(CanonicalUpstream, EnvelopeBoundsRefuseBeforeAnyEntryEffectIncludingDuplicates) {
    auto small=limits();small.entries=2;small.field_bytes=32;adapter=canonical_writer_adapter::attach_upstream_for_qualification(owner,p,small);
    auto a=entry(101,1);ASSERT_EQ(apply({a}).size(),1u);const auto before=state(*owner,p);auto b=entry(102,2);b.changed_fields["body"]=any_property(std::string(33,'x'));
    EXPECT_THROW(apply({a,b}),db_error);EXPECT_EQ(state(*owner,p),before);
    EXPECT_THROW(apply({a,a,a}),db_error);EXPECT_EQ(state(*owner,p),before);
    a.changed_fields["body"]=any_property(std::string(33,'x'));EXPECT_THROW(apply({a}),db_error);
}
TEST_F(CanonicalUpstream, LinkInsertUpdateDeleteProduceTargetReceiptsAndActualTouchPositions) {
    auto root=owner->add(UpstreamRoot{"root"});auto leaf=owner->add(UpstreamLeaf{"leaf"});attach();
    audit_log_entry e;e.global_id=uuid(101);e.global_row_id=uuid(1);e.table_name=relation;e.operation="INSERT";e.timestamp="1789819200";
    e.changed_fields_names={"lhs","rhs"};e.changed_fields={{"lhs",any_property(root.global_id())},{"rhs",any_property(leaf.global_id())}};
    ASSERT_EQ(apply({e}).size(),1u);const auto insert=receipt(*owner,p,e.global_id);ASSERT_TRUE(insert);EXPECT_EQ(insert->position,2);
    auto equal=e;equal.global_id=uuid(102);equal.operation="UPDATE";ASSERT_EQ(apply({equal}).size(),1u);
    const auto retained=receipt(*owner,p,equal.global_id);ASSERT_TRUE(retained);
    EXPECT_EQ(retained->original.outcome,canonical_receipt_outcome::no_op);
    auto deletion=e;deletion.global_id=uuid(103);deletion.operation="DELETE";ASSERT_EQ(apply({deletion}).size(),1u);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM "+std::string(relation)),0);
    const auto s=state(*owner,p);EXPECT_EQ(s.markers,1);EXPECT_EQ(s.receipts,3);EXPECT_EQ(s.head,5);
}
TEST(CanonicalUpstreamFault, ReceiptDeniedRollsBackEntryAndPreservesSiblingAndFreshSuccessor) {fault_case(Fault::Kind::receipt_deny);}
TEST(CanonicalUpstreamFault, ReceiptIgnoredRollsBackEntryAndPreservesSiblingAndFreshSuccessor) {fault_case(Fault::Kind::receipt_ignore);}
TEST(CanonicalUpstreamFault, HeadIgnoredRollsBackEntryAndPreservesSiblingAndFreshSuccessor) {fault_case(Fault::Kind::head_ignore);}
TEST(CanonicalUpstreamFault, CountIgnoredRollsBackEntryAndPreservesSiblingAndFreshSuccessor) {fault_case(Fault::Kind::count_ignore);}
TEST(CanonicalUpstreamFault, BytesIgnoredRollsBackEntryAndPreservesSiblingAndFreshSuccessor) {fault_case(Fault::Kind::bytes_ignore);}
TEST_F(CanonicalUpstream, CommitRefusalRollsBackWholeChunkAndAllowsSuccessor) {
    attach();auto a=entry(101,1),b=entry(102,2);const auto before=state(*owner,p);int calls=0;
    auto token=owner->add_table_observer("UpstreamNode",[&](const auto&){++calls;});
    {Fault fault(owner->db(),Fault::Kind::commit_deny,false);EXPECT_TRUE(apply({a,b}).empty());EXPECT_GE(fault.hits,1);}
    owner->remove_table_observer("UpstreamNode",token);EXPECT_EQ(calls,0);EXPECT_EQ(state(*owner,p),before);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM UpstreamNode"),0);EXPECT_EQ(scalar(owner->db(),"SELECT disabled FROM _SyncControl WHERE id=1"),0);
    EXPECT_EQ(apply({a}).size(),1u);EXPECT_TRUE(receipt(*owner,p,a.global_id));
}
TEST_F(CanonicalUpstream, BindingLengthFailureCannotBecomeNoopAcceptance) {
    attach();auto bad=entry(101,1),good=entry(102,2);bad.changed_fields["body"]=any_property(std::string(8192,'x'));
    struct Limit {sqlite3* db;int old;explicit Limit(sqlite3* h):db(h),old(sqlite3_limit(h,SQLITE_LIMIT_LENGTH,2048)){}~Limit(){sqlite3_limit(db,SQLITE_LIMIT_LENGTH,old);}};
    {Limit limited(canonical_writer_custody_test_access::fault_handle(owner->db()));EXPECT_EQ(apply({bad,good}),std::vector<std::string>{good.global_id});}
    EXPECT_FALSE(receipt(*owner,p,bad.global_id));EXPECT_TRUE(receipt(*owner,p,good.global_id));
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM UpstreamNode"),1);EXPECT_EQ(state(*owner,p).head,2);
}
TEST_F(CanonicalUpstream, OwnerRetentionSurvivesObserverReleasingOwnerAndAdapter) {
    attach();auto e=entry(101,1);std::weak_ptr<lattice_db> weak=owner;bool retained=false;
    owner->add_table_observer("UpstreamNode",[&](const auto&){owner.reset();adapter.reset();retained=!weak.expired();});
    auto result=adapter->apply_upstream_owned(owner,{e});EXPECT_EQ(result,std::vector<std::string>{e.global_id});
    EXPECT_TRUE(retained);EXPECT_TRUE(weak.expired());
}
TEST_F(CanonicalUpstream, ObserverThrowKeepsCommitAndDoesNotRollbackSuccessorTransaction) {
    attach();auto e=entry(101,1);int calls=0;auto token=owner->add_table_observer("UpstreamNode",[&](const auto&){
        ++calls;owner->db().begin_transaction();owner->db().execute("INSERT INTO _lattice_meta(key,value) VALUES('upstream-successor','pending')");
        throw std::runtime_error("after commit");});
    EXPECT_EQ(apply({e}),std::vector<std::string>{e.global_id});owner->remove_table_observer("UpstreamNode",token);
    EXPECT_EQ(calls,1);EXPECT_TRUE(owner->db().is_in_transaction());owner->db().rollback();
    EXPECT_EQ(name(*owner,e.global_row_id),"remote");EXPECT_TRUE(receipt(*owner,p,e.global_id));
    const auto before=state(*owner,p);EXPECT_EQ(apply({e}).size(),1u);EXPECT_EQ(state(*owner,p),before);
}
TEST_F(CanonicalUpstream, BorrowedWrongOwnerRevokedAndExpiredEntryGuardsRefuse) {
    attach();auto e=entry(101,1);EXPECT_THROW(apply_remote_changes(*owner,{e}),db_error);
    auto other=std::make_shared<lattice_db>(config(":memory:"));EXPECT_THROW(adapter->apply_upstream_owned(other,{e}),db_error);
    const auto key=blob(e.global_id.begin(),e.global_id.end()),target=blob(e.global_row_id.begin(),e.global_row_id.end());
    const auto table=blob(e.table_name.begin(),e.table_name.end());
    EXPECT_THROW(owner->db().query("SELECT lattice_canonical_entry_v1(?,?,?)",{key,table,target}),db_error);
    std::atomic<bool> refused=false;std::thread thread([&]{try{owner->db().query("SELECT lattice_canonical_entry_v1(?,?,?)",{key,table,target});}catch(const db_error&){refused=true;}});thread.join();EXPECT_TRUE(refused);
    EXPECT_THROW(owner->db().execute("UPDATE _lattice_canonical_store SET head=head+1"),db_error);
    adapter.reset();EXPECT_THROW(owner->db().execute("INSERT INTO UpstreamNode(name,body,data) VALUES('raw','body',X'00')"),db_error);
}
TEST(CanonicalUpstreamFile, ReopenRetainsFirstReceiptWithoutAdoptingReplacementPayload) {
    TempDB file{"canonical_upstream_reopen"};auto p=profile();auto e=entry(101,1);
    {auto owner=std::make_shared<lattice_db>(config(file.str()));ASSERT_NO_FATAL_FAILURE(stop_notifier(*owner));
        auto adapter=canonical_writer_adapter::attach_upstream_for_qualification(owner,p,limits());ASSERT_EQ(adapter->apply_upstream_owned(owner,{e}).size(),1u);}
    {auto owner=std::make_shared<lattice_db>(config(file.str()));ASSERT_NO_FATAL_FAILURE(stop_notifier(*owner));
        auto adapter=canonical_writer_adapter::attach_upstream_for_qualification(owner,p,limits());const auto before=state(*owner,p);
        e.changed_fields["name"]=any_property("replacement");EXPECT_EQ(adapter->apply_upstream_owned(owner,{e}).size(),1u);
        EXPECT_EQ(name(*owner,e.global_row_id),"remote");EXPECT_EQ(state(*owner,p),before);}
}
TEST(CanonicalUpstreamFile, CloseFromObserverRetainsCommittedReceiptAndRefusesNewAdmission) {
    TempDB file{"canonical_upstream_close"};auto p=profile();auto e=entry(101,1);
    {auto owner=std::make_shared<lattice_db>(config(file.str()));ASSERT_NO_FATAL_FAILURE(stop_notifier(*owner));
        auto adapter=canonical_writer_adapter::attach_upstream_for_qualification(owner,p,limits());
        const auto token=owner->add_table_observer("UpstreamNode",[&](const auto&){owner->close();});
        EXPECT_EQ(adapter->apply_upstream_owned(owner,{e}),std::vector<std::string>{e.global_id});
        EXPECT_TRUE(owner->is_closed());EXPECT_THROW(adapter->apply_upstream_owned(owner,{entry(102,2)}),db_error);
        owner->remove_table_observer("UpstreamNode",token);}
    {auto owner=std::make_shared<lattice_db>(config(file.str()));ASSERT_NO_FATAL_FAILURE(stop_notifier(*owner));
        auto adapter=canonical_writer_adapter::attach_upstream_for_qualification(owner,p,limits());
        EXPECT_TRUE(receipt(*owner,p,e.global_id));EXPECT_FALSE(receipt(*owner,p,uuid(102)));EXPECT_EQ(name(*owner,e.global_row_id),"remote");}
}
TEST_F(CanonicalUpstream, ReceiptCapacityRefusalRollsBackTouchModelAuditAndObserverTail) {
    p.limits.receipts=1;attach();auto first=entry(101,1),overflow=entry(102,2);std::vector<std::string> observed;
    const auto token=owner->add_table_observer("UpstreamNode",[&](const auto& rows){for(const auto& row:rows)observed.push_back(std::get<3>(row));});
    EXPECT_EQ(apply({first,overflow}),std::vector<std::string>{first.global_id});owner->remove_table_observer("UpstreamNode",token);
    EXPECT_EQ(observed,std::vector<std::string>{first.global_row_id});EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM UpstreamNode"),1);
    EXPECT_EQ(scalar(owner->db(),"SELECT COUNT(*) FROM AuditLog WHERE globalId=?",{overflow.global_id}),0);
    EXPECT_FALSE(receipt(*owner,p,overflow.global_id));const auto before=state(*owner,p);EXPECT_EQ(before.head,2);EXPECT_EQ(before.markers,1);
    EXPECT_EQ(apply({first}),std::vector<std::string>{first.global_id});EXPECT_EQ(state(*owner,p),before);
}
TEST_F(CanonicalUpstream, UuidComparisonNormalizesWithoutRewritingStoredRowOrAuditSpelling) {
    attach();auto e=entry(101,1);e.global_id="AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA";e.global_row_id="BBBBBBBB-BBBB-4BBB-8BBB-BBBBBBBBBBBB";
    ASSERT_EQ(apply({e}),std::vector<std::string>{e.global_id});const auto before=state(*owner,p);
    auto retry=e;retry.global_id=canonical_writer_adapter::uuid_key(e.global_id);retry.global_row_id=canonical_writer_adapter::uuid_key(e.global_row_id);
    retry.changed_fields["name"]=any_property("replacement");EXPECT_EQ(apply({retry}),std::vector<std::string>{retry.global_id});
    EXPECT_EQ(state(*owner,p),before);EXPECT_EQ(name(*owner,e.global_row_id),"remote");
    EXPECT_EQ(std::get<std::string>(owner->db().query("SELECT globalId FROM UpstreamNode").at(0).at("globalId")),e.global_row_id);
    EXPECT_EQ(std::get<std::string>(owner->db().query("SELECT globalId FROM AuditLog WHERE globalId=?",{e.global_id}).at(0).at("globalId")),e.global_id);
    const auto retained=receipt(*owner,p,e.global_id);ASSERT_TRUE(retained);
    EXPECT_EQ(retained->original.target,(canonical_identity{"UpstreamNode",retry.global_row_id}));
}
TEST_F(CanonicalUpstream, PreparedRuntimeGuardCannotBeReusedAfterEntryUnwind) {
    attach();auto e=entry(101,1);sqlite3_stmt* statement=nullptr;
    const char* sql="SELECT lattice_canonical_entry_v1(X'30303030303030302d303030302d343030302d383030302d303030303030303030313031',X'557073747265616d4e6f6465',X'30303030303030302d303030302d343030302d383030302d303030303030303030303031')";
    ASSERT_EQ(sqlite3_prepare_v2(canonical_writer_custody_test_access::fault_handle(owner->db()),sql,-1,&statement,nullptr),SQLITE_OK);
    struct Finalize {sqlite3_stmt* statement;~Finalize(){sqlite3_finalize(statement);}} cleanup{statement};
    EXPECT_EQ(apply({e}),std::vector<std::string>{e.global_id});
    EXPECT_EQ(sqlite3_step(statement),SQLITE_ERROR) << "prepare-time function visibility is not an entry capability";
    sqlite3_finalize(cleanup.statement);cleanup.statement=nullptr;
    EXPECT_EQ(state(*owner,p).receipts,1);
}
