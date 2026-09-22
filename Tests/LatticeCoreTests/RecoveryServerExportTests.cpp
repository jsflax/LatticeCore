#include "TestHelpers.hpp"
#include <lattice.hpp>
#include "../../Sources/LatticeCore/src/recovery_server_export.hpp"
#include <chrono>
#include <functional>

#ifndef __EMSCRIPTEN__
struct ServerExportRow { std::string value; };
LATTICE_SCHEMA(ServerExportRow,value);
namespace {
using namespace lattice;
using namespace lattice::detail;
void server_committed(const recovery_install_result& result){
    if(result.primary_error)std::rethrow_exception(result.primary_error);
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
    if(result.notification_error)std::rethrow_exception(result.notification_error);
    if(result.state!=recovery_install_state::committed)throw std::runtime_error("server fixture did not commit");
}
struct sink_receipt {
    int calls=0,destroyed=0;
    std::thread::id destroyed_on;
    std::vector<std::string> frames;
    std::vector<uint64_t> serials;
    std::function<int32_t()> action;
    std::function<void()> on_destroy;
};
struct sink_context { std::shared_ptr<sink_receipt> receipt; };
int32_t server_enqueue(void* pointer,const uint8_t* bytes,size_t size,uint64_t serial){
    const auto receipt=static_cast<sink_context*>(pointer)->receipt;
    ++receipt->calls;receipt->frames.emplace_back(reinterpret_cast<const char*>(bytes),size);receipt->serials.push_back(serial);
    return receipt->action?receipt->action():1;
}
void server_destroy(void* pointer){
    std::unique_ptr<sink_context> context(static_cast<sink_context*>(pointer));
    ++context->receipt->destroyed;context->receipt->destroyed_on=std::this_thread::get_id();
    if(context->receipt->on_destroy)context->receipt->on_destroy();
}
// Watchdogs are failure termination only, never a scheduling oracle.
class server_gate {
    std::mutex mutex_;std::condition_variable cv_;bool open_=false;
public:
    void open(){std::lock_guard<std::mutex> lock(mutex_);open_=true;cv_.notify_all();}
    void wait(){std::unique_lock<std::mutex> lock(mutex_);if(!cv_.wait_for(lock,std::chrono::seconds(10),[&]{return open_;}))std::abort();}
};
thread_local std::function<void()> server_claim_action;
struct server_claim_hook {
    void(**slot)();void(*old)();
    std::function<void()> previous=std::move(server_claim_action);
    explicit server_claim_hook(std::function<void()> fn,bool before=false):slot(before?&recovery_export_test_hooks::before_claim_commit:&recovery_export_test_hooks::after_claim_commit),old(*slot){server_claim_action=std::move(fn);*slot=[] {server_claim_action();};}
    ~server_claim_hook(){*slot=old;server_claim_action=std::move(previous);}
};
struct server_commit_fault {
    recovery_local_producer_test_hooks::authorizer_fault fault;
    const recovery_local_producer_test_hooks::authorizer_fault* previous;
    int hits=0;static thread_local server_commit_fault* current;server_commit_fault* old;
    static int restrict_action(int action,const char* one,const char*,const char*)noexcept{
        if(action==SQLITE_TRANSACTION&&one&&!std::strcmp(one,"COMMIT")){++current->hits;return SQLITE_DENY;}return SQLITE_OK;
    }
    explicit server_commit_fault(const lattice_db* owner):fault{owner,&restrict_action},previous(recovery_local_producer_test_hooks::fault),old(current){
        current=this;recovery_local_producer_test_hooks::fault=&fault;
    }
    ~server_commit_fault(){recovery_local_producer_test_hooks::fault=previous;current=old;}
};
thread_local server_commit_fault* server_commit_fault::current=nullptr;
class RecoveryServerExport:public ::testing::TestWithParam<bool> {
protected:
    TempDB file{"server_export"};
    std::shared_ptr<lattice_db> owner;
    recovery_obligation_producer_discovery_limits caps{{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
    recovery_obligation_address address;
    std::shared_ptr<sink_receipt> receipt=std::make_shared<sink_receipt>();
    recovery_server_export_endpoint endpoint;
    static receive_install_binding binding(){return {"server","authority","source","epoch","scope","schema"};}
    template<class F>void transaction(F&& fn){server_committed(recovery_writer_access::install(owner,std::forward<F>(fn)));}
    recovery_obligation_store journal(){return {owner,caps.obligations,caps.installations};}
    void enroll(){
        transaction([&](database&){receive_install_store receiver(owner,caps.installations);receiver.initialize();receiver.bind(binding());
            auto storage=journal();storage.initialize();address=storage.bind({binding(),"grant","receipts"}).address;});
        server_committed(recovery_local_producer_adapter::enroll_for_qualification(owner,{address,{"ServerExportRow"},{'s'}},caps));
    }
    void SetUp()override{
        configuration cfg(GetParam()?file.str():":memory:");cfg.audit_retention_seconds=0;cfg.busy_timeout_ms=100;cfg.sched=std::make_shared<immediate_scheduler>();
        owner=std::make_shared<lattice_db>(cfg);
        if(GetParam()){auto* notifier=instance_registry::instance().get_or_create_notifier(file.str());if(notifier)notifier->stop_listening();}
        enroll();endpoint=recovery_server_export_endpoint::create_for_qualification(owner,new sink_context{receipt},server_enqueue,server_destroy,{});
    }
    void TearDown()override{
        receipt->action={};receipt->on_destroy={};endpoint.close_on_io();endpoint={};if(owner){owner->close();owner.reset();}
    }
    recovery_obligation_record add(const std::string& value="value"){
        owner->add(ServerExportRow{value});const auto rows=owner->db().query("SELECT id,globalId,tableName,globalRowId FROM AuditLog ORDER BY id DESC LIMIT 1");
        if(rows.size()!=1)throw std::runtime_error("missing server original");const auto& row=rows[0];
        return {std::get<int64_t>(row.at("id")),std::get<std::string>(row.at("globalId")),std::get<std::string>(row.at("tableName")),
            std::get<std::string>(row.at("globalRowId")),recovery_obligation_origin::local_candidate};
    }
    recovery_obligation_entry entry(const recovery_obligation_record& row){
        std::optional<recovery_obligation_entry> result;transaction([&](database&){result=journal().find(address,row.original_id);});
        if(!result)throw std::runtime_error("missing server obligation");return *result;
    }
    void freeze(){recovery_obligation_address next;transaction([&](database&){next=journal().freeze(address,1).address;});address=next;}
};
}

TEST_P(RecoveryServerExport, CommittedClaimAndExactPayloadPrecedeImmutableCallbackOffWriterLocks){
    const std::string value("one\0two",7);const auto row=add(value);
    auto page=endpoint.prepare_history(0,1);ASSERT_EQ(page.status(),server_export_status::ready);
    EXPECT_EQ(page.count(),1);EXPECT_EQ(page.last_audit_id(),row.audit_id);const auto claim=entry(row).first_export_claim;ASSERT_TRUE(claim);
    bool durable=false;receipt->action=[&]{
        durable=entry(row).first_export_claim==claim&&!owner->db().is_in_transaction();
        // A distinct writer must complete while callback is on stack. A held
        // writer/SQLite gate fails the bounded watchdog instead of sleeping.
        server_gate done;std::exception_ptr failure;
        std::thread writer([&]{try{owner->add(ServerExportRow{"later"});}catch(...){failure=std::current_exception();}done.open();});
        done.wait();writer.join();if(failure)std::rethrow_exception(failure);return 1;
    };
    const auto completion=page.completion();EXPECT_EQ(page.consume(),server_export_status::enqueued);EXPECT_TRUE(durable);
    ASSERT_EQ(receipt->frames.size(),1u);const auto wire=server_sent_event::from_json(receipt->frames[0]);ASSERT_TRUE(wire);
    ASSERT_EQ(wire->audit_logs.size(),1u);EXPECT_EQ(wire->audit_logs[0].global_id,row.original_id);EXPECT_EQ(wire->audit_logs[0].id,row.audit_id);
    EXPECT_EQ(std::get<std::string>(wire->audit_logs[0].changed_fields.at("value").value),value);
    EXPECT_TRUE(completion.record_result(true));EXPECT_TRUE(completion.permits_advance());
}
TEST_P(RecoveryServerExport, OnePageCreditWaitsForNativeTailAndExactCompletion){
    add();auto page=endpoint.prepare_history(0,1);ASSERT_EQ(page.status(),server_export_status::ready);
    const auto completion=page.completion();EXPECT_FALSE(completion.record_result(true));
    EXPECT_EQ(endpoint.prepare_history(0,1).status(),server_export_status::busy);
    receipt->action=[&]{EXPECT_TRUE(completion.record_result(true));EXPECT_FALSE(completion.permits_advance());
        EXPECT_EQ(endpoint.prepare_history(0,1).status(),server_export_status::busy);return 1;};
    EXPECT_EQ(page.consume(),server_export_status::enqueued);EXPECT_TRUE(completion.permits_advance());EXPECT_FALSE(completion.record_result(false));
    auto next=endpoint.prepare_history(0,1);ASSERT_EQ(next.status(),server_export_status::ready);EXPECT_GT(next.serial(),page.serial());
    EXPECT_FALSE(completion.permits_advance());EXPECT_FALSE(completion.record_result(true));
    EXPECT_EQ(endpoint.prepare_history(0,1).status(),server_export_status::busy);next.close_on_io();
    EXPECT_EQ(endpoint.prepare_history(0,1).status(),server_export_status::ready);
}
TEST_P(RecoveryServerExport, DeferredCompletionRetainsCreditAfterFrameRelease){
    add();auto page=endpoint.prepare_history(0,1);const auto completion=page.completion();
    ASSERT_EQ(page.consume(),server_export_status::enqueued);page.close_on_io();
    EXPECT_EQ(endpoint.prepare_history(0,1).status(),server_export_status::busy);EXPECT_FALSE(completion.permits_advance());
    EXPECT_TRUE(completion.record_result(false));EXPECT_FALSE(completion.permits_advance());
    auto next=endpoint.prepare_history(0,1);EXPECT_EQ(next.status(),server_export_status::ready);next.close_on_io();
}
TEST_P(RecoveryServerExport, ConcurrentPageCopiesInvokeSinkOnlyOnce){
    add();auto page=endpoint.prepare_history(0,1);const auto copy=page;const auto completion=page.completion();
    server_gate entered,release;receipt->action=[&]{entered.open();release.wait();return 1;};
    server_export_status first=server_export_status::invalid;std::thread worker([&]{first=page.consume();});entered.wait();
    EXPECT_EQ(copy.consume(),server_export_status::consumed);copy.close_on_io();
    EXPECT_EQ(endpoint.prepare_history(0,1).status(),server_export_status::busy);
    release.open();worker.join();EXPECT_EQ(first,server_export_status::enqueued);EXPECT_EQ(receipt->calls,1);
    EXPECT_TRUE(completion.record_result(true));EXPECT_FALSE(completion.record_result(true));
}
TEST_P(RecoveryServerExport, EqualSerialEndpointsDoNotExchangeSinkOrCompletionCustody){
    add();const auto other_receipt=std::make_shared<sink_receipt>();
    auto other=recovery_server_export_endpoint::create_for_qualification(owner,new sink_context{other_receipt},server_enqueue,server_destroy,{});
    auto one=endpoint.prepare_history(0,1),two=other.prepare_history(0,1);ASSERT_EQ(one.status(),server_export_status::ready);ASSERT_EQ(two.status(),server_export_status::ready);
    ASSERT_EQ(one.serial(),two.serial());const auto a=one.completion(),b=two.completion();
    EXPECT_FALSE(b.record_result(true));EXPECT_EQ(one.consume(),server_export_status::enqueued);EXPECT_TRUE(a.record_result(true));
    EXPECT_EQ(receipt->calls,1);EXPECT_EQ(other_receipt->calls,0);EXPECT_EQ(other.prepare_history(0,1).status(),server_export_status::busy);
    EXPECT_EQ(two.consume(),server_export_status::enqueued);EXPECT_EQ(other_receipt->calls,1);EXPECT_FALSE(a.record_result(true));
    EXPECT_EQ(other.prepare_history(0,1).status(),server_export_status::busy);EXPECT_TRUE(b.record_result(true));
    other.close_on_io();EXPECT_EQ(other_receipt->destroyed,1);
}
TEST_P(RecoveryServerExport, StopBeforePreparationDoesNotCreateClaim){
    const auto row=add();const auto stop=endpoint.stop_token();stop.request_stop();
    auto page=endpoint.prepare_history(0,1);EXPECT_EQ(page.status(),server_export_status::stopped);EXPECT_FALSE(page.last_audit_id());
    EXPECT_FALSE(entry(row).first_export_claim);EXPECT_EQ(receipt->calls,0);EXPECT_FALSE(stop.resources_released());
    endpoint.close_on_io();EXPECT_EQ(receipt->destroyed,1);EXPECT_TRUE(stop.resources_released());
}
TEST_P(RecoveryServerExport, StopAfterClaimCommitDiscardsPageButKeepsPositiveClaim){
    const auto row=add();const auto stop=endpoint.stop_token();bool claim_seen=false;
    server_claim_hook hook([&]{claim_seen=entry(row).first_export_claim.has_value();stop.request_stop();});
    auto page=endpoint.prepare_history(0,1);EXPECT_TRUE(claim_seen);EXPECT_EQ(page.status(),server_export_status::stopped);
    EXPECT_FALSE(page.last_audit_id());EXPECT_TRUE(entry(row).first_export_claim);EXPECT_EQ(receipt->calls,0);
    endpoint.close_on_io();EXPECT_TRUE(stop.resources_released());
}
TEST_P(RecoveryServerExport, StopDuringOwnedClaimPreparationFinishesAdmittedCommitWithoutEnqueue){
    const auto row=add();const auto stop=endpoint.stop_token();bool active_claim=false;
    {server_claim_hook hook([&]{
        const auto rows=owner->db().query("SELECT first_export FROM _lattice_obligation_entry WHERE audit_id=?",{row.audit_id});
        active_claim=owner->db().is_in_transaction()&&rows.size()==1&&std::holds_alternative<int64_t>(rows[0].at("first_export"));
        stop.request_stop();endpoint.close_on_io();EXPECT_EQ(receipt->destroyed,0);
    },true);
    const auto page=endpoint.prepare_history(0,1);EXPECT_EQ(page.status(),server_export_status::stopped);EXPECT_FALSE(page.last_audit_id());}
    EXPECT_TRUE(active_claim);EXPECT_TRUE(entry(row).first_export_claim);EXPECT_EQ(receipt->calls,0);EXPECT_EQ(receipt->destroyed,1);EXPECT_TRUE(stop.resources_released());
}
TEST_P(RecoveryServerExport, PreparedStopAndClosedOwnerNeverInvokeSink){
    const auto row=add();auto page=endpoint.prepare_history(0,1);ASSERT_EQ(page.status(),server_export_status::ready);const auto completion=page.completion();
    endpoint.stop_token().request_stop();EXPECT_EQ(page.consume(),server_export_status::stopped);EXPECT_FALSE(completion.record_result(true));
    EXPECT_TRUE(entry(row).first_export_claim);EXPECT_EQ(receipt->calls,0);
    const auto another=std::make_shared<sink_receipt>();auto other=recovery_server_export_endpoint::create_for_qualification(owner,new sink_context{another},server_enqueue,server_destroy,{});
    auto ready=other.prepare_history(0,1);ASSERT_EQ(ready.status(),server_export_status::ready);owner->close();
    EXPECT_EQ(ready.consume(),server_export_status::stopped);EXPECT_EQ(another->calls,0);other.close_on_io();EXPECT_EQ(another->destroyed,1);
}
TEST_P(RecoveryServerExport, FinalDurableFreezeCheckRefusesWithoutClaimLoss){
    const auto row=add();auto page=endpoint.prepare_history(0,1);ASSERT_EQ(page.status(),server_export_status::ready);const auto claim=entry(row).first_export_claim;ASSERT_TRUE(claim);
    freeze();EXPECT_EQ(page.consume(),server_export_status::failed);EXPECT_TRUE(page.failure());EXPECT_EQ(receipt->calls,0);
    EXPECT_EQ(entry(row).first_export_claim,claim);EXPECT_FALSE(page.completion().record_result(true));
    EXPECT_EQ(endpoint.prepare_history(0,1).status(),server_export_status::failed);
}
TEST_P(RecoveryServerExport, ReentrantCloseRetainsContextThroughCallbackAndReleasesOffLeafLocks){
    add();auto page=endpoint.prepare_history(0,1);const auto stop=endpoint.stop_token();const auto completion=page.completion();bool reentered=false;
    receipt->on_destroy=[&]{reentered=stop.stopped();stop.request_stop();};
    receipt->action=[&]{stop.request_stop();endpoint.close_on_io();page.close_on_io();EXPECT_EQ(receipt->destroyed,0);return 1;};
    EXPECT_EQ(page.consume(),server_export_status::enqueued);EXPECT_EQ(receipt->destroyed,1);EXPECT_TRUE(reentered);
    EXPECT_FALSE(stop.resources_released());EXPECT_TRUE(completion.record_result(true));EXPECT_FALSE(completion.permits_advance());EXPECT_TRUE(stop.resources_released());
}
TEST_P(RecoveryServerExport, RefusedEnqueueReleasesButThrowRequiresExplicitSettlement){
    const auto row=add();receipt->action=[] {return 0;};auto rejected=endpoint.prepare_history(0,1);
    EXPECT_EQ(rejected.consume(),server_export_status::failed);EXPECT_TRUE(rejected.completion().completed());EXPECT_FALSE(rejected.completion().permits_advance());
    const auto claim=entry(row).first_export_claim;ASSERT_TRUE(claim);
    receipt->action=[]()->int32_t{throw std::runtime_error("possibly enqueued");};auto throwing=endpoint.prepare_history(0,1);ASSERT_EQ(throwing.status(),server_export_status::ready);
    EXPECT_EQ(throwing.consume(),server_export_status::failed);EXPECT_TRUE(throwing.failure());EXPECT_FALSE(throwing.completion().completed());
    EXPECT_EQ(endpoint.prepare_history(0,1).status(),server_export_status::busy);EXPECT_TRUE(throwing.completion().record_result(false));
    receipt->action=[] {return 1;};auto retry=endpoint.prepare_history(0,1);ASSERT_EQ(retry.status(),server_export_status::ready);
    EXPECT_EQ(entry(row).first_export_claim,claim);EXPECT_EQ(retry.consume(),server_export_status::enqueued);EXPECT_TRUE(retry.completion().record_result(true));
    ASSERT_EQ(receipt->frames.size(),3u);EXPECT_EQ(receipt->frames[0],receipt->frames[1]);EXPECT_EQ(receipt->frames[1],receipt->frames[2]);
}
TEST_P(RecoveryServerExport, FailedPageCopiesBoundedTextWithoutRetainingForeignExceptionCustody){
    struct foreign_error:std::runtime_error {
        std::shared_ptr<int> custody;
        explicit foreign_error(std::shared_ptr<int> value):std::runtime_error(std::string(4096,'x')),custody(std::move(value)){}
    };
    add();std::weak_ptr<int> weak;
    receipt->action=[&]()->int32_t{auto custody=std::make_shared<int>(42);weak=custody;throw foreign_error(std::move(custody));};
    auto page=endpoint.prepare_history(0,1);EXPECT_EQ(page.consume(),server_export_status::failed);EXPECT_TRUE(weak.expired());
    ASSERT_TRUE(page.failure());try{std::rethrow_exception(page.failure());}
    catch(const std::exception& error){EXPECT_LT(std::strlen(error.what()),600u);EXPECT_NE(std::string(error.what()).find("server export consumption"),std::string::npos);}
    EXPECT_TRUE(page.completion().record_result(false));EXPECT_EQ(endpoint.prepare_history(0,1).status(),server_export_status::ready);
}
TEST_P(RecoveryServerExport, InvalidEnqueueResultNeverAdvancesEvenWithPositiveCompletion){
    add();receipt->action=[] {return 17;};auto page=endpoint.prepare_history(0,1);EXPECT_EQ(page.consume(),server_export_status::failed);
    EXPECT_EQ(endpoint.prepare_history(0,1).status(),server_export_status::busy);EXPECT_TRUE(page.completion().record_result(true));EXPECT_FALSE(page.completion().permits_advance());
    EXPECT_EQ(endpoint.prepare_history(0,1).status(),server_export_status::ready);
}
TEST_P(RecoveryServerExport, DroppedPreparedCopiesReleaseCreditOnlyOnActualFrameRetirement){
    add();auto first=endpoint.prepare_history(0,1);auto copy=first;first={};
    EXPECT_EQ(endpoint.prepare_history(0,1).status(),server_export_status::busy);copy.close_on_io();copy.close_on_io();
    EXPECT_EQ(endpoint.prepare_history(0,1).status(),server_export_status::ready);EXPECT_EQ(receipt->calls,0);
}
TEST_P(RecoveryServerExport, ClaimCommitFailureIsNotEmptyAndRetryRetainsCorrectCredit){
    const auto row=add();int hits=0;
    {server_commit_fault fault(owner.get());const auto failed=endpoint.prepare_history(0,1);EXPECT_EQ(failed.status(),server_export_status::failed);
     EXPECT_TRUE(failed.failure());EXPECT_FALSE(failed.last_audit_id());hits=fault.hits;}
    EXPECT_GT(hits,0);EXPECT_FALSE(entry(row).first_export_claim);EXPECT_EQ(receipt->calls,0);
    auto retry=endpoint.prepare_history(0,1);EXPECT_EQ(retry.status(),server_export_status::ready);EXPECT_TRUE(entry(row).first_export_claim);retry.close_on_io();
}
TEST_P(RecoveryServerExport, EmptyProtectedAndUnprotectedResultsAreDistinctFromFailure){
    const auto empty=endpoint.prepare_history(0,1);EXPECT_EQ(empty.status(),server_export_status::empty);EXPECT_EQ(empty.count(),0);EXPECT_FALSE(empty.last_audit_id());
    auto cfg=configuration(":memory:");cfg.audit_retention_seconds=0;cfg.sched=std::make_shared<immediate_scheduler>();auto unprotected=std::make_shared<lattice_db>(cfg);
    const auto other_receipt=std::make_shared<sink_receipt>();auto other=recovery_server_export_endpoint::create_for_qualification(unprotected,new sink_context{other_receipt},server_enqueue,server_destroy,{});
    EXPECT_EQ(other.prepare_history(0,1).status(),server_export_status::unprotected);other.close_on_io();EXPECT_EQ(other_receipt->destroyed,1);
    const auto failed=endpoint.prepare_history(-1,1);EXPECT_EQ(failed.status(),server_export_status::failed);EXPECT_TRUE(failed.failure());
}
TEST_P(RecoveryServerExport, FailedFactoryConsumesContextExactlyOnce){
    for(int variant=0;variant<4;++variant){const auto rejected=std::make_shared<sink_receipt>();recovery_export_limits limits;
        if(variant==2)limits.entries=0;if(variant==3)owner->close();
        EXPECT_ANY_THROW(recovery_server_export_endpoint::create_for_qualification(variant==0?std::shared_ptr<lattice_db>{}:owner,
            new sink_context{rejected},variant==1?nullptr:server_enqueue,server_destroy,limits));
        EXPECT_EQ(rejected->destroyed,1);EXPECT_EQ(rejected->calls,0);EXPECT_EQ(rejected->destroyed_on,std::this_thread::get_id());
    }
}
TEST_P(RecoveryServerExport, MissingDestroyRejectsBeforeOwnershipTransfer){
    const auto retained=std::make_shared<sink_receipt>();
    std::unique_ptr<sink_context,void(*)(void*)> context(new sink_context{retained},server_destroy);
    EXPECT_ANY_THROW(recovery_server_export_endpoint::create_for_qualification(owner,context.get(),server_enqueue,nullptr,{}));
    EXPECT_EQ(retained->destroyed,0);EXPECT_EQ(retained->calls,0);
    context.reset();EXPECT_EQ(retained->destroyed,1);
}
INSTANTIATE_TEST_SUITE_P(MemoryAndFile,RecoveryServerExport,::testing::Values(false,true),
    [](const ::testing::TestParamInfo<bool>& info){return info.param?"File":"Memory";});

namespace {
std::unique_ptr<swift_lattice_ref> server_ref(const std::string& path){
    swift_configuration cfg(path,std::make_shared<immediate_scheduler>());cfg.audit_retention_seconds=0;
#if LATTICE_HAS_FRT
    auto ref=std::unique_ptr<swift_lattice_ref>(swift_lattice_ref::create(cfg,SchemaVector{}));
#else
    auto ref=std::make_unique<swift_lattice_ref>(swift_lattice_ref::create(cfg,SchemaVector{}));
#endif
    if(auto* notifier=instance_registry::instance().get_or_create_notifier(path))notifier->stop_listening();return ref;
}
server_export_limits bridge_caps(){return {2,65536,262144,524288};}
void bridge_enroll(const std::shared_ptr<lattice_db>& owner){
    const recovery_obligation_producer_discovery_limits caps{{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
    recovery_obligation_address address;const receive_install_binding binding{"bridge","authority","source","epoch","scope","schema"};
    server_committed(recovery_writer_access::install(owner,[&](database&){receive_install_store receiver(owner,caps.installations);receiver.initialize();receiver.bind(binding);
        recovery_obligation_store journal(owner,caps.obligations,caps.installations);journal.initialize();address=journal.bind({binding,"grant","receipts"}).address;}));
    server_committed(recovery_local_producer_adapter::enroll_for_qualification(owner,{address,{"ServerExportRow"},{'b'}},caps));
}
}
TEST(RecoveryServerExportBridge, PortableCopiesRetainActualRefOwnerAndTokensDoNotRetainNativeCustody){
    TempDB file("server_bridge_owner");auto ref=server_ref(file.str());std::shared_ptr<lattice_db> owner=swift_lattice_ref::shared_for_lattice(ref->get());ASSERT_TRUE(owner);bridge_enroll(owner);
    owner->add(ServerExportRow{std::string("bridge\0value",12)});const std::weak_ptr<lattice_db> weak=owner;
    const auto receipt=std::make_shared<sink_receipt>();auto endpoint=ref->make_server_export_endpoint_for_qualification(new sink_context{receipt},server_enqueue,server_destroy,bridge_caps());
    ASSERT_TRUE(endpoint.valid());auto stop=endpoint.stop_token();ASSERT_TRUE(stop.valid());auto page=endpoint.prepare_history(0,2);ASSERT_EQ(page.status_code(),1);ASSERT_EQ(page.count(),1);ASSERT_TRUE(page.has_last_audit_id());
    auto copy=page;auto completion=page.completion_token();ASSERT_TRUE(completion.valid());ref.reset();owner.reset();EXPECT_FALSE(weak.expired());
    std::thread::id io_id;int32_t consumed=0,duplicate=0;
    std::thread io([&]{io_id=std::this_thread::get_id();consumed=page.consume();duplicate=copy.consume();page.close_on_io();copy.close_on_io();endpoint.close_on_io();
        page={};copy={};endpoint={};});io.join();
    EXPECT_EQ(consumed,7);EXPECT_EQ(duplicate,8);EXPECT_EQ(receipt->calls,1);EXPECT_EQ(receipt->destroyed,1);EXPECT_EQ(receipt->destroyed_on,io_id);
    EXPECT_TRUE(weak.expired());EXPECT_TRUE(stop.stopped());EXPECT_FALSE(stop.resources_released());
    // Completion/stop still live on this different thread, yet all native
    // owner/context custody is already gone. Only metadata credit remains.
    EXPECT_TRUE(completion.record_result(true));EXPECT_FALSE(completion.permits_advance());EXPECT_TRUE(stop.resources_released());
    EXPECT_FALSE(completion.record_result(true));
}
TEST(RecoveryServerExportBridge, InvalidAndClosedFactoriesConsumeContextAndReportFailure){
    TempDB file("server_bridge_refused");auto ref=server_ref(file.str());
    const auto invalid=std::make_shared<sink_receipt>();auto absent=ref->make_server_export_endpoint_for_qualification(new sink_context{invalid},server_enqueue,server_destroy,{});
    EXPECT_FALSE(absent.valid());EXPECT_EQ(invalid->destroyed,1);EXPECT_FALSE(last_bridge_error().empty());
    ref->close();const auto closed=std::make_shared<sink_receipt>();auto refused=ref->make_server_export_endpoint_for_qualification(new sink_context{closed},server_enqueue,server_destroy,bridge_caps());
    EXPECT_FALSE(refused.valid());EXPECT_EQ(closed->destroyed,1);EXPECT_FALSE(last_bridge_error().empty());
    server_export_page empty;EXPECT_EQ(empty.status_code(),0);EXPECT_EQ(empty.consume(),0);EXPECT_FALSE(empty.has_last_audit_id());EXPECT_FALSE(empty.completion_token().valid());EXPECT_FALSE(empty.completion_token().record_result(true));
}
TEST(RecoveryServerExportBridge, MissingDestroyLeavesCallerCustodyWhileMissingEnqueueConsumesIt){
    TempDB file("server_bridge_callback_precondition");auto ref=server_ref(file.str());
    const auto retained=std::make_shared<sink_receipt>();
    std::unique_ptr<sink_context,void(*)(void*)> context(new sink_context{retained},server_destroy);
    const auto invalid=ref->make_server_export_endpoint_for_qualification(context.get(),server_enqueue,nullptr,bridge_caps());
    EXPECT_FALSE(invalid.valid());EXPECT_EQ(retained->destroyed,0);EXPECT_NE(last_bridge_error().find("context not transferred"),std::string::npos);
    context.reset();EXPECT_EQ(retained->destroyed,1);
    const auto consumed=std::make_shared<sink_receipt>();
    const auto missing=ref->make_server_export_endpoint_for_qualification(new sink_context{consumed},nullptr,server_destroy,bridge_caps());
    EXPECT_FALSE(missing.valid());EXPECT_EQ(consumed->destroyed,1);EXPECT_EQ(consumed->calls,0);
}

#endif
