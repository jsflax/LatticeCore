#include "TestHelpers.hpp"
#include <lattice.hpp>
#include "../../Sources/LatticeCore/src/recovery_server_export.hpp"
#include "../../Sources/LatticeCore/src/canonical_writer_adapter.hpp"
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <thread>
#if defined(__APPLE__) || defined(__linux__)
#include <fcntl.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
extern char** environ;
#endif

#ifndef __EMSCRIPTEN__
namespace {
using namespace lattice;
using namespace lattice::detail;
const recovery_obligation_producer_discovery_limits owner_caps{{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
const receive_install_binding owner_binding{"owner-schema","authority","source","epoch","scope","schema"};
const std::string row_id="11111111-1111-4111-8111-111111111111";
void owner_committed(const recovery_install_result& result) {
    if(result.primary_error)std::rethrow_exception(result.primary_error);
    if(result.cleanup_error)std::rethrow_exception(result.cleanup_error);
    if(result.postcommit_error)std::rethrow_exception(result.postcommit_error);
    if(result.notification_error)std::rethrow_exception(result.notification_error);
    if(result.state!=recovery_install_state::committed)throw std::runtime_error("owner schema transaction did not commit");
}
swift_schema_entry owner_schema(bool no_history=true,bool reverse=false) {
    swift_schema_entry result;result.table_name="RecoverySwiftOwnerRow";
    property_descriptor first{};first.name="text";first.type=column_type::text;
    property_descriptor second{};second.name="note";second.type=column_type::text;second.no_history=no_history;
    if(reverse){result.properties[second.name]=second;result.properties[first.name]=first;}
    else {result.properties[first.name]=first;result.properties[second.name]=second;}
    return result;
}
struct owner_fixture {
    std::unique_ptr<swift_lattice_ref> ref;
    std::shared_ptr<swift_lattice> owner;
    owner_fixture(const std::string& path,const SchemaVector& schemas) {
        swift_configuration config(path,std::make_shared<immediate_scheduler>());
        config.audit_retention_seconds=0;config.busy_timeout_ms=100;
#if LATTICE_HAS_FRT
        ref.reset(swift_lattice_ref::create(config,schemas));
#else
        ref=std::make_unique<swift_lattice_ref>(swift_lattice_ref::create(config,schemas));
#endif
        owner=swift_lattice_ref::shared_for_lattice(ref->get());
        if(!owner||owner->is_closed())throw std::runtime_error("missing actual Swift owner");
        if(!configuration::path_is_memory(path)) {
            if(auto* notifier=instance_registry::instance().get_or_create_notifier(path))notifier->stop_listening();
        }
    }
    ~owner_fixture(){if(owner)owner->close();owner.reset();ref.reset();}
    owner_fixture(const owner_fixture&)=delete;
    recovery_install_result enroll_result(const std::vector<std::string>& models={"RecoverySwiftOwnerRow"}) {
        recovery_obligation_address address;
        owner_committed(recovery_writer_access::install(owner,[&](database&){
            receive_install_store receiver(owner,owner_caps.installations);receiver.initialize();receiver.bind(owner_binding);
            recovery_obligation_store journal(owner,owner_caps.obligations,owner_caps.installations);journal.initialize();
            const auto prior=journal.read(owner_binding.channel);
            address=prior?prior->address:journal.bind({owner_binding,"grant","receipts"}).address;
        }));
        return recovery_local_producer_adapter::enroll_for_qualification(owner,{address,models,{'r'}},owner_caps);
    }
    void enroll(const std::vector<std::string>& models={"RecoverySwiftOwnerRow"}){owner_committed(enroll_result(models));}
    void insert(const std::string& id=row_id) {
        owner->begin_transaction();
        try {owner->db().execute("INSERT INTO RecoverySwiftOwnerRow(globalId,text,note) VALUES(?,?,?)",{id,std::string("before\0after",12),std::string("initial")});owner->commit();}
        catch(...){if(owner->db().is_in_transaction())owner->rollback();throw;}
    }
    int64_t count(const std::string& table) {
        const auto rows=owner->db().query("SELECT COUNT(*) AS n FROM "+table);
        return std::get<int64_t>(rows.at(0).at("n"));
    }
};
struct owner_sink {
    std::vector<std::string> frames;
    static int32_t enqueue(void* context,const uint8_t* bytes,size_t size,uint64_t) {
        static_cast<owner_sink*>(context)->frames.emplace_back(reinterpret_cast<const char*>(bytes),size);return 1;
    }
    static void destroy(void*){} // test stack outlives every actual endpoint/page
};
std::vector<database::row_t> durable_programs(database& db) {
    return db.query("SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name");
}
class RecoveryOwnerSchema:public ::testing::TestWithParam<bool> {
protected:
    TempDB file{"recovery_swift_owner"};
    std::string path()const{return GetParam()?file.str():":memory:";}
};
}

TEST_P(RecoveryOwnerSchema, RealSwiftDeclarationProducesOriginalStampClaimAndExactBridgePayload) {
    ASSERT_EQ(schema_registry::instance().get_schema("RecoverySwiftOwnerRow"),nullptr);
    owner_fixture store(path(),SchemaVector{owner_schema()});store.enroll();store.insert();
    EXPECT_EQ(store.count("AuditLog"),1);EXPECT_EQ(store.count("_lattice_obligation_producer_stamp"),1);
    owner_sink sink;
    auto endpoint=store.ref->make_server_export_endpoint_for_qualification(&sink,owner_sink::enqueue,owner_sink::destroy,{8,65536,262144,524288});
    ASSERT_TRUE(endpoint.valid());auto page=endpoint.prepare_history(0,8);ASSERT_EQ(page.status_code(),1);
    auto completion=page.completion_token();EXPECT_EQ(page.consume(),7);EXPECT_TRUE(completion.record_result(true));
    EXPECT_TRUE(completion.permits_advance());ASSERT_EQ(sink.frames.size(),1u);
    const auto event=server_sent_event::from_json(sink.frames[0]);ASSERT_TRUE(event);ASSERT_EQ(event->audit_logs.size(),1u);
    EXPECT_EQ(std::get<std::string>(event->audit_logs[0].changed_fields.at("text").value),std::string("before\0after",12));
    const auto claimed=store.owner->db().query("SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE first_export IS NOT NULL");
    EXPECT_EQ(std::get<int64_t>(claimed[0].at("n")),1);
    page.close_on_io();endpoint.close_on_io();
    EXPECT_EQ(schema_registry::instance().get_schema("RecoverySwiftOwnerRow"),nullptr);
}

TEST_P(RecoveryOwnerSchema, NoHistoryDescriptorSurvivesActualLocalUpdateAndLateBoundExport) {
    owner_fixture store(path(),SchemaVector{owner_schema()});store.enroll();store.insert();
    store.owner->db().execute("UPDATE RecoverySwiftOwnerRow SET note='latest' WHERE globalId=?",{row_id});
    EXPECT_EQ(store.count("_lattice_obligation_producer_stamp"),2);
    auto prepared=recovery_export_adapter::prepare_history_page(store.owner,0,0,8,{});
    ASSERT_TRUE(prepared.protected_store);ASSERT_TRUE(prepared.frame);ASSERT_EQ(prepared.frame->entries().size(),2u);
    EXPECT_EQ(std::get<std::string>(prepared.frame->entries().back().changed_fields.at("note").value),"latest");
}

TEST_P(RecoveryOwnerSchema, IncomingSwiftRelationshipRequiresWholeDeclaredClosure) {
    auto incoming=owner_schema();incoming.table_name="RecoverySwiftIncoming";
    property_descriptor link{};link.name="target";link.kind=property_kind::link;link.type=column_type::text;link.nullable=true;link.target_table="RecoverySwiftOwnerRow";
    incoming.properties[link.name]=link;
    owner_fixture store(path(),SchemaVector{owner_schema(),incoming});
    const auto refused=store.enroll_result();EXPECT_NE(refused.state,recovery_install_state::committed);
    EXPECT_EQ(store.count("AuditLog"),0);
    EXPECT_NO_THROW(store.enroll({"RecoverySwiftOwnerRow","RecoverySwiftIncoming"}));
    EXPECT_NO_THROW(store.insert());EXPECT_EQ(store.count("_lattice_obligation_producer_stamp"),1);
}

TEST_P(RecoveryOwnerSchema, CanonicalAttachmentUsesActualSwiftDeclarationAndGeneratedLayout) {
    owner_fixture store(path(),SchemaVector{owner_schema()});
    canonical_writer_profile profile{{"source","epoch","scope","schema"},{32,16384,2048,262144,32,64,64},{"RecoverySwiftOwnerRow"},false};
    auto attached=canonical_writer_adapter::attach(*store.owner,profile);store.insert();
    EXPECT_EQ(store.count("_lattice_canonical_receipt"),1);
    EXPECT_EQ(store.count("RecoverySwiftOwnerRow"),1);
    attached.reset();
}

TEST_P(RecoveryOwnerSchema, UnsupportedDerivedPropertyIsNotErasedFromOwnerDescriptor) {
    auto schema=owner_schema();schema.properties.at("text").is_full_text=true;
    owner_fixture store(path(),SchemaVector{schema});
    const auto before=durable_programs(store.owner->db());
    canonical_writer_profile profile{{"source","epoch","scope","schema"},{32,16384,2048,262144,32,64,64},{"RecoverySwiftOwnerRow"},false};
    EXPECT_ANY_THROW(canonical_writer_adapter::attach(*store.owner,profile));
    EXPECT_EQ(durable_programs(store.owner->db()),before);
    EXPECT_EQ(store.count("AuditLog"),0);
}

INSTANTIATE_TEST_SUITE_P(MemoryAndFile,RecoveryOwnerSchema,::testing::Values(false,true),
    [](const ::testing::TestParamInfo<bool>& info){return info.param?"File":"Memory";});

TEST(RecoveryOwnerSchemaReopen, FreshOwnerKeepsProgramsPendingOriginalsAndCanWriteAndExport) {
    TempDB file("recovery_owner_reopen");std::vector<database::row_t> programs;
    {owner_fixture first(file.str(),SchemaVector{owner_schema()});first.enroll();first.insert();programs=durable_programs(first.owner->db());}
    owner_fixture reopened(file.str(),SchemaVector{owner_schema(true,true)});
    EXPECT_EQ(durable_programs(reopened.owner->db()),programs);
    EXPECT_EQ(reopened.count("AuditLog"),1);EXPECT_EQ(reopened.count("_lattice_obligation_producer_stamp"),1);
    ASSERT_NE(reopened.owner->get_properties_for_table("RecoverySwiftOwnerRow"),nullptr);
    reopened.insert("22222222-2222-4222-8222-222222222222");
    auto prepared=recovery_export_adapter::prepare_history_page(reopened.owner,0,0,8,{});
    ASSERT_TRUE(prepared.frame);EXPECT_EQ(prepared.frame->entries().size(),2u);
    EXPECT_EQ(reopened.count("_lattice_obligation_producer_stamp"),2);
}

TEST(RecoveryOwnerSchemaReopen, ChangedNoHistoryDeclarationRefusesBeforeRewritingDurablePrograms) {
    TempDB file("recovery_owner_schema_mismatch");std::vector<database::row_t> programs;
    {owner_fixture first(file.str(),SchemaVector{owner_schema()});first.enroll();first.insert();programs=durable_programs(first.owner->db());}
    EXPECT_ANY_THROW(owner_fixture bad(file.str(),SchemaVector{owner_schema(false)}));
    owner_fixture valid(file.str(),SchemaVector{owner_schema()});
    EXPECT_EQ(durable_programs(valid.owner->db()),programs);
    EXPECT_EQ(valid.count("AuditLog"),1);EXPECT_EQ(valid.count("_lattice_obligation_producer_stamp"),1);
}

TEST(RecoveryOwnerSchemaReopen, SameModelNameInTwoOwnersKeepsIndependentDeclarations) {
    owner_fixture first(":memory:",SchemaVector{owner_schema()});
    auto other=owner_schema();other.properties.erase("note");
    owner_fixture second(":memory:",SchemaVector{other});
    first.enroll();second.enroll();first.insert();
    second.owner->db().execute("INSERT INTO RecoverySwiftOwnerRow(globalId,text) VALUES(?,?)",{row_id,std::string("second")});
    EXPECT_EQ(first.count("_lattice_obligation_producer_stamp"),1);
    EXPECT_EQ(second.count("_lattice_obligation_producer_stamp"),1);
    EXPECT_EQ(schema_registry::instance().get_schema("RecoverySwiftOwnerRow"),nullptr);
    EXPECT_EQ(first.owner->get_properties_for_table("RecoverySwiftOwnerRow")->count("note"),1u);
    EXPECT_EQ(second.owner->get_properties_for_table("RecoverySwiftOwnerRow")->count("note"),0u);
}

TEST(RecoveryOwnerSchemaCatalog, RejectsNestedDescriptorOverflowBeforeModelCopy) {
    recovery_owner_schema catalog;model_schema model;model.table_name="Bounded";
    property_descriptor property{};property.name="item";property.type=column_type::text;property.is_union=true;
    property.union_desc.cases.resize(65);model.properties.push_back(property);
    EXPECT_FALSE(catalog.admit_model(model));EXPECT_FALSE(catalog.valid());EXPECT_TRUE(catalog.models.empty());
}

#if defined(__APPLE__) || defined(__linux__)
TEST(RecoveryOwnerSchemaReopen, FreshProcessReopensProtectedSwiftStoreAndPreservesPendingChanges) {
    constexpr const char* variable="LATTICE_OWNER_SCHEMA_PEER=";
    if(const auto* path=std::getenv("LATTICE_OWNER_SCHEMA_PEER")) {
        sigset_t signals;sigemptyset(&signals);sigaddset(&signals,SIGALRM);
        ASSERT_NE(std::signal(SIGALRM,SIG_DFL),SIG_ERR);ASSERT_EQ(sigprocmask(SIG_UNBLOCK,&signals,nullptr),0);
        alarm(30);
        {
            owner_fixture peer(path,SchemaVector{owner_schema(true,true)});
            ASSERT_EQ(peer.count("AuditLog"),1);ASSERT_EQ(peer.count("_lattice_obligation_producer_stamp"),1);
            peer.owner->db().execute("UPDATE RecoverySwiftOwnerRow SET note='from-peer' WHERE globalId=?",{row_id});
            auto prepared=recovery_export_adapter::prepare_history_page(peer.owner,0,0,8,{});
            ASSERT_TRUE(prepared.frame);ASSERT_EQ(prepared.frame->entries().size(),2u);
            EXPECT_EQ(std::get<std::string>(prepared.frame->entries().back().changed_fields.at("note").value),"from-peer");
        }
        alarm(0);return;
    }
    TempDB file("recovery_owner_process");
    {owner_fixture original(file.str(),SchemaVector{owner_schema()});original.enroll();original.insert();}
    char executable[4096];
#if defined(__APPLE__)
    uint32_t size=sizeof(executable);ASSERT_EQ(_NSGetExecutablePath(executable,&size),0);
#else
    const auto size=readlink("/proc/self/exe",executable,sizeof(executable)-1);
    ASSERT_GT(size,0);ASSERT_LT(size,static_cast<ssize_t>(sizeof(executable)-1));executable[size]=0;
#endif
    std::vector<std::string> values;
    for(char** entry=environ;*entry;++entry)
        if(std::strncmp(*entry,variable,std::strlen(variable))!=0)values.emplace_back(*entry);
    values.push_back(std::string(variable)+file.str());std::vector<char*> environment;
    for(auto& value:values)environment.push_back(value.data());environment.push_back(nullptr);
    std::string filter="--gtest_filter=RecoveryOwnerSchemaReopen.FreshProcessReopensProtectedSwiftStoreAndPreservesPendingChanges";
    std::string color="--gtest_color=no",repeat="--gtest_repeat=1",output="--gtest_output=";
    char* arguments[]={executable,filter.data(),color.data(),repeat.data(),output.data(),nullptr};
    posix_spawn_file_actions_t actions;ASSERT_EQ(posix_spawn_file_actions_init(&actions),0);
    struct cleanup_actions {posix_spawn_file_actions_t& value;~cleanup_actions(){posix_spawn_file_actions_destroy(&value);}} cleanup{actions};
    // Keep nested GoogleTest terminals out of the parent's exact-case log.
    const auto log=file.str()+".peer.log";
    ASSERT_EQ(posix_spawn_file_actions_addopen(&actions,STDOUT_FILENO,log.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600),0);
    ASSERT_EQ(posix_spawn_file_actions_adddup2(&actions,STDOUT_FILENO,STDERR_FILENO),0);
    pid_t child=-1;ASSERT_EQ(posix_spawn(&child,executable,&actions,nullptr,arguments,environment.data()),0);
    int status=0;pid_t waited=-1;
    const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(45);
    do {
        waited=waitpid(child,&status,WNOHANG);
        if(waited==child)break;
        if(waited<0&&errno!=EINTR)break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    } while(std::chrono::steady_clock::now()<deadline);
    ASSERT_FALSE(waited<0&&errno!=EINTR)<<"could not observe owned child; refusing to signal an unverified PID";
    if(waited!=child) {
        kill(child,SIGKILL);
        do {waited=waitpid(child,&status,0);}while(waited<0&&errno==EINTR);
        FAIL()<<"fresh owner process failed to finish within its watchdog; log: "<<log;
    }
    ASSERT_TRUE(WIFEXITED(status))<<log;ASSERT_EQ(WEXITSTATUS(status),0)<<log;
    owner_fixture verified(file.str(),SchemaVector{owner_schema()});
    EXPECT_EQ(verified.count("AuditLog"),2);EXPECT_EQ(verified.count("_lattice_obligation_producer_stamp"),2);
    const auto rows=verified.owner->db().query("SELECT text,note FROM RecoverySwiftOwnerRow WHERE globalId=?",{row_id});
    ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::string>(rows[0].at("text")),std::string("before\0after",12));
    EXPECT_EQ(std::get<std::string>(rows[0].at("note")),"from-peer");
}
#endif
#endif
