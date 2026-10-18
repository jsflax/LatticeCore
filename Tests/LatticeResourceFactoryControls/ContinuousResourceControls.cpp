#include <gtest/gtest.h>
#include <lattice/lattice.hpp>
#include <lattice/store_resource_context.hpp>
#include "../../Sources/LatticeCore/src/recovery_producer_continuity.hpp"
#include "../../Sources/LatticeCore/src/observer_delivery_startup_test_probe.hpp"
#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include <nlohmann/json.hpp>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include <fcntl.h>
#include <pwd.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

// Genuine installed controls: each invocation needs a fresh actual account
// snapshot and the approved whole current control image. Environment selects
// a declared container only; the real issuer independently checks every root,
// profile, identity, original and ending. No model is added to this executable:
// InitializationReplacementModel is registered by its whole existing TU.
namespace {
using namespace lattice;
using namespace lattice::detail;
using json=nlohmann::json;
using observation=observer_delivery_startup_test_probe::continuous_native_open_observation;
std::string selected_path(){const auto* value=std::getenv("LATTICE_RESOURCE_CONTINUOUS_PATH");
    if(!value||!*value)throw std::runtime_error("actual declared container selection required");return value;}
std::string actual_home(){passwd data{},*found=nullptr;std::array<char,16384> bytes{};
    if(::getpwuid_r(::geteuid(),&data,bytes.data(),bytes.size(),&found)||found!=&data||!data.pw_dir||data.pw_dir[0]!='/')
        throw std::runtime_error("actual account home required");return data.pw_dir;}
std::string raw_record(const std::filesystem::path& path){std::ifstream input(path,std::ios::binary|std::ios::ate);const auto n=input.tellg();
    if(!input||n<=0||n>128*1024)throw std::runtime_error("bounded actual record required");
    std::string value(static_cast<std::size_t>(n),'\0');input.seekg(0);input.read(value.data(),static_cast<std::streamsize>(value.size()));
    if(!input||input.gcount()!=static_cast<std::streamsize>(value.size())||json::parse(value).dump()!=value)
        throw std::runtime_error("complete canonical actual record required");return value;}
struct file_id {std::uint64_t device=0,inode=0;bool operator==(const file_id&)const=default;};
struct fact {file_id identity;std::uint64_t mode=0,size=0;bool operator==(const fact&)const=default;};
fact actual_file(const std::filesystem::path& path){struct stat value{};
    if(::lstat(path.c_str(),&value)||value.st_size<0)throw std::runtime_error("actual nofollow object required");
    return {{static_cast<std::uint64_t>(value.st_dev),static_cast<std::uint64_t>(value.st_ino)},
        static_cast<std::uint64_t>(value.st_mode),static_cast<std::uint64_t>(value.st_size)};}
bool absent(const std::filesystem::path& path){struct stat value{};if(::lstat(path.c_str(),&value)==0)return false;
    if(errno!=ENOENT)throw std::runtime_error("exact absent result required");return true;}
using names=std::map<std::string,fact>;
names shallow(const std::filesystem::path& directory){names value;for(const auto& row:std::filesystem::directory_iterator(directory)){
    if(value.size()==4096||!value.emplace(row.path().filename().string(),actual_file(row.path())).second)
        throw std::runtime_error("bounded complete namespace required");}return value;}
struct checked_fd {int value=-1;explicit checked_fd(const std::filesystem::path& path):value(::open(path.c_str(),O_RDONLY|O_NOFOLLOW|O_CLOEXEC)){
    if(value<0)throw std::runtime_error("actual control descriptor required");}
    checked_fd(const checked_fd&)=delete;
    ~checked_fd(){if(value>=0&&::close(std::exchange(value,-1))){std::fputs("physical-fixture-ending-debt:continuous-resource-control\n",stderr);::_exit(96);}}};
void actual_lease_retired(const std::filesystem::path& member){
    checked_fd entry(member/"entry.lock"),physical(member/"physical.lock");
    if(::flock(entry.value,LOCK_EX|LOCK_NB)||::flock(physical.value,LOCK_EX|LOCK_NB))
        throw std::runtime_error("actual failed-attempt guard/Native physical ending absent");
}
struct fixture {
    const std::string home=actual_home();const std::filesystem::path requested=selected_path();
    std::filesystem::path logical,generation,control,member;
    fixture(){
        if(!requested.is_absolute()||requested.filename()!="store.sqlite"||requested.lexically_normal()!=requested)
            throw std::runtime_error("exact logical fixed-leaf request required");
        const auto active=json::parse(raw_record(std::filesystem::path(home)/".lattice-resource-installation/active.v1"));
        if(active.at("roots").size()>16)throw std::runtime_error("bounded actual roots required");
        bool selected=false;json root;for(const auto& row:active.at("roots")){
            if(row.at("logicalPath").get<std::string>()!=requested.parent_path().string())continue;
            if(selected)throw std::runtime_error("unique actual root required");root=row;selected=true;}
        if(!selected||root.at("rule")!=json{{"names",json::array({"store.sqlite"})},{"suffixes",json::array()}})
            throw std::runtime_error("actual declared continuous fixed-leaf root required");
        logical=root.at("logicalPath").get<std::string>();generation=root.at("generationPath").get<std::string>();control=root.at("controlPath").get<std::string>();
        if(!logical.filename().string().ends_with(".lattice-continuous")||!generation.filename().string().ends_with(".lattice-continuous"))
            throw std::runtime_error("actual logical and generation containers required");
        const std::string name="store.sqlite";member=control/picosha2::hash256_hex_string(std::vector<std::uint8_t>(name.begin(),name.end()));
        for(const auto& suffix:{std::string(),std::string("-wal"),std::string("-shm"),std::string("-journal")})
            if(!absent(logical/(name+suffix))||!absent(generation/(name+suffix)))throw std::runtime_error("fresh genuine container required");
        if(!absent(member))throw std::runtime_error("genuine never-issued member required");
    }
    configuration config()const{configuration value(requested.string());value.audit_retention_seconds=0;value.busy_timeout_ms=100;return value;}
    recovery_continuous_policy policy()const{
        recovery_continuous_policy value;value.limits={{4,128,128,2*1024*1024},{4,128,8192},{4,128,128,1048576,8*1024*1024}};
        value.owners=2;value.physical_routes=2;value.operations=2;value.frozen_entries=128;value.frozen_bytes=2*1024*1024;
        value.contributions.push_back({{{"resource-control","authority","source","epoch","scope","schema"},"grant","resource-receipts"},
            {"InitializationReplacementModel"},{'g'}});
        value.routes.push_back({"wss:wss://continuous.invalid/resource-control","wss://continuous.invalid/resource-control"});return value;
    }
};
struct native_stop {};
struct observed {const fixture* selected=nullptr;bool stop=true;unsigned calls=0;observation value{};};
thread_local observed* selected_continuous=nullptr;
void actual_native_return(const observation& value){
    if(!selected_continuous||!selected_continuous->selected)throw std::runtime_error("actual observation selection absent");
    auto& data=*selected_continuous;const auto& f=*data.selected;++data.calls;data.value=value;
    if(!value.managed||!value.admission||!value.startup||!value.writer_open||!value.device||!value.inode||
       value.original_end==std::chrono::steady_clock::time_point::max()||std::chrono::steady_clock::now()>=value.original_end)
        throw std::runtime_error("actual original resource/startup custody absent");
    if(actual_file(f.generation/"store.sqlite").identity!=file_id{value.device,value.inode})throw std::runtime_error("actual Native MAIN identity differs");
    const auto member=json::parse(raw_record(f.member/"member.v1")),state=json::parse(raw_record(f.member/"state.v1"));
    if(state.at("phase")!="active"||member.at("main").at("device")!=value.device||member.at("main").at("inode")!=value.inode)
        throw std::runtime_error("actual producer member/MAIN binding differs");
    const auto stub=actual_file(f.logical/"store.sqlite");if((stub.mode&07777)||stub.size)throw std::runtime_error("actual denial stub absent");
    // Real kernel outcomes at the actual cut: pre-open entry SH has ended;
    // Native/VFS independently retains physical SH after genuine transfer.
    checked_fd entry(f.member/"entry.lock");if(::flock(entry.value,LOCK_EX|LOCK_NB))throw std::runtime_error("actual temporary entry guard did not end");
    checked_fd physical(f.member/"physical.lock");errno=0;const int rc=::flock(physical.value,LOCK_EX|LOCK_NB),error=errno;
    if(rc==0||(error!=EWOULDBLOCK&&error!=EAGAIN))throw std::runtime_error("actual Native physical lease absent");
    if(data.stop)throw native_stop{};
}
struct probe {explicit probe(observed& value){if(selected_continuous||observer_delivery_startup_test_probe::after_continuous_native_open)
        throw std::runtime_error("continuous probe already selected");selected_continuous=&value;
        observer_delivery_startup_test_probe::after_continuous_native_open=actual_native_return;}
    ~probe(){observer_delivery_startup_test_probe::after_continuous_native_open=nullptr;selected_continuous=nullptr;}};
}
TEST(ContinuousResourceStartup, GenuineCreationOriginalTransfersToNativeAndRetryCannotReissueFreshness){
    fixture f;observed data;data.selected=&f;const auto config=f.config();const auto policy=f.policy();
    recovery_continuous_open_result first;{probe selected(data);first=recovery_continuous_producer::open(config,policy);}
    ASSERT_EQ(data.calls,1u);EXPECT_TRUE(data.value.fresh);EXPECT_TRUE(data.value.managed);
    EXPECT_FALSE(first.owner);EXPECT_NE(first.settlement.state,recovery_install_state::committed);
    ASSERT_TRUE(first.settlement.primary_error);EXPECT_THROW(std::rethrow_exception(first.settlement.primary_error),native_stop);
    EXPECT_FALSE(first.settlement.cleanup_error);EXPECT_FALSE(first.settlement.postcommit_error);
    ASSERT_NO_THROW(actual_lease_retired(f.member));
    const auto main=actual_file(f.generation/"store.sqlite").identity,stub=actual_file(f.logical/"store.sqlite").identity;
    const auto member=raw_record(f.member/"member.v1"),state=raw_record(f.member/"state.v1");
    // Closed attempt, same actual files. No registry reset, fake epoch or
    // READY-to-freshness shortcut; a durable missing lineage must still refuse.
    data.stop=false;recovery_continuous_open_result retry;{probe selected(data);retry=recovery_continuous_producer::open(config,policy);}
    ASSERT_EQ(data.calls,2u);EXPECT_FALSE(data.value.fresh);EXPECT_TRUE(data.value.managed);
    EXPECT_EQ((file_id{data.value.device,data.value.inode}),main);
    EXPECT_FALSE(retry.owner);EXPECT_NE(retry.settlement.state,recovery_install_state::committed);
    ASSERT_TRUE(retry.settlement.primary_error);EXPECT_THROW(std::rethrow_exception(retry.settlement.primary_error),db_error);
    EXPECT_FALSE(retry.settlement.cleanup_error);EXPECT_FALSE(retry.settlement.postcommit_error);
    ASSERT_NO_THROW(actual_lease_retired(f.member));
    EXPECT_EQ(actual_file(f.generation/"store.sqlite").identity,main);EXPECT_EQ(actual_file(f.logical/"store.sqlite").identity,stub);
    EXPECT_EQ(raw_record(f.member/"member.v1"),member);EXPECT_EQ(raw_record(f.member/"state.v1"),state);
}
TEST(ContinuousResourceStartup, UndeclaredLeafRefusesBeforeActualMemberOrNativeCreation){
    fixture f;const auto generation=shallow(f.generation),logical=shallow(f.logical),control=shallow(f.control);
    auto config=f.config();config.path=(f.logical/"foreign.sqlite").string();observed data;data.selected=&f;
    recovery_continuous_open_result refused;{probe selected(data);refused=recovery_continuous_producer::open(config,f.policy());}
    EXPECT_FALSE(refused.owner);EXPECT_NE(refused.settlement.state,recovery_install_state::committed);
    EXPECT_TRUE(refused.settlement.primary_error);EXPECT_FALSE(refused.settlement.cleanup_error);EXPECT_FALSE(refused.settlement.postcommit_error);
    EXPECT_EQ(data.calls,0u);EXPECT_EQ(shallow(f.generation),generation);EXPECT_EQ(shallow(f.logical),logical);EXPECT_EQ(shallow(f.control),control);
    EXPECT_TRUE(absent(f.member));EXPECT_TRUE(absent(f.generation/"foreign.sqlite"));
}
TEST(ContinuousResourceStartup, InvalidPolicyRefusesBeforeActualMemberOrNativeCreation){
    fixture f;const auto generation=shallow(f.generation),logical=shallow(f.logical),control=shallow(f.control);
    auto policy=f.policy();policy.owners=0;observed data;data.selected=&f;
    recovery_continuous_open_result refused;{probe selected(data);refused=recovery_continuous_producer::open(f.config(),policy);}
    EXPECT_FALSE(refused.owner);EXPECT_NE(refused.settlement.state,recovery_install_state::committed);
    EXPECT_TRUE(refused.settlement.primary_error);EXPECT_FALSE(refused.settlement.cleanup_error);EXPECT_FALSE(refused.settlement.postcommit_error);
    EXPECT_EQ(data.calls,0u);EXPECT_EQ(shallow(f.generation),generation);EXPECT_EQ(shallow(f.logical),logical);EXPECT_EQ(shallow(f.control),control);
    EXPECT_TRUE(absent(f.member));EXPECT_TRUE(absent(f.generation/"store.sqlite"));
}
#endif
#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
struct enrollment_stop {};
struct enrollment_observed {unsigned calls=0;const void* admission=nullptr;observer_delivery_startup_test_probe::owner_binding_observation data{};};
thread_local enrollment_observed* selected_enrollment=nullptr;
void actual_enrollment_bound(lattice_db& owner,const void* admission,
    const observer_delivery_startup_test_probe::owner_binding_observation& actual){
    if(!selected_enrollment)throw std::runtime_error("actual enrollment observation selection absent");
    ++selected_enrollment->calls;selected_enrollment->admission=admission;selected_enrollment->data=actual;
    if(!admission||!actual.catalog||!actual.budget||!actual.profile||!(actual.roles&1u)||actual.roles>7u||
       actual.actual[0]!=&owner.db()||owner.db().is_in_transaction())throw std::runtime_error("actual retained pending enrollment family differs");
    for(unsigned index=0;index<3;++index)if(actual.roles&(1u<<index)){
        if(!actual.actual[index]||!actual.open[index]||!actual.runtime[index]||actual.role_budget[index]!=actual.budget||
           actual.role_profile[index]!=actual.profile)throw std::runtime_error("actual enrollment role lost the common client/budget/profile");
        for(unsigned prior=0;prior<index;++prior)if((actual.roles&(1u<<prior))&&
           (actual.actual[index]==actual.actual[prior]||actual.open[index]==actual.open[prior]||actual.runtime[index]==actual.runtime[prior]))
            throw std::runtime_error("actual enrollment role originals alias");
    }
    bool refused=false;try{(void)owner.db().query("SELECT 1");}catch(const db_error&){refused=true;}
    if(!refused)throw std::runtime_error("pending enrollment binding granted public SQL");
    throw enrollment_stop{};
}
struct enrollment_probe {explicit enrollment_probe(enrollment_observed& actual){
    if(selected_enrollment||observer_delivery_startup_test_probe::after_continuous_enrollment_bound)
        throw std::runtime_error("actual enrollment observation already selected");
    selected_enrollment=&actual;observer_delivery_startup_test_probe::after_continuous_enrollment_bound=actual_enrollment_bound;}
    ~enrollment_probe(){observer_delivery_startup_test_probe::after_continuous_enrollment_bound=nullptr;selected_enrollment=nullptr;}};
}
TEST(ContinuousResourceStartup, ActualRetainedEnrollmentSharesOriginalRolesWithoutPublishingSQL){
    fixture f;enrollment_observed observed;recovery_continuous_open_result stopped;
    {enrollment_probe selected(observed);stopped=recovery_continuous_producer::open(f.config(),f.policy());}
    ASSERT_EQ(observed.calls,1u);EXPECT_NE(observed.admission,nullptr);EXPECT_NE(observed.data.budget,nullptr);EXPECT_NE(observed.data.profile,nullptr);
    EXPECT_FALSE(stopped.owner);EXPECT_NE(stopped.settlement.state,recovery_install_state::committed);
    ASSERT_TRUE(stopped.settlement.primary_error);EXPECT_THROW(std::rethrow_exception(stopped.settlement.primary_error),enrollment_stop);
    EXPECT_FALSE(stopped.settlement.cleanup_error);EXPECT_FALSE(stopped.settlement.postcommit_error);
    ASSERT_NO_THROW(actual_lease_retired(f.member));
    // Genuine whole constructor/model prefix is required to reach this cut;
    // the control never constructs a fake pending owner or bypasses finishes.
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
#include "../../Sources/LatticeCore/src/recovery_writer_access.hpp"
#include "../../Sources/LatticeCore/src/database_sql_dispatcher.hpp"
namespace lattice::detail {
struct continuous_enrollment_transaction_test_access {
    // Restriction-only actual dispatcher role. No raw authorizer replacement,
    // SQL grant, fake Native return or issuer/profile construction.
    static int restrict_rollback(database& actual,
        int(*callback)(void*,int,const char*,const char*,const char*,const char*)noexcept,void* context)noexcept {
        return actual.set_sql_authorizer_(database_authorizer_slot::owned_test_restriction,callback,context);
    }
};
}
namespace {
struct enrollment_read_stop {};
struct enrollment_read_observed {
    unsigned bound=0,reads=0,rollback_authorizations=0;
    bool deny_rollback=false,restriction_installed=false;
    observer_delivery_startup_test_probe::owner_binding_observation original{};
    observer_delivery_startup_test_probe::enrollment_transaction_observation actual{};
    std::exception_ptr sentinel;
};
thread_local enrollment_read_observed* selected_enrollment_reads=nullptr;
void retain_actual_enrollment_binding(lattice_db& owner,const void* admission,
    const observer_delivery_startup_test_probe::owner_binding_observation& original){
    if(!selected_enrollment_reads||!admission||original.actual[0]!=&owner.db())
        throw std::runtime_error("real retained enrollment binding absent");
    ++selected_enrollment_reads->bound;selected_enrollment_reads->original=original;
}
int deny_actual_enrollment_rollback(void* context,int action,const char* first,const char*,const char*,const char*)noexcept {
    auto* actual=static_cast<enrollment_read_observed*>(context);
    if(action==SQLITE_TRANSACTION&&first&&sqlite3_stricmp(first,"ROLLBACK")==0){
        ++actual->rollback_authorizations;return SQLITE_DENY;
    }
    return SQLITE_OK;
}
void stop_actual_enrollment_after_reads(lattice_db& owner,
    const observer_delivery_startup_test_probe::enrollment_transaction_observation& observed){
    if(!selected_enrollment_reads)throw std::runtime_error("actual enrollment read observation absent");
    auto& data=*selected_enrollment_reads;++data.reads;data.actual=observed;
    if(data.bound!=1||observed.existing||observed.actual!=&owner.db()||!observed.admission||!observed.episode||
       observed.open!=data.original.open[0]||observed.budget!=data.original.budget)
        throw std::runtime_error("actual enrollment reads lost original owner/open/budget/episode");
    auto* writer=recovery_writer_access::active_writer(owner);
    if(writer!=&owner.db())throw std::runtime_error("actual private BEGIN did not own the recovery frame");
    auto* native=recovery_writer_access::active_handle(owner,*writer);
    if(!native||sqlite3_get_autocommit(native)||sqlite3_txn_state(native,"main")!=SQLITE_TXN_WRITE)
        throw std::runtime_error("actual enrollment reads do not hold the same Native WRITE");
    if(data.deny_rollback){
        const int rc=continuous_enrollment_transaction_test_access::restrict_rollback(*writer,deny_actual_enrollment_rollback,&data);
        if(rc!=SQLITE_OK){std::fputs("actual enrollment restriction publication ending unknown\n",stderr);::_exit(96);}
        data.restriction_installed=true;
    }
    std::rethrow_exception(data.sentinel);
}
struct enrollment_read_probe {
    explicit enrollment_read_probe(enrollment_read_observed& data){
        if(selected_enrollment_reads||observer_delivery_startup_test_probe::after_continuous_enrollment_bound||
           observer_delivery_startup_test_probe::after_continuous_enrollment_reads)
            throw std::runtime_error("actual enrollment read probe already selected");
        try{throw enrollment_read_stop{};}catch(...){data.sentinel=std::current_exception();}
        selected_enrollment_reads=&data;
        observer_delivery_startup_test_probe::after_continuous_enrollment_bound=retain_actual_enrollment_binding;
        observer_delivery_startup_test_probe::after_continuous_enrollment_reads=stop_actual_enrollment_after_reads;
    }
    ~enrollment_read_probe(){observer_delivery_startup_test_probe::after_continuous_enrollment_reads=nullptr;
        observer_delivery_startup_test_probe::after_continuous_enrollment_bound=nullptr;selected_enrollment_reads=nullptr;}
};
void require_actual_enrollment_end_before_restriction_context_retires(const fixture& f){
    // Context remains alive until the actual Native physical ending is proved.
    // Unknown cleanup must retain evidence, never unwind dangling role DATA.
    try{actual_lease_retired(f.member);}catch(...){
        std::fputs("physical-fixture-ending-debt:continuous-enrollment-restriction\n",stderr);::_exit(96);}
}
}
TEST(ContinuousResourceStartup, ActualEnrollmentWriteReadsKeepOriginalBudgetAndSentinelRollback){
    fixture f;enrollment_read_observed data;recovery_continuous_open_result stopped;
    {enrollment_read_probe selected(data);stopped=recovery_continuous_producer::open(f.config(),f.policy());}
    ASSERT_EQ(data.bound,1u);ASSERT_EQ(data.reads,1u);EXPECT_FALSE(data.actual.existing);
    EXPECT_FALSE(stopped.owner);EXPECT_EQ(stopped.settlement.state,recovery_install_state::rolled_back);
    EXPECT_EQ(stopped.settlement.primary_error,data.sentinel);ASSERT_TRUE(stopped.settlement.primary_error);
    EXPECT_THROW(std::rethrow_exception(stopped.settlement.primary_error),enrollment_read_stop);
    EXPECT_FALSE(stopped.settlement.cleanup_error);EXPECT_FALSE(stopped.settlement.postcommit_error);
    EXPECT_FALSE(stopped.settlement.notification_error);EXPECT_FALSE(stopped.settlement.unexpected_commit_observed);
    ASSERT_NO_THROW(actual_lease_retired(f.member));
}
TEST(ContinuousResourceStartup, ActualEnrollmentRollbackDenialRetainsBothErrorsWithoutDestructorRetry){
    fixture f;enrollment_read_observed data;data.deny_rollback=true;recovery_continuous_open_result stopped;
    {enrollment_read_probe selected(data);stopped=recovery_continuous_producer::open(f.config(),f.policy());}
    // Do this before ANY fatal assertion can unwind the actual role context.
    require_actual_enrollment_end_before_restriction_context_retires(f);
    ASSERT_EQ(data.bound,1u);ASSERT_EQ(data.reads,1u);ASSERT_TRUE(data.restriction_installed);
    EXPECT_EQ(data.rollback_authorizations,1u); // actual denied prepare, no second receipt/destructor attempt
    EXPECT_FALSE(stopped.owner);EXPECT_EQ(stopped.settlement.state,recovery_install_state::unsettled);
    EXPECT_EQ(stopped.settlement.primary_error,data.sentinel);ASSERT_TRUE(stopped.settlement.primary_error);
    EXPECT_THROW(std::rethrow_exception(stopped.settlement.primary_error),enrollment_read_stop);
    ASSERT_TRUE(stopped.settlement.cleanup_error);EXPECT_NE(stopped.settlement.cleanup_error,data.sentinel);
    EXPECT_THROW(std::rethrow_exception(stopped.settlement.cleanup_error),db_error);
    EXPECT_FALSE(stopped.settlement.postcommit_error);EXPECT_FALSE(stopped.settlement.notification_error);
    EXPECT_FALSE(stopped.settlement.unexpected_commit_observed);
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
TEST(ContinuousResourceStartup, GenuineEnrollmentReachesZeroResultSettlementAndKnownCommit){
    // This is a genuine complete-factory positive control. It must pass only
    // after the real typed enrollment DML and all private finishes exist; no
    // fake startup/receipt or expected pending refusal substitutes for success.
    fixture f;auto opened=recovery_continuous_producer::open(f.config(),f.policy());
    ASSERT_EQ(opened.settlement.state,recovery_install_state::committed);
    EXPECT_FALSE(opened.settlement.primary_error);EXPECT_FALSE(opened.settlement.cleanup_error);
    EXPECT_FALSE(opened.settlement.postcommit_error);EXPECT_FALSE(opened.settlement.notification_error);
    EXPECT_FALSE(opened.settlement.unexpected_commit_observed);ASSERT_TRUE(opened.owner);
    // The actual owned engine reaches known COMMIT only after its two exact
    // zero-result body controls, busy-corpus check and flush returned. Public
    // SQL here additionally requires genuine completed startup publication.
    const auto rows=opened.owner->db().query("SELECT incarnation,phase FROM main._lattice_producer_continuity WHERE id=1");
    ASSERT_EQ(rows.size(),1u);ASSERT_TRUE(std::holds_alternative<std::int64_t>(rows[0].at("incarnation")));
    ASSERT_TRUE(std::holds_alternative<std::int64_t>(rows[0].at("phase")));
    EXPECT_EQ(std::get<std::int64_t>(rows[0].at("incarnation")),1);
    EXPECT_EQ(std::get<std::int64_t>(rows[0].at("phase")),0);
    ASSERT_NO_THROW(opened.owner->close());opened.owner.reset();
    ASSERT_NO_THROW(actual_lease_retired(f.member));
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
struct actual_close_failure_scheduler final : scheduler {
    std::exception_ptr actual_error;
    unsigned shutdown_calls=0,failures=0;
    bool stopped=false;
    actual_close_failure_scheduler(){
        try{throw std::runtime_error("actual continuous owner scheduler shutdown failure");}
        catch(...){actual_error=std::current_exception();}
    }
    void invoke(std::function<void()>&& body)override{if(!stopped)body();}
    bool is_on_thread()const noexcept override{return true;}
    bool is_same_as(const scheduler* other)const noexcept override{return other==this;}
    bool can_invoke()const noexcept override{return !stopped;}
    void shutdown()override{
        ++shutdown_calls;if(stopped)return;stopped=true;++failures;
        // The real close_checked() calls this application's actual scheduler.
        // Preserve the actual exception, not a fabricated close/Native result.
        std::rethrow_exception(actual_error);
    }
};
}
TEST(ContinuousResourceStartup, RealLaterOwnerCloseFailureCannotOverwriteRollbackCleanup){
    fixture f;enrollment_read_observed data;data.deny_rollback=true;
    auto scheduled=std::make_shared<actual_close_failure_scheduler>();auto config=f.config();config.sched=scheduled;
    recovery_continuous_open_result stopped;
    {enrollment_read_probe selected(data);stopped=recovery_continuous_producer::open(config,f.policy());}
    require_actual_enrollment_end_before_restriction_context_retires(f);
    ASSERT_EQ(data.bound,1u);ASSERT_EQ(data.reads,1u);ASSERT_TRUE(data.restriction_installed);
    EXPECT_EQ(data.rollback_authorizations,1u);EXPECT_FALSE(stopped.owner);
    EXPECT_EQ(stopped.settlement.state,recovery_install_state::unsettled);
    EXPECT_EQ(stopped.settlement.primary_error,data.sentinel);
    ASSERT_TRUE(stopped.settlement.cleanup_error);EXPECT_NE(stopped.settlement.cleanup_error,data.sentinel);
    EXPECT_NE(stopped.settlement.cleanup_error,scheduled->actual_error);
    EXPECT_THROW(std::rethrow_exception(stopped.settlement.cleanup_error),db_error);
    EXPECT_EQ(stopped.owner_close_error,scheduled->actual_error);
    ASSERT_TRUE(stopped.owner_close_error);EXPECT_THROW(std::rethrow_exception(stopped.owner_close_error),std::runtime_error);
    EXPECT_GE(scheduled->shutdown_calls,1u);EXPECT_EQ(scheduled->failures,1u);
    EXPECT_FALSE(stopped.settlement.postcommit_error);EXPECT_FALSE(stopped.settlement.notification_error);
    EXPECT_FALSE(stopped.settlement.unexpected_commit_observed);
}
TEST(ContinuousResourceStartup, RealOwnerCloseFailurePreservesOriginalFirstCleanupCompatibility){
    fixture f;enrollment_read_observed data;
    auto scheduled=std::make_shared<actual_close_failure_scheduler>();auto config=f.config();config.sched=scheduled;
    recovery_continuous_open_result stopped;
    {enrollment_read_probe selected(data);stopped=recovery_continuous_producer::open(config,f.policy());}
    ASSERT_NO_THROW(actual_lease_retired(f.member));ASSERT_EQ(data.bound,1u);ASSERT_EQ(data.reads,1u);
    EXPECT_EQ(stopped.settlement.state,recovery_install_state::rolled_back);EXPECT_FALSE(stopped.owner);
    EXPECT_EQ(stopped.settlement.primary_error,data.sentinel);
    EXPECT_EQ(stopped.settlement.cleanup_error,scheduled->actual_error);
    EXPECT_EQ(stopped.owner_close_error,scheduled->actual_error);EXPECT_EQ(scheduled->failures,1u);
    EXPECT_FALSE(stopped.settlement.postcommit_error);EXPECT_FALSE(stopped.settlement.notification_error);
    EXPECT_FALSE(stopped.settlement.unexpected_commit_observed);
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
struct ledger_family_observed {
    enrollment_read_observed enrollment;
    unsigned actual_ledger_cut=0,actual_create_denials=0;
    sqlite3_int64 changes_before=0,changes_after=0;
    bool deny_create=false,restriction_installed=false;
    const void* original_episode=nullptr;
};
thread_local ledger_family_observed* selected_ledger_family=nullptr;
int deny_actual_install_store_create(void* context,int action,const char* first,const char*,const char*,const char*)noexcept {
    auto& data=*static_cast<ledger_family_observed*>(context);
    if(action==SQLITE_CREATE_TABLE&&first&&sqlite3_stricmp(first,"_lattice_install_store")==0){
        ++data.actual_create_denials;return SQLITE_DENY;
    }
    return SQLITE_OK;
}
void continue_actual_enrollment_to_ledger(lattice_db& owner,
    const observer_delivery_startup_test_probe::enrollment_transaction_observation& observed){
    if(!selected_ledger_family)throw std::runtime_error("actual ledger control absent");
    auto& data=*selected_ledger_family;++data.enrollment.reads;data.enrollment.actual=observed;
    if(data.enrollment.bound!=1||observed.existing||observed.actual!=&owner.db()||!observed.episode||
       observed.budget!=data.enrollment.original.budget||observed.open!=data.enrollment.original.open[0])
        throw std::runtime_error("actual ledger control lost same owner/open/budget/episode");
    auto* native=recovery_writer_access::active_handle(owner,owner.db());
    if(!native||sqlite3_get_autocommit(native)||sqlite3_txn_state(native,"main")!=SQLITE_TXN_WRITE)
        throw std::runtime_error("actual ledger control does not own the genuine WRITE");
    data.original_episode=observed.episode;data.changes_before=sqlite3_total_changes64(native);
    if(data.deny_create){
        const int rc=continuous_enrollment_transaction_test_access::restrict_rollback(owner.db(),deny_actual_install_store_create,&data);
        if(rc!=SQLITE_OK){std::fputs("actual ledger test restriction publication ending unknown\n",stderr);::_exit(96);}
        data.restriction_installed=true;
    }
}
void stop_actual_completed_ledger(lattice_db& owner,
    const observer_delivery_startup_test_probe::enrollment_transaction_observation& observed){
    if(!selected_ledger_family)throw std::runtime_error("actual ledger completion control absent");
    auto& data=*selected_ledger_family;++data.actual_ledger_cut;
    if(observed.episode!=data.original_episode||observed.actual!=&owner.db()||
       observed.open!=data.enrollment.original.open[0]||observed.budget!=data.enrollment.original.budget)
        throw std::runtime_error("completed ledger family switched the actual original");
    auto* native=recovery_writer_access::active_handle(owner,owner.db());
    if(!native||sqlite3_get_autocommit(native)||sqlite3_txn_state(native,"main")!=SQLITE_TXN_WRITE)
        throw std::runtime_error("completed ledger family lost its original WRITE");
    data.changes_after=sqlite3_total_changes64(native);
    std::rethrow_exception(data.enrollment.sentinel);
}
struct ledger_family_probe {
    enrollment_read_probe original;
    explicit ledger_family_probe(ledger_family_observed& data):original(data.enrollment){
        if(selected_ledger_family||observer_delivery_startup_test_probe::after_continuous_enrollment_ledgers)
            throw std::runtime_error("actual ledger family probe already selected");
        selected_ledger_family=&data;
        observer_delivery_startup_test_probe::after_continuous_enrollment_reads=continue_actual_enrollment_to_ledger;
        observer_delivery_startup_test_probe::after_continuous_enrollment_ledgers=stop_actual_completed_ledger;
    }
    ~ledger_family_probe(){observer_delivery_startup_test_probe::after_continuous_enrollment_ledgers=nullptr;selected_ledger_family=nullptr;}
};
}
TEST(ContinuousResourceStartup, ActualFreshLedgerFamilyHasCompleteTablesAndThreeExactSeedsOnSameWrite){
    fixture f;ledger_family_observed data;recovery_continuous_open_result stopped;
    {ledger_family_probe selected(data);stopped=recovery_continuous_producer::open(f.config(),f.policy());}
    ASSERT_EQ(data.enrollment.bound,1u);ASSERT_EQ(data.enrollment.reads,1u);ASSERT_EQ(data.actual_ledger_cut,1u);
    // DDL does not count application row changes. Only these three real
    // fixed policy INSERTs occur between the two actual Native observations.
    EXPECT_EQ(data.changes_after-data.changes_before,3);
    EXPECT_EQ(stopped.settlement.state,recovery_install_state::rolled_back);EXPECT_FALSE(stopped.owner);
    EXPECT_EQ(stopped.settlement.primary_error,data.enrollment.sentinel);
    EXPECT_FALSE(stopped.settlement.cleanup_error);EXPECT_FALSE(stopped.owner_close_error);
    EXPECT_FALSE(stopped.settlement.postcommit_error);EXPECT_FALSE(stopped.settlement.notification_error);
    EXPECT_FALSE(stopped.settlement.unexpected_commit_observed);ASSERT_NO_THROW(actual_lease_retired(f.member));
}
TEST(ContinuousResourceStartup, ActualLedgerCreateDenialKeepsFirstErrorAndPrepaidRollback){
    fixture f;ledger_family_observed data;data.deny_create=true;recovery_continuous_open_result stopped;
    {ledger_family_probe selected(data);stopped=recovery_continuous_producer::open(f.config(),f.policy());}
    require_actual_enrollment_end_before_restriction_context_retires(f);
    ASSERT_EQ(data.enrollment.bound,1u);ASSERT_EQ(data.enrollment.reads,1u);ASSERT_TRUE(data.restriction_installed);
    EXPECT_EQ(data.actual_create_denials,1u);EXPECT_EQ(data.actual_ledger_cut,0u);
    EXPECT_EQ(stopped.settlement.state,recovery_install_state::rolled_back);EXPECT_FALSE(stopped.owner);
    ASSERT_TRUE(stopped.settlement.primary_error);EXPECT_NE(stopped.settlement.primary_error,data.enrollment.sentinel);
    EXPECT_THROW(std::rethrow_exception(stopped.settlement.primary_error),db_error);
    EXPECT_FALSE(stopped.settlement.cleanup_error);EXPECT_FALSE(stopped.owner_close_error);
    EXPECT_FALSE(stopped.settlement.postcommit_error);EXPECT_FALSE(stopped.settlement.notification_error);
    EXPECT_FALSE(stopped.settlement.unexpected_commit_observed);
}
#endif

#if (defined(__APPLE__) || defined(__linux__)) && !defined(__EMSCRIPTEN__)
namespace {
struct receiver_family_observed {
    ledger_family_observed ledger;
    unsigned receiver_cuts=0,insert_denials=0,usage_denials=0,stop_after=1;
    bool deny_insert=false,deny_usage=false,restriction_installed=false;
    std::array<std::int64_t,2> expected_bytes{};
    sqlite3_int64 before_receiver=0,after_receiver=0;
};
thread_local receiver_family_observed* selected_receiver_family=nullptr;
int deny_actual_receiver_write(void* context,int action,const char* first,const char* second,const char* schema,const char*)noexcept {
    auto& data=*static_cast<receiver_family_observed*>(context);
    if(!schema||sqlite3_stricmp(schema,"main"))return SQLITE_OK;
    if(data.deny_insert&&action==SQLITE_INSERT&&first&&sqlite3_stricmp(first,"_lattice_install_channel")==0) {
        ++data.insert_denials;return SQLITE_DENY;
    }
    if(data.deny_usage&&action==SQLITE_UPDATE&&first&&second&&sqlite3_stricmp(first,"_lattice_install_store")==0&&
       (sqlite3_stricmp(second,"channels")==0||sqlite3_stricmp(second,"bytes")==0)) {
        ++data.usage_denials;return SQLITE_DENY;
    }
    return SQLITE_OK;
}
void continue_actual_ledger_to_receiver(lattice_db& owner,
    const observer_delivery_startup_test_probe::enrollment_transaction_observation& observed) {
    if(!selected_receiver_family||selected_ledger_family!=&selected_receiver_family->ledger)
        throw std::runtime_error("actual receiver family control absent");
    auto& data=*selected_receiver_family;auto& ledger=data.ledger;++ledger.actual_ledger_cut;
    if(observed.episode!=ledger.original_episode||observed.actual!=&owner.db()||
       observed.open!=ledger.enrollment.original.open[0]||observed.budget!=ledger.enrollment.original.budget)
        throw std::runtime_error("actual receiver prefix switched the original owner/open/budget/episode");
    auto* native=recovery_writer_access::active_handle(owner,owner.db());
    if(!native||sqlite3_get_autocommit(native)||sqlite3_txn_state(native,"main")!=SQLITE_TXN_WRITE)
        throw std::runtime_error("actual receiver prefix lacks its genuine WRITE");
    data.before_receiver=sqlite3_total_changes64(native);
    if(data.before_receiver-ledger.changes_before!=3)
        throw std::runtime_error("actual receiver prefix did not produce the three real policy seeds");
    if(data.deny_insert||data.deny_usage) {
        const auto rc=continuous_enrollment_transaction_test_access::restrict_rollback(owner.db(),deny_actual_receiver_write,&data);
        if(rc!=SQLITE_OK){std::fputs("actual receiver restriction ending unknown\n",stderr);::_exit(96);}
        data.restriction_installed=true;
    }
}
void stop_actual_receiver_after_readback(lattice_db& owner,
    const observer_delivery_startup_test_probe::enrollment_transaction_observation& observed,
    std::size_t contribution,std::int64_t channels,std::int64_t bytes) {
    if(!selected_receiver_family)throw std::runtime_error("actual receiver readback observation absent");
    auto& data=*selected_receiver_family;const auto& ledger=data.ledger;
    if(contribution!=data.receiver_cuts||contribution>=data.expected_bytes.size()||
       channels!=std::int64_t(contribution+1)||bytes!=data.expected_bytes[contribution]||
       observed.existing||observed.episode!=ledger.original_episode||observed.actual!=&owner.db()||
       observed.open!=ledger.enrollment.original.open[0]||observed.budget!=ledger.enrollment.original.budget)
        throw std::runtime_error("actual receiver readback lost exact row/counter/original correspondence");
    auto* native=recovery_writer_access::active_handle(owner,owner.db());
    if(!native||sqlite3_get_autocommit(native)||sqlite3_txn_state(native,"main")!=SQLITE_TXN_WRITE)
        throw std::runtime_error("actual receiver readback no longer owns the genuine WRITE");
    data.after_receiver=sqlite3_total_changes64(native);++data.receiver_cuts;
    if(data.after_receiver-data.before_receiver!=2*sqlite3_int64(data.receiver_cuts))
        throw std::runtime_error("actual receiver did not perform exactly one channel INSERT and counter CAS per contribution");
    if(data.receiver_cuts==data.stop_after)std::rethrow_exception(data.ledger.enrollment.sentinel);
}
struct receiver_family_probe {
    ledger_family_probe original;
    explicit receiver_family_probe(receiver_family_observed& data):original(data.ledger) {
        if(selected_receiver_family||observer_delivery_startup_test_probe::after_continuous_enrollment_receiver)
            throw std::runtime_error("actual receiver probe already selected");
        selected_receiver_family=&data;
        observer_delivery_startup_test_probe::after_continuous_enrollment_ledgers=continue_actual_ledger_to_receiver;
        observer_delivery_startup_test_probe::after_continuous_enrollment_receiver=stop_actual_receiver_after_readback;
    }
    ~receiver_family_probe(){observer_delivery_startup_test_probe::after_continuous_enrollment_receiver=nullptr;selected_receiver_family=nullptr;}
};
void receiver_expected_usage(receiver_family_observed& data,const recovery_continuous_policy& policy) {
    std::int64_t bytes=0;
    for(std::size_t index=0;index<policy.contributions.size();++index) {
        if(index>=data.expected_bytes.size())throw std::runtime_error("receiver control contribution bound exceeded");
        const auto& b=policy.contributions[index].profile.binding;
        for(const auto* value:{&b.channel,&b.authority,&b.source,&b.epoch,&b.scope,&b.schema})bytes+=std::int64_t(value->size());
        data.expected_bytes[index]=bytes;
    }
}
void expect_receiver_failure_ending(const fixture& f,const receiver_family_observed& data,const recovery_continuous_open_result& actual) {
    EXPECT_EQ(data.ledger.enrollment.bound,1u);EXPECT_EQ(data.ledger.enrollment.reads,1u);EXPECT_EQ(data.ledger.actual_ledger_cut,1u);
    EXPECT_EQ(actual.settlement.state,recovery_install_state::rolled_back);EXPECT_FALSE(actual.owner);
    EXPECT_FALSE(actual.settlement.cleanup_error);EXPECT_FALSE(actual.owner_close_error);
    EXPECT_FALSE(actual.settlement.postcommit_error);EXPECT_FALSE(actual.settlement.notification_error);
    EXPECT_FALSE(actual.settlement.unexpected_commit_observed);ASSERT_NO_THROW(actual_lease_retired(f.member));
}
}
TEST(ContinuousResourceStartup, ActualReceiverBindingReadsBackNewRowAndExactCountersOnOriginalWrite) {
    fixture f;const auto policy=f.policy();receiver_family_observed data;receiver_expected_usage(data,policy);recovery_continuous_open_result actual;
    {receiver_family_probe selected(data);actual=recovery_continuous_producer::open(f.config(),policy);}
    ASSERT_EQ(data.receiver_cuts,1u);EXPECT_EQ(data.after_receiver-data.before_receiver,2);
    EXPECT_EQ(actual.settlement.primary_error,data.ledger.enrollment.sentinel);expect_receiver_failure_ending(f,data,actual);
}
TEST(ContinuousResourceStartup, ActualSharedReceiverAliasValidatesPriorCompleteRowAndDistinctChannelUsage) {
    fixture f;auto policy=f.policy();policy.receiver=recovery_continuous_receiver_profile::full_canonical_v2;
    auto alias=policy.contributions.front();alias.profile.binding.channel+="_second";policy.contributions.push_back(std::move(alias));
    receiver_family_observed data;data.stop_after=2;receiver_expected_usage(data,policy);recovery_continuous_open_result actual;
    {receiver_family_probe selected(data);actual=recovery_continuous_producer::open(f.config(),policy);}
    ASSERT_EQ(data.receiver_cuts,2u);EXPECT_EQ(data.after_receiver-data.before_receiver,4);
    EXPECT_EQ(actual.settlement.primary_error,data.ledger.enrollment.sentinel);expect_receiver_failure_ending(f,data,actual);
}
TEST(ContinuousResourceStartup, ActualReceiverInsertDenialPreservesFirstErrorAndKnownWholeRollback) {
    fixture f;const auto policy=f.policy();receiver_family_observed data;data.deny_insert=true;receiver_expected_usage(data,policy);recovery_continuous_open_result actual;
    {receiver_family_probe selected(data);actual=recovery_continuous_producer::open(f.config(),policy);}
    require_actual_enrollment_end_before_restriction_context_retires(f);
    EXPECT_TRUE(data.restriction_installed);EXPECT_EQ(data.insert_denials,1u);EXPECT_EQ(data.usage_denials,0u);EXPECT_EQ(data.receiver_cuts,0u);
    ASSERT_TRUE(actual.settlement.primary_error);EXPECT_NE(actual.settlement.primary_error,data.ledger.enrollment.sentinel);
    EXPECT_THROW(std::rethrow_exception(actual.settlement.primary_error),db_error);expect_receiver_failure_ending(f,data,actual);
}
TEST(ContinuousResourceStartup, ActualReceiverUsageCasDenialCannotPublishItsAlreadyInsertedChannel) {
    fixture f;const auto policy=f.policy();receiver_family_observed data;data.deny_usage=true;receiver_expected_usage(data,policy);recovery_continuous_open_result actual;
    {receiver_family_probe selected(data);actual=recovery_continuous_producer::open(f.config(),policy);}
    require_actual_enrollment_end_before_restriction_context_retires(f);
    EXPECT_TRUE(data.restriction_installed);EXPECT_EQ(data.insert_denials,0u);EXPECT_GT(data.usage_denials,0u);EXPECT_EQ(data.receiver_cuts,0u);
    ASSERT_TRUE(actual.settlement.primary_error);EXPECT_NE(actual.settlement.primary_error,data.ledger.enrollment.sentinel);
    EXPECT_THROW(std::rethrow_exception(actual.settlement.primary_error),db_error);expect_receiver_failure_ending(f,data,actual);
}
#endif
