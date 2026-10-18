#include <gtest/gtest.h>
#include <lattice/lattice.hpp>
#include <lattice/store_resource_context.hpp>
#include "../../Sources/LatticeCore/src/ordinary_resource_cold_restart_test_probe.hpp"
#include "../../Sources/LatticeCore/src/ordinary_resource_normal_startup.hpp"
#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include <nlohmann/json.hpp>
#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <optional>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <fcntl.h>
#include <poll.h>
#include <pwd.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

// Each invocation needs an independently prepared, fresh ACTUAL test account
// or VM snapshot and an approved package containing these actual test images.
// No HOME override, fake epoch, registry bypass or fabricated ending is used.
// Scenario selection only chooses a control; production independently checks
// account/Host/package/plan/roles/objects on every actual activation attempt.
namespace {
using json=nlohmann::json;
using cut=lattice::detail::resource_cold_restart_test::cut;
using clock_type=std::chrono::steady_clock;
std::string home(){passwd p{},*found=nullptr;std::array<char,16384> b{};
    if(::getpwuid_r(::geteuid(),&p,b.data(),b.size(),&found)||found!=&p||!p.pw_dir||p.pw_dir[0]!='/')throw std::runtime_error("actual account required");return p.pw_dir;}
std::string required(const char* key){const char* value=std::getenv(key);if(!value||!*value)throw std::runtime_error(std::string("required control selection: ")+key);return value;}
unsigned selected_occurrence(){const auto raw=required("LATTICE_RESOURCE_COLD_OCCURRENCE");std::size_t used=0;const auto value=std::stoul(raw,&used);
    if(used!=raw.size()||value<1||value>4096)throw std::runtime_error("bounded occurrence required");return static_cast<unsigned>(value);}
json record(const std::filesystem::path& p){std::ifstream file(p,std::ios::binary|std::ios::ate);
    const auto length=file.tellg();if(!file||length<=0||length>128*1024)throw std::runtime_error("actual bounded record required");
    std::string raw(static_cast<std::size_t>(length),'\0');file.seekg(0);file.read(raw.data(),static_cast<std::streamsize>(raw.size()));
    if(!file||file.gcount()!=static_cast<std::streamsize>(raw.size()))throw std::runtime_error("complete actual record required");
    auto value=json::parse(raw);if(value.dump()!=raw)throw std::runtime_error("canonical actual record required");return value;}

struct identity {std::uint64_t device=0,inode=0;bool operator==(const identity&)const=default;};
identity inode(const std::filesystem::path& p){struct stat value{};if(::lstat(p.c_str(),&value))throw std::runtime_error("actual object required");return {static_cast<std::uint64_t>(value.st_dev),static_cast<std::uint64_t>(value.st_ino)};}
identity identity_of(const json& value){return {value.at("device").get<std::uint64_t>(),value.at("inode").get<std::uint64_t>()};}
using namespace_snapshot=std::map<std::filesystem::path,identity>;
namespace_snapshot control_namespace(const json& plan){namespace_snapshot result;
    // Complete shallow census of each actual control root, including pending
    // publication names. No recursive traversal of unknown occupied vaults.
    for(const auto& root:plan.at("roots"))for(const auto& row:std::filesystem::directory_iterator(root.at("controlPath").get<std::string>()))
        if(!result.emplace(row.path(),inode(row.path())).second)throw std::runtime_error("unique actual control names required");
    return result;
}
void write_new_record(const std::filesystem::path& path,const json& value){
    const auto raw=value.dump();if(raw.empty()||raw.size()>128*1024)throw std::runtime_error("bounded new control record required");
    const int fd=::open(path.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW,0600);if(fd<0)throw std::runtime_error("new control record failed");
    struct ending {int fd;~ending(){if(fd>=0)::close(fd);}} owned{fd};if(::fchmod(fd,0600))throw std::runtime_error("control record mode failed");
    std::size_t at=0;while(at<raw.size()){const auto n=::write(fd,raw.data()+at,raw.size()-at);if(n<0&&errno==EINTR)continue;if(n<=0)throw std::runtime_error("control record write failed");at+=static_cast<std::size_t>(n);}
    if(::fsync(fd))throw std::runtime_error("control record sync failed");owned.fd=-1;if(::close(fd))throw std::runtime_error("control record ending failed");
}
unsigned cut_index(const std::string& name){for(unsigned i=0;i<static_cast<unsigned>(cut::count);++i)if(name==lattice::detail::resource_cold_restart_test::names[i])return i;throw std::runtime_error("known cut required");}
struct child final {
    pid_t pid=-1;int read=-1;bool reaped=false;
    ~child(){if(read>=0)::close(read);if(pid>1&&!reaped){::kill(pid,SIGKILL);int status=0;while(::waitpid(pid,&status,0)<0&&errno==EINTR){}}}
    child(const child&)=delete;child& operator=(const child&)=delete;
    child(pid_t value,int pipe):pid(value),read(pipe){}
    int ending(){const auto end=clock_type::now()+std::chrono::seconds(150);int status=0;
        for(;;){const auto result=::waitpid(pid,&status,WNOHANG);if(result==pid){reaped=true;return status;}if(result<0&&errno!=EINTR)throw std::runtime_error("actual child ending required");
            if(clock_type::now()>=end)throw std::runtime_error("child did not end");std::this_thread::sleep_for(std::chrono::milliseconds(10));}}
};
std::unique_ptr<child> launch(const std::string& image,const std::optional<std::pair<std::string,unsigned>>& stop,
    std::optional<rlim_t> descriptor_limit={}){
    int pipe[2]{-1,-1};if(::pipe(pipe))throw std::runtime_error("stop pipe failed");
    const auto p=::fork();if(p<0){::close(pipe[0]);::close(pipe[1]);throw std::runtime_error("actual child failed");}
    if(p==0){::close(pipe[0]);
        if(descriptor_limit){struct rlimit actual{};if(::getrlimit(RLIMIT_NOFILE,&actual)||actual.rlim_cur<*descriptor_limit)::_exit(77);
            actual.rlim_cur=*descriptor_limit;if(::setrlimit(RLIMIT_NOFILE,&actual))::_exit(77);}
        if(stop){const auto count=std::to_string(stop->second),fd=std::to_string(pipe[1]);
            ::execl(image.c_str(),image.c_str(),"test-activate-resource-cold-restart",stop->first.c_str(),count.c_str(),fd.c_str(),static_cast<char*>(nullptr));}
        else {::close(pipe[1]);::execl(image.c_str(),image.c_str(),"activate-staged-resource-cold-restart",static_cast<char*>(nullptr));}
        ::_exit(77);
    }
    ::close(pipe[1]);return std::make_unique<child>(p,pipe[0]);
}
// Returns false only for an actual refusal BEFORE the selected cut; callers
// must separately prove why that ending is the intended capacity oracle.
bool kill_at(child& process,const std::string& point,unsigned occurrence,int& refused_status){
    const auto end=clock_type::now()+std::chrono::seconds(140);std::array<std::uint32_t,3> message{};std::size_t at=0;
    while(at<sizeof(message)){
        if(clock_type::now()>=end)throw std::runtime_error("actual stop cut not observed");pollfd pipe{process.read,POLLIN|POLLHUP,0};
        const int ready=::poll(&pipe,1,100);if(ready<0&&errno==EINTR)continue;if(ready<0)throw std::runtime_error("stop poll failed");if(!ready)continue;
        const auto n=::read(process.read,reinterpret_cast<char*>(message.data())+at,sizeof(message)-at);
        if(n<0&&errno==EINTR)continue;if(n<0)throw std::runtime_error("stop read failed");
        if(n==0){refused_status=process.ending();return false;}at+=static_cast<std::size_t>(n);
    }
    if(message[0]!=0xc01d7101u||message[1]!=cut_index(point)||message[2]!=occurrence)throw std::runtime_error("exact real stop marker required");
    if(::kill(process.pid,SIGKILL))throw std::runtime_error("actual installer kill failed");
    const int status=process.ending();if(!WIFSIGNALED(status)||WTERMSIG(status)!=SIGKILL)throw std::runtime_error("SIGKILL ending required");return true;
}
struct fixture {
    std::string actual_home=home();std::filesystem::path installation=actual_home+"/.lattice-resource-installation";
    json plan=record(installation/"restart.v1"),root,store;std::string image,slot;std::filesystem::path control,logical,generation;
    fixture(){if(std::filesystem::exists(installation/"active.v1"))throw std::runtime_error("fresh actual test-account snapshot required");
        image=plan.at("host").at("path").get<std::string>();
        // Preserve the original concrete public-factory seed contract.
        bool selected=false;for(const auto& r:plan.at("roots"))for(const auto& s:r.at("stores"))if(s.at("name")=="memory.sqlite"&&r.at("logicalPath")==actual_home+"/.claude"){root=r;store=s;selected=true;}
        if(!selected)throw std::runtime_error("genuine old memory.sqlite seed required");
        control=root.at("controlPath").get<std::string>();logical=root.at("logicalPath").get<std::string>();generation=root.at("generationPath").get<std::string>();
        const auto name=store.at("name").get<std::string>();slot=picosha2::hash256_hex_string(std::vector<std::uint8_t>(name.begin(),name.end()));
        if(inode(logical/name)!=identity_of(store.at("main")))throw std::runtime_error("actual original seed identity required");
    }
    std::filesystem::path ticket_path()const{return control/("cold-issue-"+slot+".v1");}
    std::filesystem::path evidence()const {const auto path=std::filesystem::path(actual_home)/"localdev"/"resource-cold-crash-evidence";
        if(std::filesystem::exists(path))throw std::runtime_error("fresh evidence directory required");std::filesystem::create_directories(path);return path;}
    void absent_active()const{EXPECT_FALSE(std::filesystem::exists(installation/"active.v1"));}
    int activate(){auto process=launch(image,{});const int status=process->ending();if(!WIFEXITED(status))throw std::runtime_error("actual normal activation ending required");return WEXITSTATUS(status);}
    void public_factory_oracle(){
        ASSERT_TRUE(std::filesystem::exists(installation/"retirement.complete.v1"));ASSERT_TRUE(std::filesystem::exists(installation/"active.v1"));
        const auto ticket=record(ticket_path());ASSERT_EQ(inode(generation/"memory.sqlite"),identity_of(store.at("main")));
        ASSERT_EQ(inode(logical/"memory.sqlite"),identity_of(ticket.at("stubs").at("main")));
        struct stat alias{};ASSERT_EQ(::lstat((logical/"memory.sqlite").c_str(),&alias),0);EXPECT_EQ(alias.st_mode&07777,0);EXPECT_EQ(alias.st_size,0);
        EXPECT_FALSE(std::filesystem::exists(control/slot/"main.move.v1.swap"));
        lattice::configuration config((logical/"memory.sqlite").string());lattice::lattice_db owner(config,true);
        ASSERT_TRUE(lattice::detail::store_resource_profile_active());const auto identity=owner.db().physical_identity("main",{},true);ASSERT_TRUE(identity);EXPECT_EQ(identity->filename,(generation/"memory.sqlite").string());
        const auto rows=owner.db().query("SELECT value FROM resource_bootstrap WHERE globalId='old'");ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::int64_t>(rows[0].at("value")),41);
        EXPECT_THROW(owner.db().handle(),lattice::db_error);
    }
};
}

TEST(StoreResourceColdCrash, SelectedActualCutKillsAndPublicFactoryResumes) {
    ASSERT_EQ(required("LATTICE_RESOURCE_COLD_SCENARIO"),"resume");
    fixture f;const auto point=required("LATTICE_RESOURCE_COLD_CUT");const auto occurrence=selected_occurrence();
    auto process=launch(f.image,std::pair{point,static_cast<unsigned>(occurrence)});int status=0;
    ASSERT_TRUE(kill_at(*process,point,static_cast<unsigned>(occurrence),status));
    // An active-publication cut may legitimately leave the actual final anchor
    // visible before parent resync. It must be this exact epoch, never a fake.
    if(std::filesystem::exists(f.installation/"active.v1"))EXPECT_EQ(record(f.installation/"active.v1").at("epoch"),f.plan.at("epoch"));
    std::vector<std::pair<std::filesystem::path,identity>> staged;
    for(const auto& [path,original]:control_namespace(f.plan))if(path.filename().string().starts_with("cold-stage-"))staged.emplace_back(path,original);
    // Fresh public command, not an internal state-object continuation.
    ASSERT_EQ(f.activate(),0);f.public_factory_oracle();
    for(const auto& [path,original]:staged)EXPECT_EQ(inode(path),original);
}

TEST(StoreResourceColdCrash, ActualIssuedReplacementRefusesWithoutUnlinkOrAdoption) {
    const auto scenario=required("LATTICE_RESOURCE_COLD_SCENARIO");
    const std::set<std::string> scenarios{"replace-entry","replace-stub","replace-vault","missing-stub","extra-vault","extra-member",
        "symlink-stub","hardlink-stub","badmode-stub","nonzero-stub","missing-ticket","tamper-ticket","missing-journal"};
    ASSERT_TRUE(scenarios.contains(scenario));fixture f;
    const std::string point=(scenario=="missing-journal"||scenario=="missing-ticket")?"stub-transferred":"ticket-durable";
    auto process=launch(f.image,std::pair{point,1u});int status=0;ASSERT_TRUE(kill_at(*process,point,1,status));
    const auto ticket=record(f.ticket_path());const auto vault=f.control/ticket.at("vaultName").get<std::string>();
    const auto member=std::filesystem::exists(vault/"member")?vault/"member":f.control/f.slot;
    std::filesystem::path source,saved;identity expected{},foreign{};bool source_present=true,saved_present=false;
    if(scenario=="missing-ticket"||scenario=="tamper-ticket")source=f.ticket_path();
    else if(scenario=="missing-journal")source=member/"main.move.v1";
    else if(scenario=="extra-vault")source=vault/"unexpected";
    else if(scenario=="extra-member")source=member/"unexpected";
    else if(scenario=="replace-vault")source=vault;
    else if(scenario=="replace-entry")source=member/"entry.lock";
    else source=vault/"main.stub";
    if(source==vault/"main.stub")ASSERT_EQ(inode(source),identity_of(ticket.at("stubs").at("main")));
    if(scenario=="replace-entry")ASSERT_EQ(inode(source),identity_of(ticket.at("entry")));
    if(scenario=="replace-vault")ASSERT_EQ(inode(source),identity_of(ticket.at("vault")));
    if(scenario=="missing-journal"){const auto journal=record(source);EXPECT_EQ(journal.at("format"),"lattice-resource-file-move-v2");
        EXPECT_EQ(identity_of(journal.at("original")),identity_of(f.store.at("main")));EXPECT_EQ(identity_of(journal.at("stub")),identity_of(ticket.at("stubs").at("main")));}
    const auto create_zero=[&](const std::filesystem::path& path,mode_t mode){const int fd=::open(path.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW,0600);
        if(fd<0||::fchmod(fd,mode)||::fsync(fd)||::close(fd))throw std::runtime_error("actual foreign control creation failed");};
    if(scenario=="extra-vault"||scenario=="extra-member")create_zero(source,0600);
    else if(scenario=="badmode-stub"||scenario=="nonzero-stub"){
        expected=inode(source);ASSERT_EQ(expected,identity_of(ticket.at("stubs").at("main")));ASSERT_EQ(::chmod(source.c_str(),0600),0);
        if(scenario=="nonzero-stub"){const int fd=::open(source.c_str(),O_WRONLY|O_NOFOLLOW);ASSERT_GE(fd,0);ASSERT_EQ(::write(fd,"x",1),1);ASSERT_EQ(::fsync(fd),0);ASSERT_EQ(::close(fd),0);ASSERT_EQ(::chmod(source.c_str(),0000),0);}
    }else{
        expected=inode(source);saved=f.evidence()/"issued-original";ASSERT_EQ(::rename(source.c_str(),saved.c_str()),0);saved_present=true;ASSERT_EQ(inode(saved),expected);
        if(scenario=="missing-stub"||scenario=="missing-ticket"||scenario=="missing-journal")source_present=false;
        else if(scenario=="replace-vault"){ASSERT_EQ(::mkdir(source.c_str(),0700),0);ASSERT_EQ(::chmod(source.c_str(),0700),0);}
        else if(scenario=="symlink-stub")ASSERT_EQ(::symlink(saved.c_str(),source.c_str()),0);
        else if(scenario=="hardlink-stub")ASSERT_EQ(::link(saved.c_str(),source.c_str()),0);
        else if(scenario=="tamper-ticket"){
            auto changed=ticket;changed["binding"]["planSHA256"]=std::string(64,'0');write_new_record(source,changed);
        }else create_zero(source,scenario=="replace-stub"?0000:0600);
    }
    if(source_present){foreign=inode(source);if(scenario=="replace-entry"||scenario=="replace-stub"||scenario=="replace-vault")EXPECT_FALSE(foreign==expected);}
    EXPECT_EQ(f.activate(),74);f.absent_active();
    if(source_present)EXPECT_EQ(inode(source),foreign);else EXPECT_FALSE(std::filesystem::exists(source));
    if(saved_present)EXPECT_EQ(inode(saved),expected);
    if(scenario!="missing-ticket"&&scenario!="tamper-ticket")EXPECT_EQ(record(f.ticket_path()),ticket);
    if(scenario=="hardlink-stub"){struct stat current{};ASSERT_EQ(::lstat(source.c_str(),&current),0);EXPECT_EQ(current.st_nlink,2);}
    if(scenario=="badmode-stub"||scenario=="nonzero-stub"){struct stat current{};ASSERT_EQ(::lstat(source.c_str(),&current),0);
        EXPECT_EQ(current.st_mode&07777,scenario=="badmode-stub"?0600:0000);EXPECT_EQ(current.st_size,scenario=="nonzero-stub"?1:0);}
    EXPECT_EQ(inode(f.logical/"memory.sqlite"),identity_of(f.store.at("main")));
}

TEST(StoreResourceColdCrash, UnissuedOccupiedCanonicalSlotIsNeverAdopted) {
    ASSERT_EQ(required("LATTICE_RESOURCE_COLD_SCENARIO"),"foreign-canonical");
    fixture f;const std::string point="preparation-synced";auto process=launch(f.image,std::pair{point,1u});int status=0;ASSERT_TRUE(kill_at(*process,point,1,status));
    ASSERT_FALSE(std::filesystem::exists(f.ticket_path()));const auto foreign_path=f.control/f.slot;ASSERT_EQ(::mkdir(foreign_path.c_str(),0700),0);ASSERT_EQ(::chmod(foreign_path.c_str(),0700),0);const auto foreign=inode(foreign_path);
    EXPECT_EQ(f.activate(),74);f.absent_active();EXPECT_EQ(inode(foreign_path),foreign);EXPECT_FALSE(std::filesystem::exists(f.ticket_path()));
    EXPECT_FALSE(std::filesystem::exists(f.control/"denial.intent.v1"));EXPECT_EQ(inode(f.logical/"memory.sqlite"),identity_of(f.store.at("main")));
}

TEST(StoreResourceColdCrash, EightRealUnissuedAttemptsRemainAndExhaustBeforeDenial) {
    ASSERT_EQ(required("LATTICE_RESOURCE_COLD_SCENARIO"),"exhaust-store");
    fixture f;std::vector<identity> originals;const std::string point="preparation-synced";
    for(unsigned attempt=0;attempt!=8;++attempt){auto process=launch(f.image,std::pair{point,1u});int status=0;ASSERT_TRUE(kill_at(*process,point,1,status));
        const auto vault=f.control/("cold-stage-"+f.slot+"-"+std::to_string(attempt));originals.push_back(inode(vault));ASSERT_FALSE(std::filesystem::exists(f.ticket_path()));}
    EXPECT_EQ(f.activate(),74);f.absent_active();EXPECT_FALSE(std::filesystem::exists(f.control/"denial.intent.v1"));
    for(unsigned attempt=0;attempt!=8;++attempt)EXPECT_EQ(inode(f.control/("cold-stage-"+f.slot+"-"+std::to_string(attempt))),originals[attempt]);
    EXPECT_EQ(inode(f.logical/"memory.sqlite"),identity_of(f.store.at("main")));
}

TEST(StoreResourceColdCrash, SixtyFivePlannedStoresRefuseBeforeAnyColdAllocationOrDenial) {
    ASSERT_EQ(required("LATTICE_RESOURCE_COLD_SCENARIO"),"capacity");
    fixture f;bool over=false;for(const auto& root:f.plan.at("roots"))if(root.at("stores").size()>64)over=true;
    ASSERT_TRUE(over) << "actual public preparation of a permitted dynamic root with 65 stores required";
    const auto before=control_namespace(f.plan);EXPECT_EQ(f.activate(),74);f.absent_active();EXPECT_EQ(control_namespace(f.plan),before);
    for(const auto& root:f.plan.at("roots")){
        const auto control=std::filesystem::path(root.at("controlPath").get<std::string>()),logical=std::filesystem::path(root.at("logicalPath").get<std::string>());
        EXPECT_FALSE(std::filesystem::exists(control/"denial.intent.v1"));
        for(const auto& store:root.at("stores")){const auto name=store.at("name").get<std::string>(),slot=picosha2::hash256_hex_string(std::vector<std::uint8_t>(name.begin(),name.end()));
            EXPECT_EQ(inode(logical/name),identity_of(store.at("main")));EXPECT_FALSE(std::filesystem::exists(control/("cold-issue-"+slot+".v1")));}
    }
}

TEST(StoreResourceColdCrash, ActualPendingTicketsAndAbandonedVaultsExhaustAggregateRootBudget) {
    ASSERT_EQ(required("LATTICE_RESOURCE_COLD_SCENARIO"),"exhaust-root");
    fixture f;bool enough=false;for(const auto& root:f.plan.at("roots"))if(root.at("stores").size()>=3)enough=true;
    ASSERT_TRUE(enough) << "genuine prepared dynamic root with at least three old stores required";
    std::map<std::filesystem::path,identity> preserved;bool refused=false;
    // Actual ticket-pending cuts leave reservation + vault + pending metadata.
    // After seven failed attempts let the eighth ticket seal, then kill the
    // next store's pending ticket. No plan/marker is synthesized by the test.
    for(unsigned round=0;round!=128&&!refused;++round){
        unsigned abandoned=0;bool selected=false;
        for(const auto& root:f.plan.at("roots")){
            const auto control=std::filesystem::path(root.at("controlPath").get<std::string>());
            for(const auto& store:root.at("stores")){
                const auto name=store.at("name").get<std::string>(),slot=picosha2::hash256_hex_string(std::vector<std::uint8_t>(name.begin(),name.end()));
                if(std::filesystem::exists(control/("cold-issue-"+slot+".v1")))continue;
                for(unsigned attempt=0;attempt!=8;++attempt)if(std::filesystem::exists(control/("cold-stage-"+slot+"-"+std::to_string(attempt))))++abandoned;
                selected=true;break;
            }
            if(selected)break;
        }
        ASSERT_TRUE(selected);ASSERT_LT(abandoned,8u);
        const unsigned occurrence=abandoned==7?4:2;const std::string point="record-pending-created";
        auto process=launch(f.image,std::pair{point,occurrence});int status=0;
        if(!kill_at(*process,point,occurrence,status)){
            ASSERT_TRUE(WIFEXITED(status));ASSERT_EQ(WEXITSTATUS(status),74);refused=true;
        }
        for(const auto& [path,original]:preserved)EXPECT_EQ(inode(path),original);
        for(const auto& root:f.plan.at("roots")){
            const auto control=std::filesystem::path(root.at("controlPath").get<std::string>());
            EXPECT_FALSE(std::filesystem::exists(control/"denial.intent.v1"));
            for(const auto& entry:std::filesystem::directory_iterator(control)){
                const auto name=entry.path().filename().string();
                if(name.starts_with("cold-stage-")||name.starts_with(".record-"))preserved.emplace(entry.path(),inode(entry.path()));
            }
        }
    }
    ASSERT_TRUE(refused) << "bounded aggregate exhaustion must be an actual refusal";
    bool charged_root=false;
    for(const auto& root:f.plan.at("roots")){
        const auto control=std::filesystem::path(root.at("controlPath").get<std::string>());unsigned debt=0;
        for(const auto& store:root.at("stores")){
            const auto name=store.at("name").get<std::string>(),slot=picosha2::hash256_hex_string(std::vector<std::uint8_t>(name.begin(),name.end()));
            std::optional<unsigned> issued;const auto ticket=control/("cold-issue-"+slot+".v1");if(std::filesystem::exists(ticket))issued=record(ticket).at("attempt").get<unsigned>();
            unsigned unissued=0;for(unsigned attempt=0;attempt!=8;++attempt){
                if(issued&&*issued==attempt)continue;
                if(std::filesystem::exists(control/("cold-reserve-"+slot+"-"+std::to_string(attempt)+".v1")))++debt;
                if(std::filesystem::exists(control/("cold-stage-"+slot+"-"+std::to_string(attempt)))){++debt;++unissued;}
            }
            EXPECT_LT(unissued,8u) << "aggregate refusal must not be explained by per-store exhaustion";
        }
        for(const auto& entry:std::filesystem::directory_iterator(control))if(entry.path().filename().string().starts_with(".record-"))++debt;
        if(debt>=62&&debt<=64)charged_root=true;
    }
    EXPECT_TRUE(charged_root) << "actual final+pending metadata and unissued vaults must account for the root refusal";
    f.absent_active();for(const auto& [path,original]:preserved)EXPECT_EQ(inode(path),original);
    EXPECT_EQ(inode(f.logical/"memory.sqlite"),identity_of(f.store.at("main")));
}

TEST(StoreResourceColdCrash, ActualReadOnlyLogicalModeRefusesWithoutManufacturedWritePermission) {
    ASSERT_EQ(required("LATTICE_RESOURCE_COLD_SCENARIO"),"logical-readonly");fixture f;bool selected=false;
    for(const auto& root:f.plan.at("roots"))if((root.at("logicalMode").get<unsigned>()&0300)!=0300){
        selected=true;struct stat before{};const auto logical=root.at("logicalPath").get<std::string>();ASSERT_EQ(::lstat(logical.c_str(),&before),0);
        EXPECT_EQ(before.st_mode&07777,root.at("logicalMode").get<unsigned>());
    }
    ASSERT_TRUE(selected) << "logical root must really be read-only before actual public package preparation";
    const auto before=control_namespace(f.plan);EXPECT_EQ(f.activate(),74);f.absent_active();EXPECT_EQ(control_namespace(f.plan),before);
    for(const auto& root:f.plan.at("roots")){const auto logical=root.at("logicalPath").get<std::string>(),control=root.at("controlPath").get<std::string>();struct stat after{};
        ASSERT_EQ(::lstat(logical.c_str(),&after),0);EXPECT_EQ(after.st_mode&07777,root.at("logicalMode").get<unsigned>());
        EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(control)/"denial.intent.v1"));
        for(const auto& store:root.at("stores")){const auto name=store.at("name").get<std::string>(),slot=picosha2::hash256_hex_string(std::vector<std::uint8_t>(name.begin(),name.end()));
            EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(control)/("cold-issue-"+slot+".v1")));}
    }
}

TEST(StoreResourceColdCrash, ActualUnsupportedRenameDomainRefusesBeforeColdAllocation) {
    ASSERT_EQ(required("LATTICE_RESOURCE_COLD_SCENARIO"),"rename-domain");fixture f;bool unsupported=false;
    for(const auto& root:f.plan.at("roots")){
        std::array<struct statfs,3> volumes{};const std::array<const char*,3> fields{"logicalPath","generationPath","controlPath"};
        for(unsigned at=0;at!=3;++at){ASSERT_EQ(::statfs(root.at(fields[at]).get<std::string>().c_str(),&volumes[at]),0);
            if(std::strncmp(volumes[at].f_fstypename,"apfs",sizeof(volumes[at].f_fstypename)))unsupported=true;}
        if(volumes[0].f_fsid.val[0]!=volumes[2].f_fsid.val[0]||volumes[0].f_fsid.val[1]!=volumes[2].f_fsid.val[1]||
            volumes[1].f_fsid.val[0]!=volumes[2].f_fsid.val[0]||volumes[1].f_fsid.val[1]!=volumes[2].f_fsid.val[1])unsupported=true;
        const auto device=identity_of(root.at("control")).device;
        for(const auto& store:root.at("stores")){if(identity_of(store.at("main")).device!=device)unsupported=true;
            for(const auto& aux:store.at("auxiliaries"))if(identity_of(aux.at("identity")).device!=device)unsupported=true;}
    }
    ASSERT_TRUE(unsupported) << "real unsupported/mounted domain fixture required, not fabricated stat data";
    const auto before=control_namespace(f.plan);EXPECT_EQ(f.activate(),74);f.absent_active();EXPECT_EQ(control_namespace(f.plan),before);
    for(const auto& root:f.plan.at("roots")){const auto control=std::filesystem::path(root.at("controlPath").get<std::string>());EXPECT_FALSE(std::filesystem::exists(control/"denial.intent.v1"));
        for(const auto& store:root.at("stores")){const auto name=store.at("name").get<std::string>(),slot=picosha2::hash256_hex_string(std::vector<std::uint8_t>(name.begin(),name.end()));
            EXPECT_FALSE(std::filesystem::exists(control/("cold-issue-"+slot+".v1")));}
    }
}

TEST(StoreResourceColdCrash, ActualActivePublicationCutResumesWithoutRetirementOrMemberReplay) {
    ASSERT_EQ(required("LATTICE_RESOURCE_COLD_SCENARIO"),"active-resume");fixture f;const std::string point="active-record";
    auto process=launch(f.image,std::pair{point,1u});int status=0;ASSERT_TRUE(kill_at(*process,point,1,status));
    const auto active=record(f.installation/"active.v1"),expected=record(f.installation/"retirement.expected.v1"),complete=record(f.installation/"retirement.complete.v1");
    const auto member=record(f.control/f.slot/"member.v1");const auto member_directory=inode(f.control/f.slot);
    ASSERT_EQ(f.activate(),0);EXPECT_EQ(record(f.installation/"active.v1"),active);EXPECT_EQ(record(f.installation/"retirement.expected.v1"),expected);
    EXPECT_EQ(record(f.installation/"retirement.complete.v1"),complete);EXPECT_EQ(record(f.control/f.slot/"member.v1"),member);EXPECT_EQ(inode(f.control/f.slot),member_directory);
    f.public_factory_oracle();
}

TEST(StoreResourceColdCrash, ActiveReplayKeepsNormalDeletedAndRecreatedMemberWithLiveConsumer) {
    ASSERT_EQ(required("LATTICE_RESOURCE_COLD_SCENARIO"),"active-recreated");fixture f;ASSERT_EQ(f.activate(),0);
    const auto active=record(f.installation/"active.v1"),retirement=record(f.installation/"retirement.complete.v1"),cold_ticket=record(f.ticket_path());
    const auto logical=(f.logical/"memory.sqlite").string();const auto end=clock_type::now()+std::chrono::seconds(20);
    auto old=std::make_unique<lattice::lattice_db>(lattice::configuration(logical),true);
    auto deletion=lattice::detail::store_deletion_operation::begin(logical,end);
    old->close();old.reset();deletion.finish();EXPECT_TRUE(deletion.outcome().ending_observed);EXPECT_TRUE(deletion.outcome().physical_ended);
    lattice::lattice_db current(lattice::configuration(logical),true);
    current.db().execute("CREATE TABLE recreated_bootstrap(globalId TEXT NOT NULL UNIQUE,value INTEGER NOT NULL)");
    current.db().execute("INSERT INTO recreated_bootstrap(globalId,value) VALUES('current',84)");
    const auto member=record(f.control/f.slot/"member.v1");ASSERT_FALSE(identity_of(member.at("main"))==identity_of(f.store.at("main")));
    const auto member_directory=inode(f.control/f.slot),main=inode(f.generation/"memory.sqlite");
    // This actual consumer retains physical SH during the new command. A cold
    // replay of stale member controls would refuse/wait; active resume must not.
    ASSERT_EQ(f.activate(),0);EXPECT_EQ(inode(f.control/f.slot),member_directory);EXPECT_EQ(inode(f.generation/"memory.sqlite"),main);
    EXPECT_EQ(record(f.control/f.slot/"member.v1"),member);EXPECT_EQ(record(f.ticket_path()),cold_ticket);
    EXPECT_EQ(record(f.installation/"active.v1"),active);EXPECT_EQ(record(f.installation/"retirement.complete.v1"),retirement);
    const auto rows=current.db().query("SELECT value FROM recreated_bootstrap WHERE globalId='current'");ASSERT_EQ(rows.size(),1u);EXPECT_EQ(std::get<std::int64_t>(rows[0].at("value")),84);
}

TEST(StoreResourceColdCrash, MismatchedPublishedActiveRootOrRetirementIsNotRepaired) {
    const auto scenario=required("LATTICE_RESOURCE_COLD_SCENARIO");
    ASSERT_TRUE(scenario=="active-mismatch"||scenario=="root-mismatch"||scenario=="retirement-expected-mismatch"||scenario=="retirement-complete-mismatch");
    fixture f;ASSERT_EQ(f.activate(),0);std::filesystem::path source;
    if(scenario=="active-mismatch")source=f.installation/"active.v1";
    else if(scenario=="root-mismatch")source=f.control/"root.v1";
    else if(scenario=="retirement-expected-mismatch")source=f.installation/"retirement.expected.v1";
    else source=f.installation/"retirement.complete.v1";
    auto changed=record(source);const auto original=inode(source);const auto saved=f.evidence()/"published-original";
    ASSERT_EQ(::rename(source.c_str(),saved.c_str()),0);ASSERT_EQ(inode(saved),original);
    if(scenario=="active-mismatch"||scenario=="root-mismatch")changed["epoch"]=std::string(64,'0');
    else if(scenario=="retirement-expected-mismatch")changed["planSHA256"]=std::string(64,'0');else changed["expectedSHA256"]=std::string(64,'0');
    write_new_record(source,changed);const auto foreign=inode(source);const auto member=record(f.control/f.slot/"member.v1");
    EXPECT_EQ(f.activate(),74);EXPECT_EQ(inode(source),foreign);EXPECT_EQ(record(source),changed);EXPECT_EQ(inode(saved),original);
    EXPECT_EQ(record(f.control/f.slot/"member.v1"),member);EXPECT_EQ(inode(f.generation/"memory.sqlite"),identity_of(f.store.at("main")));
}

TEST(StoreResourceColdCrash, ActualDescriptorCapacityRefusesBeforeColdAllocationOrDenial) {
    ASSERT_EQ(required("LATTICE_RESOURCE_COLD_SCENARIO"),"fd-capacity");fixture f;
    // Actual per-child kernel limit. No fake fd census, budget response or
    // producer marker is supplied. Earlier real image/open refusal is also
    // retained as the first error; no post-denial allocation is permitted.
    const auto before=control_namespace(f.plan);auto process=launch(f.image,{},rlim_t{32});const int status=process->ending();
    ASSERT_TRUE(WIFEXITED(status));EXPECT_EQ(WEXITSTATUS(status),74);f.absent_active();EXPECT_EQ(control_namespace(f.plan),before);
    for(const auto& root:f.plan.at("roots")){
        const auto control=std::filesystem::path(root.at("controlPath").get<std::string>()),logical=std::filesystem::path(root.at("logicalPath").get<std::string>());
        EXPECT_FALSE(std::filesystem::exists(control/"denial.intent.v1"));
        for(const auto& store:root.at("stores")){const auto name=store.at("name").get<std::string>(),slot=picosha2::hash256_hex_string(std::vector<std::uint8_t>(name.begin(),name.end()));
            EXPECT_EQ(inode(logical/name),identity_of(store.at("main")));EXPECT_FALSE(std::filesystem::exists(control/("cold-issue-"+slot+".v1")));}
    }
}


TEST(ResourceColdRestart, DeclaredContinuousRootsActivateAsExactEmptyFixedLeafRoots) {
    ASSERT_EQ(required("LATTICE_RESOURCE_COLD_SCENARIO"),"continuous-roots");
    fixture f;
    const auto package=record(std::filesystem::path(f.image).parent_path()/"resource-package.v1");
    ASSERT_TRUE(package.contains("continuousContainers"));const auto& declarations=package.at("continuousContainers");
    ASSERT_TRUE(declarations.is_array());ASSERT_FALSE(declarations.empty());ASSERT_LE(declarations.size(),12u);
    ASSERT_EQ(f.plan.at("roots").size(),4u+declarations.size());ASSERT_LE(f.plan.at("roots").size(),16u);
    std::map<std::string,json> actual;
    for(const auto& root:f.plan.at("roots"))ASSERT_TRUE(actual.emplace(root.at("logicalPath").get<std::string>(),root).second);
    std::vector<json> selected;
    for(const auto& declaration:declarations){
        const auto logical=f.actual_home+"/"+declaration.get<std::string>();const auto found=actual.find(logical);ASSERT_NE(found,actual.end());
        const auto& root=found->second;
        const json fixed={{"names",json::array({"store.sqlite"})},{"suffixes",json::array()}};
        EXPECT_EQ(root.at("rule"),fixed);EXPECT_TRUE(root.at("stores").empty());
        const auto generation=std::filesystem::path(root.at("generationPath").get<std::string>());
        EXPECT_TRUE(generation.filename().string().ends_with(".lattice-continuous"));
        EXPECT_EQ(inode(logical),identity_of(root.at("logical")));EXPECT_EQ(inode(generation),identity_of(root.at("generation")));
        struct stat before{};ASSERT_EQ(::lstat(logical.c_str(),&before),0);EXPECT_EQ(before.st_mode&07777,0700);
        ASSERT_TRUE(std::filesystem::is_empty(logical));ASSERT_TRUE(std::filesystem::is_empty(generation));selected.push_back(root);
    }
    // Actual approved Host performs all root admission, denial, old original
    // retirement and activation. No plan, root, profile or ending is invented.
    ASSERT_EQ(f.activate(),0);
    const auto active=record(f.installation/"active.v1");ASSERT_EQ(active.at("roots").size(),f.plan.at("roots").size());
    for(const auto& root:selected){
        bool matched=false;
        for(const auto& current:active.at("roots"))if(current.at("logicalPath")==root.at("logicalPath")){
            auto expected=root;expected.erase("stores");expected.erase("logicalMode");
            const auto control=std::filesystem::path(root.at("controlPath").get<std::string>());
            const auto published=record(control/"root.v1");const auto raw=published.dump();
            expected["rootSHA256"]=picosha2::hash256_hex_string(std::vector<std::uint8_t>(raw.begin(),raw.end()));
            EXPECT_EQ(current,expected);matched=true;
        }
        EXPECT_TRUE(matched);
        const auto logical=std::filesystem::path(root.at("logicalPath").get<std::string>());
        const auto generation=std::filesystem::path(root.at("generationPath").get<std::string>());
        EXPECT_EQ(inode(logical),identity_of(root.at("logical")));EXPECT_EQ(inode(generation),identity_of(root.at("generation")));
        // This SOURCE family prepares directories only. First-start MAIN and
        // genuine continuous enrollment belong to their separate producers.
        EXPECT_TRUE(std::filesystem::is_empty(logical));EXPECT_TRUE(std::filesystem::is_empty(generation));
        for(const auto* suffix:{"","-wal","-shm","-journal"}){
            EXPECT_FALSE(std::filesystem::exists(logical/(std::string("store.sqlite")+suffix)));
            EXPECT_FALSE(std::filesystem::exists(generation/(std::string("store.sqlite")+suffix)));
        }
    }
    EXPECT_EQ(inode(f.generation/"memory.sqlite"),identity_of(f.store.at("main")));
    struct stat old_alias{};ASSERT_EQ(::lstat((f.logical/"memory.sqlite").c_str(),&old_alias),0);
    EXPECT_EQ(old_alias.st_mode&07777,0);EXPECT_EQ(old_alias.st_size,0);
}
