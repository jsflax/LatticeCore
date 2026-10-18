#include "ordinary_resource_memory_route.hpp"
#include "ordinary_resource_cold_restart_test_probe.hpp"
#include "observer_delivery_startup_policy.hpp"
#include "ordinary_resource_normal_startup.hpp"
#include <lattice/db.hpp>
#include <lattice/detail/observer_delivery_startup.hpp>
#include "ordinary_installation_files.hpp"
#include "ordinary_observed_process.hpp"
#include "store_physical_vfs.hpp"
#include "vendor/picosha2/picosha2.h"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <limits>
#include <new>
#include <optional>
#include <set>
#include <thread>
#include <utility>
#if defined(__APPLE__)
#include <TargetConditionals.h>
#endif
#if !defined(__EMSCRIPTEN__) && ((defined(__APPLE__) && TARGET_OS_OSX) || defined(__linux__))
#include <fcntl.h>
#include <dirent.h>
#include <pwd.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <sys/mount.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <TargetConditionals.h>
#include <libproc.h>
#include <mach/vm_prot.h>
#include <sys/proc_info.h>
#include <sys/random.h>
#endif
#define LATTICE_RESOURCE_POSIX 1
#endif

namespace lattice::detail {
namespace {
using namespace ordinary_installation;
using deadline=ordinary_launch::deadline;
using json=nlohmann::json;
[[noreturn]] void resource_fail(){throw std::runtime_error("ordinary store resource custody unavailable");}
void bound(deadline end){if(std::chrono::steady_clock::now()>=end)resource_fail();}
// This resource exemption matches actual supported SQLite memory semantics,
// not the broader configuration/caching substring heuristic. Uninstalled
// legacy opens still receive their original filename and flags unchanged.
bool resource_memory_path(const std::string& p,bool uri_enabled=true)noexcept {
    return resource_memory_route::exempt(p,uri_enabled);
}

#ifdef LATTICE_RESOURCE_POSIX
std::uint64_t pid()noexcept{return static_cast<std::uint64_t>(::getpid());}
struct descriptor final {
    const std::uint64_t creator=pid();
    int value=-1;
    descriptor()noexcept=default;
    explicit descriptor(int fd)noexcept:value(fd){}
    descriptor(descriptor&& other)noexcept:creator(other.creator),value(std::exchange(other.value,-1)){}
    descriptor(const descriptor&)=delete;
    descriptor& operator=(descriptor&&)=delete;
    ~descriptor(){if(creator==pid()&&value>=0)::close(value);}
    int release()noexcept{return creator==pid()?std::exchange(value,-1):-1;}
};
file_identity id(const struct stat& s){return {static_cast<std::uint64_t>(s.st_dev),static_cast<std::uint64_t>(s.st_ino)};}
struct stat metadata(int fd){struct stat s{};if(fd<0||::fstat(fd,&s))resource_fail();return s;}
bool same(const struct stat& a,const struct stat& b){return id(a)==id(b)&&a.st_mode==b.st_mode&&a.st_uid==b.st_uid&&a.st_gid==b.st_gid&&a.st_nlink==b.st_nlink;}
void leaf(const std::string& value){if(value.empty()||value.size()>255||value=="."||value==".."||value.find('/')!=std::string::npos||value.find('\0')!=std::string::npos)resource_fail();}
std::uint64_t number(const json& object,const char* key){
    const auto& v=object.at(key);if(!v.is_number_unsigned()&&(!v.is_number_integer()||v.get<std::int64_t>()<0))resource_fail();return v.get<std::uint64_t>();
}
std::string text(const json& object,const char* key,std::size_t max=4096){
    const auto& v=object.at(key);if(!v.is_string())resource_fail();auto s=v.get<std::string>();if(s.empty()||s.size()>max||s.find('\0')!=std::string::npos)resource_fail();return s;
}
json identity_json(file_identity v){return {{"device",v.device},{"inode",v.inode}};}
file_identity identity_json(const json& v){file_identity out{number(v,"device"),number(v,"inode")};if(!out.inode)resource_fail();return out;}
std::string digest_string(const std::vector<std::uint8_t>& bytes){return picosha2::hash256_hex_string(bytes);}
std::vector<std::uint8_t> bytes(const json& value){const auto s=value.dump();if(s.size()>ordinary_launch::maximum_frame_bytes)resource_fail();return {s.begin(),s.end()};}
json parse(const std::vector<std::uint8_t>& raw){
    if(raw.empty()||raw.size()>ordinary_launch::maximum_frame_bytes)resource_fail();
    std::vector<std::set<std::string>> keys;std::size_t events=0;
    auto result=json::parse(raw.begin(),raw.end(),[&](int depth,json::parse_event_t e,json& v){
        if(depth<0||depth>12||++events>8192||(v.is_string()&&v.get_ref<const std::string&>().size()>8192))resource_fail();
        if(e==json::parse_event_t::object_start)keys.emplace_back();
        if(e==json::parse_event_t::key&&(keys.empty()||!keys.back().insert(v.get<std::string>()).second))resource_fail();
        if(e==json::parse_event_t::object_end)keys.pop_back();return true;
    });
    if(bytes(result)!=raw)resource_fail();return result;
}
std::vector<std::uint8_t> read_record(int directory,const std::string& name,deadline end){
    leaf(name);descriptor file(::openat(directory,name.c_str(),O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));const auto before=metadata(file.value);
    if(!S_ISREG(before.st_mode)||before.st_uid!=::geteuid()||(before.st_mode&07777)!=0600||before.st_nlink!=1||before.st_size<=0||before.st_size>ordinary_launch::maximum_frame_bytes)resource_fail();
    std::vector<std::uint8_t> raw(static_cast<std::size_t>(before.st_size));std::size_t at=0;
    while(at<raw.size()){bound(end);const auto n=::pread(file.value,raw.data()+at,raw.size()-at,static_cast<off_t>(at));if(n<0&&errno==EINTR)continue;if(n<=0)resource_fail();at+=static_cast<std::size_t>(n);}
    struct stat named{};const auto after=metadata(file.value);
    if(!same(before,after)||before.st_size!=after.st_size||::fstatat(directory,name.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same(before,named)||before.st_size!=named.st_size)resource_fail();
    // Compare bytes a second time, rather than relying on timestamp precision.
    std::vector<std::uint8_t> again(raw.size());at=0;
    while(at<again.size()){bound(end);const auto n=::pread(file.value,again.data()+at,again.size()-at,static_cast<off_t>(at));if(n<0&&errno==EINTR)continue;if(n<=0)resource_fail();at+=static_cast<std::size_t>(n);}
    if(raw!=again||!same(before,metadata(file.value)))resource_fail();bound(end);return raw;
}
descriptor absolute_directory(const std::string& path,bool strict){
    if(path.empty()||path.front()!='/'||path.size()>4096||path.back()=='/')resource_fail();
    descriptor current(::open("/",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));metadata(current.value);
    for(std::size_t at=1;at<path.size();){const auto slash=path.find('/',at);auto part=path.substr(at,slash==std::string::npos?path.size()-at:slash-at);leaf(part);
        descriptor next(::openat(current.value,part.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));const auto s=metadata(next.value);
        if(!S_ISDIR(s.st_mode)||(s.st_uid!=::geteuid()&&s.st_uid!=0)||(s.st_mode&0022))resource_fail();
        current.~descriptor();new(&current)descriptor(std::move(next));if(slash==std::string::npos)break;at=slash+1;}
    const auto s=metadata(current.value);if(strict&&(s.st_uid!=::geteuid()||(s.st_mode&07777)!=0700))resource_fail();return current;
}
void actual_directory(int fd,const std::string& path,file_identity expected,bool strict){
    auto named=absolute_directory(path,strict);const auto a=metadata(fd),b=metadata(named.value);
    if(id(a)!=expected||id(b)!=expected||!same(a,b))resource_fail();
}
void lock(int fd,int kind,deadline end){
    for(;;){bound(end);if(::flock(fd,kind|LOCK_NB)==0)return;if(errno==EINTR)continue;if(errno!=EWOULDBLOCK&&errno!=EAGAIN)resource_fail();std::this_thread::sleep_for(std::chrono::milliseconds(1));}
}
descriptor regular_control(int directory,const char* name,std::optional<file_identity> expected={}){
    descriptor result(::openat(directory,name,O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));const auto s=metadata(result.value);struct stat named{};
    if(!S_ISREG(s.st_mode)||s.st_uid!=::geteuid()||(s.st_mode&07777)!=0600||s.st_nlink!=1||s.st_size||(expected&&id(s)!=*expected)||::fstatat(directory,name,&named,AT_SYMLINK_NOFOLLOW)||!same(s,named))resource_fail();return result;
}
void publish_record(int directory,const std::string& name,const json& value,std::uint64_t creator,deadline end){
    if(creator!=pid())resource_fail();leaf(name);const auto raw=bytes(value);if(creator!=pid())resource_fail();bound(end);
    // This namespace is reserved by the actual issuer for this exact parent,
    // target name and canonical body. A pending producer may be resumed; an
    // unknown/truncated FINAL record is never overwritten or learned as valid.
    const auto parent=metadata(directory);
    if(!S_ISDIR(parent.st_mode)||parent.st_uid!=::geteuid()||(parent.st_mode&07777)!=0700)resource_fail();
    const json binding={{"format","lattice-resource-record-publication-v1"},{"parent",identity_json(id(parent))},
        {"name",name},{"bodySHA256",digest_string(raw)}};
    const auto pending=std::string(".record-")+digest_string(bytes(binding));leaf(pending);
    const auto inspect_pending=[&](int fd){
        const auto actual=metadata(fd);struct stat named{};
        if(!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||(actual.st_mode&07777)!=0600||actual.st_nlink!=1||
           actual.st_size<0||static_cast<std::uint64_t>(actual.st_size)>raw.size()||
           ::fstatat(directory,pending.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same(actual,named)||actual.st_size!=named.st_size)resource_fail();
        return actual;
    };
    const auto clean_pending=[&]{
        if(creator!=pid())resource_fail();bound(end);
        struct stat present{};
        if(::fstatat(directory,pending.c_str(),&present,AT_SYMLINK_NOFOLLOW)==0){
            descriptor held(::openat(directory,pending.c_str(),O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
            const auto original=inspect_pending(held.value);struct stat again{};
            if(creator!=pid()||::fstatat(directory,pending.c_str(),&again,AT_SYMLINK_NOFOLLOW)||!same(original,again)||
               ::unlinkat(directory,pending.c_str(),0))resource_fail();
        }else if(errno!=ENOENT)resource_fail();
    };
    if(creator!=pid())resource_fail();bound(end);
    struct stat final_entry{};
    if(::fstatat(directory,name.c_str(),&final_entry,AT_SYMLINK_NOFOLLOW)==0){
        if(read_record(directory,name,end)!=raw)resource_fail();clean_pending();
        // Required even if a previous rename/rmdir succeeded but its parent
        // fsync did not. Observing the name is not proof of its durability.
        if(creator!=pid()||::fsync(directory)||id(metadata(directory))!=id(parent))resource_fail();return;
    }else if(errno!=ENOENT)resource_fail();
    descriptor output(::openat(directory,pending.c_str(),O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600));
    if(creator!=pid())resource_fail();
    if(output.value<0){
        if(errno!=EEXIST)resource_fail();
        struct stat incomplete{};
        if(::fstatat(directory,pending.c_str(),&incomplete,AT_SYMLINK_NOFOLLOW)||!S_ISREG(incomplete.st_mode)||
           incomplete.st_uid!=::geteuid()||incomplete.st_nlink!=1||(incomplete.st_mode&07777&~0600)||
           incomplete.st_size<0||static_cast<std::uint64_t>(incomplete.st_size)>raw.size())resource_fail();
        // O_CREAT's mode may have been reduced by umask before a crash could
        // reach fchmod. This is the exact reserved unpublished metadata slot.
        if((incomplete.st_mode&07777)!=0600){
            if(creator!=pid()||::fchmodat(directory,pending.c_str(),0600,AT_SYMLINK_NOFOLLOW))resource_fail();
        }
        output.~descriptor();new(&output)descriptor(::openat(directory,pending.c_str(),O_RDWR|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        if(id(metadata(output.value))!=id(incomplete))resource_fail();
    }else if(::fchmod(output.value,0600))resource_fail();
    const auto original=inspect_pending(output.value);
    resource_cold_restart_test::at(resource_cold_restart_test::cut::record_pending_created);
    // Only this exact private pending producer slot may be rewritten after a
    // torn/short write. The final name has never been published from it yet.
    std::size_t at=0;
    while(at<raw.size()){
        if(creator!=pid())resource_fail();bound(end);
        const auto n=::pwrite(output.value,raw.data()+at,raw.size()-at,static_cast<off_t>(at));
        if(n<0&&errno==EINTR)continue;if(n<=0)resource_fail();at+=static_cast<std::size_t>(n);
    }
    if(creator!=pid()||::ftruncate(output.value,static_cast<off_t>(raw.size()))||::fsync(output.value))resource_fail();
    resource_cold_restart_test::at(resource_cold_restart_test::cut::record_body_synced);
    const auto written=inspect_pending(output.value);if(id(written)!=id(original)||written.st_size!=static_cast<off_t>(raw.size()))resource_fail();
    std::vector<std::uint8_t> verify(raw.size());at=0;
    while(at<verify.size()){
        if(creator!=pid())resource_fail();bound(end);const auto n=::pread(output.value,verify.data()+at,verify.size()-at,static_cast<off_t>(at));
        if(n<0&&errno==EINTR)continue;if(n<=0)resource_fail();at+=static_cast<std::size_t>(n);
    }
    if(verify!=raw||creator!=pid())resource_fail();const int closed=::close(output.release());if(closed)resource_fail();
    struct stat again{};if(::fstatat(directory,pending.c_str(),&again,AT_SYMLINK_NOFOLLOW)||!same(written,again)||
       written.st_size!=again.st_size||creator!=pid())resource_fail();bound(end);
#if defined(__APPLE__)
    if(::renameatx_np(directory,pending.c_str(),directory,name.c_str(),RENAME_EXCL))resource_fail();
#else
    // The production cold epoch currently requires Darwin's genuine atomic
    // exclusive name contract; do not emulate publication by overwrite/unlink.
    resource_fail();
#endif
    resource_cold_restart_test::at(resource_cold_restart_test::cut::record_renamed_before_parent_sync);
    if(creator!=pid()||::fsync(directory)||id(metadata(directory))!=id(parent)||read_record(directory,name,end)!=raw)resource_fail();
}
void create_control(int directory,const char* name,std::uint64_t creator,deadline end){
    if(creator!=pid())resource_fail();bound(end);descriptor output(::openat(directory,name,O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600));
    if(output.value<0||::fchmod(output.value,0600)||::fsync(output.value))resource_fail();
    const auto s=metadata(output.value);if(!S_ISREG(s.st_mode)||s.st_uid!=::geteuid()||s.st_nlink!=1||s.st_size)resource_fail();
    if(creator!=pid())resource_fail();const int closed=::close(output.release());if(closed)resource_fail();
}
std::string actual_home(){
    std::array<char,16384> buffer{};passwd record{},*found=nullptr;
    if(::getpwuid_r(::geteuid(),&record,buffer.data(),buffer.size(),&found)||found!=&record||record.pw_uid!=::geteuid()||!record.pw_dir)resource_fail();
    const auto first=reinterpret_cast<std::uintptr_t>(buffer.data()),p=reinterpret_cast<std::uintptr_t>(record.pw_dir);
    if(p<first||p-first>=buffer.size()||!std::memchr(record.pw_dir,0,buffer.size()-(p-first)))resource_fail();
    std::string home=record.pw_dir;if(home.empty()||home.front()!='/'||home.size()>4096||home.back()=='/')resource_fail();return home;
}
ordinary_launch::process_identity process(const json& v){
    const auto p=number(v,"pid"),parent=number(v,"parent"),a=number(v,"birthMajor"),b=number(v,"birthMinor");
    if(p<=1||p>INT_MAX||!parent||parent>INT_MAX||!a)resource_fail();return {static_cast<std::int64_t>(p),static_cast<std::int64_t>(parent),a,b};
}
json process(ordinary_launch::process_identity p){return {{"pid",p.pid},{"parent",p.parent},{"birthMajor",p.birth_major},{"birthMinor",p.birth_minor}};}
executable_fact executable(const json& v){
    executable_fact result{text(v,"path"),identity_json(v.at("identity")),{}};const auto hash=text(v,"sha256",64);if(hash.size()!=64)resource_fail();
    auto nibble=[](char c){if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;resource_fail();};
    for(std::size_t n=0;n<32;++n)result.content[n]=static_cast<std::uint8_t>((nibble(hash[2*n])<<4)|nibble(hash[2*n+1]));return result;
}
void loaded_image(std::int64_t selected,const executable_fact& expected,deadline end){
    auto held=retained_installation_file::open_application_image(expected,end);
#if defined(__APPLE__)
    std::array<char,PROC_PIDPATHINFO_MAXSIZE> path{};if(::proc_pidpath(static_cast<int>(selected),path.data(),path.size())<=0||!std::memchr(path.data(),0,path.size())||expected.path!=path.data())resource_fail();
    std::uint64_t address=0;bool matched=false;
    for(std::size_t count=0;count<65536;++count){bound(end);proc_regionwithpathinfo region{};
        if(::proc_pidinfo(static_cast<int>(selected),PROC_PIDREGIONPATHINFO,address,&region,sizeof(region))!=sizeof(region))resource_fail();
        const auto& map=region.prp_prinfo;const auto& vnode=region.prp_vip.vip_vi.vi_stat;
        const auto path_end=std::find(std::begin(region.prp_vip.vip_path),std::end(region.prp_vip.vip_path),'\0');if(path_end==std::end(region.prp_vip.vip_path))resource_fail();
        if((map.pri_protection&VM_PROT_EXECUTE)&&std::string(std::begin(region.prp_vip.vip_path),path_end)==expected.path){
            if(!S_ISREG(vnode.vst_mode)||(vnode.vst_uid!=::geteuid()&&vnode.vst_uid!=0)||vnode.vst_nlink!=1||file_identity{static_cast<std::uint64_t>(static_cast<dev_t>(vnode.vst_dev)),vnode.vst_ino}!=expected.identity)resource_fail();matched=true;break;}
        if(!map.pri_size||map.pri_address<address||map.pri_size>UINT64_MAX-map.pri_address)resource_fail();address=map.pri_address+map.pri_size;
    }if(!matched)resource_fail();
#else
    descriptor image(::open(("/proc/"+std::to_string(selected)+"/exe").c_str(),O_RDONLY|O_CLOEXEC));if(id(metadata(image.value))!=expected.identity)resource_fail();
#endif
    held.verify(end);bound(end);
}
struct resource_root final {
    const std::uint64_t creator=pid();
    std::string logical,generation,control,epoch,record_hash;
    file_identity logical_id,generation_id,control_id;
    file_identity root_entry_id;
    descriptor logical_fd,generation_fd,control_fd;
    json rule;
    void check(deadline end)const{
        if(creator!=pid())resource_fail();bound(end);
        actual_directory(logical_fd.value,logical,logical_id,false);actual_directory(generation_fd.value,generation,generation_id,true);actual_directory(control_fd.value,control,control_id,true);
        if(digest_string(read_record(control_fd.value,"root.v1",end))!=record_hash)resource_fail();
        const auto root=parse(read_record(control_fd.value,"root.v1",end));
        if(text(root,"format")!="lattice-resource-root-v1"||text(root,"epoch",64)!=epoch||identity_json(root.at("logical"))!=logical_id||identity_json(root.at("generation"))!=generation_id||identity_json(root.at("control"))!=control_id||identity_json(root.at("entry"))!=root_entry_id||root.at("rule")!=rule)resource_fail();
        auto entry=regular_control(control_fd.value,"root.entry.lock",root_entry_id);
    }
    bool permits(const std::string& name)const{
        leaf(name);for(const auto& value:rule.at("names"))if(value.is_string()&&value.get<std::string>()==name)return true;
        for(const auto& value:rule.at("suffixes"))if(value.is_string()){const auto s=value.get<std::string>();if(!s.empty()&&name.size()>s.size()&&name.ends_with(s))return true;}return false;
    }
};
#include "ordinary_resource_continuous_creation.inc"
std::optional<observer_delivery_startup_limits> delivery_limits(const json& role) {
    // Existing resource-only roles remain delivery-unselected. Absence never
    // invents limits or claims bounded delivery. A PRESENT policy is strict:
    // malformed/partial/unknown versions refuse, rather than falling back.
    if(!role.contains("observerDelivery"))return std::nullopt;
    const auto& value=role.at("observerDelivery");
    if(!value.is_object()||value.size()!=17||
       text(value,"format",64)!="lattice-observer-delivery-startup-v1")resource_fail();
    observer_delivery_startup_limits out;
    out.clients=number(value,"clients");out.waiters=number(value,"waiters");
    out.guaranteed_count=number(value,"guaranteedCount");out.guaranteed_bytes=number(value,"guaranteedBytes");
    out.burst_count=number(value,"burstCount");out.burst_bytes=number(value,"burstBytes");
    out.events=number(value,"events");out.event_bytes=number(value,"eventBytes");out.workspace_bytes=number(value,"workspaceBytes");
    out.read_workspace_bytes=number(value,"readWorkspaceBytes");out.read_columns=number(value,"readColumns");out.read_rows=number(value,"readRows");
    out.admission_wait_ms=number(value,"admissionWaitMs");out.read_duration_ms=number(value,"readDurationMs");
    out.initialization_statements=number(value,"initializationStatements");
    out.initialization_workspace_bytes=number(value,"initializationWorkspaceBytes");
    if(!out.clients||out.clients>64||!out.waiters||out.waiters>256||
       !out.guaranteed_count||!out.guaranteed_bytes||!out.events||out.events>INT_MAX||
       !out.event_bytes||out.event_bytes>INT_MAX||!out.workspace_bytes||
       !out.read_workspace_bytes||!out.read_columns||!out.read_rows||
       !out.initialization_statements||!out.initialization_workspace_bytes||
       !out.admission_wait_ms||out.admission_wait_ms>60000||!out.read_duration_ms||out.read_duration_ms>60000)
        resource_fail();
    const auto finite=[](std::uint64_t n){if(n==UINT64_MAX)resource_fail();};
    finite(out.guaranteed_count);finite(out.guaranteed_bytes);finite(out.burst_count);finite(out.burst_bytes);
    finite(out.workspace_bytes);finite(out.read_workspace_bytes);finite(out.read_columns);finite(out.read_rows);
    finite(out.initialization_statements);finite(out.initialization_workspace_bytes);
    if(out.guaranteed_count>(UINT64_MAX-out.burst_count)/out.clients||
       out.guaranteed_bytes>(UINT64_MAX-out.burst_bytes)/out.clients)resource_fail();
    // Exact same issued limits, never a fallback inferred from current load or
    // resource readiness. Full meter/plan compatibility is checked before open.
    return out;
}
void validate_roles(const json& roles,const executable_fact& host,deadline end,bool verify_bytes){
    if(!roles.is_array()||roles.empty()||roles.size()>64)resource_fail();
    std::set<std::string> names;
    for(const auto& role:roles){
        if(!names.insert(text(role,"name",128)).second)resource_fail();
        (void)delivery_limits(role); // validate a present independently approved delivery policy
        const auto image=executable(role.at("image"));const auto runtime=text(role,"runtime",16);
        // A declared native consumer still proves its actual loaded executable
        // and every Core code image. Swift consumers additionally prove both
        // real Bridge and SDK marker images at their actual factory boundary.
        // No role obtains resource authority merely from its runtime label.
        if(runtime!="swift"&&runtime!="native")resource_fail();
        if(!role.at("codeImages").is_array()||role.at("codeImages").empty()||role.at("codeImages").size()>16)resource_fail();
        bool executable_present=false;std::set<std::string> code_paths;
        for(const auto& code:role.at("codeImages")){
            const auto actual=executable(code);if(!code_paths.insert(actual.path).second)resource_fail();
            if(actual==image)executable_present=true;
            if(verify_bytes){auto held=retained_installation_file::open_application_image(actual,end);held.verify(end);}
        }
        if(!executable_present||!role.at("arguments").is_array()||role.at("arguments").empty()||role.at("arguments").size()>128||
           !role.at("environment").is_array()||role.at("environment").size()>127)resource_fail();
        for(const auto& arg:role.at("arguments"))if(!arg.is_string()||arg.get_ref<const std::string&>().size()>8192||arg.get_ref<const std::string&>().find('\0')!=std::string::npos)resource_fail();
        if(role.at("arguments").at(0).get<std::string>()!=image.path)resource_fail();
        std::set<std::string> environment_keys;
        for(const auto& value:role.at("environment")){
            if(!value.is_string())resource_fail();const auto env=value.get<std::string>();const auto equals=env.find('=');
            if(env.size()>8192||equals==std::string::npos||!equals||env.find('\0')!=std::string::npos)resource_fail();
            const auto key=env.substr(0,equals);
            if(!environment_keys.insert(key).second||key=="HOME"||key=="TMPDIR"||key=="LATTICE_RESOURCE_LAUNCH"||key.starts_with("DYLD_")||key.starts_with("LD_"))resource_fail();
        }
        auto cwd=absolute_directory(text(role,"cwd"),false);bound(end);
        if(verify_bytes){auto held=retained_installation_file::open_application_image(image,end);held.verify(end);}
    }
}
struct installation final {
    const std::uint64_t creator=pid();
    std::string home,directory,epoch,anchor_hash;
    descriptor directory_fd;
    file_identity directory_id;
    std::vector<std::uint8_t> anchor;
    json record;
    executable_fact host;
    std::size_t selected_role=std::numeric_limits<std::size_t>::max();
    std::atomic<std::uintptr_t> bound_bridge{0},bound_sdk{0};
    std::vector<resource_root*> roots; // heap capsules retained/creator-fenced
    ~installation(){if(creator!=pid())return;for(auto* root:roots)if(root->creator==pid())delete root;}
    void check(deadline end)const{
        if(creator!=pid())resource_fail();actual_directory(directory_fd.value,directory,directory_id,true);
        if(read_record(directory_fd.value,"active.v1",end)!=anchor)resource_fail();
        for(const auto* root:roots)root->check(end);
    }
};
installation* installed(deadline end){
    const auto creator=pid();auto* out=new installation;if(creator!=pid())resource_fail();
    try{out->home=actual_home();out->directory=out->home+"/.lattice-resource-installation";
        auto directory=absolute_directory(out->directory,true);out->directory_fd.~descriptor();new(&out->directory_fd)descriptor(std::move(directory));out->directory_id=id(metadata(out->directory_fd.value));
        out->anchor=read_record(out->directory_fd.value,"active.v1",end);out->anchor_hash=digest_string(out->anchor);out->record=parse(out->anchor);const auto& v=out->record;
        if(text(v,"format")!="lattice-resource-installation-v1"||number(v,"uid")!=::geteuid()||text(v,"home")!=out->home||identity_json(v.at("installation"))!=out->directory_id)resource_fail();
        out->epoch=text(v,"epoch",64);out->host=executable(v.at("host"));
        if(!v.at("roots").is_array()||v.at("roots").empty()||v.at("roots").size()>16||!v.at("roles").is_array()||v.at("roles").empty()||v.at("roles").size()>64)resource_fail();
        std::set<std::string> paths,roles;
        for(const auto& value:v.at("roots")){
            auto* root=new resource_root;if(creator!=pid())resource_fail();out->roots.push_back(root);
            root->logical=text(value,"logicalPath");root->generation=text(value,"generationPath");root->control=text(value,"controlPath");root->epoch=out->epoch;
            root->logical_id=identity_json(value.at("logical"));root->generation_id=identity_json(value.at("generation"));root->control_id=identity_json(value.at("control"));root->root_entry_id=identity_json(value.at("entry"));root->record_hash=text(value,"rootSHA256",64);root->rule=value.at("rule");
            if(root->record_hash.size()!=64||!paths.insert(root->logical).second||!paths.insert(root->generation).second||!paths.insert(root->control).second||!root->rule.at("names").is_array()||!root->rule.at("suffixes").is_array()||root->rule.at("names").size()+root->rule.at("suffixes").size()>64)resource_fail();
            for(const auto& name:root->rule.at("names")){if(!name.is_string())resource_fail();leaf(name.get<std::string>());}
            for(const auto& suffix:root->rule.at("suffixes")){if(!suffix.is_string())resource_fail();leaf(suffix.get<std::string>());}
            auto a=absolute_directory(root->logical,false),b=absolute_directory(root->generation,true),c=absolute_directory(root->control,true);
            root->logical_fd.~descriptor();new(&root->logical_fd)descriptor(std::move(a));root->generation_fd.~descriptor();new(&root->generation_fd)descriptor(std::move(b));root->control_fd.~descriptor();new(&root->control_fd)descriptor(std::move(c));root->check(end);
        }
        validate_roles(v.at("roles"),out->host,end,false);
        out->check(end);return out;
    }catch(...){if(creator==pid())delete out;throw;}
}
template<class Function> std::uintptr_t code_address(Function actual)noexcept{return reinterpret_cast<std::uintptr_t>(actual);}
void verify_code_address(installation& state,std::uintptr_t address,deadline end){
    if(state.creator!=pid()||!address||state.selected_role>=state.record.at("roles").size())resource_fail();
    const auto& role=state.record.at("roles").at(state.selected_role);
#if defined(__APPLE__)
    proc_regionwithpathinfo region{};bound(end);
    if(::proc_pidinfo(::getpid(),PROC_PIDREGIONPATHINFO,address,&region,sizeof(region))!=sizeof(region))resource_fail();
    const auto& map=region.prp_prinfo;const auto& vnode=region.prp_vip.vip_vi.vi_stat;
    const auto end_path=std::find(std::begin(region.prp_vip.vip_path),std::end(region.prp_vip.vip_path),'\0');
    if(end_path==std::end(region.prp_vip.vip_path)||!(map.pri_protection&VM_PROT_EXECUTE)||map.pri_address>address||!map.pri_size||map.pri_size>UINT64_MAX-map.pri_address||address>=map.pri_address+map.pri_size||!S_ISREG(vnode.vst_mode)||vnode.vst_nlink!=1||(vnode.vst_uid!=::geteuid()&&vnode.vst_uid!=0))resource_fail();
    const std::string path(std::begin(region.prp_vip.vip_path),end_path);const file_identity actual{static_cast<std::uint64_t>(static_cast<dev_t>(vnode.vst_dev)),vnode.vst_ino};
    const auto& candidates=role.at("codeImages");const auto found=std::find_if(candidates.begin(),candidates.end(),[&](const auto& value){return text(value,"path")==path&&identity_json(value.at("identity"))==actual;});
    if(found==candidates.end())resource_fail();auto held=retained_installation_file::open_application_image(executable(*found),end);held.verify(end);
    proc_regionwithpathinfo again{};if(::proc_pidinfo(::getpid(),PROC_PIDREGIONPATHINFO,address,&again,sizeof(again))!=sizeof(again)||again.prp_prinfo.pri_address!=map.pri_address||again.prp_prinfo.pri_size!=map.pri_size||again.prp_vip.vip_vi.vi_stat.vst_dev!=vnode.vst_dev||again.prp_vip.vip_vi.vi_stat.vst_ino!=vnode.vst_ino)resource_fail();
#else
    // No unverified pathname-only Linux image fallback. The initial actual
    // Engram/Orbital resource issuer is the qualified Darwin desktop profile.
    (void)role;(void)end;resource_fail();
#endif
}
void core_code_current(installation& state,deadline end){
    verify_code_address(state,code_address(&store_resource_factory_path),end);
    verify_code_address(state,code_address(&store_resource_database_code_anchor),end);
    verify_code_address(state,code_address(&store_physical_close),end);
}
// The dedicated CMake Host links the actual Core archive statically. Its
// original loaded executable therefore must also contain the real issuer,
// database-constructor and physical-return instructions, not an older dylib.
void host_core_code_current(const executable_fact& host,deadline end){
#if defined(LATTICE_RESOURCE_POSIX) && defined(__APPLE__)
    const std::array<std::uintptr_t,3> addresses{code_address(&store_resource_factory_path),
        code_address(&store_resource_database_code_anchor),code_address(&store_physical_close)};
    for(const auto address:addresses){
        bound(end);proc_regionwithpathinfo first{},last{};
        if(!address||::proc_pidinfo(::getpid(),PROC_PIDREGIONPATHINFO,address,&first,sizeof(first))!=sizeof(first))resource_fail();
        const auto& map=first.prp_prinfo;const auto& vnode=first.prp_vip.vip_vi.vi_stat;
        const auto z=std::find(std::begin(first.prp_vip.vip_path),std::end(first.prp_vip.vip_path),'\0');
        if(z==std::end(first.prp_vip.vip_path)||!(map.pri_protection&VM_PROT_EXECUTE)||map.pri_address>address||!map.pri_size||address-map.pri_address>=map.pri_size||
           file_identity{static_cast<std::uint64_t>(static_cast<dev_t>(vnode.vst_dev)),vnode.vst_ino}!=host.identity||std::string(std::begin(first.prp_vip.vip_path),z)!=host.path)resource_fail();
        if(::proc_pidinfo(::getpid(),PROC_PIDREGIONPATHINFO,address,&last,sizeof(last))!=sizeof(last)||last.prp_prinfo.pri_address!=map.pri_address||last.prp_prinfo.pri_size!=map.pri_size||last.prp_vip.vip_vi.vi_stat.vst_dev!=vnode.vst_dev||last.prp_vip.vip_vi.vi_stat.vst_ino!=vnode.vst_ino)resource_fail();
    }
#else
    (void)host;(void)end;resource_fail();
#endif
}
void require_runtime_code(installation& state){
    if(state.creator!=pid()||state.selected_role>=state.record.at("roles").size())resource_fail();
    if(text(state.record.at("roles").at(state.selected_role),"runtime",16)=="swift"&&
       (!state.bound_bridge.load(std::memory_order_acquire)||!state.bound_sdk.load(std::memory_order_acquire)))resource_fail();
}
struct resource_policy final {
    enum class phase{untouched,legacy,installing,managed,failed};
    const std::uint64_t creator=pid();std::atomic<phase> state{phase::untouched};
    std::atomic<std::thread::id> installer_thread{};installation* actual=nullptr;
};
const std::uint64_t initial_resource_process=pid();
std::atomic<bool> resource_process_protected{false};
resource_policy& policy(){
    if(initial_resource_process!=pid())resource_fail();
    static auto* state=[](){auto* p=new resource_policy;if(initial_resource_process!=pid())resource_fail();return p;}();return *state;
}
bool anchor_present(){
    const auto home=actual_home();auto directory=absolute_directory(home,false);struct stat s{};
    if(::fstatat(directory.value,".lattice-resource-installation",&s,AT_SYMLINK_NOFOLLOW)==0)return true;
    if(errno!=ENOENT)resource_fail();return false;
}
installation* await_installation(deadline end){
    auto& state=policy();for(;;){if(state.creator!=pid())resource_fail();const auto phase=state.state.load(std::memory_order_acquire);
        if(phase==resource_policy::phase::managed){if(!state.actual)resource_fail();return state.actual;}
        if(phase==resource_policy::phase::legacy)return nullptr;
        if(phase!=resource_policy::phase::installing||state.installer_thread.load(std::memory_order_acquire)==std::this_thread::get_id())resource_fail();
        bound(end);std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}
installation* factory_installation(deadline end){
    if(resource_continuous_ending_debt.load(std::memory_order_acquire))resource_fail();
    if(initial_resource_process!=pid()){
        // Do not load inherited heap policy/roots or enter their mutexes. A
        // never-enrolled child preserves the old fresh-Native-open route ONLY
        // after actual fixed-account anchor absence; managed/failed originals
        // cannot erase their inherited denial by removing the installation.
        if(resource_process_protected.load(std::memory_order_acquire))resource_fail();
        bound(end);if(anchor_present())resource_fail();bound(end);return nullptr;
    }
    auto& state=policy();if(state.creator!=pid())resource_fail();auto phase=state.state.load(std::memory_order_acquire);
    if(phase==resource_policy::phase::untouched){
        bound(end);const bool present=anchor_present();bound(end);
        if(!present){auto expected=phase;state.state.compare_exchange_strong(expected,resource_policy::phase::legacy);phase=state.state.load(std::memory_order_acquire);}
        else{
            resource_process_protected.store(true,std::memory_order_release);
            // Selection is untrusted. Both paths still establish real Native
            // image/root/epoch custody. The ordinary factory needs no remote
            // history grant or authenticated parent merely to enroll resources.
            const char* exchange=std::getenv("LATTICE_RESOURCE_LAUNCH");
            if(exchange&&std::strcmp(exchange,"1")==0){resource_normal_receiver::receive(end);phase=state.state.load(std::memory_order_acquire);}
            else{
                auto expected=phase;
                if(!state.state.compare_exchange_strong(expected,resource_policy::phase::installing,std::memory_order_acq_rel))return await_installation(end);
                state.installer_thread.store(std::this_thread::get_id(),std::memory_order_release);
                try{
                    auto* actual=installed(end);const auto self=ordinary_launch::current_process();
                    std::string image_path;
#if defined(__APPLE__)
                    std::array<char,PROC_PIDPATHINFO_MAXSIZE> path{};
                    if(::proc_pidpath(::getpid(),path.data(),path.size())<=0||!std::memchr(path.data(),0,path.size()))resource_fail();image_path=path.data();
#else
                    std::array<char,4097> path{};const auto n=::readlink("/proc/self/exe",path.data(),path.size());
                    if(n<=0||std::size_t(n)>=path.size())resource_fail();image_path.assign(path.data(),static_cast<std::size_t>(n));
#endif
                    const auto& roles=actual->record.at("roles");const auto role=std::find_if(roles.begin(),roles.end(),[&](const auto& row){return text(row.at("image"),"path")==image_path;});
                    if(role==roles.end())resource_fail();actual->selected_role=static_cast<std::size_t>(std::distance(roles.begin(),role));
                    loaded_image(self.pid,executable(role->at("image")),end);core_code_current(*actual,end);actual->check(end);
                    if(state.creator!=pid()||ordinary_launch::current_process()!=self)resource_fail();
                    state.actual=actual;state.state.store(resource_policy::phase::managed,std::memory_order_release);phase=resource_policy::phase::managed;
                }catch(...){state.state.store(resource_policy::phase::failed,std::memory_order_release);throw;}
            }
        }
    }
    if(phase==resource_policy::phase::legacy)return nullptr;
    if(phase==resource_policy::phase::installing)return await_installation(end);
    if(phase!=resource_policy::phase::managed||!state.actual)resource_fail();return state.actual;
}
std::pair<resource_root*,std::string> select(installation& installed,const std::string& path,deadline end){
    installed.check(end);require_runtime_code(installed);
    resource_root* selected=nullptr;std::size_t parent_size=0;
    for(auto* root:installed.roots){for(const auto* parent:{&root->logical,&root->generation}){
        if(path.size()>parent->size()+1&&path.compare(0,parent->size(),*parent)==0&&path[parent->size()]=='/'&&
           parent->size()>parent_size){selected=root;parent_size=parent->size();}
    }}
    if(!selected)resource_fail();
    // A nested approved root owns its own rule. Denial never falls back to a
    // broader parent; already translated generation paths use the same check.
    auto name=path.substr(parent_size+1);if(!selected->permits(name))resource_fail();
    return {selected,std::move(name)};
}
#include "ordinary_resource_pending_files.inc"
#include "ordinary_resource_initial_member.inc"
struct member_view final {descriptor directory,entry;struct stat main{};file_identity physical_id,member_identity;std::string record_sha256;};
member_view active_member(resource_root& root,const std::string& name,deadline end){
    root.check(end);const std::vector<std::uint8_t> key(name.begin(),name.end());const auto slot=digest_string(key);leaf(slot);
    descriptor member(::openat(root.control_fd.value,slot.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));const auto folder=metadata(member.value);
    if(!S_ISDIR(folder.st_mode)||folder.st_uid!=::geteuid()||(folder.st_mode&07777)!=0700)resource_fail();
    const auto original=read_record(member.value,"member.v1",end);const auto record=parse(original);
    auto entry=regular_control(member.value,"entry.lock",identity_json(record.at("entry")));lock(entry.value,LOCK_SH,end);
    if(read_record(member.value,"member.v1",end)!=original)resource_fail();const auto state=parse(read_record(member.value,"state.v1",end));
    struct stat deletion{};
    if(::fstatat(member.value,"deletion.v1",&deletion,AT_SYMLINK_NOFOLLOW)==0||errno!=ENOENT)resource_fail();
    struct stat actual{},stub{},named_folder{};
    if(text(record,"format")!="lattice-resource-member-v1"||text(record,"epoch",64)!=root.epoch||text(record,"name",255)!=name||identity_json(record.at("parent"))!=root.generation_id||text(state,"phase",32)!="active"||text(state,"epoch",64)!=root.epoch||identity_json(state.at("main"))!=identity_json(record.at("main"))||::fstatat(root.generation_fd.value,name.c_str(),&actual,AT_SYMLINK_NOFOLLOW)||!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||actual.st_nlink!=1||(actual.st_mode&0022)||id(actual)!=identity_json(record.at("main"))||::fstatat(root.logical_fd.value,name.c_str(),&stub,AT_SYMLINK_NOFOLLOW)||!S_ISREG(stub.st_mode)||stub.st_uid!=::geteuid()||stub.st_nlink!=1||stub.st_size||(stub.st_mode&07777)||id(stub)!=identity_json(record.at("logicalStub"))||::fstatat(root.control_fd.value,slot.c_str(),&named_folder,AT_SYMLINK_NOFOLLOW)||!same(folder,named_folder))resource_fail();
    resource_initial_ready(root,member.value,name,original,end);
    const auto physical=identity_json(record.at("physical"));auto checked=regular_control(member.value,"physical.lock",physical);
    return {std::move(member),std::move(entry),actual,physical,id(folder),digest_string(original)};
}
#include "ordinary_resource_recreation.inc"
std::optional<resource_creation_original> issue_new_member(resource_root& root,const std::string& name,deadline end){
    root.check(end);if(!root.permits(name))resource_fail();const auto creator=root.creator;
    const std::vector<std::uint8_t> key(name.begin(),name.end());const auto slot=digest_string(key);struct stat found{};
    if(::fstatat(root.control_fd.value,slot.c_str(),&found,AT_SYMLINK_NOFOLLOW)==0){
        // A real factory builder already holds member SH through its return.
        // Never reacquire root EX for its existing active CREATE open: a
        // deleter can hold root EX while waiting for that very member SH.
        if(!resource_member_needs_change(root,slot,end))return {};
    }else if(errno!=ENOENT)resource_fail();
    auto admission=regular_control(root.control_fd.value,"root.entry.lock",root.root_entry_id);lock(admission.value,LOCK_EX,end);root.check(end);
    if(::fstatat(root.control_fd.value,slot.c_str(),&found,AT_SYMLINK_NOFOLLOW)==0){
        // Another builder may have completed while this original waited.
        if(!resource_member_needs_change(root,slot,end))return {};
        if(!S_ISDIR(found.st_mode)||found.st_uid!=::geteuid()||(found.st_mode&07777&~0700))resource_fail();
        if((found.st_mode&07777)!=0700){return resource_create_initial_member(root,name,slot,true,end);}
        descriptor member(::openat(root.control_fd.value,slot.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        const auto folder=metadata(member.value);struct stat named{};
        if(!same(found,folder)||::fstatat(root.control_fd.value,slot.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same(folder,named))resource_fail();
        bool incomplete=false;
        for(const auto* record:{"member.v1","state.v1"}){
            if(::fstatat(member.value,record,&named,AT_SYMLINK_NOFOLLOW)){
                if(errno!=ENOENT)resource_fail();incomplete=true;
            }
        }
        if(!incomplete){
            const auto record=parse(read_record(member.value,"member.v1",end));
            if(record.contains("initialIntentSHA256")&&::fstatat(member.value,"creation-complete.v1",&named,AT_SYMLINK_NOFOLLOW)){
                if(errno!=ENOENT)resource_fail();incomplete=true;
            }
        }
        if(incomplete)return resource_create_initial_member(root,name,slot,true,end);
        return resource_recreate_completed_member(root,name,slot,end);
    }
    if(errno!=ENOENT)resource_fail();
    return resource_create_initial_member(root,name,slot,false,end);
}
#endif
}

bool store_resource_profile_active()noexcept{
#ifdef LATTICE_RESOURCE_POSIX
    if(initial_resource_process!=pid())return false;
    try{auto& state=policy();return state.creator==pid()&&state.state.load(std::memory_order_acquire)==resource_policy::phase::managed;}catch(...){return false;}
#else
    return false;
#endif
}
void store_resource_bind_library_code(std::uintptr_t bridge,std::uintptr_t sdk,deadline end){
#ifdef LATTICE_RESOURCE_POSIX
    auto* state=factory_installation(end);if(!state)return;
    const auto self=ordinary_launch::current_process();verify_code_address(*state,bridge,end);verify_code_address(*state,sdk,end);core_code_current(*state,end);
    state->check(end);if(state->creator!=pid()||ordinary_launch::current_process()!=self)resource_fail();
    std::uintptr_t expected=0;if(!state->bound_bridge.compare_exchange_strong(expected,bridge,std::memory_order_acq_rel)&&expected!=bridge)resource_fail();
    expected=0;if(!state->bound_sdk.compare_exchange_strong(expected,sdk,std::memory_order_acq_rel)&&expected!=sdk)resource_fail();
#else
    (void)bridge;(void)sdk;(void)end;
#endif
}
std::string store_resource_factory_path(const std::string& requested,deadline end){
#ifdef LATTICE_RESOURCE_POSIX
    if(resource_memory_path(requested))return requested;auto* installed=factory_installation(end);if(!installed)return requested;
    auto [root,name]=select(*installed,requested,end);auto path=root->generation+"/"+name;
    if(installed->creator!=pid())resource_fail();bound(end);return path;
#else
    (void)end;return requested;
#endif
}
std::optional<observer_delivery_startup_policy> store_resource_delivery_for_factory(
    const std::string& path,deadline end) {
#ifdef LATTICE_RESOURCE_POSIX
    const auto issuer=pid();
    if(resource_memory_path(path))return std::nullopt;
    auto* actual=factory_installation(end);if(!actual)return std::nullopt;
    require_runtime_code(*actual);core_code_current(*actual,end);
    auto [root,name]=select(*actual,path,end);
    const auto limits=delivery_limits(actual->record.at("roles").at(actual->selected_role));
    if(!limits)return std::nullopt; // physical resource role is not delivery authority
    actual->check(end);
    if(issuer!=pid()||actual->creator!=issuer||root->creator!=issuer)resource_fail();
    return observer_delivery_startup_policy(issuer,actual,root,actual->selected_role,name,end,*limits);
#else
    (void)path;(void)end;return std::nullopt;
#endif
}
void observer_delivery_startup_policy::verify_for_factory_(const std::string& path)const {
#ifdef LATTICE_RESOURCE_POSIX
    if(creator_!=pid())resource_fail();bound(original_end_);
    auto* actual=factory_installation(original_end_);
    if(actual!=installation_||actual->creator!=creator_||actual->selected_role!=role_)resource_fail();
    require_runtime_code(*actual);core_code_current(*actual,original_end_);
    auto [root,name]=select(*actual,path,original_end_);
    if(root!=root_||root->creator!=creator_||name!=std::string_view(member_.data(),member_size_))resource_fail();
    actual->check(original_end_);if(creator_!=pid())resource_fail();
#else
    (void)path;resource_fail();
#endif
}
void store_resource_before_cache_return(const std::string& path,deadline end){
#ifdef LATTICE_RESOURCE_POSIX
    if(resource_memory_path(path))return;auto* installed=factory_installation(end);if(!installed)return;
    auto [root,name]=select(*installed,path,end);auto held=active_member(*root,name,end);root->check(end);
#else
    (void)path;(void)end;
#endif
}

namespace ordinary_installation {
struct retained_resource_normal_host::implementation {
#ifdef LATTICE_RESOURCE_POSIX
    const std::uint64_t creator=pid();const std::thread::id thread=std::this_thread::get_id();installation* installed=nullptr;
    ~implementation(){if(creator==pid())delete installed;}
    void check(deadline end)const{if(creator!=pid()||thread!=std::this_thread::get_id()||!installed)resource_fail();installed->check(end);loaded_image(::getpid(),installed->host,end);host_core_code_current(installed->host,end);}
#endif
};
retained_resource_normal_host::retained_resource_normal_host(implementation* value)noexcept:impl_(value){}
retained_resource_normal_host::~retained_resource_normal_host(){
#ifdef LATTICE_RESOURCE_POSIX
    if(impl_&&impl_->creator==pid())delete impl_;
#endif
}
retained_resource_normal_host::retained_resource_normal_host(retained_resource_normal_host&& other)noexcept:impl_(std::exchange(other.impl_,nullptr)){}
retained_resource_normal_host retained_resource_normal_host::open_installed(deadline end){
#ifdef LATTICE_RESOURCE_POSIX
    const auto creator=pid();auto* state=new implementation;if(creator!=pid())resource_fail();
    try{state->installed=installed(end);state->check(end);return retained_resource_normal_host(state);}catch(...){if(creator==pid())delete state;throw;}
#else
    (void)end;resource_fail();
#endif
}
resource_application_child retained_resource_normal_host::launch(const std::string& name,deadline end){
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_)resource_fail();impl_->check(end);const auto& anchor=*impl_->installed;
    const auto& roles=anchor.record.at("roles");const auto role=std::find_if(roles.begin(),roles.end(),[&](const auto& v){return text(v,"name",128)==name;});if(role==roles.end())resource_fail();
    const auto image=executable(role->at("image"));auto held_image=retained_installation_file::open_application_image(image,end);
    ordinary_launch::launch_specification spec;spec.executable=image.path;spec.working_directory=text(*role,"cwd");
    for(const auto& v:role->at("arguments")){if(!v.is_string()||spec.arguments.size()==128)resource_fail();auto arg=v.get<std::string>();if(arg.size()>8192||arg.find('\0')!=std::string::npos)resource_fail();spec.arguments.push_back(std::move(arg));}
    for(const auto& v:role->at("environment")){if(!v.is_string()||spec.environment.size()==128)resource_fail();auto env=v.get<std::string>();if(env.size()>8192||env.find('\0')!=std::string::npos)resource_fail();spec.environment.push_back(std::move(env));}
    spec.environment.push_back("LATTICE_RESOURCE_LAUNCH=1"); // selector only; actual four-phase proof still required
    spec.inherited_directories.push_back(anchor.directory_fd.value);
    for(const auto* root:anchor.roots){spec.inherited_directories.push_back(root->logical_fd.value);spec.inherited_directories.push_back(root->generation_fd.value);spec.inherited_directories.push_back(root->control_fd.value);}
    impl_->check(end);auto child=ordinary_launch::owned_child::spawn(spec);const auto who=child.identity();
    std::array<std::uint8_t,32> nonce{};if(::getentropy(nonce.data(),nonce.size()))resource_fail();
    const auto nonce_hex=picosha2::bytes_to_hex_string(nonce.begin(),nonce.end());
    json offer={{"format","lattice-resource-normal-v1"},{"phase",1},{"nonce",nonce_hex},{"anchorSHA256",anchor.anchor_hash},{"supervisor",process(ordinary_launch::current_process())},{"child",process(who)},{"role",name},{"roots",anchor.roots.size()}};
    child.send(bytes(offer),end);offer["phase"]=2;if(child.receive(end)!=bytes(offer))resource_fail();
    impl_->check(end);held_image.verify(end);loaded_image(who.pid,image,end);if(child.identity()!=who)resource_fail();
    offer["phase"]=3;child.send(bytes(offer),end);offer["phase"]=4;if(child.receive(end)!=bytes(offer))resource_fail();
    impl_->check(end);held_image.verify(end);return resource_application_child(std::move(child));
#else
    (void)name;(void)end;resource_fail();
#endif
}
void resource_normal_receiver::receive(deadline end){
#ifdef LATTICE_RESOURCE_POSIX
    auto& gate=policy();if(gate.creator!=pid())resource_fail();resource_process_protected.store(true,std::memory_order_release);auto prior=resource_policy::phase::untouched;
    if(!gate.state.compare_exchange_strong(prior,resource_policy::phase::installing,std::memory_order_acq_rel)){
        if(prior==resource_policy::phase::installing&&gate.installer_thread.load(std::memory_order_acquire)!=std::this_thread::get_id()){(void)await_installation(end);return;}
        resource_fail();
    }
    gate.installer_thread.store(std::this_thread::get_id(),std::memory_order_release);
    installation* actual=nullptr;
    try{ordinary_launch::inherited_channel channel(3);actual=installed(end);const auto self=ordinary_launch::current_process(),parent=ordinary_launch::current_parent_process();
        auto offer=parse(channel.receive(end));if(text(offer,"format")!="lattice-resource-normal-v1"||number(offer,"phase")!=1||text(offer,"anchorSHA256",64)!=actual->anchor_hash||process(offer.at("child"))!=self||process(offer.at("supervisor"))!=parent||self.parent!=parent.pid||number(offer,"roots")!=actual->roots.size()||text(offer,"nonce",64).size()!=64)resource_fail();
        const auto name=text(offer,"role",128);const auto& roles=actual->record.at("roles");auto role=std::find_if(roles.begin(),roles.end(),[&](const auto& v){return text(v,"name",128)==name;});if(role==roles.end())resource_fail();
        actual->selected_role=static_cast<std::size_t>(std::distance(roles.begin(),role));
        loaded_image(self.pid,executable(role->at("image")),end);core_code_current(*actual,end);loaded_image(parent.pid,actual->host,end);
        descriptor inherited_installation(4);if(::fcntl(4,F_SETFD,FD_CLOEXEC)||id(metadata(4))!=actual->directory_id)resource_fail();
        int next=5;for(auto* root:actual->roots){
            const std::array<file_identity,3> expected{root->logical_id,root->generation_id,root->control_id};
            for(const auto want:expected){descriptor received(next++);if(::fcntl(received.value,F_SETFD,FD_CLOEXEC)||id(metadata(received.value))!=want)resource_fail();}
        }
        actual->check(end);if(ordinary_launch::current_process()!=self||ordinary_launch::current_parent_process()!=parent)resource_fail();
        offer["phase"]=2;channel.send(bytes(offer),end);offer["phase"]=3;if(channel.receive(end)!=bytes(offer))resource_fail();
        actual->check(end);if(ordinary_launch::current_process()!=self||ordinary_launch::current_parent_process()!=parent)resource_fail();
        gate.actual=actual;gate.state.store(resource_policy::phase::managed,std::memory_order_release);
        offer["phase"]=4;channel.send(bytes(offer),end);
    }catch(...){gate.state.store(resource_policy::phase::failed,std::memory_order_release);throw;} // actual capsules intentionally retained on refusal
#else
    (void)end;resource_fail();
#endif
}
#include "ordinary_resource_cold_restart.inc"
}

#include "ordinary_resource_continuous_startup.inc"
#include "ordinary_resource_initialization_original.inc"

struct store_resource_open::implementation {
#ifdef LATTICE_RESOURCE_POSIX
    const std::uint64_t creator=pid();resource_root* root=nullptr;std::string path;
    descriptor entry;store_physical_lifetime::pin physical;struct stat expected{};
    store_resource_initialization_original creation;
#endif
};
store_resource_open::store_resource_open(implementation* value)noexcept:impl_(value){}
store_resource_open::~store_resource_open(){
#ifdef LATTICE_RESOURCE_POSIX
    if(impl_&&impl_->creator==pid())delete impl_;
#endif
}
store_resource_open::store_resource_open(store_resource_open&& other)noexcept:impl_(std::exchange(other.impl_,nullptr)){}
store_resource_open store_resource_open::acquire_(const std::string& path,bool create,deadline end,bool uri_enabled,bool capture_creation){
#ifdef LATTICE_RESOURCE_POSIX
    if(resource_memory_path(path,uri_enabled))return {};auto* installed=factory_installation(end);if(!installed)return {};auto [root,name]=select(*installed,path,end);
    const auto issue_creator=pid();std::optional<resource_creation_original> original;
    if(create) {
        auto issued=issue_new_member(*root,name,end);
        if(issued&&capture_creation) {
            // SAME actual creation descriptions, converted to shared custody
            // before the independent VFS obtains its own real SH description.
            lock(issued->physical,LOCK_SH,end);lock(issued->entry,LOCK_SH,end);
            original.emplace(std::move(*issued));
        }
    }
    // Uncaptured creators have REALLY ended above. Their actual first close
    // debt still refuses before all further admission/Native effects.
    if(issue_creator!=pid()||resource_continuous_ending_debt.load(std::memory_order_acquire))resource_fail();
    auto member=active_member(*root,name,end);auto physical=regular_control(member.directory.value,"physical.lock",member.physical_id);
    lock(physical.value,LOCK_SH,end);root->check(end);
    const auto creator=pid();auto* state=new implementation;if(creator!=pid())resource_fail();
    try{state->root=root;state->path=root->generation+"/"+name;state->expected=member.main;state->entry.~descriptor();new(&state->entry)descriptor(std::move(member.entry));
        state->physical=store_physical_lifetime::adopt_lease_(physical.value);if(creator!=pid())resource_fail();physical.release();
        if(capture_creation) {
            auto* captured=new store_resource_initialization_original::implementation(*installed,*root,state->path,state->expected,end,std::move(original));
            if(creator!=pid())resource_fail();state->creation.impl_=captured;state->creation.creator_=creator;
            state->creation.builder_=std::this_thread::get_id();state->creation.end_=end;
        }
        struct stat again{};if(::fstatat(root->generation_fd.value,name.c_str(),&again,AT_SYMLINK_NOFOLLOW)||!same(state->expected,again))resource_fail();root->check(end);return store_resource_open(state);
    }catch(...){if(creator==pid())delete state;throw;}
#else
    (void)path;(void)end;(void)uri_enabled;(void)capture_creation;return {};
#endif
}
store_resource_file_metadata store_resource_file_metadata_at(const std::string& path,deadline end){
#ifdef LATTICE_RESOURCE_POSIX
    const auto creator=pid();auto held=store_resource_open::acquire_(path,false,end);
    if(creator!=pid())resource_fail();if(!held.impl_)return {};
    auto* state=held.impl_;if(state->creator!=pid()||!state->root||!state->physical.current())resource_fail();
    auto& root=*state->root;root.check(end);
    const auto name=state->path.substr(root.generation.size()+1);struct stat actual{};
    if(::fstatat(root.generation_fd.value,name.c_str(),&actual,AT_SYMLINK_NOFOLLOW)||
       !same(state->expected,actual)||actual.st_size<0)resource_fail();
#if defined(__APPLE__)
    const auto stamp=actual.st_mtimespec;
#else
    const auto stamp=actual.st_mtim;
#endif
    static_assert(sizeof(stamp.tv_sec)<=sizeof(std::int64_t));
    if(stamp.tv_nsec<0||stamp.tv_nsec>=1000000000)resource_fail();root.check(end);
    if(creator!=pid()||!state->physical.current())resource_fail();
    return {true,static_cast<std::uint64_t>(actual.st_size),static_cast<std::int64_t>(stamp.tv_sec),
        static_cast<std::uint32_t>(stamp.tv_nsec)};
#else
    (void)path;(void)end;return {};
#endif
}
store_resource_initialization_original store_resource_open::take_for_builder_() {
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_||impl_->creator!=pid())resource_fail();
    if(!impl_->creation.impl_)return {};
    impl_->creation.impl_->check_before_native_();
    return store_resource_initialization_original(std::exchange(impl_->creation.impl_,nullptr),impl_->creation.end_);
#else
    return {};
#endif
}
void store_resource_open::require_no_unclaimed_creation_()const {
#ifdef LATTICE_RESOURCE_POSIX
    if(impl_&&(impl_->creator!=pid()||(impl_->creation.impl_&&impl_->creation.impl_->original)))resource_fail();
#endif
}
int store_resource_open::open_(sqlite3_vfs* lower,sqlite3** actual,int flags,store_physical_open_original* issued){
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_||impl_->creator!=pid()||!impl_->physical.current()||!impl_->root)resource_fail();
    return store_physical_vfs::open_(impl_->physical,lower,impl_->path,impl_->expected,actual,flags,issued);
#else
    (void)lower;(void)actual;(void)flags;(void)issued;resource_fail();
#endif
}
int store_resource_open::open_initializing_(sqlite3_vfs* lower,sqlite3** actual,int flags,
    store_physical_open_original& issued,store_resource_initialization_original& recipient,
    store_resource_initialization_original* supplied) {
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_||impl_->creator!=pid()||!impl_->physical.current()||!impl_->root||recipient.creator_||recipient.impl_)resource_fail();
    auto* selected=supplied?supplied:&impl_->creation;
    if(!selected->transport_current_()||!selected->impl_)resource_fail();auto& origin=*selected->impl_;
    if(origin.attempted||origin.owner||origin.open||origin.physical||origin.native||
       origin.root!=impl_->root||origin.path!=impl_->path||origin.main_id!=id(impl_->expected))resource_fail();
    origin.check_before_native_();origin.attempted=true;
    // Empty recipient + closed creator-fenced payload: this one-use transfer
    // allocates nothing and occurs BEFORE the actual Native open attempt.
    selected->transfer_to_(recipient);
    return open_(lower,actual,flags,&issued);
#else
    (void)lower;(void)actual;(void)flags;(void)issued;(void)recipient;(void)supplied;resource_fail();
#endif
}
store_physical_vfs* store_resource_open::enroll_(store_physical_vfs& recipient,sqlite3* actual,
    const std::string& alias,const ::lattice::physical_store_identity& expected,std::int64_t token,int attached_limit){
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_||impl_->creator!=pid()||!impl_->physical.current()||!impl_->root||
       impl_->path!=expected.filename||static_cast<std::uint64_t>(impl_->expected.st_dev)!=expected.device||
       static_cast<std::uint64_t>(impl_->expected.st_ino)!=expected.inode)resource_fail();
    return recipient.enroll_(actual,impl_->physical,impl_->path,impl_->expected,alias,token,attached_limit);
#else
    (void)recipient;(void)actual;(void)alias;(void)expected;(void)token;(void)attached_limit;resource_fail();
#endif
}
struct store_resource_factory_admission::implementation {
#ifdef LATTICE_RESOURCE_POSIX
    const std::uint64_t creator=pid();const std::thread::id builder=std::this_thread::get_id();
    bool taken=false;
#endif
    const deadline end;store_resource_open held;std::string actual;
    implementation(store_resource_open owner,std::string path,deadline original_end):end(original_end),held(std::move(owner)),actual(std::move(path)){}
};
store_resource_factory_admission::store_resource_factory_admission(const std::string& path,bool allow_new,deadline end){
#ifdef LATTICE_RESOURCE_POSIX
    const auto creator=pid();auto actual=store_resource_factory_path(path,end);
    auto held=resource_memory_path(path)?store_resource_open{}:store_resource_open::acquire_(actual,allow_new,end,true,true);
    auto* state=new implementation(std::move(held),std::move(actual),end);
    if(creator!=pid())resource_fail();impl_=state;
#else
    (void)allow_new;(void)end;impl_=new implementation({},path,end);
#endif
}
store_resource_factory_admission::~store_resource_factory_admission(){
#ifdef LATTICE_RESOURCE_POSIX
    if(impl_&&impl_->creator==pid())delete impl_;
#else
    delete impl_;
#endif
}
store_resource_factory_admission::store_resource_factory_admission(store_resource_factory_admission&& other)noexcept:impl_(std::exchange(other.impl_,nullptr)){}
store_resource_initialization_original store_resource_factory_admission::take_for_builder_() {
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_||impl_->creator!=pid()||impl_->builder!=std::this_thread::get_id()||impl_->taken)resource_fail();
    impl_->taken=true;
    if(!impl_->held.impl_)return store_resource_initialization_original(nullptr,impl_->end);
    return impl_->held.take_for_builder_();
#else
    if(!impl_)resource_fail();return store_resource_initialization_original(nullptr,impl_->end);
#endif
}
void store_resource_factory_admission::require_no_unclaimed_creation_()const {
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_||impl_->creator!=pid()||impl_->builder!=std::this_thread::get_id())resource_fail();
    impl_->held.require_no_unclaimed_creation_();
#endif
}
const std::string& store_resource_factory_admission::actual_path()const{
#ifdef LATTICE_RESOURCE_POSIX
    if(!impl_||impl_->creator!=pid())resource_fail();
#else
    if(!impl_)resource_fail();
#endif
    return impl_->actual;
}

#include "ordinary_resource_deletion.inc"

struct store_resource_name_observation::implementation {
#ifdef LATTICE_RESOURCE_POSIX
    const std::uint64_t creator=pid();
    std::atomic<std::uint64_t> references{1};
    descriptor parent;
    file_identity parent_identity,main_identity,member_identity;
    std::string parent_path,name,member_record_sha256;
    resource_root* root=nullptr; // process-retained DATA issuer, no Native owner
#endif
};
store_resource_name_observation store_resource_name_observation::capture_(
    const ::lattice::physical_store_identity& actual,deadline end){
#ifdef LATTICE_RESOURCE_POSIX
    const auto creator=pid();bound(end);
    if(!actual.device||!actual.inode||actual.filename.empty()||actual.filename.front()!='/')resource_fail();
    const auto at=actual.filename.rfind('/');if(at==std::string::npos||!at||at+1==actual.filename.size())resource_fail();
    auto* state=new implementation;if(creator!=pid())resource_fail();
    try{
        state->main_identity={actual.device,actual.inode};state->parent_path=actual.filename.substr(0,at);state->name=actual.filename.substr(at+1);leaf(state->name);
        auto parent=absolute_directory(state->parent_path,false);
        state->parent_identity=id(metadata(parent.value));state->parent.~descriptor();new(&state->parent)descriptor(std::move(parent));
        if(auto* installed=factory_installation(end)){
            auto [root,name]=select(*installed,actual.filename,end);
            if(name!=state->name||root->generation!=state->parent_path||root->generation_id!=state->parent_identity)resource_fail();
            auto member=active_member(*root,name,end);if(id(member.main)!=state->main_identity)resource_fail();
            state->member_identity=member.member_identity;state->member_record_sha256=member.record_sha256;state->root=root;
        }
        struct stat named{};
        if(::fstatat(state->parent.value,state->name.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!S_ISREG(named.st_mode)||id(named)!=state->main_identity)resource_fail();
        actual_directory(state->parent.value,state->parent_path,state->parent_identity,false);
        if(creator!=pid())resource_fail();store_resource_name_observation result;result.creator_=creator;result.impl_=state;return result;
    }catch(...){if(creator==pid())delete state;throw;}
#else
    (void)actual;(void)end;resource_fail();
#endif
}
store_resource_name_observation::store_resource_name_observation(const store_resource_name_observation& other)noexcept:creator_(other.creator_){
#ifdef LATTICE_RESOURCE_POSIX
    if(creator_!=pid()||!other.impl_)return;
    auto count=other.impl_->references.load(std::memory_order_acquire);
    while(count&&count!=UINT64_MAX){if(other.impl_->references.compare_exchange_weak(count,count+1,std::memory_order_acq_rel)){impl_=other.impl_;return;}}
#endif
}
store_resource_name_observation::store_resource_name_observation(store_resource_name_observation&& other)noexcept:creator_(other.creator_),impl_(std::exchange(other.impl_,nullptr)){}
store_resource_name_observation& store_resource_name_observation::operator=(const store_resource_name_observation& other)noexcept{
    if(this!=&other){store_resource_name_observation next(other);std::swap(creator_,next.creator_);std::swap(impl_,next.impl_);}return *this;
}
store_resource_name_observation& store_resource_name_observation::operator=(store_resource_name_observation&& other)noexcept{
    if(this!=&other){store_resource_name_observation next(std::move(other));std::swap(creator_,next.creator_);std::swap(impl_,next.impl_);}return *this;
}
store_resource_name_observation::~store_resource_name_observation()noexcept{
#ifdef LATTICE_RESOURCE_POSIX
    if(creator_==pid()&&impl_&&impl_->creator==pid()&&impl_->references.fetch_sub(1,std::memory_order_acq_rel)==1)delete impl_;
#endif
}
bool store_resource_name_observation::current()const noexcept{
#ifdef LATTICE_RESOURCE_POSIX
    if(creator_!=pid()||!impl_||impl_->creator!=pid())return false;
    try{
        const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(1);
        actual_directory(impl_->parent.value,impl_->parent_path,impl_->parent_identity,false);
        if(impl_->root){auto member=active_member(*impl_->root,impl_->name,end);
            if(id(member.main)!=impl_->main_identity||member.member_identity!=impl_->member_identity||
               member.record_sha256!=impl_->member_record_sha256)return false;}
        struct stat named{};return creator_==pid()&&::fstatat(impl_->parent.value,impl_->name.c_str(),&named,AT_SYMLINK_NOFOLLOW)==0&&S_ISREG(named.st_mode)&&id(named)==impl_->main_identity;
    }catch(...){return false;}
#else
    return false;
#endif
}
int store_resource_name_observation::duplicate_directory_for_events()const noexcept{
#ifdef LATTICE_RESOURCE_POSIX
    if(creator_!=pid()||!impl_||impl_->creator!=pid()||!current())return -1;
    const int duplicate=::fcntl(impl_->parent.value,F_DUPFD_CLOEXEC,3);
    if(creator_!=pid())return -1; // foreign copied descriptor is abandoned
    if(duplicate<0)return -1;
    struct stat actual{};if(::fstat(duplicate,&actual)||!S_ISDIR(actual.st_mode)||id(actual)!=impl_->parent_identity){::close(duplicate);return -1;}
    return duplicate;
#else
    return -1;
#endif
}
}
