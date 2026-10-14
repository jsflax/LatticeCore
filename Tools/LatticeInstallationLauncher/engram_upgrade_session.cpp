#include "engram_upgrade_session.hpp"
#ifdef LATTICE_MANAGED_TARGET_RECLAMATION_FIXTURE
#include <cstdio>
#include <sqlite3.h>
#endif
#include "engram_build_stamp.hpp"
#include "engram_managed_pair_stamp.hpp"
#include "engram_legacy_profile.hpp"
#include "../../Sources/LatticeCore/src/ordinary_namespace_retirement.hpp"
#include "../../Sources/LatticeCore/src/ordinary_observed_process.hpp"
#include "../../Sources/LatticeCore/src/vendor/picosha2/picosha2.h"
#include "../../Sources/LatticeCore/include/nlohmann/json.hpp"
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <vector>
#include <set>
#include <thread>
#include <utility>
#if defined(__APPLE__)
#include <TargetConditionals.h>
#endif
#if defined(__APPLE__) && TARGET_OS_OSX && !defined(__EMSCRIPTEN__)
#include <fcntl.h>
#include <dirent.h>
#include <libproc.h>
#include <mach/mach.h>
#include <mach-o/dyld.h>
#include <pwd.h>
#include <grp.h>
#include "engram_applications_policy.hpp"
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <stdio.h>
#include <unistd.h>
#define LATTICE_ENGRAM_UPGRADE_DARWIN 1
#endif

namespace lattice::detail::ordinary_installation {
namespace {
using deadline = ordinary_launch::deadline;
using json = nlohmann::json;
[[noreturn]] void refused() { throw std::runtime_error("historical upgrade custody remains incomplete"); }
void bound(deadline end) { if (std::chrono::steady_clock::now() >= end) refused(); }
#ifdef LATTICE_ENGRAM_UPGRADE_DARWIN
struct fd {
    int value=-1;
    explicit fd(int n=-1):value(n){}
    ~fd(){if(value>=0)::close(value);}
    fd(fd&& other)noexcept:value(std::exchange(other.value,-1)){}
    fd& operator=(fd&& other)noexcept{if(this!=&other){if(value>=0)::close(value);value=std::exchange(other.value,-1);}return *this;}
    fd(const fd&)=delete;
};
struct stat metadata(int n) {struct stat s{};if(n<0||::fstat(n,&s))refused();return s;}
bool same_identity(const struct stat& a,const struct stat& b){return a.st_dev==b.st_dev&&a.st_ino==b.st_ino;}
gid_t actual_admin_group(deadline end){
    // Fixed caller storage; no growing ERANGE retry, member enumeration or
    // guessed GID. The OS lookup itself is not a hard wall-clock time bound.
    std::array<char,16384> buffer{};struct group record{},*result=nullptr;
    const auto valid=[&]{
        const auto first=reinterpret_cast<std::uintptr_t>(buffer.data());
        const auto name=reinterpret_cast<std::uintptr_t>(record.gr_name);
        return result==&record&&name>=first&&name-first<=buffer.size()-6&&
            std::memcmp(record.gr_name,"admin",6)==0;
    };
    bound(end);if(::getgrnam_r("admin",&record,buffer.data(),buffer.size(),&result)||!valid())refused();
    bound(end);const auto gid=record.gr_gid;record={};result=nullptr;buffer.fill(0);
    if(::getgrgid_r(gid,&record,buffer.data(),buffer.size(),&result)||!valid()||record.gr_gid!=gid)refused();
    bound(end);return gid;
}
std::int64_t nanoseconds(const timespec& t){
    if(t.tv_sec<0||t.tv_sec>INT64_MAX/1000000000||t.tv_nsec<0||t.tv_nsec>=1000000000)refused();
    const auto seconds=std::int64_t(t.tv_sec)*1000000000;
    if(t.tv_nsec>INT64_MAX-seconds)refused();return seconds+t.tv_nsec;
}
bool stable(const struct stat& a,const struct stat& b){return same_identity(a,b)&&a.st_mode==b.st_mode&&a.st_uid==b.st_uid&&
    a.st_nlink==b.st_nlink&&a.st_size==b.st_size&&nanoseconds(a.st_mtimespec)==nanoseconds(b.st_mtimespec)&&nanoseconds(a.st_ctimespec)==nanoseconds(b.st_ctimespec);}
std::uint64_t number(const json& v,const char* key){const auto& x=v.at(key);if(!x.is_number_integer()||(x.is_number_integer()&&!x.is_number_unsigned()&&x.get<std::int64_t>()<0))refused();return x.get<std::uint64_t>();}
std::string text(const json& v,const char* key,std::size_t maximum){const auto& x=v.at(key);if(!x.is_string())refused();auto s=x.get<std::string>();if(s.empty()||s.size()>maximum||s.find('\0')!=std::string::npos)refused();return s;}
json directory_fact(const struct stat& value){
    return {{"device",value.st_dev},{"inode",value.st_ino},{"uid",value.st_uid},{"gid",value.st_gid},{"mode",value.st_mode}};
}
void fact(const struct stat& s,const json& v){
    if(!S_ISREG(s.st_mode)||s.st_nlink!=1||s.st_size<0||(s.st_mode&06022)||
       std::uint64_t(s.st_dev)!=number(v,"device")||std::uint64_t(s.st_ino)!=number(v,"inode")||
       s.st_uid!=number(v,"uid")||s.st_mode!=number(v,"mode")||s.st_nlink!=number(v,"links")||
       std::uint64_t(s.st_size)!=number(v,"bytes")||std::uint64_t(nanoseconds(s.st_mtimespec))!=number(v,"modifiedNanoseconds")||
       std::uint64_t(nanoseconds(s.st_ctimespec))!=number(v,"changedNanoseconds"))refused();
}
std::string hash_file(int descriptor,std::size_t maximum,deadline end){
    const auto before=metadata(descriptor);if(!S_ISREG(before.st_mode)||before.st_size<0||std::uint64_t(before.st_size)>maximum)refused();
    picosha2::hash256_one_by_one hash;std::array<unsigned char,65536> buffer{};off_t at=0;
    while(at<before.st_size){bound(end);const auto count=::pread(descriptor,buffer.data(),std::min<std::uint64_t>(buffer.size(),before.st_size-at),at);
        if(count<0&&errno==EINTR)continue;if(count<=0)refused();hash.process(buffer.begin(),buffer.begin()+count);at+=count;}
    bound(end);if(!stable(before,metadata(descriptor)))refused();hash.finish();return picosha2::get_hash_hex_string(hash);
}
std::string read_file(int descriptor,std::size_t maximum,deadline end){
    const auto before=metadata(descriptor);if(!S_ISREG(before.st_mode)||before.st_size<0||std::uint64_t(before.st_size)>maximum)refused();
    std::string raw(static_cast<std::size_t>(before.st_size),'\0');std::size_t at=0;
    while(at<raw.size()){bound(end);const auto n=::pread(descriptor,raw.data()+at,raw.size()-at,at);if(n<0&&errno==EINTR)continue;if(n<=0)refused();at+=n;}
    if(!stable(before,metadata(descriptor)))refused();bound(end);return raw;
}
json parse(const std::string& raw){
    if(raw.empty()||raw.size()>131072)refused();std::size_t events=0;std::vector<std::set<std::string>> keys;
    return json::parse(raw,[&](int depth,json::parse_event_t e,json& v){
        if(depth<0||depth>12||++events>8192||(v.is_string()&&v.get_ref<const std::string&>().size()>8192))refused();
        if(e==json::parse_event_t::object_start)keys.emplace_back();
        if(e==json::parse_event_t::key&&(!keys.size()||!keys.back().insert(v.get<std::string>()).second))refused();
        if(e==json::parse_event_t::object_end)keys.pop_back();return true;
    });
}
std::vector<std::string> components(const std::string& path){
    if(path.empty()||path[0]!='/'||path.size()>4096||path.back()=='/')refused();std::vector<std::string> out;
    for(std::size_t at=1;at<path.size();){const auto end=path.find('/',at);const auto part=path.substr(at,end==std::string::npos?end:end-at);
        if(part.empty()||part=="."||part==".."||part.size()>255||out.size()==32)refused();out.push_back(part);if(end==std::string::npos)break;at=end+1;}return out;
}
#endif
}

struct retained_engram_upgrade_session::implementation {
#ifdef LATTICE_ENGRAM_UPGRADE_DARWIN
    struct applications_binding {struct stat root;std::optional<gid_t> admin;};
    struct directory {fd handle;int parent;std::string leaf;struct stat original;std::optional<applications_binding> applications;};
    struct retained_file {fd handle;int parent;std::string leaf;struct stat original;std::string digest;};
    const pid_t owner=::getpid();const std::thread::id thread=std::this_thread::get_id();
    std::vector<directory> directories;std::vector<retained_file> files;
    std::vector<std::pair<int,std::string>> absent;
    std::optional<std::pair<int,struct stat>> applications_root;
    std::string home_path;int home=-1,data=-1,stage=-1,old_bin=-1;
    fd lock;json record;
    std::map<std::string,std::size_t> installed_images;
    std::map<std::string,std::size_t> cli_images;
    std::optional<engram_namespace_retirement> namespace_transition;
    // The objects, not their serialized observations, remain the live owners.
    struct role_entry {
        std::size_t image=0;
        std::string path, role;
        ordinary_launch::observed_incarnation expected;
        std::optional<ordinary_launch::observed_process> process;
        std::string terminal; // selected only by actual kernel observation below
    };
    struct retirement_phase {
        std::vector<role_entry> roles;
        json expected_event, terminal_event;
        bool recorded=false;
    };
    retirement_phase preliminary, final_phase;
    std::set<std::array<std::uint8_t,32>> admitted_incarnations;
    std::optional<historical_namespace_binding> namespace_binding;
    bool role_phase_started=false;
    void remember_incarnation(const ordinary_launch::observed_incarnation& value) {
        if(!admitted_incarnations.contains(value.audit_token)&&admitted_incarnations.size()==64)refused();
        admitted_incarnations.insert(value.audit_token);
    }
    struct stat lock_original{};
    void owner_check(deadline end)const{bound(end);if(::getpid()!=owner||std::this_thread::get_id()!=thread)refused();}
    int open_directory(int parent,const std::string& leaf,bool private_directory,deadline end){
        owner_check(end);fd next(::openat(parent,leaf.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));const auto s=metadata(next.value);
        if(!S_ISDIR(s.st_mode)||(s.st_uid!=::geteuid()&&s.st_uid!=0)||(s.st_mode&0022)||
           (private_directory&&(s.st_uid!=::geteuid()||(s.st_mode&07777)!=0700)))refused();
        const int result=next.value;directories.push_back({std::move(next),parent,leaf,s,std::nullopt});return result;
    }
    int open_global_applications(int root,deadline end){
        owner_check(end);const auto before=metadata(root);struct stat actual_root{};
        if(::fstatat(AT_FDCWD,"/",&actual_root,AT_SYMLINK_NOFOLLOW)||before.st_uid!=0||
           !applications_policy::strict_directory(before,::geteuid())||
           !applications_policy::unchanged(before,actual_root))refused();
        fd next(::openat(root,"Applications",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        const auto s=metadata(next.value);std::optional<gid_t> admin;
        if(!applications_policy::strict_directory(s,::geteuid()))admin=actual_admin_group(end);
        struct stat named{};
        if(!applications_policy::directory(s,::geteuid(),admin)||
           ::fstatat(root,"Applications",&named,AT_SYMLINK_NOFOLLOW)||!applications_policy::unchanged(s,named)||
           !applications_policy::unchanged(before,metadata(root)))refused();
        const int result=next.value;
        directories.push_back({std::move(next),root,"Applications",s,applications_binding{before,admin}});
        return result;
    }
    std::size_t open_file(int parent,const std::string& leaf,std::size_t maximum,const json* expected,bool executable,deadline end){
        owner_check(end);fd next(::openat(parent,leaf.c_str(),O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));const auto s=metadata(next.value);
        if(!S_ISREG(s.st_mode)||s.st_nlink!=1||(s.st_uid!=::geteuid()&&s.st_uid!=0)||(s.st_mode&06022)||bool(s.st_mode&0111)!=executable)refused();
        if(expected)fact(s,*expected);const auto hash=hash_file(next.value,maximum,end);
        if(expected&&hash!=text(*expected,"sha256",64))refused();
        struct stat named{};if(::fstatat(parent,leaf.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!stable(s,named))refused();
        const auto index=files.size();files.push_back({std::move(next),parent,leaf,s,hash});return index;
    }
    void verify(deadline end)const{
        owner_check(end);
        if(applications_root){struct stat root{};
            if(::fstatat(AT_FDCWD,"/",&root,AT_SYMLINK_NOFOLLOW)||
               !applications_policy::unchanged(applications_root->second,root)||
               !applications_policy::unchanged(applications_root->second,metadata(applications_root->first)))refused();}
        for(const auto& d:directories){owner_check(end);const auto now=metadata(d.handle.value);struct stat named{};
            if(::fstatat(d.parent,d.leaf.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same_identity(now,d.original)||!same_identity(now,named)||
               !S_ISDIR(now.st_mode)||!S_ISDIR(named.st_mode))refused();
            if(d.applications){
                const auto& saved=*d.applications;struct stat root{};
                if(::fstatat(AT_FDCWD,"/",&root,AT_SYMLINK_NOFOLLOW)||
                   !applications_policy::unchanged(saved.root,root)||!applications_policy::unchanged(saved.root,metadata(d.parent))||
                   !applications_policy::unchanged(d.original,now)||!applications_policy::unchanged(d.original,named)||
                   !applications_policy::directory(now,::geteuid(),saved.admin)||
                   (saved.admin&&actual_admin_group(end)!=*saved.admin))refused();
            }else if(!applications_policy::strict_directory(now,::geteuid()))refused();}
        for(const auto& f:files){owner_check(end);struct stat named{};if(::fstatat(f.parent,f.leaf.c_str(),&named,AT_SYMLINK_NOFOLLOW)||
            !stable(f.original,named)||!stable(f.original,metadata(f.handle.value)))refused();}
        for(const auto& [parent,leaf]:absent){owner_check(end);struct stat s{};if(::fstatat(parent,leaf.c_str(),&s,AT_SYMLINK_NOFOLLOW)==0||errno!=ENOENT)refused();}
        if(lock.value>=0){struct stat named{};
            if(::fstatat(stage,"lock",&named,AT_SYMLINK_NOFOLLOW)||!stable(lock_original,named)||!stable(lock_original,metadata(lock.value)))refused();}
        owner_check(end);
    }
    static std::string hex(const std::uint8_t* first,std::size_t size){
        constexpr char alphabet[]="0123456789abcdef";std::string out;out.reserve(size*2);
        for(std::size_t n=0;n<size;++n){out.push_back(alphabet[first[n]>>4]);out.push_back(alphabet[first[n]&15]);}return out;
    }
    static std::array<std::uint8_t,32> token(const std::string& value){
        if(value.size()!=64)refused();std::array<std::uint8_t,32> out{};
        const auto nibble=[](char c)->unsigned{if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;refused();};
        for(std::size_t n=0;n<32;++n)out[n]=static_cast<std::uint8_t>((nibble(value[n*2])<<4)|nibble(value[n*2+1]));return out;
    }
    json binding()const{
        if(!namespace_binding)refused();const auto& b=*namespace_binding;json ids=json::array();
        for(const auto& id:b.originals)ids.push_back({{"device",id.device},{"inode",id.inode}});
        return {{"format","engram-historical-role-retirement-v1"},{"profile",engram_legacy_profile::profile},
            {"bootstrapSHA256",picosha2::hash256_hex_string(record.dump()+"\n")},
            {"dataDevice",b.data.device},{"dataInode",b.data.inode},{"controlDevice",b.control.device},{"controlInode",b.control.inode},
            {"barrierSHA256",hex(b.barrier.data(),b.barrier.size())},{"originals",std::move(ids)}};
    }
    std::optional<json> read_event(const char* name,deadline end){
        owner_check(end);struct stat named{};
        if(::fstatat(stage,name,&named,AT_SYMLINK_NOFOLLOW)){if(errno==ENOENT)return std::nullopt;refused();}
        const auto index=open_file(stage,name,131072,nullptr,false,end);
        if(files[index].original.st_uid!=::geteuid()||(files[index].original.st_mode&07777)!=0600)refused();
        const auto bytes=read_file(files[index].handle.value,131072,end);auto parsed=parse(bytes);
        if(parsed.dump()+"\n"!=bytes)refused();
        if(::fsync(files[index].handle.value)||::fsync(stage))refused();owner_check(end);return parsed;
    }
    void write_event(const char* name,const json& value,deadline end){
        owner_check(end);const auto raw=value.dump()+"\n";if(raw.size()>131072)refused();
        if(const auto old=read_event(name,end)){if(*old!=value)refused();return;}
        const std::string temporary=std::string(name)+".pending";
        struct stat partial{};
        if(::fstatat(stage,temporary.c_str(),&partial,AT_SYMLINK_NOFOLLOW)==0){
            if(!S_ISREG(partial.st_mode)||partial.st_uid!=::geteuid()||partial.st_nlink!=1||(partial.st_mode&07777)!=0600)refused();
            // No signal/exchange can have depended on this unpublished name.
            // The complete expected journal and durable namespace remain.
            if(::unlinkat(stage,temporary.c_str(),0)||::fsync(stage))refused();
        }else if(errno!=ENOENT)refused();
        fd output(::openat(stage,temporary.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600));
        if(output.value<0||::fchmod(output.value,0600))refused();std::size_t at=0;
        while(at<raw.size()){owner_check(end);const auto n=::write(output.value,raw.data()+at,raw.size()-at);
            if(n<0&&errno==EINTR)continue;if(n<=0)refused();at+=n;}
        if(::fsync(output.value))refused();owner_check(end);
        struct stat actual{};if(::fstatat(stage,temporary.c_str(),&actual,AT_SYMLINK_NOFOLLOW)||!same_identity(actual,metadata(output.value)))refused();
        if(::renameatx_np(stage,temporary.c_str(),stage,name,RENAME_EXCL)||::fsync(stage))refused();
        const auto checked=read_event(name,end);if(!checked||*checked!=value)refused();
    }
    bool is_original(const ordinary_launch::observed_open_vnode& file)const{
        if(!namespace_binding)refused();return std::any_of(namespace_binding->originals.begin(),namespace_binding->originals.end(),
            [&](const auto& id){return id.device==file.device&&id.inode==file.inode;});
    }
    std::vector<pid_t> process_ids(deadline end)const{
        owner_check(end);std::array<pid_t,1025> values{};
        const auto bytes=::proc_listpids(PROC_UID_ONLY,::geteuid(),values.data(),sizeof(values));
        if(bytes<=0||bytes%sizeof(pid_t)||std::size_t(bytes)>=sizeof(values))refused();
        std::vector<pid_t> result;
        for(std::size_t n=0;n<std::size_t(bytes)/sizeof(pid_t);++n)if(values[n]>1&&values[n]!=owner)result.push_back(values[n]);
        std::sort(result.begin(),result.end());if(std::adjacent_find(result.begin(),result.end())!=result.end())refused();return result;
    }
    std::optional<std::size_t> supported_image(const std::string& path,deadline end){
        if(const auto found=installed_images.find(path);found!=installed_images.end())return found->second;
        const auto split=path.find_last_of('/');if(split==std::string::npos)refused();
        const auto cli=cli_images.find(path.substr(split+1));
        const auto claude_cache=home_path+"/.claude/plugins/cache/";
        const bool cache=path.starts_with(home_path+"/.codex/plugins/cache/personal/engram/")||
            (path.starts_with(claude_cache)&&path.find("/engram/",claude_cache.size())!=std::string::npos);
        if(cli==cli_images.end()||!cache)return std::nullopt;
        if(installed_images.size()==69)refused();
        const auto parts=components(path);int directory=open_directory(AT_FDCWD,"/",false,end);
        for(std::size_t n=0;n+1<parts.size();++n)directory=open_directory(directory,parts[n],false,end);
        const auto alias=open_file(directory,parts.back(),512*1024*1024,nullptr,true,end);
        if(files[alias].digest!=files[cli->second].digest)refused();
        installed_images.emplace(path,alias);return alias;
    }
    std::string process_path(pid_t pid,deadline end)const{
        owner_check(end);std::array<char,PROC_PIDPATHINFO_MAXSIZE> path{};
        if(::proc_pidpath(pid,path.data(),path.size())<=0||!std::memchr(path.data(),0,path.size()))refused();return path.data();
    }
    json role_fact(const role_entry& role)const{
        const auto& image=files[role.image];const auto& value=role.expected;
        return {{"pid",value.pid},{"auditToken",hex(value.audit_token.data(),value.audit_token.size())},
            {"imagePath",role.path},{"role",role.role},{"imageDevice",value.executable_device},{"imageInode",value.executable_inode},
            {"imageSHA256",image.digest}};
    }
    void verify_no_remaining_writer(deadline end){
        // Reject debt. This scan is never the positive retirement witness.
        // Known newly launched readers may survive the 0400 profile; they
        // cannot upgrade a read-only descriptor or reopen the denied old path
        // for writing. Unknown holders and every writable old vnode refuse.
        for(const auto pid:process_ids(end)){
            const auto state=ordinary_launch::observed_process::inspect_files(pid,end);if(!state)continue;
            bool relevant=false;for(const auto& file:state->files)if(is_original(file)){
                relevant=true;if((file.flags&FWRITE)||!file.close_on_exec)refused();}
            if(relevant&&!supported_image(process_path(pid,end),end))refused();
        }
    }
    void verify_phase(const retirement_phase& phase,bool first,deadline end){
        verify(end);if(!phase.recorded||!namespace_binding)refused();
        for(const auto& role:phase.roles){if(role.terminal!="note-exit"&&role.terminal!="kernel-old-incarnation-retired")refused();
            if(role.terminal=="note-exit"){
                if(!role.process)refused();const auto actual=role.process->wait_for_exit(end);
                if(actual.pid!=role.expected.pid||actual.audit_token!=role.expected.audit_token)refused();
            }else{
                const auto current=ordinary_launch::observed_process::reobserve(role.expected,files[role.image].handle.value,end);
                if(current)refused();
            }}
        const auto saved=read_event(first?"provisional-retirement.complete.json":"retirement.complete.json",end);
        if(!saved||*saved!=phase.terminal_event)refused();
        verify(end);
    }
    void verify_retired(deadline end){
        if(namespace_transition->has_provisional_root())verify_phase(preliminary,true,end);
        verify_phase(final_phase,false,end);verify_no_remaining_writer(end);verify(end);
    }
    void retire_phase(bool first,deadline end);
#include "engram_managed_restart_journal.inc"
#include "engram_managed_pair_restart_journal.inc"

#endif
};
retained_engram_upgrade_session::retained_engram_upgrade_session(std::shared_ptr<implementation> p):impl_(std::move(p)){}
retained_engram_upgrade_session::~retained_engram_upgrade_session()=default;
retained_engram_upgrade_session::retained_engram_upgrade_session(retained_engram_upgrade_session&&)noexcept=default;
retained_engram_upgrade_session& retained_engram_upgrade_session::operator=(retained_engram_upgrade_session&&)noexcept=default;

retained_engram_upgrade_session retained_engram_upgrade_session::open_staged(deadline end){
#ifdef LATTICE_ENGRAM_UPGRADE_DARWIN
    bound(end);
    if(!engram_legacy_profile::available||!engram_build_stamp::available||engram_build_stamp::product!="engram"||engram_build_stamp::initializer_leaf!="memory")refused();
    if(engram_legacy_profile::profile!="engram-v0.14.10-darwin-qualified-spawn-v1"||
       engram_legacy_profile::engram_revision!="5e0862b6af90578a0e2899bd379fb2b9be9f9703"||
       engram_legacy_profile::sdk_revision!="0461b210aa028f3c4bb6165fed895e0f4385b565"||
       engram_legacy_profile::core_revision!="eb9a99c5d3945108ab9d9d0d39622616d82f3586")refused();
    auto p=std::make_unique<implementation>();
    std::array<char,16384> scratch{};passwd account{};passwd* result=nullptr;
    if(::getpwuid_r(::geteuid(),&account,scratch.data(),scratch.size(),&result)||!result||account.pw_uid!=::geteuid()||!account.pw_dir)refused();
    p->home_path=account.pw_dir;
    int parent=p->open_directory(AT_FDCWD,"/",false,end);
    for(const auto& component:components(p->home_path))parent=p->open_directory(parent,component,false,end);
    p->home=parent;if(metadata(parent).st_uid!=::geteuid())refused();
    p->data=p->open_directory(p->home,".claude",false,end);
    p->old_bin=p->open_directory(p->data,"bin",false,end);
    const int state=p->open_directory(p->home,".engram-installation",true,end);
    p->stage=p->open_directory(state,"upgrade-bootstrap.v1",true,end);
    const int controller=p->open_directory(p->stage,"controller",true,end);
    const int original_configs=p->open_directory(p->stage,"original-configurations",true,end);
    p->lock=fd(::openat(p->stage,"lock",O_RDWR|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
    const auto lock_stat=metadata(p->lock.value);p->lock_original=lock_stat;struct stat lock_named{};
    if(!S_ISREG(lock_stat.st_mode)||lock_stat.st_uid!=::geteuid()||(lock_stat.st_mode&07777)!=0600||lock_stat.st_nlink!=1||lock_stat.st_size||
       ::fstatat(p->stage,"lock",&lock_named,AT_SYMLINK_NOFOLLOW)||!same_identity(lock_stat,lock_named)||::flock(p->lock.value,LOCK_EX|LOCK_NB))refused();
    const auto record_index=p->open_file(p->stage,"bootstrap.json",131072,nullptr,false,end);
    const auto sum_index=p->open_file(p->stage,"bootstrap.sha256",65,nullptr,false,end);
    for(const auto index:{record_index,sum_index})if((p->files[index].original.st_mode&07777)!=0600||p->files[index].original.st_uid!=::geteuid())refused();
    const auto raw=read_file(p->files[record_index].handle.value,131072,end);
    if(read_file(p->files[sum_index].handle.value,65,end)!=picosha2::hash256_hex_string(raw)+"\n")refused();
    p->record=parse(raw);const auto& record=p->record;
    if(text(record,"format",64)!="engram-upgrade-bootstrap-v1"||number(record,"accountUID")!=::geteuid()||
       record.at("legacyBuildProvenanceObserved")!=false||record.at("coveredCachedAliases")!=false||
       record.at("wholeCohortRetired")!=false||record.at("historicalAdoptionAuthorized")!=false)refused();
    const auto data_stat=metadata(p->data);const auto& data=record.at("dataDirectory");
    if(std::uint64_t(data_stat.st_dev)!=number(data,"device")||std::uint64_t(data_stat.st_ino)!=number(data,"inode"))refused();
    const auto staged_index=p->open_file(controller,"memory-installation-launcher",512*1024*1024,&record.at("stagedController"),true,end);
    if(p->files[staged_index].original.st_uid!=::geteuid()||(p->files[staged_index].original.st_mode&07777)!=0700)refused();
    const auto provenance=text(record,"stagedProvenanceHex",256);if(provenance.size()!=256)refused();
    std::array<unsigned char,128> bytes{};
    const auto nibble=[](char c)->unsigned{if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;refused();};
    for(std::size_t n=0;n<bytes.size();++n)bytes[n]=static_cast<unsigned char>((nibble(provenance[2*n])<<4)|nibble(provenance[2*n+1]));
    const std::array<unsigned char,24> prefix{'L','A','T','P','K','G','1',0,1,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0};
    if(!std::equal(prefix.begin(),prefix.end(),bytes.begin())||
       provenance.substr(48,40)!=engram_build_stamp::source_revision||provenance.substr(88,40)!=engram_build_stamp::core_revision||
       !std::equal(engram_build_stamp::initializer_sha256.begin(),engram_build_stamp::initializer_sha256.end(),bytes.begin()+64)||
       provenance.substr(192,64)!=p->files[staged_index].digest)refused();
    // Bind the loaded program, not merely a path that now names another image.
    std::array<char,4097> loaded{},canonical{};std::uint32_t size=loaded.size();
    if(::_NSGetExecutablePath(loaded.data(),&size)||!::realpath(loaded.data(),canonical.data())||
       p->home_path+"/.engram-installation/upgrade-bootstrap.v1/controller/memory-installation-launcher"!=canonical.data())refused();
    bool mapped=false;std::uint64_t address=0;const auto& staged=p->files[staged_index].original;
    for(std::size_t n=0;n<4096;++n){p->owner_check(end);proc_regionwithpathinfo region{};
        if(::proc_pidinfo(::getpid(),PROC_PIDREGIONPATHINFO,address,&region,sizeof(region))!=sizeof(region))refused();
        const auto& v=region.prp_vip.vip_vi.vi_stat;
        if((region.prp_prinfo.pri_protection&VM_PROT_EXECUTE)&&v.vst_ino==std::uint64_t(staged.st_ino)&&v.vst_dev==std::uint32_t(staged.st_dev)){mapped=true;break;}
        const auto start=region.prp_prinfo.pri_address,count=region.prp_prinfo.pri_size;
        if(!count||start<address||count>UINT64_MAX-start)refused();address=start+count;
    }
    if(!mapped)refused();
    const auto& images=record.at("originalInstalledImages");if(!images.is_array()||images.size()!=3)refused();
    constexpr std::array<const char*,3> image_names{"memory","memory-hooks","memory-sync"};
    for(std::size_t n=0;n<images.size();++n){if(text(images[n],"leaf",64)!=image_names[n])refused();
        const auto index=p->open_file(p->old_bin,image_names[n],512*1024*1024,&images[n],true,end);
        p->installed_images.emplace(p->home_path+"/.claude/bin/"+image_names[n],index);
        p->cli_images.emplace(image_names[n],index);
        if(engram_legacy_profile::image_sha256[n].size()!=64||p->files[index].digest!=engram_legacy_profile::image_sha256[n])refused();}
    const auto& gui=record.at("originalInstalledGUIImages");
    const std::array<std::string,2> gui_paths{"/Applications/Engram.app/Contents/MacOS/Engram",
        p->home_path+"/Applications/Engram.app/Contents/MacOS/Engram"};
    const auto& applications=record.at("globalApplicationsAnchor");
    if(text(applications,"format",64)!="engram-global-applications-anchor-v1"||!applications.at("present").is_boolean())refused();
    if(!gui.is_array()||gui.size()!=gui_paths.size())refused();
    for(std::size_t n=0;n<gui.size();++n){const auto& value=gui[n];
        if(text(value,"path",4096)!=gui_paths[n]||!value.at("present").is_boolean())refused();
        int directory=p->open_directory(AT_FDCWD,"/",false,end);std::string walked;bool missing=false;
        if(n==0){const auto root=metadata(directory);
            if(root.st_uid!=0||directory_fact(root)!=applications.at("root"))refused();
            p->applications_root=std::pair{directory,root};}
        const auto parts=components(gui_paths[n]);
        for(std::size_t i=0;i<parts.size();++i){const auto& part=parts[i];walked+="/"+part;struct stat named{};
            if(::fstatat(directory,part.c_str(),&named,AT_SYMLINK_NOFOLLOW)){
                if(errno!=ENOENT||value.at("present")!=false||text(value,"absentComponent",4096)!=walked)refused();
                if(n==0&&i==0&&applications.at("present")!=false)refused();
                p->absent.emplace_back(directory,part);missing=true;break;
            }
            if(n==0&&i==0&&applications.at("present")!=true)refused();
            if(i+1<parts.size())directory=(n==0&&i==0)
                ?p->open_global_applications(directory,end):p->open_directory(directory,part,false,end);
            else{if(value.at("present")!=true)refused();
                const auto image=p->open_file(directory,part,512*1024*1024,&value.at("original"),true,end);
                if(engram_legacy_profile::image_sha256[3].size()!=64||p->files[image].digest!=engram_legacy_profile::image_sha256[3])refused();
                p->installed_images.emplace(gui_paths[n],image);}
            if(n==0&&i==0){
                const auto& held=p->directories.back();
                if(!held.applications||directory_fact(held.original)!=applications.at("original"))refused();
                const json gid=held.applications->admin?json(*held.applications->admin):json(nullptr);
                if(applications.at("adminGID")!=gid)refused();
            }
        }
        if(value.at("present")==false&&!missing)refused();
    }
    const auto& policies=record.at("originalLaunchConfigurations");
    const std::array<std::vector<std::string>,5> paths{{{".claude.json"},{".claude","settings.json"},{".codex","config.toml"},
        {"Library","LaunchAgents","io.engram.sync.plist"},{"Library","LaunchAgents","io.engram.sync-daemon.plist"}}};
    if(!policies.is_array()||policies.size()!=paths.size())refused();
    for(std::size_t n=0;n<paths.size();++n){std::string path;for(const auto& part:paths[n])path+=(path.empty()?"":"/")+part;
        const auto& policy=policies[n];if(text(policy,"path",256)!=path||!policy.at("present").is_boolean())refused();
        int directory=p->home;std::string walked;bool saw_absent=false;
        for(std::size_t i=0;i<paths[n].size();++i){const auto& part=paths[n][i];walked+=(walked.empty()?"":"/")+part;
            struct stat named{};
            if(::fstatat(directory,part.c_str(),&named,AT_SYMLINK_NOFOLLOW)){
                if(errno!=ENOENT||policy.at("present")!=false||text(policy,"absentComponent",256)!=walked)refused();
                p->absent.emplace_back(directory,part);saw_absent=true;break;
            }
            if(i+1<paths[n].size())directory=p->open_directory(directory,part,false,end);
            else{if(policy.at("present")!=true)refused();const auto original=p->open_file(directory,part,512*1024,&policy.at("original"),false,end);
                const auto copy_name="policy-"+std::to_string(n);if(text(policy,"retainedCopy",32)!=copy_name)refused();
                const auto copy=p->open_file(original_configs,copy_name,512*1024,nullptr,false,end);
                if(p->files[copy].digest!=p->files[original].digest||p->files[copy].original.st_uid!=::geteuid()||
                   (p->files[copy].original.st_mode&07777)!=0600)refused();}
        }
        if(policy.at("present")==false&&!saw_absent)refused();
    }
    p->verify(end);return retained_engram_upgrade_session(std::move(p));
#else
    (void)end;refused();
#endif
}

#ifdef LATTICE_ENGRAM_UPGRADE_DARWIN
void retained_engram_upgrade_session::implementation::retire_phase(bool first,deadline end){
    auto& p=*this;auto& phase=first?p.preliminary:p.final_phase;
    auto base=p.binding();
    if(first){base["format"]="engram-provisional-role-retirement-v1";base["phase"]="branch-capable";}
    else if(p.namespace_transition->has_provisional_root()){
        p.verify_phase(p.preliminary,true,end);
        base["provisionalExpectedSHA256"]=picosha2::hash256_hex_string(p.preliminary.expected_event.dump()+"\n");
        base["provisionalCompleteSHA256"]=picosha2::hash256_hex_string(p.preliminary.terminal_event.dump()+"\n");
    }
    const char* expected_name=first?"provisional-retirement.expected.json":"retirement.expected.json";
    const char* completed_name=first?"provisional-retirement.complete.json":"retirement.complete.json";
    const auto prior=p.read_event(expected_name,end);
    if(prior){
        if(prior->at("binding")!=base||!prior->at("roles").is_array()||prior->at("roles").size()>64)refused();
        std::set<std::string> unique;
        for(const auto& row:prior->at("roles")){
            implementation::role_entry entry;entry.path=text(row,"imagePath",4096);entry.role=text(row,"role",32);
            auto image=p.supported_image(entry.path,end);
            if(!image&&entry.role=="repair-sqlite3"&&entry.path=="/usr/bin/sqlite3"){
                int directory=p.open_directory(AT_FDCWD,"/",false,end);
                directory=p.open_directory(directory,"usr",false,end);directory=p.open_directory(directory,"bin",false,end);
                image=p.open_file(directory,"sqlite3",512*1024*1024,nullptr,true,end);
                if(p.files[*image].original.st_uid!=0)refused();
            }
            if(!image||(entry.role!="installed-role"&&entry.role!="repair-sqlite3"))refused();entry.image=*image;
            const auto pid=number(row,"pid");if(pid<=1||pid>std::uint64_t(std::numeric_limits<pid_t>::max()))refused();
            entry.expected.pid=static_cast<pid_t>(pid);entry.expected.audit_token=implementation::token(text(row,"auditToken",64));
            entry.expected.executable_device=number(row,"imageDevice");entry.expected.executable_inode=number(row,"imageInode");
            const auto& file=p.files[*image];
            if(file.digest!=text(row,"imageSHA256",64)||std::uint64_t(file.original.st_dev)!=entry.expected.executable_device||
               std::uint64_t(file.original.st_ino)!=entry.expected.executable_inode||!unique.insert(text(row,"auditToken",64)).second)refused();
            // Empty is selected only by a NEW actual kernel lookup of this
            // persisted incarnation, not by an old serialized completion flag.
            entry.process=ordinary_launch::observed_process::reobserve(entry.expected,file.handle.value,end);
            if(!entry.process)entry.terminal="kernel-old-incarnation-retired";
            p.remember_incarnation(entry.expected);phase.roles.push_back(std::move(entry));
        }
        phase.expected_event=*prior;
    }else{
        // All ordinary role processes are captured after durable denial. A
        // process created later cannot start a writable old-store open. The
        // qualified spawn profile plus actual CLEXEC checks cover inherited
        // descriptors; unknown live holders are refused in the second pass.
        std::vector<ordinary_launch::observed_process_files> snapshots;
        for(const auto pid:p.process_ids(end)){
            auto state=ordinary_launch::observed_process::inspect_files(pid,end);if(!state)continue;
            const auto path=p.process_path(pid,end);const auto image=p.supported_image(path,end);
            if(image){
                if(phase.roles.size()==64)refused();implementation::role_entry entry;entry.image=*image;entry.path=path;entry.role="installed-role";
                entry.process=ordinary_launch::observed_process::attach(pid,p.files[*image].handle.value,end);
                entry.expected=entry.process->incarnation(end);if(entry.expected.audit_token!=state->audit_token)refused();
                for(const auto& file:state->files)if(p.is_original(file)&&!file.close_on_exec)refused();
                p.remember_incarnation(entry.expected);phase.roles.push_back(std::move(entry));
            }
            snapshots.push_back(std::move(*state));
        }
        for(const auto& state:snapshots){
            const bool holds=std::any_of(state.files.begin(),state.files.end(),[&](const auto& file){return p.is_original(file);});
            if(!holds && !first)continue;
            if(std::any_of(phase.roles.begin(),phase.roles.end(),[&](const auto& role){return role.expected.audit_token==state.audit_token;}))continue;
            // Only the actual direct child of a still-retained memory-sync
            // incarnation, using the fixed system SQLite image, is accepted
            // as the old product's repair child. Orphans/argv claims refuse.
            const auto parent=std::find_if(phase.roles.begin(),phase.roles.end(),[&](const auto& role){
                return role.expected.pid==state.parent_pid&&role.path.ends_with("/memory-sync");});
            const bool repair=parent!=phase.roles.end() &&
                p.process_path(static_cast<pid_t>(state.pid),end)=="/usr/bin/sqlite3";
            if(!holds&&!repair)continue;
            if(!repair||!parent->process||parent->process->incarnation(end).audit_token!=parent->expected.audit_token)refused();
            for(const auto& file:state.files)if(p.is_original(file)&&!file.close_on_exec)refused();
            int directory=p.open_directory(AT_FDCWD,"/",false,end);
            directory=p.open_directory(directory,"usr",false,end);directory=p.open_directory(directory,"bin",false,end);
            const auto image=p.open_file(directory,"sqlite3",512*1024*1024,nullptr,true,end);
            if(p.files[image].original.st_uid!=0||phase.roles.size()==64)refused();
            implementation::role_entry child;child.image=image;child.path="/usr/bin/sqlite3";child.role="repair-sqlite3";
            child.process=ordinary_launch::observed_process::attach(state.pid,p.files[image].handle.value,end);
            child.expected=child.process->incarnation(end);
            const auto current=ordinary_launch::observed_process::inspect_files(state.pid,end);
            if(!current||current->audit_token!=state.audit_token||child.expected.audit_token!=state.audit_token||
               current->parent_pid!=parent->expected.pid||parent->process->incarnation(end).audit_token!=parent->expected.audit_token)refused();
            p.remember_incarnation(child.expected);phase.roles.push_back(std::move(child));
        }
        json rows=json::array();for(const auto& role:phase.roles)rows.push_back(p.role_fact(role));
        phase.expected_event={{"binding",base},{"roles",std::move(rows)}};
        p.write_event(expected_name,phase.expected_event,end);
    }
    // Present expected incarnations must still carry the exact current FD
    // discipline. Completed/vanished records do not skip the fresh debt scan.
    for(auto& role:phase.roles)if(role.process){
        const auto state=ordinary_launch::observed_process::inspect_files(role.expected.pid,end);
        if(!state||state->audit_token!=role.expected.audit_token)refused();
        for(const auto& file:state->files)if(p.is_original(file)&&!file.close_on_exec)refused();
    }
    p.verify(end);
    for(auto& role:phase.roles)if(role.process)role.process->request_termination(end);
    for(auto& role:phase.roles)if(role.process){
        const auto observed=role.process->wait_for_exit(end);
        if(observed.pid!=role.expected.pid||observed.audit_token!=role.expected.audit_token||
           observed.executable_device!=role.expected.executable_device||observed.executable_inode!=role.expected.executable_inode)refused();
        role.terminal="note-exit";
    }
    p.verify_no_remaining_writer(end);p.verify(end);
    json observed=json::array();for(const auto& role:phase.roles){auto row=p.role_fact(role);row["actualKernelOutcome"]=role.terminal;observed.push_back(std::move(row));}
    phase.terminal_event={{"binding",base},{"expectedSHA256",picosha2::hash256_hex_string(phase.expected_event.dump()+"\n")},
        {"observations",std::move(observed)},{"historicalAdmissionAuthorized",false}};
    if(const auto completed=p.read_event(completed_name,end)){
        // Preserve the first actual outcomes, which may have been NOTE_EXIT;
        // the resumed session independently re-observed each expectation above.
        if(completed->at("binding")!=base||completed->at("expectedSHA256")!=phase.terminal_event.at("expectedSHA256")||
           completed->at("historicalAdmissionAuthorized")!=false||!completed->at("observations").is_array()||
           completed->at("observations").size()!=phase.roles.size())refused();
        for(std::size_t n=0;n<phase.roles.size();++n){auto fact=completed->at("observations")[n];
            const auto outcome=text(fact,"actualKernelOutcome",64);fact.erase("actualKernelOutcome");
            if(fact!=p.role_fact(phase.roles[n])||(outcome!="note-exit"&&outcome!="kernel-old-incarnation-retired"))refused();}
        phase.terminal_event=*completed;
    }else p.write_event(completed_name,phase.terminal_event,end);
    phase.recorded=true;
}
#endif

void retained_engram_upgrade_session::retire_and_preserve(deadline end){
#ifdef LATTICE_ENGRAM_UPGRADE_DARWIN
    if(!impl_)refused();auto& p=*impl_;p.verify(end);if(p.role_phase_started)refused();p.role_phase_started=true;
    // Resolve recognized live installed/cache images before the first denial.
    // Selection is not exit authority. Unknown cache generations fail here.
    for(const auto pid:p.process_ids(end)){
        const auto present=ordinary_launch::observed_process::inspect_files(pid,end);if(!present)continue;
        (void)p.supported_image(p.process_path(pid,end),end);
    }
    p.verify(end);
    struct stat present{};
    const bool resume=::fstatat(p.stage,"namespace",&present,AT_SYMLINK_NOFOLLOW)==0;
    if(!resume&&errno!=ENOENT)refused();
    p.namespace_transition.emplace(engram_namespace_retirement::stage_historical(p.home,".claude",p.stage,"namespace",end,resume));
    if(p.namespace_transition->has_provisional_root()){
        p.namespace_binding=p.namespace_transition->provisional_binding(end);
        p.retire_phase(true,end);
        provisional_namespace_custody first(p.namespace_binding->data,p.namespace_binding->control,p.namespace_binding->barrier,
            [&p](deadline check_end){p.verify_phase(p.preliminary,true,check_end);});
        p.namespace_transition->finalize_provisional_root(first,end);
    }
    p.namespace_transition->deny_new_opens(end);
    p.namespace_binding=p.namespace_transition->denied_historical_binding(end);
    p.verify(end);
    p.retire_phase(false,end);
    historical_namespace_custody custody(p.namespace_binding->data,p.namespace_binding->control,p.namespace_binding->barrier,
        [&p](deadline check_end){p.verify_retired(check_end);});
    p.namespace_transition->preserve_historical_namespace(custody,end);
    if(!p.namespace_transition->observe(end).preserved_unadopted)refused();
#else
    (void)end;refused();
#endif
}

retained_historical_generation retained_engram_upgrade_session::retire_and_retain(deadline end) {
#ifdef LATTICE_ENGRAM_UPGRADE_DARWIN
    // The old API's complete body and its actual two-phase kernel retirement
    // remain the sole entry to this transition. A second call refuses there.
    retire_and_preserve(end);
    auto owner = impl_;
    if (!owner || !owner->namespace_transition || !owner->namespace_binding) refused();
    const auto& binding = *owner->namespace_binding;
    historical_namespace_custody custody(binding.data, binding.control, binding.barrier,
        [owner](deadline check_end) { owner->verify_retired(check_end); });
    // This closure retains the actual bootstrap lock, qualified image files,
    // original policy anchors, old process owners and namespace controller.
    // It never borrows the session wrapper or reconstructs them from JSON.
    return owner->namespace_transition->retain_preserved_generation(custody, end);
#else
    (void)end; refused();
#endif
}

int retained_engram_upgrade_session::run_managed_recovery(deadline end) {
#ifdef LATTICE_ENGRAM_UPGRADE_DARWIN
    if (!impl_) refused();
    auto owner = impl_;
    owner->verify(end);
    // Resolve the new runtime from the independently fixed installer stage,
    // then bind its real bytes to this launcher's embedded final signed image.
    const int runtime = owner->open_directory(owner->stage, "managed-runtime.v1", true, end);
    const auto image_record = owner->open_file(runtime, "image.json", 4096, nullptr, false, end);
    if ((owner->files[image_record].original.st_mode & 07777) != 0600 ||
        owner->files[image_record].original.st_uid != ::geteuid()) refused();
    const auto raw = read_file(owner->files[image_record].handle.value, 4096, end);
    const auto record = parse(raw);
    if (record.dump() + "\n" != raw || text(record, "format", 64) != "engram-managed-runtime-image-v1" ||
        number(record, "accountUID") != ::geteuid() || record.size() != 4 ||
        text(record, "provenanceHex", 256) != text(owner->record, "stagedProvenanceHex", 256)) refused();
    const auto image = owner->open_file(runtime, "memory", 512 * 1024 * 1024,
        &record.at("executable"), true, end);
    const auto& actual_image = owner->files[image];
    if ((actual_image.original.st_mode & 07777) != 0700 || actual_image.original.st_uid != ::geteuid() ||
        actual_image.digest != implementation::hex(engram_build_stamp::initializer_sha256.data(), 32)) refused();
    owner->verify(end);
    owner->managed_load(image,end);
    auto retired = [&]() -> retained_historical_generation {
        if(!owner->managed_head) return retire_and_retain(end);
        if(owner->role_phase_started)refused();owner->role_phase_started=true;
        const auto predecessor=owner->managed_head->at("current");
        owner->managed_verify_terminal(predecessor,image,end);
        const auto& launch=predecessor.at("expected").at("launch");
        auto proof=std::make_shared<retained_managed_runtime_retirement::state>();
        proof->historical=implementation::managed_id(launch.at("historicalParent"));
        proof->control=implementation::managed_id(launch.at("control"));
        proof->generation=implementation::managed_id(launch.at("generation"));
        proof->barrier=implementation::token(text(launch,"barrier",64));
        // The eventual generation closure retains owner. This verifier uses a
        // weak reference to avoid owner -> namespace -> verifier -> owner.
        const std::weak_ptr<implementation> weak=owner;
        proof->verify=[weak,predecessor,image](deadline until) {
            const auto live=weak.lock();if(!live)refused();
            live->managed_check_head(until);live->managed_verify_terminal(predecessor,image,until);
        };
        retained_managed_runtime_retirement runtime_retired(std::move(proof));
        // Fresh actual old-role observations remain mandatory. Saved completion
        // records never substitute for the fixed controller's kernel checks.
        for(const auto pid:owner->process_ids(end)) {
            const auto present=ordinary_launch::observed_process::inspect_files(pid,end);if(!present)continue;
            (void)owner->supported_image(owner->process_path(pid,end),end);
        }
        owner->namespace_transition.emplace(engram_namespace_retirement::resume_after_managed_runtime(
            owner->home,".claude",owner->stage,"namespace",runtime_retired,end));
        if(owner->namespace_transition->has_provisional_root()) {
            owner->namespace_binding=owner->namespace_transition->managed_original_binding(runtime_retired,true,end);
            owner->retire_phase(true,end);
        }
        owner->namespace_binding=owner->namespace_transition->managed_original_binding(runtime_retired,false,end);
        owner->retire_phase(false,end);
        const auto& binding=*owner->namespace_binding;
        historical_namespace_custody old(binding.data,binding.control,binding.barrier,
            [owner](deadline until){owner->verify_retired(until);});
        return owner->namespace_transition->retain_after_managed_runtime(runtime_retired,old,end);
    }();
    const auto observed = retired.before_launch(end);
    fd generation(retired.duplicate_before_launch(end));
    const int control = owner->open_directory(owner->stage, "namespace", true, end);
    if (file_identity{std::uint64_t(metadata(control).st_dev),std::uint64_t(metadata(control).st_ino)} != observed.control)
        refused();
    // Cleanup is permitted only for the older, fully proved retired prior.
    // Its exact record remains durable cleanup debt until every known resource
    // is actually removed and flushed; unresolved current is never evicted.
    if(owner->managed_head && !owner->managed_head->at("prior").is_null()) {
        const auto prior=owner->managed_head->at("prior");owner->managed_verify_terminal(prior,image,end);
        auto next=*owner->managed_head;next["cleanup"]=prior;next["prior"]=nullptr;
        owner->managed_publish(std::move(next),end);owner->managed_cleanup_prior(image,end);
    }
    const auto previous_sequence=owner->managed_head?number(owner->managed_head->at("current"),"sequence"):0;
    if(previous_sequence==std::numeric_limits<std::uint64_t>::max())refused();
    const auto sequence=previous_sequence+1;const auto cohort_leaf=implementation::managed_leaf(sequence);
    auto cohort = created_launch_cohort::create_before_child(owner->stage, cohort_leaf);
    cohort.retain_managed_custody(retired.impl_);
    fd cohort_root(cohort.duplicate_accepting_root());
    const auto cohort_stat = metadata(cohort_root.value);
    std::array<std::uint8_t,16> nonce{};
    if (::getentropy(nonce.data(), nonce.size()) ||
        std::all_of(nonce.begin(), nonce.end(), [](auto byte) { return byte == 0; })) refused();
    auto identity_json = [](file_identity value) {
        return json{{"device",value.device},{"inode",value.inode}};
    };
    auto process_json = [](const ordinary_launch::process_identity& value) {
        return json{{"pid",value.pid},{"parent",value.parent},{"birthMajor",value.birth_major},{"birthMinor",value.birth_minor}};
    };
    json members = json::array();
    for (const auto& value : observed.members) members.push_back({{"name",value.relative_name},
        {"identity",identity_json(value.identity)},{"refusal",identity_json(value.refusal)},
        {"mode",value.mode},{"directory",value.directory}});
    json refusals=json::array();
    for(const auto& value:observed.old_refusals)refusals.push_back({{"name",value.relative_name},
        {"identity",identity_json(value.identity)},{"refusal",identity_json(value.refusal)},
        {"mode",value.mode},{"directory",value.directory}});
    json offer{{"format","engram-managed-history-launch-v2"},{"phase",1},
        {"cohortLeaf",cohort_leaf},{"launchSequence",sequence},{"oldRefusals",std::move(refusals)},
        {"nonce",implementation::hex(nonce.data(),nonce.size())},
        {"supervisor",process_json(ordinary_launch::current_process())},
        {"historicalParent",identity_json(observed.historical_parent)},
        {"control",identity_json(observed.control)},{"generation",identity_json(observed.generation)},
        {"cohort",identity_json({std::uint64_t(cohort_stat.st_dev),std::uint64_t(cohort_stat.st_ino)})},
        {"barrier",implementation::hex(observed.barrier.data(),observed.barrier.size())},
        {"members",std::move(members)},{"runtimeImage",record}};
    auto frame = [&](const json& value) {
        const auto bytes = value.dump();
        if (bytes.empty() || bytes.size() > ordinary_launch::maximum_frame_bytes) refused();
        return ordinary_launch::frame(bytes.begin(), bytes.end());
    };
    ordinary_launch::launch_specification specification;
    specification.executable = owner->home_path + "/.engram-installation/upgrade-bootstrap.v1/managed-runtime.v1/memory";
    specification.working_directory = owner->home_path + "/.engram-installation/upgrade-bootstrap.v1/namespace/generation";
    specification.arguments = {"--lattice-managed-history-v1"};
    specification.environment = {"PATH=/usr/bin:/bin"};
    specification.inherited_directories = {generation.value,cohort_root.value,owner->stage,runtime,owner->data,control};
    // This real observer is registered while the child is blocked in its
    // phase-2 receiver, before database admission. Its knote rejects any
    // subsequent fork/exec instead of interpreting a direct-child exit as
    // descendant closure. Losing this observation never yields completion.
    std::optional<ordinary_launch::observed_process> runtime_observer;
    std::optional<ordinary_launch::observed_incarnation> runtime_incarnation;
    json runtime_expected;
    auto cohort_snapshot = [&](deadline check_end) {
        owner->owner_check(check_end);
        fd input(::openat(cohort_root.value,"launch.v1",O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        const auto actual=metadata(input.value);struct stat named{};
        if(!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||actual.st_nlink!=1||
            (actual.st_mode&07777)!=0600||actual.st_size!=208||
            ::fstatat(cohort_root.value,"launch.v1",&named,AT_SYMLINK_NOFOLLOW)||!stable(actual,named)) refused();
        const auto bytes=read_file(input.value,208,check_end);
        if(::fstatat(cohort_root.value,"launch.v1",&named,AT_SYMLINK_NOFOLLOW)||!stable(actual,named)) refused();
        owner->owner_check(check_end);
        return json{{"device",std::uint64_t(actual.st_dev)},{"inode",std::uint64_t(actual.st_ino)},
            {"bytes",bytes.size()},{"sha256",picosha2::hash256_hex_string(bytes)}};
    };
    json journal=owner->managed_head?*owner->managed_head:json{
        {"format","engram-managed-launch-journal-v2"},{"accountUID",::geteuid()},
        {"bootstrapSHA256",picosha2::hash256_hex_string(owner->record.dump()+"\n")},
        {"revision",0},{"current",nullptr},{"prior",nullptr},{"cleanup",nullptr}};
    journal["prior"]=journal.at("current");
    journal["current"]={{"sequence",sequence},{"leaf",cohort_leaf},
        {"directory",identity_json({std::uint64_t(cohort_stat.st_dev),std::uint64_t(cohort_stat.st_ino)})},
        {"expected",nullptr},{"complete",nullptr}};
    owner->managed_publish(std::move(journal),end);
    try {
        owner->verify(end); (void)retired.before_launch(end);
        const auto index = cohort.launch(specification);
        offer["child"] = process_json(cohort.child_identity(index));
        cohort.send(index, frame(offer), end);
        auto reply = offer; reply["phase"] = 2;
        if (cohort.receive(index,end) != frame(reply)) refused();
        runtime_observer.emplace(ordinary_launch::observed_process::attach(
            cohort.child_identity(index).pid,owner->files[image].handle.value,end));
        runtime_incarnation=runtime_observer->incarnation(end);
        const auto owned_child_identity=cohort.child_identity(index);
        if(runtime_incarnation->pid!=owned_child_identity.pid||owned_child_identity.parent!=owner->owner) refused();
        // This is still before the child may open any database. Recheck actual
        // old process retirement and all old-path fences, then durably close
        // future launch admission before the one real child obtains its grant.
        owner->verify(end); (void)retired.before_launch(end);
        cohort.close_launch_admission(end);
        // Persist exact kernel expectation before sending the first open
        // grant. The closed cohort is still the actual retained object; these
        // bytes are observations only and cannot independently reopen it.
        runtime_expected={{"format","engram-managed-runtime-expectation-v1"},
            {"accountUID",::geteuid()},{"bootstrapSHA256",picosha2::hash256_hex_string(owner->record.dump()+"\n")},
            {"launch",offer},{"closedGateSnapshot",cohort_snapshot(end)},
            {"incarnation",{{"pid",runtime_incarnation->pid},
                {"auditToken",implementation::hex(runtime_incarnation->audit_token.data(),runtime_incarnation->audit_token.size())},
                {"imageDevice",runtime_incarnation->executable_device},{"imageInode",runtime_incarnation->executable_inode},
                {"imageSHA256",owner->files[image].digest}}}};
        auto expected_head=*owner->managed_head;
        expected_head["current"]["expected"]=runtime_expected;
        owner->managed_publish(std::move(expected_head),end);
        owner->verify(end);(void)retired.before_launch(end);
        if(runtime_observer->incarnation(end).audit_token!=runtime_incarnation->audit_token) refused();
        offer["phase"] = 3;
        cohort.send_closed_child(index, frame(offer), end);
        reply = offer; reply["phase"] = 4;
        if (cohort.receive_closed_child(index,end) != frame(reply)) refused();
        // Phase 4 only confirms the actual receiver accepted the launch. It
        // does not fabricate a source schema, COMMIT, canonical cut or receipt.
        std::optional<ordinary_launch::terminal_observation> terminal;
        while (!(terminal = cohort.observe_terminal(index,std::chrono::steady_clock::now()+std::chrono::seconds(1))))
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        const auto retirement_end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        const auto completed = cohort.close_and_join(retirement_end);
        if (completed.children.size()!=1 || completed.children.front().child!=terminal->child ||
            completed.children.front().wait_status!=terminal->wait_status || !runtime_observer || !runtime_incarnation) refused();
        const auto actual_exit=runtime_observer->wait_for_exit(retirement_end);
        if(actual_exit.pid!=runtime_incarnation->pid||actual_exit.audit_token!=runtime_incarnation->audit_token||
            actual_exit.executable_device!=runtime_incarnation->executable_device||
            actual_exit.executable_inode!=runtime_incarnation->executable_inode) refused();
        owner->verify(retirement_end);
        // Both the actual owned-child wait/reap and the retained fork/exec
        // rejecting knote must finish. No missing observation is replaced by
        // an exit status, elapsed deadline, empty process census or DB flags.
        const json runtime_complete{{"format","engram-managed-runtime-retirement-v1"},
            {"expectedSHA256",picosha2::hash256_hex_string(runtime_expected.dump()+"\n")},
            {"directory",identity_json(completed.directory)},{"gate",identity_json(completed.gate)},
            {"revision",completed.revision},{"terminalChild",process_json(terminal->child)},
            {"waitStatus",terminal->wait_status},{"terminalSnapshot",cohort_snapshot(retirement_end)},
            {"actualKernelOutcome","note-exit-without-observed-fork-or-exec"},
            {"observationInterval","phase2-before-open-through-actual-owned-join"},
            {"restartAdmissionGranted",false}};
        auto complete_head=*owner->managed_head;
        complete_head["current"]["complete"]=runtime_complete;
        owner->managed_publish(std::move(complete_head),retirement_end);
        owner->verify(retirement_end);
        if (WIFEXITED(terminal->wait_status)) return WEXITSTATUS(terminal->wait_status);
        if (WIFSIGNALED(terminal->wait_status)) return 128+WTERMSIG(terminal->wait_status);
        refused();
    } catch (...) {
        const auto primary = std::current_exception();
        try { (void)cohort.close_and_join(std::chrono::steady_clock::now()+std::chrono::seconds(5)); } catch (...) {}
        std::rethrow_exception(primary);
    }
#else
    (void)end; refused();
#endif
}

#include "engram_managed_pair_runtime.inc"

} // namespace lattice::detail::ordinary_installation
