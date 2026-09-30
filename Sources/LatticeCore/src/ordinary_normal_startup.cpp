#include "ordinary_normal_startup.hpp"
#include "vendor/picosha2/picosha2.h"
#include <algorithm>
#include <atomic>
#include <iterator>
#include <limits>
#include <mutex>
#include <utility>
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include <cerrno>
#include <fcntl.h>
#include <pwd.h>
#include <sys/stat.h>
#include <unistd.h>
#if defined(__APPLE__)
#include <libproc.h>
#include <mach/vm_prot.h>
#include <sys/proc_info.h>
#endif
#define LATTICE_ORDINARY_NORMAL_POSIX 1
#endif
namespace lattice::detail {
namespace {
[[noreturn]] void normal_fail(const char* reason) { throw std::runtime_error(reason); }
using namespace ordinary_installation;
constexpr std::size_t maximum_normal_catalog = 16 * 1024;
bool zero(const identifier& value) { return std::all_of(value.begin(), value.end(), [](auto c) { return c == 0; }); }
struct policy_state {
    enum class phase { untouched, uncontrolled, installing, managed, failed };
    std::atomic<phase> state{phase::untouched};
    ordinary_context context;
};
policy_state& policy() {
    // Intentionally process-retained, including during C++ static destruction.
    // One slot, one context and one generation hold; OS process exit settles it.
    static auto* value = new policy_state;
    return *value;
}
struct writer {
    std::vector<std::uint8_t> bytes;
    void u64(std::uint64_t value) { for (unsigned n = 0; n != 64; n += 8) bytes.push_back(static_cast<std::uint8_t>(value >> n)); }
    template<class T> void fixed(const T& v) { bytes.insert(bytes.end(), v.begin(), v.end()); }
    void block(const std::vector<std::uint8_t>& v) { u64(v.size()); fixed(v); }
    void text(const std::string& v) { u64(v.size()); bytes.insert(bytes.end(), v.begin(), v.end()); }
    void identity(file_identity v) { u64(v.device); u64(v.inode); }
    void process(ordinary_launch::process_identity v) { u64(v.pid); u64(v.parent); u64(v.birth_major); u64(v.birth_minor); }
};
struct reader {
    const std::vector<std::uint8_t>& bytes;
    std::size_t at = 0;
    std::uint64_t u64() {
        if (at > bytes.size() || bytes.size() - at < 8) normal_fail("managed normal offer truncated");
        std::uint64_t v = 0; for (unsigned n = 0; n != 64; n += 8) v |= std::uint64_t(bytes[at++]) << n; return v;
    }
    template<std::size_t N> std::array<std::uint8_t, N> fixed() {
        if (at > bytes.size() || bytes.size() - at < N) normal_fail("managed normal offer truncated");
        std::array<std::uint8_t, N> v{}; std::copy_n(bytes.begin() + at, N, v.begin()); at += N; return v;
    }
    std::vector<std::uint8_t> block(std::size_t maximum) {
        const auto n = u64();
        if (n > maximum || at > bytes.size() || n > bytes.size() - at) normal_fail("managed normal offer bound exceeded");
        std::vector<std::uint8_t> v(bytes.begin() + at, bytes.begin() + at + n); at += n; return v;
    }
    std::string text(std::size_t maximum) { const auto v = block(maximum); return {v.begin(), v.end()}; }
    file_identity identity() { const auto dev = u64(); return {dev, u64()}; }
    ordinary_launch::process_identity process() {
        const auto pid = u64(), parent = u64(), major = u64(), minor = u64();
        if (!pid || !parent || pid > std::uint64_t(std::numeric_limits<int>::max()) || parent > std::uint64_t(std::numeric_limits<int>::max()) || !major)
            normal_fail("managed normal process fact invalid");
        return {static_cast<std::int64_t>(pid), static_cast<std::int64_t>(parent), major, minor};
    }
};
constexpr std::array<std::uint8_t,8> magic{'L','A','T','N','O','R','1',0};
digest hash(const std::vector<std::uint8_t>& v) { digest out{}; picosha2::hash256(v.begin(),v.end(),out.begin(),out.end()); return out; }
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
struct fd_owner {
    int value = -1;
    explicit fd_owner(int v = -1) : value(v) {}
    ~fd_owner() { if (value >= 0) ::close(value); }
    fd_owner(fd_owner&& v) noexcept : value(std::exchange(v.value,-1)) {}
    fd_owner& operator=(fd_owner&& v) noexcept { if (this != &v) { if (value >= 0) ::close(value); value = std::exchange(v.value,-1); } return *this; }
    fd_owner(const fd_owner&) = delete;
};
file_identity identity(int fd, bool directory) {
    struct stat st{};
    if (::fstat(fd,&st) || st.st_uid != ::geteuid() ||
        (directory ? (!S_ISDIR(st.st_mode) || (st.st_mode & 07777) != 0700) :
                     (!S_ISREG(st.st_mode) || st.st_nlink != 1 || (st.st_mode & 0022))))
        normal_fail("managed normal descriptor ownership changed");
    return {static_cast<std::uint64_t>(st.st_dev),static_cast<std::uint64_t>(st.st_ino)};
}
void named(int parent, const char* leaf, int opened, file_identity expected, bool directory) {
    struct stat st{};
    if (identity(opened,directory) != expected || ::fstatat(parent,leaf,&st,AT_SYMLINK_NOFOLLOW) ||
        (directory ? !S_ISDIR(st.st_mode) : !S_ISREG(st.st_mode)) ||
        file_identity{static_cast<std::uint64_t>(st.st_dev),static_cast<std::uint64_t>(st.st_ino)} != expected)
        normal_fail("managed normal named object changed");
}
fd_owner absolute_directory(const std::string& path) {
    if(path.empty() || path.front()!='/')normal_fail("managed normal absolute alias required");
    fd_owner current(::open("/",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
    if(current.value<0)normal_fail("managed normal root directory unavailable");
    std::size_t at=1;
    while(at<path.size()){
        const auto end=path.find('/',at);const auto leaf=path.substr(at,end==std::string::npos?path.size()-at:end-at);
        if(leaf.empty() || leaf=="." || leaf=="..")normal_fail("managed normal alias component invalid");
        fd_owner next(::openat(current.value,leaf.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        struct stat st{};
        if(next.value<0 || ::fstat(next.value,&st) || !S_ISDIR(st.st_mode) ||
            (st.st_uid!=::geteuid() && st.st_uid!=0) || (st.st_mode&0022))
            normal_fail("managed normal alias component unavailable");
        current=std::move(next);if(end==std::string::npos)break;at=end+1;
    }
    return current;
}
void loaded_image(std::int64_t pid, const executable_fact& expected, ordinary_launch::deadline end) {
    auto held = retained_installation_file::open_executable(expected,end);
#if defined(__linux__)
    const auto path = "/proc/" + std::to_string(pid) + "/exe";
    fd_owner loaded(::open(path.c_str(),O_RDONLY|O_CLOEXEC));
#else
    std::array<char,PROC_PIDPATHINFO_MAXSIZE> path{};
    if (::proc_pidpath(static_cast<int>(pid),path.data(),static_cast<std::uint32_t>(path.size())) <= 0)
        normal_fail("managed normal loaded image unavailable");
    if (std::string(path.data()) != expected.path)
        normal_fail("managed normal loaded executable path mismatch");
    // proc_pidpath alone followed by open(path) can observe a replacement.
    // Require the executable mapping's kernel vnode identity as well. A
    // missing/denied/truncated region observation is refusal, never a fallback
    // to the named file. The approved image and controlled package mutation
    // boundary exclude hostile self-remapping by the already running product.
    std::uint64_t address=0;bool matched=false;
    for (std::size_t count=0;count!=65536;++count) {
        if (std::chrono::steady_clock::now()>=end)normal_fail("managed normal image observation expired");
        struct proc_regionwithpathinfo region{};
        if (::proc_pidinfo(static_cast<int>(pid),PROC_PIDREGIONPATHINFO,address,&region,sizeof(region)) != sizeof(region))
            normal_fail("managed normal mapped image unavailable");
        const auto& map=region.prp_prinfo;
        const auto& vnode=region.prp_vip.vip_vi.vi_stat;
        const auto path_end=std::find(std::begin(region.prp_vip.vip_path),std::end(region.prp_vip.vip_path),'\0');
        if (path_end==std::end(region.prp_vip.vip_path))normal_fail("managed normal mapped path truncated");
        if ((map.pri_protection & VM_PROT_EXECUTE) &&
            std::string(std::begin(region.prp_vip.vip_path),path_end)==expected.path) {
            if (!S_ISREG(vnode.vst_mode) || vnode.vst_uid!=::geteuid() || vnode.vst_nlink!=1 ||
                file_identity{static_cast<std::uint64_t>(static_cast<dev_t>(vnode.vst_dev)),vnode.vst_ino}!=expected.identity)
                normal_fail("managed normal mapped executable mismatch");
            matched=true;break;
        }
        if (!map.pri_size || map.pri_address<address ||
            map.pri_size>std::numeric_limits<std::uint64_t>::max()-map.pri_address)
            normal_fail("managed normal mapped image progress invalid");
        address=map.pri_address+map.pri_size;
    }
    if (!matched)normal_fail("managed normal mapped image bound exceeded");
#endif
#if defined(__linux__)
    if (loaded.value < 0 || identity(loaded.value,false) != expected.identity)
        normal_fail("managed normal loaded executable mismatch");
#endif
    held.verify(end);
}
struct installation_anchor {
    fd_owner home, directory;
    std::string home_path, directory_path, package_path;
    file_identity home_identity, directory_identity, record_identity, package_directory, package_record, memory, launcher;
    digest package_content{}, record_content{};
    void verify(ordinary_launch::deadline end) const {
        auto current_home=absolute_directory(home_path),current_directory=absolute_directory(directory_path);
        struct stat home_stat{},directory_stat{};
        if(::fstat(current_home.value,&home_stat) || ::fstat(current_directory.value,&directory_stat) ||
            home_stat.st_uid!=::geteuid() ||
            file_identity{static_cast<std::uint64_t>(home_stat.st_dev),static_cast<std::uint64_t>(home_stat.st_ino)}!=home_identity ||
            file_identity{static_cast<std::uint64_t>(directory_stat.st_dev),static_cast<std::uint64_t>(directory_stat.st_ino)}!=directory_identity ||
            directory_stat.st_uid!=::geteuid() || (directory_stat.st_mode&07777)!=0700)
            normal_fail("managed normal installation authority directory changed");
        auto record=retained_installation_file::open_record(directory.value,"managed-package.v1",record_identity,record_content,168,end);
        record.verify(end);
    }
    void match(const executable_fact& memory_file,const executable_fact& launcher_file,
            file_identity provenance,const digest& content,ordinary_launch::deadline end) const {
        verify(end);
        auto actual=absolute_directory(package_path);struct stat info{};
        if(::fstat(actual.value,&info) ||
            file_identity{static_cast<std::uint64_t>(info.st_dev),static_cast<std::uint64_t>(info.st_ino)}!=package_directory ||
            memory_file.path!=package_path+"/memory" || launcher_file.path!=package_path+"/memory-installation-launcher" ||
            memory_file.identity!=memory || launcher_file.identity!=launcher ||
            provenance!=package_record || content!=package_content)
            normal_fail("managed normal package is not the independently installed package");
    }
};
installation_anchor observe_installation_anchor(ordinary_launch::deadline end) {
    // Fixed application-managed state outside the package. The normal runtime
    // has no publisher or repair path. Controlled changes by the authorized
    // installer are a trust boundary; arbitrary same-user hostile state writes
    // are not claimed to be prevented by mode bits or a record checksum.
    struct passwd account{},*found=nullptr;std::array<char,65536> buffer{};
    if(::getpwuid_r(::geteuid(),&account,buffer.data(),buffer.size(),&found) || !found ||
        account.pw_uid!=::geteuid() || !account.pw_dir || !*account.pw_dir)
        normal_fail("managed normal OS account home unavailable");
    installation_anchor anchor;anchor.home_path=account.pw_dir;
    if(anchor.home_path.size()>4096 || anchor.home_path.back()=='/')normal_fail("managed normal OS account home invalid");
    anchor.directory_path=anchor.home_path+"/.claude/installation";
    anchor.package_path=anchor.home_path+"/.claude/bin";
    anchor.home=absolute_directory(anchor.home_path);anchor.directory=absolute_directory(anchor.directory_path);
    struct stat home{},directory{},record{};
    fd_owner source(::openat(anchor.directory.value,"managed-package.v1",O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
    if(::fstat(anchor.home.value,&home) || home.st_uid!=::geteuid() ||
        ::fstat(anchor.directory.value,&directory) || directory.st_uid!=::geteuid() || (directory.st_mode&07777)!=0700 ||
        source.value<0 || ::fstat(source.value,&record) || !S_ISREG(record.st_mode) || record.st_uid!=::geteuid() ||
        (record.st_mode&07777)!=0600 || record.st_nlink!=1 || record.st_size!=168)
        normal_fail("managed normal independently installed authority unavailable");
    anchor.home_identity={static_cast<std::uint64_t>(home.st_dev),static_cast<std::uint64_t>(home.st_ino)};
    anchor.directory_identity={static_cast<std::uint64_t>(directory.st_dev),static_cast<std::uint64_t>(directory.st_ino)};
    anchor.record_identity={static_cast<std::uint64_t>(record.st_dev),static_cast<std::uint64_t>(record.st_ino)};
    std::vector<std::uint8_t> bytes(168);std::size_t at=0;
    while(at<bytes.size()) {
        if(std::chrono::steady_clock::now()>=end)normal_fail("managed normal installed authority observation expired");
        const auto n=::pread(source.value,bytes.data()+at,bytes.size()-at,static_cast<off_t>(at));
        if(n<0 && errno==EINTR)continue;
        if(n<=0)normal_fail("managed normal installed authority truncated");at+=static_cast<std::size_t>(n);
    }
    reader in{bytes};
    if(in.fixed<8>()!=std::array<std::uint8_t,8>{'L','A','T','I','N','S','1',0} || in.u64()!=1 || in.u64()!=::geteuid() ||
        in.identity()!=anchor.home_identity)normal_fail("managed normal installation authority binding mismatch");
    anchor.package_directory=in.identity();anchor.package_record=in.identity();anchor.memory=in.identity();anchor.launcher=in.identity();
    anchor.package_content=in.fixed<32>();const auto checksum=in.fixed<32>();
    if(in.at!=bytes.size() || hash(std::vector<std::uint8_t>(bytes.begin(),bytes.begin()+136))!=checksum ||
        !anchor.package_directory.inode || !anchor.package_record.inode || !anchor.memory.inode || !anchor.launcher.inode)
        normal_fail("managed normal installation authority invalid");
    anchor.record_content=hash(bytes);anchor.verify(end);return anchor;
}
struct installed_package {
    installation_anchor authority;
    fd_owner directory, provenance;
    std::string directory_path;
    file_identity directory_identity;
    file_identity record_identity;
    struct stat record_metadata{};
    std::vector<std::uint8_t> record;
    executable_fact memory, launcher;
    void verify(ordinary_launch::deadline end) const {
        authority.match(memory,launcher,record_identity,hash(record),end);
        auto current_directory=absolute_directory(directory_path);
        struct stat directory_stat{},current{},named_record{};
        if (::fstat(current_directory.value,&directory_stat) ||
            file_identity{static_cast<std::uint64_t>(directory_stat.st_dev),static_cast<std::uint64_t>(directory_stat.st_ino)}!=directory_identity)
            normal_fail("managed normal installed package directory changed");
        auto matches=[&](const struct stat& value) {
#if defined(__APPLE__)
            const bool times=value.st_mtimespec.tv_sec==record_metadata.st_mtimespec.tv_sec &&
                value.st_mtimespec.tv_nsec==record_metadata.st_mtimespec.tv_nsec &&
                value.st_ctimespec.tv_sec==record_metadata.st_ctimespec.tv_sec &&
                value.st_ctimespec.tv_nsec==record_metadata.st_ctimespec.tv_nsec;
#else
            const bool times=value.st_mtim.tv_sec==record_metadata.st_mtim.tv_sec &&
                value.st_mtim.tv_nsec==record_metadata.st_mtim.tv_nsec &&
                value.st_ctim.tv_sec==record_metadata.st_ctim.tv_sec &&
                value.st_ctim.tv_nsec==record_metadata.st_ctim.tv_nsec;
#endif
            return file_identity{static_cast<std::uint64_t>(value.st_dev),static_cast<std::uint64_t>(value.st_ino)}==record_identity &&
                value.st_mode==record_metadata.st_mode && value.st_uid==record_metadata.st_uid && value.st_nlink==1 &&
                value.st_size==128 && times;
        };
        if (::fstat(provenance.value,&current) || !matches(current) ||
            ::fstatat(directory.value,"engram-installation.provenance",&named_record,AT_SYMLINK_NOFOLLOW) || !matches(named_record))
            normal_fail("managed normal installed provenance changed");
        std::array<std::uint8_t,128> current_bytes{};std::size_t at=0;
        while(at<current_bytes.size()) {
            if(std::chrono::steady_clock::now()>=end)normal_fail("managed normal provenance observation expired");
            const auto n=::pread(provenance.value,current_bytes.data()+at,current_bytes.size()-at,static_cast<off_t>(at));
            if(n<0 && errno==EINTR)continue;
            if(n<=0)normal_fail("managed normal provenance truncated");at+=static_cast<std::size_t>(n);
        }
        if(!std::equal(current_bytes.begin(),current_bytes.end(),record.begin()) ||
            ::fstat(provenance.value,&current) || !matches(current) ||
            ::fstatat(directory.value,"engram-installation.provenance",&named_record,AT_SYMLINK_NOFOLLOW) || !matches(named_record) ||
            std::chrono::steady_clock::now()>=end)
            normal_fail("managed normal installed provenance changed during observation");
    }
};
installed_package independently_installed_package(ordinary_launch::deadline end) {
    // Locate the actually loaded memory image through the kernel, never an
    // offer, argv[0], PATH search, environment variable or requested DB path.
    std::string loaded;
#if defined(__linux__)
    std::array<char,4097> path{};
    const auto n=::readlink("/proc/self/exe",path.data(),path.size());
    if(n<=0 || static_cast<std::size_t>(n)>=path.size())normal_fail("managed normal own image path unavailable");
    loaded.assign(path.data(),static_cast<std::size_t>(n));
#else
    std::array<char,PROC_PIDPATHINFO_MAXSIZE> path{};
    if(::proc_pidpath(::getpid(),path.data(),static_cast<std::uint32_t>(path.size()))<=0)
        normal_fail("managed normal own image path unavailable");
    loaded=path.data();
#endif
    const auto slash=loaded.rfind('/');
    if(slash==std::string::npos || loaded.substr(slash+1)!="memory")normal_fail("managed normal product image required");
    const auto directory=loaded.substr(0,slash);
    installed_package result;result.authority=observe_installation_anchor(end);
    if(directory!=result.authority.package_path)normal_fail("managed normal copied package cannot enroll itself");
    result.directory_path=directory;result.directory=absolute_directory(directory);
    struct stat directory_stat{};
    if(::fstat(result.directory.value,&directory_stat))normal_fail("managed normal package directory unavailable");
    result.directory_identity={static_cast<std::uint64_t>(directory_stat.st_dev),static_cast<std::uint64_t>(directory_stat.st_ino)};
    fd_owner record(::openat(result.directory.value,"engram-installation.provenance",O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
    struct stat st{};
    if(record.value<0 || ::fstat(record.value,&st) || !S_ISREG(st.st_mode) || st.st_uid!=::geteuid() ||
       (st.st_mode&06133) || st.st_nlink!=1 || st.st_size!=128)
        normal_fail("managed normal installed provenance unavailable");
    result.record_identity={static_cast<std::uint64_t>(st.st_dev),static_cast<std::uint64_t>(st.st_ino)};
    result.record_metadata=st;
    result.record.resize(128);std::size_t at=0;
    while(at<result.record.size()){
        if(std::chrono::steady_clock::now()>=end)normal_fail("managed normal provenance observation expired");
        const auto n=::read(record.value,result.record.data()+at,result.record.size()-at);
        if(n<0 && errno==EINTR)continue;if(n<=0)normal_fail("managed normal provenance truncated");at+=static_cast<std::size_t>(n);
    }
    const std::array<std::uint8_t,24> prefix{'L','A','T','P','K','G','1',0,1,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0};
    if(!std::equal(prefix.begin(),prefix.end(),result.record.begin()))normal_fail("managed normal installed product mismatch");
    auto binary=[&](const char* leaf,std::size_t offset){
        fd_owner file(::openat(result.directory.value,leaf,O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        if(file.value<0)normal_fail("managed normal installed binary missing");
        executable_fact value;value.path=directory+"/"+leaf;value.identity=identity(file.value,false);
        std::copy_n(result.record.begin()+offset,32,value.content.begin());
        auto held=retained_installation_file::open_executable(value,end);held.verify(end);return value;
    };
    result.memory=binary("memory",64);result.launcher=binary("memory-installation-launcher",96);
    loaded_image(::getpid(),result.memory,end);
    loaded_image(ordinary_launch::current_parent_process().pid,result.launcher,end);
    result.provenance=std::move(record);result.verify(end);
    return result;
}
void bound(ordinary_launch::deadline end) { if (std::chrono::steady_clock::now() >= end) normal_fail("managed normal handshake expired"); }
#endif
}
namespace ordinary_installation {
void verify_normal_installed_root(const executable_fact& memory,const executable_fact& launcher,
        file_identity record,const digest& content,ordinary_launch::deadline end) {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    auto authority=observe_installation_anchor(end);authority.match(memory,launcher,record,content,end);
    loaded_image(ordinary_launch::current_process().pid,launcher,end);
    authority.verify(end);
#else
    (void)memory;(void)launcher;(void)record;(void)content;(void)end;normal_fail("managed normal installed authority unsupported");
#endif
}
void validate_primary_mcp_contract(const manifest& origin, const manifest& normal) {
    (void)encode_manifest(origin); (void)encode_manifest(normal);
    if (origin.application != product::engram || normal.application != product::engram ||
        origin.revision != 1 || normal.revision != 1 || origin.installation != normal.installation ||
        origin.stores.size() != 1 || normal.stores != origin.stores || origin.roles.size() != 1 || normal.roles.size() != 1 ||
        origin.supervisor != normal.supervisor || normal.catalog_directory == origin.catalog_directory ||
        normal.launch_gate == origin.launch_gate)
        normal_fail("managed primary MCP catalog mismatch");
    const auto& store=origin.stores.front(); const auto& seed=origin.roles.front(); const auto& role=normal.roles.front();
    if (store.aliases.size()!=1 || store.aliases.front().leaf!="memory.sqlite" || store.control_leaf!="control" ||
        seed.name!="engram-initializer-v1" || seed.arguments!=std::vector<std::string>{"--lattice-seed-store-v1"} ||
        seed.environment!=std::vector<std::string>{"PATH=/usr/bin:/bin"} || seed.custody!=lifetime::caller_bound ||
        role.name!="engram-primary-mcp-v1" || role.arguments!=std::vector<std::string>{"--lattice-managed-mcp-v1"} ||
        role.executable!=seed.executable || role.working_directory!=seed.working_directory || role.stores!=seed.stores ||
        seed.stores!=std::vector<identifier>{store.binding.store} || role.custody!=lifetime::caller_bound ||
        role.working_directory!=store.aliases.front().parent_path)
        normal_fail("managed primary MCP invocation mismatch");
    // Fixed installation profile. No inherited argv/environment can grant a
    // role or authorize dynamic synced/group paths. Explicit compatibility
    // expansion must preserve actual normal behavior and receive its own gate.
    const auto primary=store.aliases.front().parent_path+"/memory.sqlite";
    if(role.environment.size()<2 || role.environment.size()>5 || role.environment[0]!="PATH=/usr/bin:/bin" ||
        role.environment[1]!="CLAUDE_MEMORY_DB="+primary)normal_fail("managed primary MCP environment mismatch");
    std::size_t at=2;
    for(const auto* key:{"CLAUDE_MEMORY_MODEL=","CLAUDE_SESSION_ID=","ENGRAM_LATTICE_LOG_LEVEL="})
        if(at<role.environment.size() && role.environment[at].starts_with(key))++at;
    if(at!=role.environment.size())normal_fail("managed primary MCP environment mismatch");
}
std::vector<std::uint8_t> encode_normal_offer(const normal_launch_offer& v) {
    const auto valid_process=[](const ordinary_launch::process_identity& p) {
        return p.pid>0 && p.parent>0 && p.pid<=std::numeric_limits<int>::max() &&
            p.parent<=std::numeric_limits<int>::max() && p.birth_major!=0;
    };
    if (v.phase<1 || v.phase>4 || zero(v.nonce) || v.external_anchor.size()!=320 ||
        !valid_process(v.supervisor) || !valid_process(v.child) || v.child.parent!=v.supervisor.pid || v.child.pid==v.supervisor.pid ||
        v.origin_catalog.size()>maximum_normal_catalog || v.normal_catalog.size()>maximum_normal_catalog ||
        v.external_leaf.empty() || v.external_leaf.size()>255 || v.external_leaf=="." || v.external_leaf==".." ||
        v.external_leaf.find('/')!=std::string::npos || v.external_leaf.find('\0')!=std::string::npos ||
        !v.external_identity.inode || !v.normal_catalog_identity.inode)
        normal_fail("managed normal offer invalid");
    const auto origin=decode_manifest(v.origin_catalog),normal=decode_manifest(v.normal_catalog);
    validate_primary_mcp_contract(origin,normal);
    if (v.admission.binding!=origin.stores.front().binding ||
        ordinary_admission::control_binding{v.admission.control,v.admission.entry,v.admission.generation}!=origin.stores.front().controls)
        normal_fail("managed normal offer store binding mismatch");
    if (v.phase<=2 ? (v.admission.state!=ordinary_admission::stage::unadopted || v.admission.revision!=1 || !zero(v.admission.normal_launch)) :
                     (v.admission.state!=ordinary_admission::stage::ordinary_admitted || v.admission.revision!=2 || v.admission.normal_launch!=v.nonce))
        normal_fail("managed normal offer admission mismatch");
    writer out;out.fixed(magic);out.u64(v.phase);out.fixed(v.nonce);out.process(v.supervisor);out.process(v.child);
    out.identity(v.external_identity);out.identity(v.normal_catalog_identity);out.text(v.external_leaf);
    out.block(v.external_anchor);out.block(v.origin_catalog);out.block(v.normal_catalog);out.fixed(ordinary_admission::encode(v.admission));
    if(out.bytes.size()>ordinary_launch::maximum_frame_bytes)normal_fail("managed normal offer too large");
    return out.bytes;
}
normal_launch_offer decode_normal_offer(const std::vector<std::uint8_t>& bytes) {
    if(bytes.size()>ordinary_launch::maximum_frame_bytes)normal_fail("managed normal offer too large");
    reader in{bytes};if(in.fixed<8>()!=magic)normal_fail("managed normal offer version invalid");
    normal_launch_offer v;v.phase=in.u64();v.nonce=in.fixed<16>();v.supervisor=in.process();v.child=in.process();
    v.external_identity=in.identity();v.normal_catalog_identity=in.identity();v.external_leaf=in.text(255);
    v.external_anchor=in.block(320);v.origin_catalog=in.block(maximum_normal_catalog);v.normal_catalog=in.block(maximum_normal_catalog);
    v.admission=ordinary_admission::decode(in.fixed<256>());
    if(in.at!=bytes.size() || encode_normal_offer(v)!=bytes)normal_fail("managed normal offer noncanonical");
    return v;
}
}
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
struct ordinary_open_context::implementation {
    fd_owner data, control, root, installer_parent, normal_root, main;
    installed_package package;
    mutable std::atomic<bool> tainted{false};
    std::unique_ptr<ordinary_launch::inherited_channel> channel;
    ordinary_admission::generation_hold generation;
    normal_launch_offer offer;
    manifest origin, normal;
    file_identity parent_identity;
    ordinary_launch::process_identity process;
    std::string path;
    void verify(ordinary_launch::deadline end, bool admitted) const {
        bound(end);
        if(tainted.load(std::memory_order_acquire))normal_fail("managed normal context previously invalidated");
        if(ordinary_launch::current_process()!=process || ordinary_launch::current_parent_process()!=offer.supervisor || process!=offer.child)
            normal_fail("managed normal process lifetime changed");
        if(normal.supervisor!=package.launcher || normal.roles.front().executable!=package.memory)
            normal_fail("managed normal offer does not match independently installed package");
        package.verify(end);
        const auto& store=origin.stores.front();
        if(identity(data.value,true)!=store.binding.parent || identity(control.value,true)!=store.controls.control ||
           identity(root.value,true)!=origin.catalog_directory || identity(normal_root.value,true)!=normal.catalog_directory ||
           identity(installer_parent.value,true)!=parent_identity)
            normal_fail("managed normal directory changed");
        if(!offer.external_leaf.ends_with(".origin"))normal_fail("managed normal external name invalid");
        const auto root_leaf=offer.external_leaf.substr(0,offer.external_leaf.size()-7);
        named(installer_parent.value,root_leaf.c_str(),root.value,origin.catalog_directory,true);
        auto alias=absolute_directory(store.aliases.front().parent_path);
        if(identity(alias.value,true)!=store.binding.parent)normal_fail("managed normal alias parent replaced");
        named(alias.value,"memory.sqlite",main.value,store.binding.main,false);
        named(root.value,"data",data.value,store.binding.parent,true);
        named(root.value,"control",control.value,store.controls.control,true);
        named(root.value,"normal-runtime",normal_root.value,normal.catalog_directory,true);
        named(data.value,"memory.sqlite",main.value,store.binding.main,false);
        auto external=retained_installation_file::open_record(installer_parent.value,offer.external_leaf,offer.external_identity,
            hash(offer.external_anchor),320,end);
        // Independent parent entry carries exact original origin bytes/identity;
        // neither an inner catalog nor a coherent journal rewrite may relearn it.
        reader ext{offer.external_anchor};
        if(ext.fixed<8>()!=std::array<std::uint8_t,8>{'L','A','T','R','E','G','1',0} || ext.u64()!=1)
            normal_fail("managed normal external anchor invalid");
        const auto origin_id=ext.identity();const auto origin_hash=ext.fixed<32>();
        const auto origin_bytes=ext.fixed<256>();
        std::vector<std::uint8_t> origin_record(origin_bytes.begin(),origin_bytes.end());
        if(hash(origin_record)!=origin_hash)normal_fail("managed normal origin digest mismatch");
        const auto package_hash=hash(package.record);
        if(!std::equal(package_hash.begin(),package_hash.end(),origin_record.begin()+96))
            normal_fail("managed normal registered package changed");
        constexpr char hex[]="0123456789abcdef";
        for(std::size_t i=0;i!=40;++i){
            const auto byte=package.record[24+i];
            if(origin_record[128+2*i]!=static_cast<std::uint8_t>(hex[byte>>4]) ||
               origin_record[129+2*i]!=static_cast<std::uint8_t>(hex[byte&15]))
                normal_fail("managed normal registered source revision changed");
        }
        auto held_origin=retained_installation_file::open_record(root.value,"origin.v1",origin_id,origin_hash,256,end);
        reader orig{origin_record};
        if(orig.fixed<8>()!=std::array<std::uint8_t,8>{'L','A','T','O','R','G','1',0} || orig.u64()!=1 || orig.fixed<16>()!=origin.installation ||
           orig.identity()!=origin.catalog_directory)normal_fail("managed normal origin binding mismatch");
        const auto catalog_id=orig.identity(); const auto catalog_hash=orig.fixed<32>();
        if(hash(offer.origin_catalog)!=catalog_hash)normal_fail("managed normal catalog digest mismatch");
        auto held_catalog=retained_installation_file::open_record(root.value,"catalog.v1",catalog_id,catalog_hash,maximum_normal_catalog,end);
        auto held_normal=retained_installation_file::open_record(normal_root.value,"normal-role.v1",offer.normal_catalog_identity,
            hash(offer.normal_catalog),maximum_normal_catalog,end);
        fd_owner launch_gate(::openat(normal_root.value,"launch.lock",O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        if(launch_gate.value<0)normal_fail("managed normal launch gate unavailable");
        named(normal_root.value,"launch.lock",launch_gate.value,normal.launch_gate,false);
        auto journal=ordinary_admission::journal::open_existing(control.value,store.binding,store.controls);
        auto expected=offer.admission;
        if(!admitted){expected.state=ordinary_admission::stage::unadopted;expected.revision=1;expected.normal_launch={};}
        if(journal.read()!=expected)normal_fail("managed normal admission not current");
        external.verify(end);held_origin.verify(end);held_catalog.verify(end);held_normal.verify(end);package.verify(end);
        bound(end);
    }
};
#else
struct ordinary_open_context::implementation {};
#endif
ordinary_open_context::ordinary_open_context(std::shared_ptr<implementation> impl):impl_(std::move(impl)){}
ordinary_open_context::~ordinary_open_context()=default;
std::string ordinary_open_context::primary_path() const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_)normal_fail("managed normal context missing");return impl_->path;
#else
    normal_fail("managed normal context unsupported");
#endif
}
void ordinary_open_context::validate_open(const std::string& path) const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_ || path!=impl_->path)normal_fail("managed normal unregistered store refused");
    try { impl_->verify(std::chrono::steady_clock::now()+std::chrono::seconds(10),true); }
    catch(const ordinary_admission::error& error) {
        if(error.code!=ordinary_admission::error_code::busy)impl_->tainted.store(true,std::memory_order_release);
        throw;
    } catch(...) { impl_->tainted.store(true,std::memory_order_release);throw; }
#else
    (void)path;normal_fail("managed normal context unsupported");
#endif
}
void ordinary_open_context::validate_physical(std::uint64_t device,std::uint64_t inode) const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_ || file_identity{device,inode}!=impl_->origin.stores.front().binding.main)
        normal_fail("managed normal opened a different physical store");
    validate_open(impl_->path);
#else
    (void)device;(void)inode;normal_fail("managed normal context unsupported");
#endif
}
void ordinary_before_open(const std::string& path,const ordinary_context& context) {
    auto& gate=policy();auto state=gate.state.load(std::memory_order_acquire);
    if(state==policy_state::phase::untouched){
        auto expected=state;gate.state.compare_exchange_strong(expected,policy_state::phase::uncontrolled,std::memory_order_acq_rel);
        state=gate.state.load(std::memory_order_acquire);
    }
    if(state==policy_state::phase::managed){
        if(path==":memory:")return; // isolated memory remains a distinct typed case
        if(!context || context!=gate.context)normal_fail("managed normal capability required before open");
        context->validate_open(path);return;
    }
    if(state==policy_state::phase::installing || state==policy_state::phase::failed || context)
        normal_fail("managed normal startup is closed");
}
bool ordinary_requires_attachment_guard() {
    const auto phase=policy().state.load(std::memory_order_acquire);
    return phase==policy_state::phase::managed || phase==policy_state::phase::installing || phase==policy_state::phase::failed;
}
void ordinary_require_no_raw_escape(const ordinary_context&) {
    auto& gate=policy();auto expected=policy_state::phase::untouched;
    gate.state.compare_exchange_strong(expected,policy_state::phase::uncontrolled,std::memory_order_acq_rel);
    const auto phase=gate.state.load(std::memory_order_acquire);
    if(phase!=policy_state::phase::uncontrolled)normal_fail("managed normal raw escape refused");
}
void ordinary_require_no_attachment(const ordinary_context&) {
    const auto phase=policy().state.load(std::memory_order_acquire);
    if(phase==policy_state::phase::managed || phase==policy_state::phase::installing || phase==policy_state::phase::failed)
        normal_fail("managed primary-only attachment refused");
}
namespace ordinary_installation {
ordinary_context normal_receiver::receive(ordinary_launch::deadline end) {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    auto& gate=policy();auto prior=policy_state::phase::untouched;
    if(!gate.state.compare_exchange_strong(prior,policy_state::phase::installing,std::memory_order_acq_rel))
        normal_fail("managed normal receiver must precede all native opens");
    try {
        // Failed initial descriptor acquisition is sticky closed as well. A
        // duplicate call cannot take descriptors owned by the existing context.
        fd_owner data(4),control(5),root(6),parent(7),normal_root(8);
        ordinary_launch::inherited_channel channel(3);
        auto state=std::make_shared<ordinary_open_context::implementation>();
        state->data=std::move(data);state->control=std::move(control);state->root=std::move(root);
        state->installer_parent=std::move(parent);state->normal_root=std::move(normal_root);
        state->channel=std::make_unique<ordinary_launch::inherited_channel>(std::move(channel));
        for(int fd:{4,5,6,7,8})if(::fcntl(fd,F_SETFD,FD_CLOEXEC))normal_fail("managed normal descriptor confinement failed");
        state->package=independently_installed_package(end);
        state->offer=decode_normal_offer(state->channel->receive(end));
        if(state->offer.phase!=1)normal_fail("managed normal initial phase invalid");
        state->origin=decode_manifest(state->offer.origin_catalog);state->normal=decode_manifest(state->offer.normal_catalog);
        state->process=ordinary_launch::current_process();state->parent_identity=identity(7,true);
        state->path=state->origin.stores.front().aliases.front().parent_path+"/memory.sqlite";
        state->main=fd_owner(::openat(4,"memory.sqlite",O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        if(state->main.value<0)normal_fail("managed normal main file unavailable");
        loaded_image(state->process.pid,state->normal.roles.front().executable,end);
        loaded_image(state->offer.supervisor.pid,state->normal.supervisor,end);
        state->verify(end,false);
        auto response=state->offer;response.phase=2;state->channel->send(encode_normal_offer(response),end);
        response.phase=3;response.admission.state=ordinary_admission::stage::ordinary_admitted;
        ++response.admission.revision;response.admission.normal_launch=response.nonce;
        if(state->channel->receive(end)!=encode_normal_offer(response))normal_fail("managed normal admission response mismatch");
        state->offer=response;
        auto journal=ordinary_admission::journal::open_existing(5,state->origin.stores.front().binding,state->origin.stores.front().controls);
        state->generation=journal.hold_ordinary(response.admission);state->verify(end,true);
        auto context=ordinary_context(new ordinary_open_context(state));
        // Publish once. The process slot deliberately retains the context even
        // if the final ready send fails or every Swift wrapper is destroyed.
        gate.context=context;gate.state.store(policy_state::phase::managed,std::memory_order_release);
        response.phase=4;state->channel->send(encode_normal_offer(response),end);return context;
    } catch (...) {gate.state.store(policy_state::phase::failed,std::memory_order_release);throw;}
#else
    (void)end;normal_fail("managed normal receiver unsupported");
#endif
}
}
} // namespace lattice::detail
