// Included only by ordinary_normal_startup.cpp, inside lattice::detail.
// Shares its actual process policy and physical/image verification primitives.
namespace {
using managed_json = nlohmann::json;
managed_json managed_parse(const std::vector<uint8_t>& raw) {
    if(raw.empty() || raw.size()>ordinary_launch::maximum_frame_bytes)
        normal_fail("managed history frame bound exceeded");
    size_t events=0;std::vector<std::set<std::string>> keys;
    return managed_json::parse(raw.begin(),raw.end(),[&](int depth,managed_json::parse_event_t event,managed_json& value){
        if(depth<0 || depth>12 || ++events>8192 ||
            (value.is_string() && value.get_ref<const std::string&>().size()>8192))
            normal_fail("managed history frame structure bound exceeded");
        if(event==managed_json::parse_event_t::object_start)keys.emplace_back();
        if(event==managed_json::parse_event_t::key &&
            (keys.empty() || !keys.back().insert(value.get<std::string>()).second))
            normal_fail("managed history duplicate key");
        if(event==managed_json::parse_event_t::object_end)keys.pop_back();return true;
    });
}
uint64_t managed_unsigned(const managed_json& value) {
    if(!value.is_number_integer() || (!value.is_number_unsigned() && value.get<int64_t>()<0))
        normal_fail("managed history unsigned fact invalid");
    return value.get<uint64_t>();
}
std::string managed_text(const managed_json& value,size_t maximum) {
    if(!value.is_string())normal_fail("managed history text fact invalid");
    const auto& text=value.get_ref<const std::string&>();
    if(text.empty() || text.size()>maximum || text.find('\0')!=std::string::npos)
        normal_fail("managed history text bound exceeded");
    return text;
}
file_identity managed_identity(const managed_json& value) {
    if(!value.is_object() || value.size()!=2 || !value.contains("device") || !value.contains("inode"))
        normal_fail("managed history identity invalid");
    return {managed_unsigned(value.at("device")),managed_unsigned(value.at("inode"))};
}
ordinary_launch::process_identity managed_process(const managed_json& value) {
    if(!value.is_object() || value.size()!=4)normal_fail("managed history process invalid");
    const auto pid=managed_unsigned(value.at("pid")),parent=managed_unsigned(value.at("parent"));
    const auto major=managed_unsigned(value.at("birthMajor")),minor=managed_unsigned(value.at("birthMinor"));
    if(!pid || !parent || pid>INT_MAX || parent>INT_MAX || !major)normal_fail("managed history process bound exceeded");
    return {int64_t(pid),int64_t(parent),major,minor};
}
std::vector<uint8_t> managed_hex(const std::string& value,size_t count) {
    if(value.size()!=count*2)normal_fail("managed history digest size invalid");
    auto nibble=[](char c)->unsigned{if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;
        normal_fail("managed history digest spelling invalid");};
    std::vector<uint8_t> result(count);
    for(size_t n=0;n<count;++n)result[n]=uint8_t(nibble(value[2*n])*16+nibble(value[2*n+1]));return result;
}
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
struct managed_record {
    fd_owner descriptor;
    file_identity identity;
    digest content{};
    std::vector<uint8_t> bytes;
};
managed_record managed_read_record(int parent,const char* name,size_t maximum,ordinary_launch::deadline end) {
    if(std::chrono::steady_clock::now()>=end)normal_fail("managed history record expired");
    managed_record result;result.descriptor=fd_owner(::openat(parent,name,O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
    // Historical bootstrap has its own established 128 KiB limit. Do not
    // route it through the unrelated 120 KiB installation-manifest helper.
    if(!maximum || maximum>131072)normal_fail("managed history record bound invalid");
    struct stat before{},after{},entry{};
    if(result.descriptor.value<0 || ::fstat(result.descriptor.value,&before) || !S_ISREG(before.st_mode) ||
        before.st_uid!=::geteuid() || before.st_nlink!=1 || (before.st_mode&07777)!=0600 ||
        before.st_size<0 || uint64_t(before.st_size)>maximum)
        normal_fail("managed history record ownership invalid");
    result.identity={uint64_t(before.st_dev),uint64_t(before.st_ino)};
    result.bytes.resize(size_t(before.st_size));
    for(size_t at=0;at<result.bytes.size();){
        if(std::chrono::steady_clock::now()>=end)normal_fail("managed history record expired");
        const auto count=::pread(result.descriptor.value,result.bytes.data()+at,result.bytes.size()-at,off_t(at));
        if(count<0&&errno==EINTR)continue;if(count<=0)normal_fail("managed history record truncated");at+=size_t(count);
    }
    auto unchanged=[&](const struct stat& current){
#if defined(__APPLE__)
        const bool times=before.st_mtimespec.tv_sec==current.st_mtimespec.tv_sec &&
            before.st_mtimespec.tv_nsec==current.st_mtimespec.tv_nsec &&
            before.st_ctimespec.tv_sec==current.st_ctimespec.tv_sec &&
            before.st_ctimespec.tv_nsec==current.st_ctimespec.tv_nsec;
#else
        const bool times=before.st_mtim.tv_sec==current.st_mtim.tv_sec &&
            before.st_mtim.tv_nsec==current.st_mtim.tv_nsec &&
            before.st_ctim.tv_sec==current.st_ctim.tv_sec &&
            before.st_ctim.tv_nsec==current.st_ctim.tv_nsec;
#endif
        return times && before.st_dev==current.st_dev && before.st_ino==current.st_ino &&
            before.st_size==current.st_size && before.st_mode==current.st_mode &&
            before.st_uid==current.st_uid && before.st_gid==current.st_gid &&
            before.st_nlink==current.st_nlink;
    };
    if(::fstat(result.descriptor.value,&after) || ::fstatat(parent,name,&entry,AT_SYMLINK_NOFOLLOW) ||
        !unchanged(after) || !unchanged(entry))normal_fail("managed history record changed");
    result.content=hash(result.bytes);
    if(std::chrono::steady_clock::now()>=end)normal_fail("managed history record expired");
    return result;
}
void managed_verify_record(int parent,const char* name,const managed_record& retained,
        size_t maximum,ordinary_launch::deadline end) {
    auto current=managed_read_record(parent,name,maximum,end);
    if(current.identity!=retained.identity || current.content!=retained.content || current.bytes!=retained.bytes ||
        identity(retained.descriptor.value,false)!=retained.identity)
        normal_fail("managed history retained record changed");
}
file_identity managed_directory_identity(int fd) {
    struct stat value{};
    if(::fstat(fd,&value) || !S_ISDIR(value.st_mode) || value.st_uid!=::geteuid() || (value.st_mode&0022))
        normal_fail("managed history directory ownership invalid");
    return {uint64_t(value.st_dev),uint64_t(value.st_ino)};
}
bool managed_group_leaf(const std::string& leaf) {
    if(leaf.size()!=6+36+7 || !leaf.starts_with("group-") || !leaf.ends_with(".sqlite"))return false;
    for(size_t n=0;n<36;++n){const auto c=leaf[n+6];
        if(n==8||n==13||n==18||n==23){if(c!='-')return false;}
        else if(!((c>='0'&&c<='9')||(c>='a'&&c<='f')||(c>='A'&&c<='F')))return false;}
    return true;
}
#endif
}
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
struct ordinary_managed_child_admission::implementation {
    fd_owner generation,cohort,stage,runtime,historical,control;
    fd_owner target_root;
    file_identity target_root_identity;
    std::string target_root_leaf;
    struct target_member {
        std::string old_member,leaf;
        file_identity old_main,directory_identity;
        fd_owner directory;
    };
    std::array<target_member,64> targets;
    std::array<std::atomic<const ordinary_unpublished_group_target*>,64> target_original{};
    // Weak census links never own the connection/IO/admission cycle. The
    // connection's process-retained registry owns every published attempt.
    mutable std::mutex target_resource_mutex;
    std::array<std::weak_ptr<ordinary_target_staging_connection>,64> target_resources;
    std::array<const ordinary_target_staging_connection*,64> target_resource_original{};
    std::size_t target_count=0;

    std::unique_ptr<ordinary_launch::inherited_channel> channel;
    ordinary_launch::process_identity process,supervisor;
    managed_json offer;
    std::string cohort_leaf="recovery-cohort.v1";
    std::string runtime_leaf="managed-runtime.v1";
    bool paired_runtime=false;
    bool daemon_entry=false;
    fd_owner peer_socket;
    std::array<uint8_t,16> peer_nonce{};
    std::atomic<bool> peer_claimed{false};
    std::array<ordinary_launch::process_identity,2> runtime_children{};
    ordinary_launch::process_identity peer;
    executable_fact peer_image;
    std::string home_path,stage_path,generation_path;
    file_identity stage_identity,runtime_identity;
    executable_fact memory,launcher;
    managed_record bootstrap,image;
    struct store {
        std::string relative,path;
        file_identity parent,main;
        fd_owner main_descriptor;
    };
    std::vector<store> stores;
    // One actual startup enrollment per admitted physical store. A failed or
    // completed attempt cannot mint another source after metadata disappears.
    // Restart must obtain a fresh audited issuer with durable continuity facts.
    std::array<std::atomic<bool>,256> enrollment_claimed{};
    std::array<std::atomic<opening_role>,256> opening_roles{};
    // Only the builder which won its opening-role CAS writes its slot. Its
    // first native/cleanup exception is retained without a new allocation.
    std::array<std::exception_ptr,256> opening_failures{};
    std::atomic<bool> invalid{false};
    bool admitted=false;

    void prepare_peer(file_identity expected) {
        struct stat value{};int type=0;socklen_t length=sizeof(type);
        sockaddr_storage remote{},local{};socklen_t remote_length=sizeof(remote),local_length=sizeof(local);
        const auto descriptor=peer_socket.value;
        if(descriptor<0||::fstat(descriptor,&value)||!S_ISSOCK(value.st_mode)||
            file_identity{uint64_t(value.st_dev),uint64_t(value.st_ino)}!=expected||
            ::getsockopt(descriptor,SOL_SOCKET,SO_TYPE,&type,&length)||length!=sizeof(type)||type!=SOCK_STREAM||
            ::getpeername(descriptor,reinterpret_cast<sockaddr*>(&remote),&remote_length)||remote.ss_family!=AF_UNIX||
            ::getsockname(descriptor,reinterpret_cast<sockaddr*>(&local),&local_length)||local.ss_family!=AF_UNIX)
            normal_fail("managed pair actual endpoint mapping changed");
        const auto flags=::fcntl(descriptor,F_GETFL);
        if(flags<0||::fcntl(descriptor,F_SETFD,FD_CLOEXEC)||::fcntl(descriptor,F_SETFL,flags|O_NONBLOCK))
            normal_fail("managed pair endpoint confinement failed");
#if defined(__APPLE__)
        int yes=1;if(::setsockopt(descriptor,SOL_SOCKET,SO_NOSIGPIPE,&yes,sizeof(yes)))
            normal_fail("managed pair endpoint signal confinement failed");
#endif
    }
    void exchange_peer(ordinary_launch::deadline end)const {
        // Fixed 64-byte mapping check behind actual launch closure. These bytes
        // cannot authorize any profile, product policy, row, receipt or ACK.
        auto hello=[&](bool daemon,const ordinary_launch::process_identity& child){
            writer out;out.bytes.reserve(64);
            out.fixed(std::array<uint8_t,8>{'L','A','T','P','E','E','R',1});
            out.fixed(peer_nonce);out.u64(daemon?1:0);out.process(child);
            if(out.bytes.size()!=64)normal_fail("managed pair hello shape invalid");return out.bytes;
        };
        const auto sent=hello(daemon_entry,process),expected=hello(!daemon_entry,peer);
        std::vector<uint8_t> received(64);const auto descriptor=peer_socket.value;
        auto transfer=[&](uint8_t* bytes,size_t count,bool output){
            size_t at=0;
            while(at<count) {
                if(std::chrono::steady_clock::now()>=end)normal_fail("managed pair hello expired");
                int send_flags=0;
#ifdef MSG_NOSIGNAL
                send_flags=MSG_NOSIGNAL;
#endif
                const auto n=output?::send(descriptor,bytes+at,count-at,send_flags) : ::recv(descriptor,bytes+at,count-at,0);
                if(n>0){at+=size_t(n);continue;}
                if(n==0)normal_fail("managed pair hello ended");
                if(errno==EINTR)continue;
                if(errno!=EAGAIN&&errno!=EWOULDBLOCK)normal_fail("managed pair hello I/O failed");
                const auto left=std::chrono::duration_cast<std::chrono::milliseconds>(end-std::chrono::steady_clock::now()).count();
                if(left<=0)normal_fail("managed pair hello expired");
                pollfd ready{descriptor,short(output?POLLOUT:POLLIN),0};
                const auto polled=::poll(&ready,1,static_cast<int>(std::min<int64_t>(left,20)));
                if(polled<0&&errno!=EINTR)normal_fail("managed pair hello wait failed");
                if(polled>0&&(ready.revents&POLLNVAL))normal_fail("managed pair hello descriptor lost");
            }
        };
        // send() does not mutate its supplied bytes; mutable storage avoids a
        // const cast in the common bounded read/write implementation.
        auto outgoing=sent;transfer(outgoing.data(),outgoing.size(),true);
        transfer(received.data(),received.size(),false);
        if(received!=expected)normal_fail("managed pair actual sibling hello disagreed");
        verify(end,true);
    }

    void verify_cohort(ordinary_launch::deadline end,bool closed) const {
        const auto value=managed_read_record(cohort.value,"launch.v1",paired_runtime?256:208,end);
        reader input{value.bytes};
        if(input.fixed<8>()!=std::array<uint8_t,8>{'L','A','T','C','O','H','1',0} || input.u64()!=1)
            normal_fail("managed history cohort version invalid");
        const auto revision=input.u64(),phase=input.u64();
        if(!revision || phase!=(closed?2u:1u) || input.identity()!=stage_identity ||
            input.identity()!=managed_identity(offer.at("cohort")))normal_fail("managed history cohort state changed");
        const auto gate=input.identity();
        fd_owner held(::openat(cohort.value,"launch.lock",O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        named(cohort.value,"launch.lock",held.value,gate,false);
        if(input.process()!=supervisor || input.u64()!=0 || input.u64()!=(paired_runtime?2u:1u))
            normal_fail("managed history cohort owner/count mismatch");
        if(paired_runtime) {
            for(const auto& child:runtime_children)
                if(input.u64()!=1 || input.process()!=child || input.u64()!=0)
                    normal_fail("managed history paired active child mismatch");
        } else if(input.u64()!=1 || input.process()!=process || input.u64()!=0)
            normal_fail("managed history cohort child mismatch");
        const auto checksum=input.fixed<32>();
        if(input.at!=value.bytes.size() || value.bytes.size()!=(paired_runtime?256u:208u) ||
            hash(std::vector<uint8_t>(value.bytes.begin(),value.bytes.end()-32))!=checksum)
            normal_fail("managed history cohort checksum mismatch");
    }
    void verify_targets(ordinary_launch::deadline end)const {
        if(!offer.contains("unpublishedGroupTargets")) {
            if(target_root.value>=0||target_count)normal_fail("legacy launch cannot carry a target role");return;
        }
        if(!paired_runtime||target_root.value<0||!target_count||target_count>targets.size())normal_fail("managed GROUP target role unavailable");
        const auto& role=offer.at("unpublishedGroupTargets");
        if(!role.is_object()||role.size()!=8||managed_text(role.at("format"),64)!="engram-unpublished-group-targets-v1"||
            managed_text(role.at("state"),32)!="retained-empty"||managed_identity(role.at("parent"))!=stage_identity||
            managed_identity(role.at("generation"))!=managed_identity(offer.at("generation"))||role.at("barrier")!=offer.at("barrier")||
            managed_text(role.at("leaf"),64)!=target_root_leaf||managed_identity(role.at("root"))!=target_root_identity)
            normal_fail("managed target association changed");
        named(stage.value,target_root_leaf.c_str(),target_root.value,target_root_identity,true);
        struct stat root{};if(::fstat(target_root.value,&root)||root.st_uid!=::geteuid()||(root.st_mode&07777)!=0700)
            normal_fail("managed target root policy changed");
        const auto names=[&](int descriptor,bool empty){
            fd_owner scan(::openat(descriptor,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
            if(scan.value<0)normal_fail("managed target census unavailable");
            DIR* raw=::fdopendir(scan.value);if(!raw)normal_fail("managed target census unavailable");scan.value=-1;
            std::unique_ptr<DIR,decltype(&::closedir)> held(raw,&::closedir);std::set<std::string> result;
            for(;;){if(std::chrono::steady_clock::now()>=end)normal_fail("managed target census expired");
                errno=0;const auto* entry=::readdir(raw);if(!entry){if(errno)normal_fail("managed target census failed");break;}
                std::string name(entry->d_name);if(name=="."||name=="..")continue;
                if(empty||result.size()==targets.size()||name.size()>64||!result.insert(name).second)
                    normal_fail("unpublished target must remain empty");}
            return result;
        };
        std::set<std::string> expected;
        for(std::size_t n=0;n<target_count;++n){const auto& target=targets[n];struct stat actual{};
            named(target_root.value,target.leaf.c_str(),target.directory.value,target.directory_identity,true);
            if(::fstat(target.directory.value,&actual)||actual.st_uid!=::geteuid()||(actual.st_mode&07777)!=0700)
                normal_fail("managed target member policy changed");
            std::shared_ptr<ordinary_target_staging_connection> resource;
            const ordinary_target_staging_connection* original=nullptr;
            {std::lock_guard<std::mutex> lock(target_resource_mutex);
                original=target_resource_original[n];resource=target_resources[n].lock();}
            if(original) {
                if(!resource||resource.get()!=original)normal_fail("managed target original resource custody unavailable");
                // No recursive IO/target current call: only the same native
                // birth ledger and this actual held member directory join.
                resource->verify_member_census(target.directory.value,end);
            } else {
                if(resource)normal_fail("managed target resource association invalid");
                (void)names(target.directory.value,true);
            }
            expected.insert(target.leaf);}
        if(names(target_root.value,false)!=expected)normal_fail("managed target root inventory changed");
    }

    void verify(ordinary_launch::deadline end,bool closed) const {
        if(invalid.load(std::memory_order_acquire) || std::chrono::steady_clock::now()>=end ||
            ordinary_launch::current_process()!=process || ordinary_launch::current_parent_process()!=supervisor)
            normal_fail("managed history actual parent lifetime unavailable");
        auto actual_stage=absolute_directory(stage_path);
        if(identity(actual_stage.value,true)!=stage_identity || identity(stage.value,true)!=stage_identity ||
            identity(runtime.value,true)!=runtime_identity)normal_fail("managed history fixed stage changed");
        named(stage.value,runtime_leaf.c_str(),runtime.value,runtime_identity,true);
        named(stage.value,"namespace",control.value,managed_identity(offer.at("control")),true);
        named(control.value,"generation",generation.value,managed_identity(offer.at("generation")),true);
        named(stage.value,cohort_leaf.c_str(),cohort.value,managed_identity(offer.at("cohort")),true);
        auto actual_historical=absolute_directory(home_path+"/.claude");
        if(managed_directory_identity(actual_historical.value)!=managed_identity(offer.at("historicalParent")) ||
            managed_directory_identity(historical.value)!=managed_identity(offer.at("historicalParent")))
            normal_fail("managed history old namespace changed");
        managed_verify_record(stage.value,"bootstrap.json",bootstrap,131072,end);
        managed_verify_record(runtime.value,"image.json",image,4096,end);
        loaded_image(process.pid,memory,end);loaded_image(supervisor.pid,launcher,end);
        if(paired_runtime)loaded_image(peer.pid,peer_image,end);
        verify_cohort(end,closed);
        verify_targets(end);
        // Top-level old names remain permanent mode-zero refusal objects. No
        // SQLite WAL/shm identity is frozen after the admitted child starts.
        const auto& old_fences=offer.contains("oldRefusals")?offer.at("oldRefusals"):offer.at("members");
        for(const auto& member:old_fences) {
            const auto relative=managed_text(member.at("name"),160);
            if(relative.find('/')!=std::string::npos)continue;
            struct stat old{};
            const auto refusal=managed_identity(member.at("refusal"));
            if(!refusal.inode || ::fstatat(historical.value,relative.c_str(),&old,AT_SYMLINK_NOFOLLOW) ||
                file_identity{uint64_t(old.st_dev),uint64_t(old.st_ino)}!=refusal || (old.st_mode&07777)!=0)
                normal_fail("managed history old path fence changed");
        }
        for(const auto& value:stores){
            fd_owner parent;
            int fd=generation.value;std::string leaf=value.relative;
            if(leaf.starts_with("sync/")){parent=fd_owner(::openat(generation.value,"sync",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));fd=parent.value;leaf.erase(0,5);}
            struct stat actual{};
            if(managed_directory_identity(fd)!=value.parent || ::fstatat(fd,leaf.c_str(),&actual,AT_SYMLINK_NOFOLLOW) ||
                !S_ISREG(actual.st_mode) || actual.st_uid!=::geteuid() || actual.st_nlink!=1 || (actual.st_mode&0022) ||
                file_identity{uint64_t(actual.st_dev),uint64_t(actual.st_ino)}!=value.main)
                normal_fail("managed history physical store changed");
            if(value.main_descriptor.value>=0 && identity(value.main_descriptor.value,false)!=value.main)
                normal_fail("managed history held store changed");
        }
        managed_verify_record(stage.value,"bootstrap.json",bootstrap,131072,end);
        managed_verify_record(runtime.value,"image.json",image,4096,end);
        if(std::chrono::steady_clock::now()>=end)normal_fail("managed history validation expired");
    }
};
#else
struct ordinary_managed_child_admission::implementation {};
#endif
ordinary_managed_child_admission::ordinary_managed_child_admission(std::shared_ptr<implementation> value):impl_(std::move(value)){}
ordinary_managed_child_admission::~ordinary_managed_child_admission()=default;
std::string ordinary_managed_child_admission::primary_path() const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_ || impl_->stores.empty() || impl_->stores.front().relative!="memory.sqlite")
        normal_fail("managed history primary unavailable");
    return impl_->stores.front().path;
#else
    normal_fail("managed history unavailable");
#endif
}
void ordinary_managed_child_admission::validate_open(const std::string& path) const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_ || !impl_->admitted || std::none_of(impl_->stores.begin(),impl_->stores.end(),[&](const auto& value){return value.path==path;}))
        normal_fail("managed history unregistered store refused");
    try{impl_->verify(std::chrono::steady_clock::now()+std::chrono::seconds(10),true);}
    catch(...){impl_->invalid.store(true,std::memory_order_release);throw;}
#else
    (void)path;normal_fail("managed history unavailable");
#endif
}
void ordinary_managed_child_admission::validate_physical(const std::string& path,uint64_t device,uint64_t inode) const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_)normal_fail("managed history unavailable");
    const auto entry=std::find_if(impl_->stores.begin(),impl_->stores.end(),[&](const auto& value){return value.path==path && value.main==file_identity{device,inode};});
    if(entry==impl_->stores.end())normal_fail("managed history physical store unregistered");
    validate_open(entry->path);
#else
    (void)path;(void)device;(void)inode;normal_fail("managed history unavailable");
#endif
}

void ordinary_managed_child_admission::validate_physical_until(const std::string& path,uint64_t device,uint64_t inode,ordinary_launch::deadline end)const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_||!impl_->admitted||std::none_of(impl_->stores.begin(),impl_->stores.end(),[&](const auto& value){
        return value.path==path&&value.main==file_identity{device,inode};}))normal_fail("managed attachment physical view unavailable");
    // Preserve the topology operation's original deadline throughout record,
    // image and cohort verification. Never open another ten-second interval.
    impl_->verify(end,true);
#else
    (void)path;(void)device;(void)inode;(void)end;normal_fail("managed attachment unavailable");
#endif
}

std::string ordinary_managed_child_admission::enrollment_binding(const std::string& path,uint64_t device,uint64_t inode) const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    validate_physical(path,device,inode);
    // This binds actual preserved generation and its permanent old-path
    // retirement barrier. It deliberately does not turn a launch nonce into a
    // durable producer identity or a peer-supplied source epoch.
    writer out;out.text("ordinary-managed-preserved-generation-v1");
    out.identity(impl_->stage_identity);out.identity(managed_identity(impl_->offer.at("control")));
    out.identity(managed_identity(impl_->offer.at("generation")));
    out.text(managed_text(impl_->offer.at("barrier"),64));out.text(path);out.identity({device,inode});
    const auto digest=hash(out.bytes);return picosha2::bytes_to_hex_string(digest.begin(),digest.end());
#else
    (void)path;(void)device;(void)inode;normal_fail("managed history unavailable");
#endif
}

std::string ordinary_managed_child_admission::enrollment_binding_until(
    const std::string& path,uint64_t device,uint64_t inode,ordinary_launch::deadline end) const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    validate_physical_until(path,device,inode,end);
    writer out;out.text("ordinary-managed-preserved-generation-v1");
    out.identity(impl_->stage_identity);out.identity(managed_identity(impl_->offer.at("control")));
    out.identity(managed_identity(impl_->offer.at("generation")));
    out.text(managed_text(impl_->offer.at("barrier"),64));out.text(path);out.identity({device,inode});
    const auto digest=hash(out.bytes);return picosha2::bytes_to_hex_string(digest.begin(),digest.end());
#else
    (void)path;(void)device;(void)inode;(void)end;normal_fail("managed history unavailable");
#endif
}

std::pair<std::string,std::string> ordinary_managed_child_admission::ipc_member_binding(
    const std::string& path,uint64_t device,uint64_t inode,ordinary_launch::deadline end) const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    validate_physical_until(path,device,inode,end);
    const auto member=std::find_if(impl_->stores.begin(),impl_->stores.end(),[&](const auto& value){
        return value.path==path&&value.main==file_identity{device,inode};});
    if(member==impl_->stores.end())normal_fail("managed IPC member unavailable");
    // A common durable preserved cohort identity, separate from each store's
    // enrollment binding and from the currently live child incarnation. This
    // passive value cannot reproduce the retained admission which issued it.
    writer out;out.text("ordinary-managed-local-pair-generation-v1");
    out.identity(impl_->stage_identity);out.identity(managed_identity(impl_->offer.at("control")));
    out.identity(managed_identity(impl_->offer.at("generation")));
    out.text(managed_text(impl_->offer.at("barrier"),64));
    const auto digest=hash(out.bytes);
    return {member->relative,picosha2::bytes_to_hex_string(digest.begin(),digest.end())};
#else
    (void)path;(void)device;(void)inode;(void)end;normal_fail("managed IPC unavailable");
#endif
}

void ordinary_managed_child_admission::claim_opening_until(
    const std::string& path,opening_role role,ordinary_launch::deadline end) const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_||!impl_->admitted||role==opening_role::unclaimed)normal_fail("managed opening admission unavailable");
    // verify checks the actual creator before filesystem/cohort leaves. The
    // operation's fixed end is never replaced by validate_open's new interval.
    impl_->verify(end,true);
    const auto entry=std::find_if(impl_->stores.begin(),impl_->stores.end(),[&](const auto& value){return value.path==path;});
    if(entry==impl_->stores.end())normal_fail("managed opening store unregistered");
    const auto index=static_cast<size_t>(entry-impl_->stores.begin());auto expected=opening_role::unclaimed;
    if(index>=impl_->opening_roles.size()||!impl_->opening_roles[index].compare_exchange_strong(expected,role,std::memory_order_acq_rel))
        normal_fail("managed physical opening attempt already consumed");
#else
    (void)path;(void)role;(void)end;normal_fail("managed history unavailable");
#endif
}
void ordinary_managed_child_admission::require_group_opening_until(
    const std::string& path,ordinary_launch::deadline end) const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_||!impl_->admitted)normal_fail("managed group opening unavailable");
    impl_->verify(end,true);
    const auto entry=std::find_if(impl_->stores.begin(),impl_->stores.end(),[&](const auto& value){return value.path==path;});
    if(entry==impl_->stores.end())normal_fail("managed group opening store unregistered");
    const auto index=static_cast<size_t>(entry-impl_->stores.begin());
    if(index>=impl_->opening_roles.size()||impl_->opening_roles[index].load(std::memory_order_acquire)!=opening_role::group_receiver)
        normal_fail("managed group receiver lacks its original opening role");
#else
    (void)path;(void)end;normal_fail("managed history unavailable");
#endif
}
void ordinary_managed_child_admission::remember_opening_failure(const std::string& path,std::exception_ptr error) const noexcept {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_||impl_->process.pid!=::getpid())return;
    const auto entry=std::find_if(impl_->stores.begin(),impl_->stores.end(),[&](const auto& value){return value.path==path;});
    if(entry==impl_->stores.end())return;const auto index=static_cast<size_t>(entry-impl_->stores.begin());
    if(index<impl_->opening_failures.size()&&!impl_->opening_failures[index])impl_->opening_failures[index]=std::move(error);
    impl_->invalid.store(true,std::memory_order_release);
#else
    (void)path;(void)error;
#endif
}
void ordinary_managed_child_admission::claim_enrollment_until(const std::string& path,ordinary_launch::deadline end) const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_||!impl_->admitted)normal_fail("managed enrollment admission unavailable");
    try{impl_->verify(end,true);}catch(...){impl_->invalid.store(true,std::memory_order_release);throw;}
    const auto entry=std::find_if(impl_->stores.begin(),impl_->stores.end(),[&](const auto& value){return value.path==path;});
    if(entry==impl_->stores.end())normal_fail("managed enrollment store unavailable");
    const auto index=static_cast<size_t>(entry-impl_->stores.begin());bool expected=false;
    if(index>=impl_->enrollment_claimed.size()||!impl_->enrollment_claimed[index].compare_exchange_strong(expected,true,std::memory_order_acq_rel))
        normal_fail("managed enrollment attempt already consumed");
#else
    (void)path;(void)end;normal_fail("managed history unavailable");
#endif
}
void ordinary_managed_child_admission::claim_enrollment(const std::string& path) const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    validate_open(path);
    const auto entry=std::find_if(impl_->stores.begin(),impl_->stores.end(),[&](const auto& value){return value.path==path;});
    if(entry==impl_->stores.end())normal_fail("managed enrollment store unavailable");
    const auto index=static_cast<size_t>(entry-impl_->stores.begin());bool expected=false;
    if(index>=impl_->enrollment_claimed.size()||!impl_->enrollment_claimed[index].compare_exchange_strong(expected,true,std::memory_order_acq_rel))
        normal_fail("managed enrollment attempt already consumed");
#else
    (void)path;normal_fail("managed history unavailable");
#endif
}
void ordinary_managed_child_admission::fail_enrollment() const noexcept {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(impl_)impl_->invalid.store(true,std::memory_order_release);
#endif
}

std::vector<std::string> ordinary_managed_child_admission::paths() const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_||!impl_->admitted)normal_fail("managed history admission unavailable");
    impl_->verify(std::chrono::steady_clock::now()+std::chrono::seconds(10),true);
    std::vector<std::string> result;result.reserve(impl_->stores.size());
    for(const auto& value:impl_->stores)result.push_back(value.path);return result;
#else
    normal_fail("managed history unavailable");
#endif
}

ordinary_unpublished_group_target::~ordinary_unpublished_group_target()=default;
void ordinary_managed_child_admission::retain_group_target(
    const std::shared_ptr<const ordinary_managed_child_admission>& original,const std::string& path,
    std::uint64_t device,std::uint64_t inode,ordinary_launch::deadline end,
    std::shared_ptr<const ordinary_unpublished_group_target>& retained)const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(original.get()!=this||retained||!impl_||!impl_->admitted||!impl_->daemon_entry||ordinary_launch::current_process()!=impl_->process)
        normal_fail("target role requires the actual original child admission");
    const auto store=std::find_if(impl_->stores.begin(),impl_->stores.end(),[&](const auto& item){return item.path==path&&item.main==file_identity{device,inode};});
    if(store==impl_->stores.end())normal_fail("target role old member differs");
    std::size_t slot=impl_->target_count;
    for(std::size_t n=0;n<impl_->target_count;++n)if(impl_->targets[n].old_member==store->relative&&impl_->targets[n].old_main==store->main){slot=n;break;}
    if(slot==impl_->target_count)normal_fail("target role is not an issued GROUP member");
    // Return/hold the actual association before any claim/current/census failure.
    retained=std::shared_ptr<const ordinary_unpublished_group_target>(new ordinary_unpublished_group_target(original,slot));
    const ordinary_unpublished_group_target* unclaimed=nullptr;
    if(!impl_->target_original[slot].compare_exchange_strong(unclaimed,retained.get(),std::memory_order_acq_rel))normal_fail("target role already consumed");
    retained->claimed_.store(true,std::memory_order_release);
    impl_->verify(end,true);
#else
    (void)original;(void)path;(void)device;(void)inode;(void)end;(void)retained;normal_fail("unpublished GROUP target unavailable");
#endif
}
void ordinary_managed_child_admission::verify_group_target(std::size_t slot,const ordinary_unpublished_group_target* original,ordinary_launch::deadline end)const {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!impl_||!impl_->admitted||!impl_->daemon_entry||ordinary_launch::current_process()!=impl_->process||slot>=impl_->target_count||
        !original||impl_->target_original[slot].load(std::memory_order_acquire)!=original||
        !original->claimed_.load(std::memory_order_acquire))normal_fail("original target role is not held");
    impl_->verify(end,true);
#else
    (void)slot;(void)original;(void)end;normal_fail("unpublished GROUP target unavailable");
#endif
}
void ordinary_unpublished_group_target::require_staging_original(ordinary_launch::deadline end)const {
    if(!admission_||abandonment_claimed_.load(std::memory_order_acquire)||terminal_sealed_.load(std::memory_order_acquire))normal_fail("original target role unavailable or terminally abandoned");
    admission_->verify_group_target(slot_,this,end);
}
std::string ordinary_unpublished_group_target::staging_main_path(ordinary_launch::deadline end)const {
    require_staging_original(end);
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    const auto& native=*admission_->impl_;
    // This fixed native name is derived only from this issued member. It is
    // never exported or accepted as a caller's physical/open permission.
    return native.stage_path+"/"+native.target_root_leaf+"/"+native.targets[slot_].leaf+"/store.sqlite";
#else
    normal_fail("unpublished GROUP target unavailable");
#endif
}
void ordinary_unpublished_group_target::bind_staging_connection(
    const std::shared_ptr<ordinary_target_staging_connection>& original,ordinary_launch::deadline end)const {
    require_staging_original(end);
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    if(!original)normal_fail("managed target original connection unavailable");
    original->require_target_identity(this,end);
    auto& native=*admission_->impl_;
    std::lock_guard<std::mutex> lock(native.target_resource_mutex);
    if(native.target_resource_original[slot_]||!native.target_resources[slot_].expired())
        normal_fail("managed target resource role already consumed");
    native.target_resources[slot_]=original;
    native.target_resource_original[slot_]=original.get();
#else
    (void)original;normal_fail("unpublished GROUP target unavailable");
#endif
}
int ordinary_unpublished_group_target::staging_member_descriptor(
    const ordinary_target_staging_connection* original,ordinary_launch::deadline end)const {
    require_staging_original(end);
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
    const auto& native=*admission_->impl_;
    {std::lock_guard<std::mutex> lock(native.target_resource_mutex);
        const auto held=native.target_resources[slot_].lock();
        if(!original||native.target_resource_original[slot_]!=original||held.get()!=original)
            normal_fail("managed target native member does not belong to this connection");}
    return native.targets[slot_].directory.value;
#else
    (void)original;normal_fail("unpublished GROUP target unavailable");
#endif
}
bool ordinary_unpublished_group_target::claim_staging_abandonment(
    const ordinary_target_staging_connection* original,abandonment_binding& retained)const noexcept {
    try {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
        // Cleanup observes only the same already retained daemon admission.
        // It does not revalidate/renew an expired live-work deadline, reopen an
        // old store, or accept caller paths/identities as deletion authority.
        if(!admission_||!admission_->impl_||!original||terminal_sealed_.load(std::memory_order_acquire))return false;
        const auto& native=*admission_->impl_;
        if(!native.admitted||!native.daemon_entry||ordinary_launch::current_process()!=native.process||
           slot_>=native.target_count||!claimed_.load(std::memory_order_acquire)||
           native.target_original[slot_].load(std::memory_order_acquire)!=this)return false;
        std::lock_guard<std::mutex> lock(native.target_resource_mutex);
        const auto connection=native.target_resources[slot_].lock();
        if(connection.get()!=original||native.target_resource_original[slot_]!=original||
           native.stage.value<0||native.target_root.value<0||native.targets[slot_].directory.value<0)return false;
        bool unclaimed=false;
        if(!abandonment_claimed_.compare_exchange_strong(unclaimed,true,std::memory_order_acq_rel))return false;
        const auto& member=native.targets[slot_];
        retained.stage=native.stage.value;retained.root=native.target_root.value;retained.member=member.directory.value;
        retained.root_leaf=&native.target_root_leaf;retained.member_leaf=&member.leaf;
        retained.stage_device=native.stage_identity.device;retained.stage_inode=native.stage_identity.inode;
        retained.root_device=native.target_root_identity.device;retained.root_inode=native.target_root_identity.inode;
        retained.member_device=member.directory_identity.device;retained.member_inode=member.directory_identity.inode;
        return true; // SAME retained role only; no native effect or generic cleanup grant.
#else
        (void)original;(void)retained;return false;
#endif
    }catch(...){return false;}
}

bool ordinary_unpublished_group_target::seal_terminal_staging(
    const ordinary_target_staging_connection* original)const noexcept {
    try {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
        // No live-work revalidation or deadline renewal. Only the actual
        // already bound, issued daemon target may acquire this terminal seal.
        if(!admission_||!admission_->impl_||!original)return false;
        const auto& native=*admission_->impl_;
        if(ordinary_launch::current_process()!=native.process)return false;
        if(!native.admitted||!native.daemon_entry||slot_>=native.target_count||
           !claimed_.load(std::memory_order_acquire)||
           native.target_original[slot_].load(std::memory_order_acquire)!=this||
           abandonment_claimed_.load(std::memory_order_acquire))return false;
        std::lock_guard<std::mutex> lock(native.target_resource_mutex);
        const auto connection=native.target_resources[slot_].lock();
        if(connection.get()!=original||native.target_resource_original[slot_]!=original||
           native.stage.value<0||native.target_root.value<0||native.targets[slot_].directory.value<0)return false;
        bool unsealed=false;
        return terminal_sealed_.compare_exchange_strong(unsealed,true,std::memory_order_acq_rel);
        // No receipt binding is issued, no Native operation executes, and no
        // admission capacity, target role or publication can be regenerated.
#else
        (void)original;return false;
#endif
    }catch(...){return false;}
}

bool ordinary_unpublished_group_target::claim_terminal_namespace(
    const ordinary_target_staging_connection* original,terminal_namespace_binding& binding)const noexcept {
    try {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
        if(!admission_||!admission_->impl_||!original)return false;
        const auto& native=*admission_->impl_;
        if(ordinary_launch::current_process()!=native.process)return false;
        if(!native.admitted||!native.daemon_entry||slot_>=native.target_count||
           !claimed_.load(std::memory_order_acquire)||!terminal_sealed_.load(std::memory_order_acquire)||
           abandonment_claimed_.load(std::memory_order_acquire)||
           native.target_original[slot_].load(std::memory_order_acquire)!=this||
           !original->terminal_namespace_binding_admissible(this))return false;
        std::lock_guard<std::mutex> lock(native.target_resource_mutex);
        const auto held=native.target_resources[slot_].lock();
        if(held.get()!=original||native.target_resource_original[slot_]!=original||
           native.stage.value<0||native.target_root.value<0||native.targets[slot_].directory.value<0)return false;
        bool unclaimed=false;
        if(!terminal_namespace_claimed_.compare_exchange_strong(unclaimed,true,std::memory_order_acq_rel))return false;
        const auto& member=native.targets[slot_];
        binding.stage=native.stage.value;binding.root=native.target_root.value;binding.member=member.directory.value;
        binding.root_leaf=&native.target_root_leaf;binding.member_leaf=&member.leaf;
        binding.stage_device=native.stage_identity.device;binding.stage_inode=native.stage_identity.inode;
        binding.root_device=native.target_root_identity.device;binding.root_inode=native.target_root_identity.inode;
        binding.member_device=member.directory_identity.device;binding.member_inode=member.directory_identity.inode;
        return true; // Separate actual terminal binding; constructor-only claim is unchanged.
#else
        (void)original;(void)binding;return false;
#endif
    }catch(...){return false;}
}

bool ordinary_unpublished_group_target::terminal_namespace_historical_current(
    const ordinary_target_staging_connection* original,const terminal_namespace_binding& binding)const noexcept {
    try {
#ifdef LATTICE_ORDINARY_NORMAL_POSIX
        if(!admission_||!admission_->impl_||!original)return false;
        const auto& native=*admission_->impl_;
        if(ordinary_launch::current_process()!=native.process||!native.admitted||!native.daemon_entry||
           slot_>=native.target_count||!claimed_.load(std::memory_order_acquire)||
           !terminal_sealed_.load(std::memory_order_acquire)||!terminal_namespace_claimed_.load(std::memory_order_acquire)||
           abandonment_claimed_.load(std::memory_order_acquire)||
           native.target_original[slot_].load(std::memory_order_acquire)!=this)return false;
        // No Conn/Native call is made under this resource leaf. This only
        // observes the already issued same namespace, including after MAIN.
        std::lock_guard<std::mutex> leaf(native.target_resource_mutex);
        const auto held=native.target_resources[slot_].lock();const auto& member=native.targets[slot_];
        return held.get()==original&&native.target_resource_original[slot_]==original&&
            native.stage.value>=0&&native.target_root.value>=0&&member.directory.value>=0&&
            binding.stage==native.stage.value&&binding.root==native.target_root.value&&binding.member==member.directory.value&&
            binding.root_leaf==&native.target_root_leaf&&binding.member_leaf==&member.leaf&&
            binding.stage_device==native.stage_identity.device&&binding.stage_inode==native.stage_identity.inode&&
            binding.root_device==native.target_root_identity.device&&binding.root_inode==native.target_root_identity.inode&&
            binding.member_device==member.directory_identity.device&&binding.member_inode==member.directory_identity.inode;
#else
        (void)original;(void)binding;return false;
#endif
    }catch(...){return false;}
}
