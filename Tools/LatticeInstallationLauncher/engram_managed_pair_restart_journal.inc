// Included in the fixed controller implementation. No public constructor or
// parsed manifest grants a launch. All operations also require its actual
// installed-image/namespace lock and fresh prior-incarnation observation.
    static constexpr std::size_t paired_journal_limit=262144;
    std::optional<json> paired_head;
    std::optional<struct stat> paired_head_stat;
    // First target preparation is permanently charged/retained in this process.
    // The static owners are not fields of an owner, so this is not an owning cycle.
    inline static const pid_t target_registry_creator=::getpid();
    inline static std::mutex target_registry_leaf;
    inline static std::array<std::shared_ptr<implementation>,64> target_registry{};
    inline static std::size_t target_registry_attempts=0;
    struct target_creation {
        struct member {
            std::string old_member,leaf;
            file_identity old_main,identity;
            fd directory;
            bool mkdir_observed=false,open_observed=false,sync_observed=false;
            int mkdir_rc=0,mkdir_errno=0,open_rc=-1,open_errno=0,sync_rc=0,sync_errno=0;
        };
        std::string leaf;
        fd root;
        file_identity identity;
        std::array<member,64> members;
        std::size_t count=0;
        bool mkdir_observed=false,open_observed=false,sync_observed=false,parent_sync_observed=false;
        int mkdir_rc=0,mkdir_errno=0,open_rc=-1,open_errno=0,sync_rc=0,sync_errno=0,parent_sync_rc=0,parent_sync_errno=0;
        json record;
    };
    std::shared_ptr<target_creation> unpublished_targets;
    bool target_preparation_attempted=false;
    json paired_target_plan(const std::shared_ptr<implementation>& original,
        const retained_historical_generation::observation& observed,std::uint64_t sequence,deadline end) {
        owner_check(end);if(original.get()!=this||::getpid()!=target_registry_creator||target_preparation_attempted)refused();
        std::size_t count=0;
        for(const auto& member:observed.members)if(member.relative_name.starts_with("sync/group-")&&member.relative_name.ends_with(".sqlite"))++count;
        if(!count)return nullptr;
        if(count>64)refused();target_preparation_attempted=true;
        // Consume and strongly publish the actual session before allocation or
        // filesystem effects. Failed originals never refund this fixed slot.
        {std::lock_guard lock(target_registry_leaf);
            if(target_registry_attempts==target_registry.size())refused();
            target_registry[target_registry_attempts++]=original;
        }
        unpublished_targets=std::make_shared<target_creation>();
        auto& target=*unpublished_targets;target.leaf="unpublished-group-targets."+std::to_string(sequence);
        json members=json::array();std::set<std::string> selected;
        for(const auto& member:observed.members) {
            const auto& name=member.relative_name;
            if(!name.starts_with("sync/group-")||!name.ends_with(".sqlite"))continue;
            if(name.size()!=5+6+36+7||member.directory||!member.identity.inode)refused();
            auto leaf=name.substr(5,42);
            for(std::size_t n=6;n<leaf.size();++n){auto& c=leaf[n];const auto at=n-6;
                if(at==8||at==13||at==18||at==23){if(c!='-')refused();}
                else {if(c>='A'&&c<='F')c+=('a'-'A');if(!((c>='0'&&c<='9')||(c>='a'&&c<='f')))refused();}}
            if(!selected.insert(leaf).second)refused();
            auto& held=target.members[target.count++];held.old_member=name;held.old_main=member.identity;held.leaf=leaf;
            members.push_back({{"member",name},{"oldMain",paired_id_json(member.identity)},{"leaf",leaf},{"directory",nullptr}});
        }
        if(target.count!=count)refused();
        target.record={{"format","engram-unpublished-group-targets-v1"},{"leaf",target.leaf},
            {"parent",paired_id_json({std::uint64_t(metadata(stage).st_dev),std::uint64_t(metadata(stage).st_ino)})},
            {"generation",paired_id_json(observed.generation)},{"barrier",hex(observed.barrier.data(),observed.barrier.size())},
            {"state","intent"},{"root",nullptr},{"members",std::move(members)}};
        return target.record; // Descriptive intent, never a target/open grant.
    }
    json paired_create_targets(deadline end) {
        owner_check(end);if(!unpublished_targets||!paired_head||paired_head->at("current").at("targets")!=unpublished_targets->record)refused();
        auto& target=*unpublished_targets;if(target.mkdir_observed)refused();
        target.mkdir_rc=::mkdirat(stage,target.leaf.c_str(),0700);target.mkdir_errno=target.mkdir_rc?errno:0;target.mkdir_observed=true;
        if(target.mkdir_rc)refused();
        target.open_rc=::openat(stage,target.leaf.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
        target.open_errno=target.open_rc<0?errno:0;target.open_observed=true;target.root=fd(target.open_rc);
        const auto actual=metadata(target.root.value);struct stat named{};
        if(!S_ISDIR(actual.st_mode)||actual.st_uid!=::geteuid()||(actual.st_mode&07777)!=0700||
            ::fstatat(stage,target.leaf.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same_identity(actual,named))refused();
        target.identity={std::uint64_t(actual.st_dev),std::uint64_t(actual.st_ino)};
        for(std::size_t n=0;n<target.count;++n) {
            owner_check(end);auto& member=target.members[n];
            member.mkdir_rc=::mkdirat(target.root.value,member.leaf.c_str(),0700);member.mkdir_errno=member.mkdir_rc?errno:0;member.mkdir_observed=true;
            if(member.mkdir_rc)refused();
            member.open_rc=::openat(target.root.value,member.leaf.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
            member.open_errno=member.open_rc<0?errno:0;member.open_observed=true;member.directory=fd(member.open_rc);
            const auto observed=metadata(member.directory.value);
            if(!S_ISDIR(observed.st_mode)||observed.st_uid!=::geteuid()||(observed.st_mode&07777)!=0700||
                ::fstatat(target.root.value,member.leaf.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same_identity(observed,named)||
                !paired_names(member.directory.value,end).empty())refused();
            member.identity={std::uint64_t(observed.st_dev),std::uint64_t(observed.st_ino)};
            member.sync_rc=::fsync(member.directory.value);member.sync_errno=member.sync_rc?errno:0;member.sync_observed=true;
            if(member.sync_rc)refused();
        }
        target.sync_rc=::fsync(target.root.value);target.sync_errno=target.sync_rc?errno:0;target.sync_observed=true;
        if(target.sync_rc)refused();
        target.parent_sync_rc=::fsync(stage);target.parent_sync_errno=target.parent_sync_rc?errno:0;target.parent_sync_observed=true;
        if(target.parent_sync_rc)refused();
        target.record["root"]=paired_id_json(target.identity);target.record["state"]="retained-empty";
        for(std::size_t n=0;n<target.count;++n)target.record["members"][n]["directory"]=paired_id_json(target.members[n].identity);
        paired_verify_targets(end);return target.record;
    }
    void paired_verify_targets(deadline end) const {
        owner_check(end);if(!unpublished_targets)refused();const auto& target=*unpublished_targets;
        struct stat named{};const auto root=metadata(target.root.value);
        if(::fstatat(stage,target.leaf.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same_identity(root,named)||
            file_identity{std::uint64_t(root.st_dev),std::uint64_t(root.st_ino)}!=target.identity||
            !S_ISDIR(root.st_mode)||root.st_uid!=::geteuid()||(root.st_mode&07777)!=0700)refused();
        std::vector<std::string> names;
        for(std::size_t n=0;n<target.count;++n){const auto& member=target.members[n];const auto actual=metadata(member.directory.value);
            if(::fstatat(target.root.value,member.leaf.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same_identity(actual,named)||
                file_identity{std::uint64_t(actual.st_dev),std::uint64_t(actual.st_ino)}!=member.identity||
                !S_ISDIR(actual.st_mode)||actual.st_uid!=::geteuid()||(actual.st_mode&07777)!=0700||
                !paired_names(member.directory.value,end).empty())refused();names.push_back(member.leaf);}
        std::sort(names.begin(),names.end());if(paired_names(target.root.value,end)!=names)refused();
    }
    static file_identity paired_id(const json& value) {
        if(!value.is_object()||value.size()!=2)refused();
        file_identity result{number(value,"device"),number(value,"inode")};
        if(!result.inode)refused();return result;
    }
    static json paired_id_json(file_identity value) {return {{"device",value.device},{"inode",value.inode}};}
    static ordinary_launch::process_identity paired_process(const json& value) {
        if(!value.is_object()||value.size()!=4)refused();
        const auto pid=number(value,"pid"),parent=number(value,"parent");
        if(pid<=1||parent<=1||pid>std::uint64_t(std::numeric_limits<pid_t>::max())||
            parent>std::uint64_t(std::numeric_limits<pid_t>::max()))refused();
        return {static_cast<pid_t>(pid),static_cast<pid_t>(parent),number(value,"birthMajor"),number(value,"birthMinor")};
    }
    static std::string paired_leaf(std::uint64_t sequence) {
        if(!sequence)refused();return "managed-cohort."+std::to_string(sequence);
    }
    static json paired_parse_head(const std::string& raw) {
        if(raw.empty()||raw.size()>paired_journal_limit)refused();
        std::size_t events=0;std::vector<std::set<std::string>> keys;
        auto value=json::parse(raw,[&](int depth,json::parse_event_t e,json& v) {
            if(depth<0||depth>18||++events>32768||(v.is_string()&&v.get_ref<const std::string&>().size()>8192))refused();
            if(e==json::parse_event_t::object_start)keys.emplace_back();
            if(e==json::parse_event_t::key&&(keys.empty()||!keys.back().insert(v.get<std::string>()).second))refused();
            if(e==json::parse_event_t::object_end)keys.pop_back();return true;
        });
        if(value.dump()+"\n"!=raw||!value.is_object()||value.size()!=7||
            text(value,"format",64)!="engram-managed-launch-journal-v3"||number(value,"accountUID")!=::geteuid())refused();
        (void)number(value,"revision");
        for(const auto* key:{"current","prior","cleanup"})
            if(!value.at(key).is_null()&&!value.at(key).is_object())refused();
        return value;
    }
    std::optional<json> paired_read_head(deadline end) {
        verify(end);struct stat named{};
        if(::fstatat(stage,"managed-launches.v3",&named,AT_SYMLINK_NOFOLLOW)) {
            if(errno!=ENOENT)refused();paired_head_stat.reset();return {};
        }
        fd input(::openat(stage,"managed-launches.v3",O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        const auto actual=metadata(input.value);
        if(!stable(actual,named)||!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||
            actual.st_nlink!=1||(actual.st_mode&07777)!=0600)refused();
        const auto bytes=read_file(input.value,paired_journal_limit,end);auto value=paired_parse_head(bytes);
        if(text(value,"bootstrapSHA256",64)!=picosha2::hash256_hex_string(record.dump()+"\n")||
            ::fstatat(stage,"managed-launches.v3",&named,AT_SYMLINK_NOFOLLOW)||!stable(actual,named))refused();
        paired_head_stat=actual;verify(end);return value;
    }
    void paired_check_head(deadline end) const {
        verify(end);struct stat actual{};
        if(!paired_head_stat) {
            if(::fstatat(stage,"managed-launches.v3",&actual,AT_SYMLINK_NOFOLLOW)==0||errno!=ENOENT)refused();
        } else if(::fstatat(stage,"managed-launches.v3",&actual,AT_SYMLINK_NOFOLLOW)||!stable(actual,*paired_head_stat))refused();
    }
    void paired_publish(json next,deadline end) {
        paired_check_head(end);
        const auto revision=paired_head?number(*paired_head,"revision"):0;
        if(revision==std::numeric_limits<std::uint64_t>::max())refused();
        next["revision"]=revision+1;
        const auto raw=next.dump()+"\n";(void)paired_parse_head(raw);
        // A pending file can never have authorized spawn: publication and its
        // directory flush precede launch. Only this exact private prefix may be
        // discarded; a published head is never reconstructed from the prefix.
        struct stat pending{};
        if(::fstatat(stage,"managed-launches.pair-pending",&pending,AT_SYMLINK_NOFOLLOW)==0) {
            if(!S_ISREG(pending.st_mode)||pending.st_uid!=::geteuid()||pending.st_nlink!=1||
                (pending.st_mode&07777)!=0600||pending.st_size<0||std::uint64_t(pending.st_size)>paired_journal_limit)refused();
            if(::unlinkat(stage,"managed-launches.pair-pending",0)||::fsync(stage))refused();
        } else if(errno!=ENOENT)refused();
        fd output(::openat(stage,"managed-launches.pair-pending",O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600));
        if(output.value<0||::fchmod(output.value,0600))refused();std::size_t at=0;
        while(at<raw.size()) {
            bound(end);const auto n=::write(output.value,raw.data()+at,raw.size()-at);
            if(n<0&&errno==EINTR)continue;if(n<=0)refused();at+=n;
        }
        if(::fsync(output.value))refused();paired_check_head(end);
        struct stat actual{};
        if(::fstatat(stage,"managed-launches.pair-pending",&actual,AT_SYMLINK_NOFOLLOW)||
            !stable(actual,metadata(output.value)))refused();
        if(::renameat(stage,"managed-launches.pair-pending",stage,"managed-launches.v3")||::fsync(stage))refused();
        const auto after=paired_read_head(end);if(!after||*after!=next)refused();paired_head=std::move(next);
    }
    std::vector<std::string> paired_names(int directory,deadline end)const {
        fd scan(::openat(directory,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        if(scan.value<0)refused();DIR* raw=::fdopendir(scan.value);if(!raw)refused();scan.value=-1;
        std::unique_ptr<DIR,decltype(&::closedir)> held(raw,&::closedir);std::vector<std::string> names;
        for(;;) {
            bound(end);errno=0;const auto* entry=::readdir(raw);if(!entry){if(errno)refused();break;}
            std::string name(entry->d_name);if(name=="."||name=="..")continue;
            if(names.size()==4096||name.size()>255)refused();names.push_back(std::move(name));
        }
        std::sort(names.begin(),names.end());return names;
    }
    json paired_snapshot(int directory,deadline end) const {
        owner_check(end);fd input(::openat(directory,"launch.v1",O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        const auto actual=metadata(input.value);struct stat named{};
        if(!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||actual.st_nlink!=1||
            (actual.st_mode&07777)!=0600||actual.st_size!=256||
            ::fstatat(directory,"launch.v1",&named,AT_SYMLINK_NOFOLLOW)||!stable(actual,named))refused();
        const auto bytes=read_file(input.value,256,end);
        if(::fstatat(directory,"launch.v1",&named,AT_SYMLINK_NOFOLLOW)||!stable(actual,named))refused();
        return {{"device",std::uint64_t(actual.st_dev)},{"inode",std::uint64_t(actual.st_ino)},
            {"bytes",bytes.size()},{"sha256",picosha2::hash256_hex_string(bytes)}};
    }
    void paired_verify_incarnation(const json& entry,const std::array<std::size_t,2>& images,deadline end) const {
        verify(end);
        if(!entry.is_object()||entry.size()!=5||text(entry,"leaf",64)!=paired_leaf(number(entry,"sequence"))||
            !entry.at("expected").is_object()||!entry.at("complete").is_object())refused();
        const auto& expected=entry.at("expected");const auto& complete=entry.at("complete");
        const auto& launch=expected.at("launch");const auto& incarnations=expected.at("incarnations");
        const auto& terminals=complete.at("terminalChildren");const auto& children=launch.at("children");
        if(expected.size()!=6||complete.size()!=10||text(expected,"format",64)!="engram-managed-runtime-expectation-v2"||
            number(expected,"accountUID")!=::geteuid()||text(expected,"bootstrapSHA256",64)!=picosha2::hash256_hex_string(record.dump()+"\n")||
            text(complete,"format",64)!="engram-managed-runtime-retirement-v2"||
            text(complete,"expectedSHA256",64)!=picosha2::hash256_hex_string(expected.dump()+"\n")||
            text(complete,"actualKernelOutcome",64)!="both-note-exit-without-observed-fork-or-exec"||
            text(complete,"observationInterval",64)!="both-phase2-before-open-through-joint-owned-join"||
            complete.at("restartAdmissionGranted")!=false||number(launch,"phase")!=1||
            text(launch,"format",64)!="engram-managed-history-launch-v3"||launch.size()!=17||
            text(launch,"cohortLeaf",64)!=text(entry,"leaf",64)||number(launch,"launchSequence")!=number(entry,"sequence")||
            paired_id(launch.at("cohort"))!=paired_id(entry.at("directory"))||
            paired_id(complete.at("directory"))!=paired_id(entry.at("directory"))||
            !launch.at("members").is_array()||launch.at("members").empty()||launch.at("members").size()>256||
            !launch.at("oldRefusals").is_array()||launch.at("oldRefusals").size()<2||launch.at("oldRefusals").size()>5||
            !children.is_array()||children.size()!=2||!incarnations.is_array()||incarnations.size()!=2||
            !terminals.is_array()||terminals.size()!=2||launch.at("child")!=children[0]||children[0]==children[1])refused();
        const auto& peer_link=launch.at("peerLink");
        if(!peer_link.is_object()||peer_link.size()!=2||text(peer_link,"nonce",32).size()!=32||
            !peer_link.at("endpoints").is_array()||peer_link.at("endpoints").size()!=2)refused();
        const auto link_nonce=text(peer_link,"nonce",32);
        if(std::any_of(link_nonce.begin(),link_nonce.end(),[](char c){
            return !((c>='0'&&c<='9')||(c>='a'&&c<='f'));})||
            std::all_of(link_nonce.begin(),link_nonce.end(),[](char c){return c=='0';}))refused();
        // The exact actual parent-created link facts remain part of expected's
        // immutable digest. They never recreate an endpoint on restart.
        for(const auto& endpoint:peer_link.at("endpoints")) {
            if(!endpoint.is_object()||endpoint.size()!=2)refused();
            (void)number(endpoint,"device");(void)number(endpoint,"inode");
        }
        const auto supervisor=paired_process(launch.at("supervisor"));
        for(std::size_t index=0;index<2;++index) {
            const auto& incarnation=incarnations[index];const auto& terminal=terminals[index];
            const auto child=paired_process(children[index]);
            if(!terminal.is_object()||terminal.size()!=2||terminal.at("child")!=children[index]||
                !incarnation.is_object()||incarnation.size()!=5||child.parent!=supervisor.pid||
                number(incarnation,"pid")!=std::uint64_t(child.pid))refused();
            const auto status=number(terminal,"waitStatus");
            if(status>std::uint64_t(std::numeric_limits<int>::max()))refused();
            const int native_status=static_cast<int>(status);
            if(!WIFEXITED(native_status)&&!WIFSIGNALED(native_status))refused();
            ordinary_launch::observed_incarnation original;
            original.pid=child.pid;original.audit_token=token(text(incarnation,"auditToken",64));
            original.executable_device=number(incarnation,"imageDevice");original.executable_inode=number(incarnation,"imageInode");
            const auto& actual=files.at(images[index]);
            if(actual.digest!=text(incarnation,"imageSHA256",64)||original.executable_device!=std::uint64_t(actual.original.st_dev)||
                original.executable_inode!=std::uint64_t(actual.original.st_ino))refused();
            if(ordinary_launch::observed_process::reobserve(original,actual.handle.value,end))refused();
        }
        verify(end);
    }
    void paired_verify_terminal(const json& entry,const std::array<std::size_t,2>& images,deadline end) const {
        paired_verify_incarnation(entry,images,end);
        const auto leaf=text(entry,"leaf",64);fd directory(::openat(stage,leaf.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        const auto ds=metadata(directory.value);struct stat named{};
        if(!S_ISDIR(ds.st_mode)||ds.st_uid!=::geteuid()||(ds.st_mode&07777)!=0700||
            file_identity{std::uint64_t(ds.st_dev),std::uint64_t(ds.st_ino)}!=paired_id(entry.at("directory"))||
            ::fstatat(stage,leaf.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same_identity(ds,named)||
            paired_names(directory.value,end)!=std::vector<std::string>{"launch.lock","launch.v1"})refused();
        const auto& complete=entry.at("complete");
        if(paired_snapshot(directory.value,end)!=complete.at("terminalSnapshot"))refused();
        fd input(::openat(directory.value,"launch.v1",O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        const auto raw=read_file(input.value,256,end);
        if(raw.size()!=256||raw.substr(0,8)!=std::string("LATCOH1\0",8))refused();
        std::size_t at=8;
        const auto u64=[&]() {if(at>raw.size()-8)refused();std::uint64_t out=0;
            for(unsigned shift=0;shift<64;shift+=8)out|=std::uint64_t(static_cast<unsigned char>(raw[at++]))<<shift;return out;};
        const auto id=[&](){return file_identity{u64(),u64()};};
        const auto process=[&](){const auto pid=u64(),parent=u64();
            if(pid<=1||parent<=1||pid>std::uint64_t(std::numeric_limits<pid_t>::max())||
                parent>std::uint64_t(std::numeric_limits<pid_t>::max()))refused();
            return ordinary_launch::process_identity{static_cast<pid_t>(pid),static_cast<pid_t>(parent),u64(),u64()};};
        if(u64()!=1||u64()!=number(complete,"revision")||u64()!=3||
            id()!=file_identity{std::uint64_t(metadata(stage).st_dev),std::uint64_t(metadata(stage).st_ino)}||
            id()!=paired_id(entry.at("directory")))refused();
        const auto gate=id();
        if(gate!=paired_id(complete.at("gate"))||process()!=paired_process(entry.at("expected").at("launch").at("supervisor"))||
            u64()!=0||u64()!=2)refused();
        for(const auto& terminal:complete.at("terminalChildren"))
            if(u64()!=2||process()!=paired_process(terminal.at("child"))||u64()!=number(terminal,"waitStatus"))refused();
        if(at!=224)refused();
        const auto digest=picosha2::hash256_hex_string(raw.substr(0,224));
        if(hex(reinterpret_cast<const std::uint8_t*>(raw.data()+224),32)!=digest)refused();
        fd lock(::openat(directory.value,"launch.lock",O_RDWR|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));const auto ls=metadata(lock.value);
        if(!S_ISREG(ls.st_mode)||ls.st_uid!=::geteuid()||ls.st_nlink!=1||(ls.st_mode&07777)!=0600||ls.st_size!=0||
            file_identity{std::uint64_t(ls.st_dev),std::uint64_t(ls.st_ino)}!=gate||::flock(lock.value,LOCK_EX|LOCK_NB))refused();
        if(paired_snapshot(directory.value,end)!=complete.at("terminalSnapshot"))refused();
        paired_verify_incarnation(entry,images,end);
    }
    void paired_cleanup_prior(const std::array<std::size_t,2>& images,deadline end) {
        if(!paired_head||paired_head->at("cleanup").is_null())return;
        const auto entry=paired_head->at("cleanup");paired_verify_incarnation(entry,images,end);
        paired_check_head(end);const auto leaf=text(entry,"leaf",64);struct stat named{};
        if(::fstatat(stage,leaf.c_str(),&named,AT_SYMLINK_NOFOLLOW)==0) {
            if(!S_ISDIR(named.st_mode)||named.st_uid!=::geteuid()||(named.st_mode&07777)!=0700||
                file_identity{std::uint64_t(named.st_dev),std::uint64_t(named.st_ino)}!=paired_id(entry.at("directory")))refused();
            fd directory(::openat(stage,leaf.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
            if(!same_identity(named,metadata(directory.value)))refused();
            for(const auto& name:paired_names(directory.value,end)) {
                if(name!="launch.v1"&&name!="launch.lock")refused();
                fd value(::openat(directory.value,name.c_str(),O_RDWR|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));const auto actual=metadata(value.value);
                if(!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||actual.st_nlink!=1||(actual.st_mode&07777)!=0600)refused();
                if(name=="launch.v1") {if(paired_snapshot(directory.value,end)!=entry.at("complete").at("terminalSnapshot"))refused();}
                else if(file_identity{std::uint64_t(actual.st_dev),std::uint64_t(actual.st_ino)}!=paired_id(entry.at("complete").at("gate"))||
                    actual.st_size!=0||::flock(value.value,LOCK_EX|LOCK_NB))refused();
                paired_check_head(end);struct stat again{};
                if(::fstatat(directory.value,name.c_str(),&again,AT_SYMLINK_NOFOLLOW)||!stable(actual,again)||
                    ::unlinkat(directory.value,name.c_str(),0)||::fsync(directory.value))refused();
            }
            paired_verify_incarnation(entry,images,end);paired_check_head(end);
            if(::fstatat(stage,leaf.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same_identity(named,metadata(directory.value))||
                ::unlinkat(stage,leaf.c_str(),AT_REMOVEDIR)||::fsync(stage))refused();
        } else if(errno!=ENOENT)refused();
        auto next=*paired_head;next["cleanup"]=nullptr;paired_publish(std::move(next),end);
    }
    void paired_load(const std::array<std::size_t,2>& images,deadline end) {
        // A prior one-child journal is preserved, never relabeled as joint
        // retirement. Migration requires its separately reviewed exact proof.
        struct stat prior_single{};
        if(::fstatat(stage,"managed-launches.v2",&prior_single,AT_SYMLINK_NOFOLLOW)==0||errno!=ENOENT)refused();
        paired_head=paired_read_head(end);
        if(paired_head) {
            for(const auto* key:{"current","prior","cleanup"})if(!paired_head->at(key).is_null()&&paired_head->at(key).contains("targets"))
                refused(); // Retained target intent/resources need their own genuine retirement/census.
            paired_cleanup_prior(images,end);
            if(paired_head->at("current").is_null())refused();
            if(!paired_head->at("prior").is_null()) {
                const auto current_sequence=number(paired_head->at("current"),"sequence");
                const auto prior_sequence=number(paired_head->at("prior"),"sequence");
                if(!prior_sequence||prior_sequence==std::numeric_limits<std::uint64_t>::max()||
                    current_sequence!=prior_sequence+1||
                    paired_head->at("current").at("directory")==paired_head->at("prior").at("directory"))refused();
            }
            paired_verify_terminal(paired_head->at("current"),images,end);
            if(!paired_head->at("prior").is_null())paired_verify_terminal(paired_head->at("prior"),images,end);
        }
        std::set<std::string> allowed;
        if(paired_head)for(const auto* key:{"current","prior"})if(!paired_head->at(key).is_null())
            allowed.insert(text(paired_head->at(key),"leaf",64));
        for(const auto& name:paired_names(stage,end)) {
            if(name.starts_with("managed-cohort.")&&!allowed.contains(name))refused();
            if(name.starts_with("unpublished-group-targets."))refused(); // Never infer absence/cleanup from a lost journal.
            // Unqualified v1 runtime records are retained as explicit debt;
            // this successor does not relabel them as v2 interval evidence.
            if(name=="recovery-cohort.v1"||name=="managed-runtime.expected.json"||name=="managed-runtime.complete.json")refused();
        }
        paired_check_head(end);
    }
