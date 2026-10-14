// Included in the fixed controller implementation. No public constructor or
// parsed manifest grants a launch. All operations also require its actual
// installed-image/namespace lock and fresh prior-incarnation observation.
    static constexpr std::size_t managed_journal_limit=262144;
    std::optional<json> managed_head;
    std::optional<struct stat> managed_head_stat;
    static file_identity managed_id(const json& value) {
        if(!value.is_object()||value.size()!=2)refused();
        file_identity result{number(value,"device"),number(value,"inode")};
        if(!result.inode)refused();return result;
    }
    static json managed_id_json(file_identity value) {return {{"device",value.device},{"inode",value.inode}};}
    static ordinary_launch::process_identity managed_process(const json& value) {
        if(!value.is_object()||value.size()!=4)refused();
        const auto pid=number(value,"pid"),parent=number(value,"parent");
        if(pid<=1||parent<=1||pid>std::uint64_t(std::numeric_limits<pid_t>::max())||
            parent>std::uint64_t(std::numeric_limits<pid_t>::max()))refused();
        return {static_cast<pid_t>(pid),static_cast<pid_t>(parent),number(value,"birthMajor"),number(value,"birthMinor")};
    }
    static std::string managed_leaf(std::uint64_t sequence) {
        if(!sequence)refused();return "managed-cohort."+std::to_string(sequence);
    }
    static json managed_parse_head(const std::string& raw) {
        if(raw.empty()||raw.size()>managed_journal_limit)refused();
        std::size_t events=0;std::vector<std::set<std::string>> keys;
        auto value=json::parse(raw,[&](int depth,json::parse_event_t e,json& v) {
            if(depth<0||depth>18||++events>32768||(v.is_string()&&v.get_ref<const std::string&>().size()>8192))refused();
            if(e==json::parse_event_t::object_start)keys.emplace_back();
            if(e==json::parse_event_t::key&&(keys.empty()||!keys.back().insert(v.get<std::string>()).second))refused();
            if(e==json::parse_event_t::object_end)keys.pop_back();return true;
        });
        if(value.dump()+"\n"!=raw||!value.is_object()||value.size()!=7||
            text(value,"format",64)!="engram-managed-launch-journal-v2"||number(value,"accountUID")!=::geteuid())refused();
        (void)number(value,"revision");
        for(const auto* key:{"current","prior","cleanup"})
            if(!value.at(key).is_null()&&!value.at(key).is_object())refused();
        return value;
    }
    std::optional<json> managed_read_head(deadline end) {
        verify(end);struct stat named{};
        if(::fstatat(stage,"managed-launches.v2",&named,AT_SYMLINK_NOFOLLOW)) {
            if(errno!=ENOENT)refused();managed_head_stat.reset();return {};
        }
        fd input(::openat(stage,"managed-launches.v2",O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        const auto actual=metadata(input.value);
        if(!stable(actual,named)||!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||
            actual.st_nlink!=1||(actual.st_mode&07777)!=0600)refused();
        const auto bytes=read_file(input.value,managed_journal_limit,end);auto value=managed_parse_head(bytes);
        if(text(value,"bootstrapSHA256",64)!=picosha2::hash256_hex_string(record.dump()+"\n")||
            ::fstatat(stage,"managed-launches.v2",&named,AT_SYMLINK_NOFOLLOW)||!stable(actual,named))refused();
        managed_head_stat=actual;verify(end);return value;
    }
    void managed_check_head(deadline end) const {
        verify(end);struct stat actual{};
        if(!managed_head_stat) {
            if(::fstatat(stage,"managed-launches.v2",&actual,AT_SYMLINK_NOFOLLOW)==0||errno!=ENOENT)refused();
        } else if(::fstatat(stage,"managed-launches.v2",&actual,AT_SYMLINK_NOFOLLOW)||!stable(actual,*managed_head_stat))refused();
    }
    void managed_publish(json next,deadline end) {
        managed_check_head(end);
        const auto revision=managed_head?number(*managed_head,"revision"):0;
        if(revision==std::numeric_limits<std::uint64_t>::max())refused();
        next["revision"]=revision+1;
        const auto raw=next.dump()+"\n";(void)managed_parse_head(raw);
        // A pending file can never have authorized spawn: publication and its
        // directory flush precede launch. Only this exact private prefix may be
        // discarded; a published head is never reconstructed from the prefix.
        struct stat pending{};
        if(::fstatat(stage,"managed-launches.pending",&pending,AT_SYMLINK_NOFOLLOW)==0) {
            if(!S_ISREG(pending.st_mode)||pending.st_uid!=::geteuid()||pending.st_nlink!=1||
                (pending.st_mode&07777)!=0600||pending.st_size<0||std::uint64_t(pending.st_size)>managed_journal_limit)refused();
            if(::unlinkat(stage,"managed-launches.pending",0)||::fsync(stage))refused();
        } else if(errno!=ENOENT)refused();
        fd output(::openat(stage,"managed-launches.pending",O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600));
        if(output.value<0||::fchmod(output.value,0600))refused();std::size_t at=0;
        while(at<raw.size()) {
            bound(end);const auto n=::write(output.value,raw.data()+at,raw.size()-at);
            if(n<0&&errno==EINTR)continue;if(n<=0)refused();at+=n;
        }
        if(::fsync(output.value))refused();managed_check_head(end);
        struct stat actual{};
        if(::fstatat(stage,"managed-launches.pending",&actual,AT_SYMLINK_NOFOLLOW)||
            !stable(actual,metadata(output.value)))refused();
        if(::renameat(stage,"managed-launches.pending",stage,"managed-launches.v2")||::fsync(stage))refused();
        const auto after=managed_read_head(end);if(!after||*after!=next)refused();managed_head=std::move(next);
    }
    std::vector<std::string> managed_names(int directory,deadline end)const {
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
    json managed_snapshot(int directory,deadline end) const {
        owner_check(end);fd input(::openat(directory,"launch.v1",O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        const auto actual=metadata(input.value);struct stat named{};
        if(!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||actual.st_nlink!=1||
            (actual.st_mode&07777)!=0600||actual.st_size!=208||
            ::fstatat(directory,"launch.v1",&named,AT_SYMLINK_NOFOLLOW)||!stable(actual,named))refused();
        const auto bytes=read_file(input.value,208,end);
        if(::fstatat(directory,"launch.v1",&named,AT_SYMLINK_NOFOLLOW)||!stable(actual,named))refused();
        return {{"device",std::uint64_t(actual.st_dev)},{"inode",std::uint64_t(actual.st_ino)},
            {"bytes",bytes.size()},{"sha256",picosha2::hash256_hex_string(bytes)}};
    }
    void managed_verify_incarnation(const json& entry,std::size_t image,deadline end) const {
        verify(end);
        if(!entry.is_object()||entry.size()!=5||text(entry,"leaf",64)!=managed_leaf(number(entry,"sequence"))||
            !entry.at("expected").is_object()||!entry.at("complete").is_object())refused();
        const auto& expected=entry.at("expected");const auto& complete=entry.at("complete");
        const auto& launch=expected.at("launch");const auto& incarnation=expected.at("incarnation");
        if(expected.size()!=6||complete.size()!=11||text(expected,"format",64)!="engram-managed-runtime-expectation-v1"||
            number(expected,"accountUID")!=::geteuid()||text(expected,"bootstrapSHA256",64)!=picosha2::hash256_hex_string(record.dump()+"\n")||
            text(complete,"format",64)!="engram-managed-runtime-retirement-v1"||
            text(complete,"expectedSHA256",64)!=picosha2::hash256_hex_string(expected.dump()+"\n")||
            text(complete,"actualKernelOutcome",64)!="note-exit-without-observed-fork-or-exec"||
            text(complete,"observationInterval",64)!="phase2-before-open-through-actual-owned-join"||
            complete.at("restartAdmissionGranted")!=false||
            launch.at("child")!=complete.at("terminalChild")||number(launch,"phase")!=1||
            text(launch,"format",64)!="engram-managed-history-launch-v2"||
            text(launch,"cohortLeaf",64)!=text(entry,"leaf",64)||number(launch,"launchSequence")!=number(entry,"sequence")||
            managed_id(launch.at("cohort"))!=managed_id(entry.at("directory"))||
            managed_id(complete.at("directory"))!=managed_id(entry.at("directory")))refused();
        if(launch.size()!=15 || !launch.at("members").is_array() || launch.at("members").empty() ||
            launch.at("members").size()>256 || !launch.at("oldRefusals").is_array() ||
            launch.at("oldRefusals").size()<2 || launch.at("oldRefusals").size()>5)refused();
        const auto status=number(complete,"waitStatus");
        if(status>std::uint64_t(std::numeric_limits<int>::max()) ||
            (!WIFEXITED(static_cast<int>(status))&&!WIFSIGNALED(static_cast<int>(status))))refused();
        const auto child=managed_process(launch.at("child")),supervisor=managed_process(launch.at("supervisor"));
        if(child.parent!=supervisor.pid||number(incarnation,"pid")!=std::uint64_t(child.pid)||incarnation.size()!=5)refused();
        ordinary_launch::observed_incarnation original;
        original.pid=child.pid;original.audit_token=token(text(incarnation,"auditToken",64));
        original.executable_device=number(incarnation,"imageDevice");original.executable_inode=number(incarnation,"imageInode");
        const auto& actual=files.at(image);
        if(actual.digest!=text(incarnation,"imageSHA256",64)||original.executable_device!=std::uint64_t(actual.original.st_dev)||
            original.executable_inode!=std::uint64_t(actual.original.st_ino))refused();
        // A real new kernel lookup is mandatory even when all saved records
        // agree. A surviving original incarnation is debt, never a second dial.
        if(ordinary_launch::observed_process::reobserve(original,actual.handle.value,end))refused();
        verify(end);
    }
    void managed_verify_terminal(const json& entry,std::size_t image,deadline end) const {
        managed_verify_incarnation(entry,image,end);
        const auto leaf=text(entry,"leaf",64);fd directory(::openat(stage,leaf.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        const auto ds=metadata(directory.value);struct stat named{};
        if(!S_ISDIR(ds.st_mode)||ds.st_uid!=::geteuid()||(ds.st_mode&07777)!=0700||
            file_identity{std::uint64_t(ds.st_dev),std::uint64_t(ds.st_ino)}!=managed_id(entry.at("directory"))||
            ::fstatat(stage,leaf.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same_identity(ds,named)||
            managed_names(directory.value,end)!=std::vector<std::string>{"launch.lock","launch.v1"})refused();
        const auto& complete=entry.at("complete");
        if(managed_snapshot(directory.value,end)!=complete.at("terminalSnapshot"))refused();
        fd input(::openat(directory.value,"launch.v1",O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        const auto raw=read_file(input.value,208,end);
        if(raw.size()!=208||raw.substr(0,8)!=std::string("LATCOH1\0",8))refused();
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
            id()!=managed_id(entry.at("directory")))refused();
        const auto gate=id();
        if(gate!=managed_id(complete.at("gate"))||process()!=managed_process(entry.at("expected").at("launch").at("supervisor"))||
            u64()!=0||u64()!=1||u64()!=2||process()!=managed_process(complete.at("terminalChild"))||
            u64()!=number(complete,"waitStatus")||at!=176)refused();
        const auto digest=picosha2::hash256_hex_string(raw.substr(0,176));
        if(hex(reinterpret_cast<const std::uint8_t*>(raw.data()+176),32)!=digest)refused();
        fd lock(::openat(directory.value,"launch.lock",O_RDWR|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));const auto ls=metadata(lock.value);
        if(!S_ISREG(ls.st_mode)||ls.st_uid!=::geteuid()||ls.st_nlink!=1||(ls.st_mode&07777)!=0600||ls.st_size!=0||
            file_identity{std::uint64_t(ls.st_dev),std::uint64_t(ls.st_ino)}!=gate||::flock(lock.value,LOCK_EX|LOCK_NB))refused();
        if(managed_snapshot(directory.value,end)!=complete.at("terminalSnapshot"))refused();
        managed_verify_incarnation(entry,image,end);
    }
    void managed_cleanup_prior(std::size_t image,deadline end) {
        if(!managed_head||managed_head->at("cleanup").is_null())return;
        const auto entry=managed_head->at("cleanup");managed_verify_incarnation(entry,image,end);
        managed_check_head(end);const auto leaf=text(entry,"leaf",64);struct stat named{};
        if(::fstatat(stage,leaf.c_str(),&named,AT_SYMLINK_NOFOLLOW)==0) {
            if(!S_ISDIR(named.st_mode)||named.st_uid!=::geteuid()||(named.st_mode&07777)!=0700||
                file_identity{std::uint64_t(named.st_dev),std::uint64_t(named.st_ino)}!=managed_id(entry.at("directory")))refused();
            fd directory(::openat(stage,leaf.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
            if(!same_identity(named,metadata(directory.value)))refused();
            for(const auto& name:managed_names(directory.value,end)) {
                if(name!="launch.v1"&&name!="launch.lock")refused();
                fd value(::openat(directory.value,name.c_str(),O_RDWR|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));const auto actual=metadata(value.value);
                if(!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||actual.st_nlink!=1||(actual.st_mode&07777)!=0600)refused();
                if(name=="launch.v1") {if(managed_snapshot(directory.value,end)!=entry.at("complete").at("terminalSnapshot"))refused();}
                else if(file_identity{std::uint64_t(actual.st_dev),std::uint64_t(actual.st_ino)}!=managed_id(entry.at("complete").at("gate"))||
                    actual.st_size!=0||::flock(value.value,LOCK_EX|LOCK_NB))refused();
                managed_check_head(end);struct stat again{};
                if(::fstatat(directory.value,name.c_str(),&again,AT_SYMLINK_NOFOLLOW)||!stable(actual,again)||
                    ::unlinkat(directory.value,name.c_str(),0)||::fsync(directory.value))refused();
            }
            managed_verify_incarnation(entry,image,end);managed_check_head(end);
            if(::fstatat(stage,leaf.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same_identity(named,metadata(directory.value))||
                ::unlinkat(stage,leaf.c_str(),AT_REMOVEDIR)||::fsync(stage))refused();
        } else if(errno!=ENOENT)refused();
        auto next=*managed_head;next["cleanup"]=nullptr;managed_publish(std::move(next),end);
    }
    void managed_load(std::size_t image,deadline end) {
        managed_head=managed_read_head(end);
        if(managed_head) {
            managed_cleanup_prior(image,end);
            if(managed_head->at("current").is_null())refused();
            if(!managed_head->at("prior").is_null()) {
                const auto current_sequence=number(managed_head->at("current"),"sequence");
                const auto prior_sequence=number(managed_head->at("prior"),"sequence");
                if(!prior_sequence||prior_sequence==std::numeric_limits<std::uint64_t>::max()||
                    current_sequence!=prior_sequence+1||
                    managed_head->at("current").at("directory")==managed_head->at("prior").at("directory"))refused();
            }
            managed_verify_terminal(managed_head->at("current"),image,end);
            if(!managed_head->at("prior").is_null())managed_verify_terminal(managed_head->at("prior"),image,end);
        }
        std::set<std::string> allowed;
        if(managed_head)for(const auto* key:{"current","prior"})if(!managed_head->at(key).is_null())
            allowed.insert(text(managed_head->at(key),"leaf",64));
        for(const auto& name:managed_names(stage,end)) {
            if(name.starts_with("managed-cohort.")&&!allowed.contains(name))refused();
            // Unqualified v1 runtime records are retained as explicit debt;
            // this successor does not relabel them as v2 interval evidence.
            if(name=="recovery-cohort.v1"||name=="managed-runtime.expected.json"||name=="managed-runtime.complete.json")refused();
        }
        managed_check_head(end);
    }
