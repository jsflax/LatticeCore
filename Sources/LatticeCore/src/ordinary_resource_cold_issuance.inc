    // Included only in the retained cold installer's implementation. A
    // reservation permits fresh allocation; only a sealed ticket owns inodes.
    static constexpr std::size_t cold_attempt_limit=8, cold_root_debt_limit=64;
    static constexpr std::size_t cold_store_limit=64, cold_root_inode_limit=2048;
    static constexpr std::size_t cold_record_limit=8192, cold_root_bytes_limit=16*1024*1024;
    // Complete current Host fd census plus headroom for the bounded local
    // descriptor scopes and existing image/path verification. No guessed PID.
    void cold_fd_budget(deadline end)const{
        if(creator!=pid())resource_fail();bound(end);struct rlimit limit{};
        if(::getrlimit(RLIMIT_NOFILE,&limit))resource_fail();
        std::array<proc_fdinfo,1024> files{};
        const int n=::proc_pidinfo(::getpid(),PROC_PIDLISTFDS,0,files.data(),sizeof(files));
        if(n<=0||n%sizeof(proc_fdinfo)||static_cast<std::size_t>(n)>=sizeof(files))resource_fail();
        const auto count=static_cast<std::size_t>(n)/sizeof(proc_fdinfo);
        const auto capacity=std::min<std::uint64_t>(1024,static_cast<std::uint64_t>(limit.rlim_cur));
        // check() opens the actual Host image and retains one parent fd per
        // path component. Charge that real depth plus 32 for the <=8 cold
        // local fds, path/record cursors and syscall boundary slack. Stores
        // are prepared/opened/closed sequentially, never retained in a vector.
        const auto depth=[](const std::string& path){return static_cast<std::size_t>(std::count(path.begin(),path.end(),'/'));};
        std::size_t image_depth=depth(text(plan.at("host"),"path"));
        for(const auto& image:plan.at("oldImages"))image_depth=std::max(image_depth,depth(text(image,"path")));
        const auto headroom=32+image_depth;
        if(capacity<headroom||count>capacity-headroom)resource_fail();
        for(std::size_t at=0;at<count;++at)if(files[at].proc_fd>=capacity-headroom)resource_fail();
    }
    void cold_once(int parent,const std::string& name,const json& body,deadline end){
        if(bytes(body).size()>cold_record_limit)resource_fail();once(parent,name,body,end);
    }
    static constexpr const char* cold_kinds[]{"main","wal","shm","journal"};
    static std::string cold_slot(const json& store){const auto name=text(store,"name",255);return digest_string(std::vector<std::uint8_t>(name.begin(),name.end()));}
    static std::string cold_issue_name(const std::string& slot){return "cold-issue-"+slot+".v1";}
    static std::string cold_reserve_name(const std::string& slot,std::size_t attempt){return "cold-reserve-"+slot+"-"+std::to_string(attempt)+".v1";}
    static std::string cold_vault_name(const std::string& slot,std::size_t attempt){return "cold-stage-"+slot+"-"+std::to_string(attempt);}
    static std::string cold_pending(int parent,const std::string& name,const json& value){
        return ".record-"+digest_string(bytes(json{{"format","lattice-resource-record-publication-v1"},
            {"parent",identity_json(id(metadata(parent)))},{"name",name},{"bodySHA256",digest_string(bytes(value))}}));
    }
    void cold_cut(resource_cold_restart_test::cut point,deadline end)const{
        if(creator!=pid()||thread!=std::this_thread::get_id())resource_fail();bound(end);
        resource_cold_restart_test::at(point);
        if(creator!=pid()||thread!=std::this_thread::get_id())resource_fail();bound(end);
    }
    json cold_domain(const root& r)const{
        if(creator!=pid()||thread!=std::this_thread::get_id())resource_fail();
        struct statfs logical{},generation{},control{};
        if(::fstatfs(r.logical.value,&logical)||::fstatfs(r.generation.value,&generation)||::fstatfs(r.control.value,&control)||
            std::strncmp(logical.f_fstypename,"apfs",sizeof(logical.f_fstypename))||
            std::strncmp(generation.f_fstypename,"apfs",sizeof(generation.f_fstypename))||
            std::strncmp(control.f_fstypename,"apfs",sizeof(control.f_fstypename)))resource_fail();
        const auto same_volume=[&](const struct statfs& value){return value.f_fsid.val[0]==control.f_fsid.val[0]&&value.f_fsid.val[1]==control.f_fsid.val[1];};
        const auto device=id(metadata(r.control.value)).device;
        if(!same_volume(logical)||!same_volume(generation)||id(metadata(r.logical.value)).device!=device||id(metadata(r.generation.value)).device!=device)resource_fail();
        return {{"filesystem","apfs"},{"fsid0",control.f_fsid.val[0]},{"fsid1",control.f_fsid.val[1]},{"device",device}};
    }
    json cold_binding(const root& r,const json& store)const{
        return {{"uid",::geteuid()},{"epoch",epoch},{"planSHA256",digest_string(plan_bytes)},
            {"installation",identity_json(directory_id)},{"controller",plan.at("controller")},
            {"logical",r.fact.at("logical")},{"generation",r.fact.at("generation")},
            {"control",r.fact.at("control")},{"rootEntry",r.fact.at("entry")},{"store",store},{"renameDomain",cold_domain(r)}};
    }
    json cold_reservation(const root& r,const json& store,std::size_t attempt)const{
        if(attempt>=cold_attempt_limit)resource_fail();
        return {{"format","lattice-resource-cold-allocation-reserved-v1"},{"binding",cold_binding(r,store)},
            {"attempt",attempt},{"vaultName",cold_vault_name(cold_slot(store),attempt)},
            {"recipe",json::array({"member/","member/entry.lock:0600","member/physical.lock:0600",
                "main.stub:0000","wal.stub:0000","shm.stub:0000","journal.stub:0000","member/.rename-probe:transient-issued-stub-only"})}};
    }
    json cold_ticket(const root& r,const json& store,std::size_t attempt,file_identity vault,
        file_identity member,file_identity entry,file_identity physical,const json& stubs)const{
        return {{"format","lattice-resource-cold-member-issued-v1"},{"binding",cold_binding(r,store)},
            {"reservationSHA256",digest_string(bytes(cold_reservation(r,store,attempt)))},
            {"attempt",attempt},{"vaultName",cold_vault_name(cold_slot(store),attempt)},
            {"vault",identity_json(vault)},{"member",identity_json(member)},
            {"entry",identity_json(entry)},{"physical",identity_json(physical)},{"stubs",stubs},{"renameProbe","exclusive-roundtrip-cross-parent-swap-v1"}};
    }
    void cold_ticket_exact(const root& r,const json& store,const json& ticket,deadline end)const{
        const auto attempt=number(ticket,"attempt");const auto mode=number(store,"mode"),logical_mode=number(r.fact,"logicalMode");
        if(attempt>=cold_attempt_limit||mode>07777||(mode&0022)||!(mode&0400)||logical_mode>07777||(logical_mode&0022)||(logical_mode&0700)!=0700)resource_fail();
        json stubs=json::object();std::set<std::pair<std::uint64_t,std::uint64_t>> originals;
        const auto distinct=[&](file_identity value){if(value.device!=identity_json(r.fact.at("control")).device||!originals.emplace(value.device,value.inode).second)resource_fail();};
        distinct(identity_json(store.at("main")));
        for(const auto& aux:store.at("auxiliaries"))if(identity_json(aux.at("identity")).device!=identity_json(r.fact.at("control")).device)resource_fail();
        distinct(identity_json(ticket.at("vault")));
        distinct(identity_json(ticket.at("member")));distinct(identity_json(ticket.at("entry")));distinct(identity_json(ticket.at("physical")));
        for(const auto* kind:cold_kinds){const auto value=identity_json(ticket.at("stubs").at(kind));distinct(value);stubs[kind]=identity_json(value);}
        if(ticket!=cold_ticket(r,store,attempt,identity_json(ticket.at("vault")),identity_json(ticket.at("member")),
            identity_json(ticket.at("entry")),identity_json(ticket.at("physical")),stubs))resource_fail();
        if(read_record(r.control.value,cold_reserve_name(cold_slot(store),attempt),end)!=bytes(cold_reservation(r,store,attempt)))resource_fail();
    }
    // Charges unissued reservation names, vaults AND pending metadata. Issued
    // objects have a fixed corpus; unknown/unissued vault contents are never
    // traversed, adopted, repaired or removed. Space bound concerns our issued
    // allocation recipe, not arbitrary data planted by another actor.
    void cold_root_budget(root& r,std::size_t reserve,deadline end)const{
        cold_fd_budget(end);check(end);if(reserve>cold_root_debt_limit||r.fact.at("stores").size()>cold_store_limit)resource_fail();
        const auto logical=identity_json(r.fact.at("logical")),generation=identity_json(r.fact.at("generation")),control=identity_json(r.fact.at("control"));
        if(logical.device!=generation.device||generation.device!=control.device)resource_fail();
        const auto planned_mode=number(r.fact,"logicalMode");const auto actual_mode=metadata(r.logical.value).st_mode&07777;
        if(planned_mode>07777||(planned_mode&0022)||(planned_mode&0700)!=0700||
            (exists(r.control.value,"denial.intent.v1")?(actual_mode!=0500&&actual_mode!=planned_mode):(actual_mode!=planned_mode)))resource_fail();
        const json root_record={{"format","lattice-resource-root-v1"},{"epoch",epoch},{"logical",r.fact.at("logical")},{"generation",r.fact.at("generation")},{"control",r.fact.at("control")},{"entry",r.fact.at("entry")},{"rule",r.fact.at("rule")}};
        const json denial={{"format","lattice-resource-denial-v1"},{"epoch",epoch},{"logical",r.fact.at("logical")},{"mode",number(r.fact,"logicalMode")},{"stores",r.fact.at("stores")}};
        const std::set<std::string> root_publications{cold_pending(r.control.value,"root.v1",root_record),cold_pending(r.control.value,"denial.intent.v1",denial)};
        if(exists(r.control.value,"root.v1")&&read_record(r.control.value,"root.v1",end)!=bytes(root_record))resource_fail();
        if(exists(r.control.value,"denial.intent.v1")&&read_record(r.control.value,"denial.intent.v1",end)!=bytes(denial))resource_fail();
        std::set<std::string> allowed{"root.entry.lock","root.v1","denial.intent.v1"};std::size_t debt=0;
        // Future final + pending member journals/records are charged before
        // denial, including successful vaults retained across delete/recreate.
        std::size_t inodes=6+12*r.fact.at("stores").size(),metadata_bytes=6*ordinary_launch::maximum_frame_bytes+12*r.fact.at("stores").size()*cold_record_limit;
        for(const auto& store:r.fact.at("stores")){
            bound(end);const auto mode=number(store,"mode"),logical_mode=number(r.fact,"logicalMode");
            if(identity_json(store.at("main")).device!=control.device||mode>07777||(mode&0022)||!(mode&0400)||
                logical_mode>07777||(logical_mode&0022)||(logical_mode&0700)!=0700)resource_fail();
            for(const auto& aux:store.at("auxiliaries"))if(identity_json(aux.at("identity")).device!=control.device)resource_fail();
            const auto slot=cold_slot(store),issue=cold_issue_name(slot);allowed.insert(slot);allowed.insert(issue);
            std::optional<std::size_t> issued;
            if(exists(r.control.value,issue)){const auto raw=read_record(r.control.value,issue,end);if(raw.size()>cold_record_limit)resource_fail();const auto ticket=parse(raw);cold_ticket_exact(r,store,ticket,end);issued=number(ticket,"attempt");++inodes;metadata_bytes+=raw.size();}
            for(std::size_t attempt=0;attempt<cold_attempt_limit;++attempt){
                const auto reservation=cold_reserve_name(slot,attempt),vault=cold_vault_name(slot,attempt);
                allowed.insert(reservation);allowed.insert(vault);
                if(exists(r.control.value,reservation)){
                    const auto raw=read_record(r.control.value,reservation,end);if(raw.size()>cold_record_limit||raw!=bytes(cold_reservation(r,store,attempt)))resource_fail();
                    ++inodes;metadata_bytes+=raw.size();if(!issued||*issued!=attempt)++debt;
                }
                if(exists(r.control.value,vault)){inodes+=8;if(!issued||*issued!=attempt)++debt;}
            }
        }
        descriptor cursor(::openat(r.control.value,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        DIR* raw=::fdopendir(cursor.release());if(!raw)resource_fail();
        struct ending {DIR* value;std::uint64_t creator;~ending(){if(creator==pid())::closedir(value);}} finish{raw,creator};
        const auto entry_limit=allowed.size()+cold_root_debt_limit;std::size_t entries=0;
        for(;;){if(creator!=pid())resource_fail();bound(end);errno=0;auto* row=::readdir(raw);if(!row){if(errno)resource_fail();break;}
            const std::string name=row->d_name;if(name=="."||name=="..")continue;if(++entries>entry_limit)resource_fail();
            if(allowed.contains(name))continue;
            if(name.size()!=72||!name.starts_with(".record-")||name.find_first_not_of("0123456789abcdef",8)!=std::string::npos)resource_fail();
            struct stat pending{};if(::fstatat(r.control.value,name.c_str(),&pending,AT_SYMLINK_NOFOLLOW)||
                !S_ISREG(pending.st_mode)||pending.st_uid!=::geteuid()||pending.st_nlink!=1||
                (pending.st_mode&07777&~0600)||pending.st_size<0||static_cast<std::uint64_t>(pending.st_size)>ordinary_launch::maximum_frame_bytes)resource_fail();
            // The two exact root-publication producers are already charged
            // at their complete maximum above; replay may finish their pending
            // bytes under publish_record's existing metadata-only contract.
            if(!root_publications.contains(name)){++debt;++inodes;metadata_bytes+=static_cast<std::size_t>(pending.st_size);}
        }
        // A fresh attempt can add one reservation, eight fixed inodes and
        // one pending ticket. Its final ticket replaces that pending name.
        if(debt>cold_root_debt_limit-reserve||inodes+(reserve?10:0)>cold_root_inode_limit||
            metadata_bytes+(reserve?2*cold_record_limit:0)>cold_root_bytes_limit)resource_fail();check(end);
    }
    descriptor cold_directory(int parent,const std::string& name,file_identity expected)const{
        descriptor held(::openat(parent,name.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        const auto actual=metadata(held.value);struct stat named{};
        if(!S_ISDIR(actual.st_mode)||actual.st_uid!=::geteuid()||(actual.st_mode&07777)!=0700||id(actual)!=expected||
            ::fstatat(parent,name.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same(actual,named))resource_fail();return held;
    }
    descriptor cold_new_directory(int parent,const std::string& name,resource_cold_restart_test::cut point,deadline end){
        cold_fd_budget(end);check(end);if(creator!=pid()||::mkdirat(parent,name.c_str(),0700))resource_fail();cold_cut(point,end);
        // This exact successful mkdir, never an occupied retry, issues this
        // permission adjustment. A crashed partial is preserved, not repaired.
        if(creator!=pid()||::fchmodat(parent,name.c_str(),0700,AT_SYMLINK_NOFOLLOW))resource_fail();
        descriptor held(::openat(parent,name.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        const auto actual=metadata(held.value);return cold_directory(parent,name,id(actual));
    }
    file_identity cold_new_stub(int parent,const std::string& name,deadline end){
        cold_fd_budget(end);check(end);descriptor held(::openat(parent,name.c_str(),O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600));
        const auto original=metadata(held.value);cold_cut(resource_cold_restart_test::cut::stub_created,end);
        if(!S_ISREG(original.st_mode)||original.st_uid!=::geteuid()||original.st_nlink!=1||original.st_size||
            creator!=pid()||::fchmod(held.value,0000)||::fsync(held.value))resource_fail();
        const auto actual=metadata(held.value);struct stat named{};
        if(id(actual)!=id(original)||(actual.st_mode&07777)||actual.st_size||
            ::fstatat(parent,name.c_str(),&named,AT_SYMLINK_NOFOLLOW)||!same(actual,named))resource_fail();
        cold_cut(resource_cold_restart_test::cut::stub_synced,end);
        if(creator!=pid()||::close(held.release()))resource_fail();return id(actual);
    }
    struct cold_issued final {
        json ticket;std::vector<std::uint8_t> raw;std::string slot,vault_name;
        descriptor vault,member,entry,physical;bool nested=false;
    };
    cold_issued cold_open_issued(root& r,const json& store,bool publish_member,deadline end){
        cold_fd_budget(end);check(end);const auto slot=cold_slot(store);const auto raw=read_record(r.control.value,cold_issue_name(slot),end);
        const auto ticket=parse(raw);cold_ticket_exact(r,store,ticket,end);const auto vault_name=text(ticket,"vaultName",255);
        auto vault=cold_directory(r.control.value,vault_name,identity_json(ticket.at("vault")));
        const bool nested=exists(vault.value,"member"),published=exists(r.control.value,slot);
        if(nested==published||(!published&&!publish_member))resource_fail();
        auto member=cold_directory(published?r.control.value:vault.value,published?slot:"member",identity_json(ticket.at("member")));
        auto entry=regular_control(member.value,"entry.lock",identity_json(ticket.at("entry")));
        auto physical=regular_control(member.value,"physical.lock",identity_json(ticket.at("physical")));
        lock(entry.value,LOCK_EX,end);lock(physical.value,LOCK_EX,end);
        if(read_record(r.control.value,cold_issue_name(slot),end)!=raw)resource_fail();
        // Mode0000 stubs are NOT reopened/chmodded on retry. Their initial
        // retained descriptors were fsynced before the durable ticket. Named
        // identity checks and directory resync are the permission-safe replay.
        return {ticket,raw,slot,vault_name,std::move(vault),std::move(member),std::move(entry),std::move(physical),nested};
    }
    void cold_publish_member(root& r,cold_issued& issued,deadline end){
        if(issued.nested){check(end);if(creator!=pid()||::renameatx_np(issued.vault.value,"member",r.control.value,issued.slot.c_str(),RENAME_EXCL))resource_fail();
            cold_cut(resource_cold_restart_test::cut::member_published,end);}
        if(creator!=pid()||::fsync(issued.member.value)||::fsync(issued.vault.value)||::fsync(r.control.value))resource_fail();
        cold_cut(resource_cold_restart_test::cut::member_parents_synced,end);
        auto named=cold_directory(r.control.value,issued.slot,identity_json(issued.ticket.at("member")));
        if(exists(issued.vault.value,"member"))resource_fail();issued.nested=false;
    }

    bool cold_stat(int parent,const std::string& name,struct stat& value)const{
        if(::fstatat(parent,name.c_str(),&value,AT_SYMLINK_NOFOLLOW)==0){
            if(!S_ISREG(value.st_mode)||value.st_uid!=::geteuid()||value.st_nlink!=1)resource_fail();return true;}
        if(errno!=ENOENT)resource_fail();return false;
    }
    static void cold_zero(const struct stat& value,file_identity expected){
        if(id(value)!=expected||(value.st_mode&07777)||value.st_size)resource_fail();
    }
    json cold_move_intent(const cold_issued& issued,const std::string& name,const std::string& kind,
        file_identity original,mode_t mode)const{
        return {{"format","lattice-resource-file-move-v2"},{"epoch",epoch},{"name",name},
            {"original",identity_json(original)},{"stub",issued.ticket.at("stubs").at(kind)},{"mode",mode},
            {"issuanceSHA256",digest_string(issued.raw)},{"member",issued.ticket.at("member")},{"vault",issued.ticket.at("vault")},{"kind",kind}};
    }
    enum class cold_move_phase {unused,in_vault,in_pending,exchanged,complete};
    cold_move_phase cold_locations(root& r,const cold_issued& issued,const std::string& name,const std::string& kind,
        std::optional<file_identity> original,bool journal,deadline end)const{
        if(creator!=pid()||thread!=std::this_thread::get_id())resource_fail();bound(end);struct stat l{},v{},p{},g{};
        const bool L=cold_stat(r.logical.value,name,l),V=cold_stat(issued.vault.value,kind+".stub",v),
            P=cold_stat(issued.member.value,kind+".move.v1.swap",p),G=cold_stat(r.generation.value,name,g);
        const auto stub=identity_json(issued.ticket.at("stubs").at(kind));
        if(V)cold_zero(v,stub);
        if(!original){if(journal||L||!V||P||G)resource_fail();return cold_move_phase::unused;}
        if(original->device!=identity_json(r.fact.at("control")).device||*original==stub||!L)resource_fail();
        if(!journal){if(id(l)!=*original||!V||P||G)resource_fail();return cold_move_phase::in_vault;}
        if(id(l)==*original&&V&&!P&&!G){if(l.st_mode&07777)resource_fail();return cold_move_phase::in_vault;}
        if(id(l)==*original&&!V&&P&&id(p)==stub&&!G){if(l.st_mode&07777)resource_fail();cold_zero(p,stub);return cold_move_phase::in_pending;}
        if(id(l)==stub&&!V&&P&&id(p)==*original&&!G){cold_zero(l,stub);if(p.st_mode&07777)resource_fail();return cold_move_phase::exchanged;}
        if(id(l)==stub&&!V&&!P&&G&&id(g)==*original){cold_zero(l,stub);
            const auto mode=kind=="main"?number(issued.ticket.at("binding").at("store"),"mode"):0600;
            if((g.st_mode&07777)!=0&&(g.st_mode&07777)!=mode)resource_fail();return cold_move_phase::complete;}
        resource_fail();
    }
    json cold_member_record(const root& r,const json& store,const cold_issued& issued)const{
        return {{"format","lattice-resource-member-v1"},{"epoch",epoch},{"name",text(store,"name",255)},
            {"parent",r.fact.at("generation")},{"main",store.at("main")},{"logicalStub",issued.ticket.at("stubs").at("main")},
            {"entry",issued.ticket.at("entry")},{"physical",issued.ticket.at("physical")}};
    }
    void cold_check_corpus(root& r,const json& store,const cold_issued& issued,deadline end)const{
        check(end);(void)cold_domain(r);
        if(read_record(r.control.value,cold_issue_name(issued.slot),end)!=issued.raw)resource_fail();
        {auto named_vault=cold_directory(r.control.value,issued.vault_name,identity_json(issued.ticket.at("vault")));
         auto named_member=cold_directory(issued.nested?issued.vault.value:r.control.value,issued.nested?"member":issued.slot,identity_json(issued.ticket.at("member")));}
        {auto named_entry=regular_control(issued.member.value,"entry.lock",identity_json(issued.ticket.at("entry")));
         auto named_physical=regular_control(issued.member.value,"physical.lock",identity_json(issued.ticket.at("physical")));}
        if(id(metadata(issued.vault.value))!=identity_json(issued.ticket.at("vault"))||id(metadata(issued.member.value))!=identity_json(issued.ticket.at("member"))||
            id(metadata(issued.entry.value))!=identity_json(issued.ticket.at("entry"))||id(metadata(issued.physical.value))!=identity_json(issued.ticket.at("physical")))resource_fail();
        std::set<std::string> member_allowed{"entry.lock","physical.lock","member.v1","state.v1"},vault_allowed;
        std::vector<std::pair<std::string,std::size_t>> pending;bool complete=true;
        if(issued.nested)vault_allowed.insert("member");
        const auto add_pending=[&](const std::string& name,const json& body){const auto leaf_name=cold_pending(issued.member.value,name,body);member_allowed.insert(leaf_name);pending.emplace_back(leaf_name,bytes(body).size());};
        for(const auto* value:cold_kinds){const std::string kind=value;const auto name=text(store,"name",255)+(kind=="main"?"":"-"+kind),journal=kind+".move.v1";
            const bool J=exists(issued.member.value,journal);std::optional<file_identity> original;mode_t mode=0600;json intent;
            if(J){intent=parse(read_record(issued.member.value,journal,end));original=identity_json(intent.at("original"));mode=static_cast<mode_t>(number(intent,"mode"));}
            else {struct stat source{};if(cold_stat(r.logical.value,name,source))original=id(source);}
            if(kind=="main"){if(!original||*original!=identity_json(store.at("main")))resource_fail();mode=static_cast<mode_t>(number(store,"mode"));}
            else if(J&&number(intent,"mode")!=0600)resource_fail();
            if(original){const auto expected=cold_move_intent(issued,name,kind,*original,mode);if(J&&intent!=expected)resource_fail();add_pending(journal,expected);}
            const auto phase=cold_locations(r,issued,name,kind,original,J,end);
            if(phase!=cold_move_phase::complete&&phase!=cold_move_phase::unused)complete=false;
            if(exists(issued.vault.value,kind+".stub"))vault_allowed.insert(kind+".stub");
            if(J)member_allowed.insert(journal);
            if(exists(issued.member.value,journal+".swap")){if(!J)resource_fail();member_allowed.insert(journal+".swap");}
        }
        const auto member=cold_member_record(r,store,issued),state=json{{"phase","active"},{"epoch",epoch},{"main",store.at("main")}};
        add_pending("member.v1",member);add_pending("state.v1",state);
        const bool M=exists(issued.member.value,"member.v1"),S=exists(issued.member.value,"state.v1");
        if((M||S)&&!complete)resource_fail();if(S&&!M)resource_fail();
        if(M&&read_record(issued.member.value,"member.v1",end)!=bytes(member))resource_fail();
        if(S&&read_record(issued.member.value,"state.v1",end)!=bytes(state))resource_fail();
        const auto census=[&](int parent,const std::set<std::string>& allowed,bool metadata_files){
            descriptor cursor(::openat(parent,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));DIR* raw=::fdopendir(cursor.release());if(!raw)resource_fail();
            struct ending {DIR* value;std::uint64_t creator;~ending(){if(creator==pid())::closedir(value);}} finish{raw,creator};std::set<std::string> seen;
            for(;;){if(creator!=pid())resource_fail();bound(end);errno=0;auto* row=::readdir(raw);if(!row){if(errno)resource_fail();break;}
                const std::string name=row->d_name;if(name=="."||name=="..")continue;
                if(!allowed.contains(name)||!seen.insert(name).second||seen.size()>allowed.size())resource_fail();
                struct stat actual{};
                if(!metadata_files&&name=="member"){
                    auto nested=cold_directory(parent,name,identity_json(issued.ticket.at("member")));continue;
                }
                if(!cold_stat(parent,name,actual))resource_fail();
                if(!metadata_files)continue; // each vault stub already checked by exact ticket identity above
                if(name.ends_with(".swap"))continue; // exact move matrix validated this original/stub
                const auto pending_it=std::find_if(pending.begin(),pending.end(),[&](const auto& item){return item.first==name;});
                if(pending_it!=pending.end()){
                    if((actual.st_mode&07777&~0600)||actual.st_size<0||static_cast<std::uint64_t>(actual.st_size)>pending_it->second)resource_fail();
                }else if((actual.st_mode&07777)!=0600||actual.st_size<0||static_cast<std::uint64_t>(actual.st_size)>ordinary_launch::maximum_frame_bytes)resource_fail();
            }
            if(!metadata_files&&seen!=allowed)resource_fail();
        };
        census(issued.vault.value,vault_allowed,false);census(issued.member.value,member_allowed,true);check(end);
    }
    void cold_probe_rename(root& r,int vault,int member,const json& stubs,deadline end){
        check(end);(void)cold_domain(r);
        // Only this live creator's freshly allocated, not-yet-issued stubs.
        // A crash leaves an unissued attempt; retry never resumes its shape.
        if(creator!=pid()||::renameatx_np(vault,"main.stub",member,".rename-probe",RENAME_EXCL))resource_fail();
        cold_cut(resource_cold_restart_test::cut::rename_probe_exclusive,end);
        if(creator!=pid()||::renameatx_np(member,".rename-probe",vault,"wal.stub",RENAME_SWAP))resource_fail();
        cold_cut(resource_cold_restart_test::cut::rename_probe_swapped,end);
        if(creator!=pid()||::renameatx_np(member,".rename-probe",vault,"wal.stub",RENAME_SWAP)||
            ::renameatx_np(member,".rename-probe",vault,"main.stub",RENAME_EXCL)||::fsync(member)||::fsync(vault))resource_fail();
        for(const auto* kind:cold_kinds){struct stat actual{};if(!cold_stat(vault,std::string(kind)+".stub",actual))resource_fail();cold_zero(actual,identity_json(stubs.at(kind)));}
        if(exists(member,".rename-probe"))resource_fail();(void)cold_domain(r);
        cold_cut(resource_cold_restart_test::cut::rename_probe_restored,end);
    }
    void cold_fresh_corpus(int vault,int member,int entry,int physical,const json& stubs,deadline end)const{
        const auto exact=[&](int parent,const std::set<std::string>& expected){
            descriptor cursor(::openat(parent,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));DIR* raw=::fdopendir(cursor.release());if(!raw)resource_fail();
            struct ending {DIR* value;std::uint64_t creator;~ending(){if(creator==pid())::closedir(value);}} finish{raw,creator};std::set<std::string> seen;
            for(;;){if(creator!=pid())resource_fail();bound(end);errno=0;auto* row=::readdir(raw);if(!row){if(errno)resource_fail();break;}
                const std::string name=row->d_name;if(name=="."||name=="..")continue;
                if(!expected.contains(name)||!seen.insert(name).second||seen.size()>expected.size())resource_fail();}
            if(seen!=expected)resource_fail();
        };
        exact(vault,{"member","main.stub","wal.stub","shm.stub","journal.stub"});exact(member,{"entry.lock","physical.lock"});
        {auto named=cold_directory(vault,"member",id(metadata(member)));}
        {auto named=regular_control(member,"entry.lock",id(metadata(entry)));}
        {auto named=regular_control(member,"physical.lock",id(metadata(physical)));}
        for(const auto* kind:cold_kinds){struct stat actual{};if(!cold_stat(vault,std::string(kind)+".stub",actual))resource_fail();cold_zero(actual,identity_json(stubs.at(kind)));}
    }
    void cold_prepare_store(root& r,const json& store,bool denied,deadline end){
        check(end);const auto slot=cold_slot(store),issue=cold_issue_name(slot);
        if(!exists(r.control.value,issue)){
            if(denied||exists(r.control.value,slot))resource_fail();
            bool prepared=false;
            for(std::size_t attempt=0;attempt<cold_attempt_limit;++attempt){
                const auto vault_name=cold_vault_name(slot,attempt),reserve_name=cold_reserve_name(slot,attempt);
                if(exists(r.control.value,vault_name))continue; // no identity ticket: preserve, do not open/repair it
                cold_root_budget(r,3,end); // reservation + vault + ticket pending all count before allocation
                const auto reservation=cold_reservation(r,store,attempt);cold_once(r.control.value,reserve_name,reservation,end);
                cold_cut(resource_cold_restart_test::cut::reservation_durable,end);
                auto vault=cold_new_directory(r.control.value,vault_name,resource_cold_restart_test::cut::vault_created,end);
                auto member=cold_new_directory(vault.value,"member",resource_cold_restart_test::cut::member_created,end);
                create_control(member.value,"entry.lock",creator,end);cold_cut(resource_cold_restart_test::cut::entry_created,end);
                create_control(member.value,"physical.lock",creator,end);cold_cut(resource_cold_restart_test::cut::physical_created,end);
                auto entry=regular_control(member.value,"entry.lock"),physical=regular_control(member.value,"physical.lock");
                lock(entry.value,LOCK_EX,end);lock(physical.value,LOCK_EX,end);
                json stubs=json::object();for(const auto* kind:cold_kinds)stubs[kind]=identity_json(cold_new_stub(vault.value,std::string(kind)+".stub",end));
                cold_probe_rename(r,vault.value,member.value,stubs,end);
                cold_fresh_corpus(vault.value,member.value,entry.value,physical.value,stubs,end);
                if(creator!=pid()||::fsync(member.value)||::fsync(vault.value)||::fsync(r.control.value))resource_fail();
                cold_cut(resource_cold_restart_test::cut::preparation_synced,end);
                const auto ticket=cold_ticket(r,store,attempt,id(metadata(vault.value)),id(metadata(member.value)),id(metadata(entry.value)),id(metadata(physical.value)),stubs);
                cold_ticket_exact(r,store,ticket,end);cold_once(r.control.value,issue,ticket,end);prepared=true;
                cold_cut(resource_cold_restart_test::cut::ticket_durable,end);break;
            }
            if(!prepared)resource_fail();
        }
        auto issued=cold_open_issued(r,store,!denied,end);cold_check_corpus(r,store,issued,end);
        cold_publish_member(r,issued,end);cold_check_corpus(r,store,issued,end);
    }
    bool cold_resume_active(deadline end){
        check(end);if(!exists(installation.value,"active.v1"))return false;
        const auto expected_raw=read_record(installation.value,"retirement.expected.v1",end);
        const auto expected=parse(expected_raw),complete=parse(read_record(installation.value,"retirement.complete.v1",end));
        if(!expected.at("roles").is_array()||expected.at("roles").size()>64||!complete.at("observations").is_array()||
            complete.at("observations").size()!=expected.at("roles").size())resource_fail();
        std::set<std::string> tokens;json observed=json::array();
        for(std::size_t at=0;at<expected.at("roles").size();++at){
            const auto& row=expected.at("roles").at(at);const auto image=executable(row.at("image"));const auto approved=approved_old(image.path);
            const auto token=text(row,"auditToken",64);const auto process=number(row,"pid");
            if(!approved||*approved!=image||process<=1||process>INT_MAX||identity_json(row.at("imageIdentity"))!=image.identity||
                !tokens.insert(token).second)resource_fail();(void)token_bytes(token);
            const json role={{"image",{{"path",image.path},{"identity",identity_json(image.identity)},{"sha256",picosha2::bytes_to_hex_string(image.content)}}},
                {"pid",process},{"auditToken",token},{"imageIdentity",identity_json(image.identity)}};
            if(row!=role)resource_fail();auto ending=complete.at("observations").at(at);const auto kind=text(ending,"actualKernelEnding",64);
            if(kind!="note-exit"&&kind!="kernel-old-incarnation-retired")resource_fail();ending.erase("actualKernelEnding");
            if(ending!=row)resource_fail();observed.push_back(complete.at("observations").at(at));
        }
        if(expected!=json{{"format","lattice-resource-retirement-expected-v1"},{"epoch",epoch},{"planSHA256",digest_string(plan_bytes)},{"roles",expected.at("roles")}}||
            complete!=json{{"format","lattice-resource-retirement-complete-v1"},{"epoch",epoch},{"expectedSHA256",digest_string(expected_raw)},{"observations",observed}})resource_fail();
        json active_roots=json::array();for(const auto& r:roots){
            const json root_record={{"format","lattice-resource-root-v1"},{"epoch",epoch},{"logical",r.fact.at("logical")},{"generation",r.fact.at("generation")},{"control",r.fact.at("control")},{"entry",r.fact.at("entry")},{"rule",r.fact.at("rule")}};
            if(read_record(r.control.value,"root.v1",end)!=bytes(root_record))resource_fail();
            auto row=r.fact;row.erase("stores");row.erase("logicalMode");row["rootSHA256"]=digest_string(bytes(root_record));active_roots.push_back(std::move(row));
        }
        const json active={{"format","lattice-resource-installation-v1"},{"uid",::geteuid()},{"home",home},{"installation",identity_json(directory_id)},
            {"epoch",epoch},{"host",plan.at("host")},{"roles",plan.at("roles")},{"roots",std::move(active_roots)}};
        if(read_record(installation.value,"active.v1",end)!=bytes(active))resource_fail();
        // Exact existing active publication only: parent resync (and existing
        // metadata pending cleanup), no root repair or stale member replay.
        once(installation.value,"active.v1",active,end);return true;
    }
    void cold_issuance_barrier(bool prepare,deadline end){
        cold_fd_budget(end);check(end);bool denied=false;for(const auto& r:roots)if(exists(r.control.value,"denial.intent.v1"))denied=true;
        // Never create an issuance set retrospectively for a legacy denied epoch.
        if(denied)for(const auto& r:roots)for(const auto& store:r.fact.at("stores"))if(!exists(r.control.value,cold_issue_name(cold_slot(store))))resource_fail();
        // Every root's domain and aggregate capacity is checked before any
        // new attempt is allocated, not after earlier roots have spent space.
        for(auto& r:roots){auto entry=regular_control(r.control.value,"root.entry.lock",identity_json(r.fact.at("entry")));lock(entry.value,LOCK_EX,end);cold_root_budget(r,0,end);(void)cold_domain(r);}
        for(auto& r:roots){auto entry=regular_control(r.control.value,"root.entry.lock",identity_json(r.fact.at("entry")));lock(entry.value,LOCK_EX,end);
            cold_root_budget(r,0,end);
            for(const auto& store:r.fact.at("stores")){
                if(prepare)cold_prepare_store(r,store,denied,end);
                else {auto issued=cold_open_issued(r,store,false,end);cold_check_corpus(r,store,issued,end);cold_publish_member(r,issued,end);}
            }
            cold_root_budget(r,0,end);if(creator!=pid()||::fsync(r.control.value))resource_fail();
        }
        cold_cut(resource_cold_restart_test::cut::all_issuance_durable,end);
    }
