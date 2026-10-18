// Included inside lattice::detail::ordinary_installation. Resource-only,
// dedicated installer process. No ordinary SQL/history grant is constructed.
#include "ordinary_resource_continuous_roots.inc"
struct retained_resource_cold_restart::implementation {
#if defined(LATTICE_RESOURCE_POSIX) && defined(__APPLE__) && TARGET_OS_OSX
    const std::uint64_t creator=pid();const std::thread::id thread=std::this_thread::get_id();
    descriptor installation,controller;
    std::string home,directory,epoch;file_identity directory_id;
    json plan;std::vector<std::uint8_t> plan_bytes;
    struct root final {descriptor logical,generation,control;json fact;};
    std::vector<root> roots;
    struct old_role final {
        executable_fact image;descriptor executable;
        ordinary_launch::observed_incarnation original;
        std::optional<ordinary_launch::observed_process> process;
        std::string actual_ending;
    };
    std::vector<old_role> roles;
    bool started=false,retirement_ready=false;
    void check(deadline end)const{
        if(creator!=pid()||thread!=std::this_thread::get_id())resource_fail();bound(end);
        actual_directory(installation.value,directory,directory_id,true);
        if(read_record(installation.value,"restart.v1",end)!=plan_bytes)resource_fail();
        loaded_image(::getpid(),executable_fact_from_plan(),end);host_core_code_current(executable_fact_from_plan(),end);
        for(const auto& r:roots){actual_directory(r.logical.value,text(r.fact,"logicalPath"),identity_json(r.fact.at("logical")),false);
            actual_directory(r.generation.value,text(r.fact,"generationPath"),identity_json(r.fact.at("generation")),true);
            actual_directory(r.control.value,text(r.fact,"controlPath"),identity_json(r.fact.at("control")),true);}
    }
    executable_fact executable_fact_from_plan()const{return executable(plan.at("host"));}
    static std::vector<std::uint8_t> token_bytes(const std::string& value){
        if(value.size()!=64)resource_fail();std::vector<std::uint8_t> out(32);
        auto n=[](char c){if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;resource_fail();};
        for(std::size_t i=0;i<32;++i)out[i]=static_cast<std::uint8_t>((n(value[i*2])<<4)|n(value[i*2+1]));return out;
    }
    bool exists(int dir,const std::string& name)const{struct stat s{};if(::fstatat(dir,name.c_str(),&s,AT_SYMLINK_NOFOLLOW)==0)return true;if(errno!=ENOENT)resource_fail();return false;}
    void once(int dir,const std::string& name,const json& value,deadline end){
        check(end);publish_record(dir,name,value,creator,end);check(end);
    }
#include "ordinary_resource_cold_issuance.inc"
    std::vector<pid_t> processes(deadline end)const{
        check(end);std::array<pid_t,1025> values{};const auto n=::proc_listpids(PROC_UID_ONLY,::geteuid(),values.data(),sizeof(values));
        if(n<=0||n%sizeof(pid_t)||std::size_t(n)>=sizeof(values))resource_fail();std::vector<pid_t> result;
        for(std::size_t at=0;at<std::size_t(n)/sizeof(pid_t);++at)if(values[at]>1&&values[at]!=static_cast<pid_t>(creator))result.push_back(values[at]);
        std::sort(result.begin(),result.end());if(std::adjacent_find(result.begin(),result.end())!=result.end())resource_fail();return result;
    }
    std::string image_path(pid_t selected,deadline end)const{
        check(end);std::array<char,PROC_PIDPATHINFO_MAXSIZE> path{};
        if(::proc_pidpath(selected,path.data(),path.size())<=0||!std::memchr(path.data(),0,path.size()))resource_fail();return path.data();
    }
    std::optional<executable_fact> approved_old(const std::string& path)const{
        for(const auto& value:plan.at("oldImages")){auto image=executable(value);if(image.path==path)return image;}return {};
    }
    bool original_file(const ordinary_launch::observed_open_vnode& file)const{
        for(const auto& r:roots)for(const auto& store:r.fact.at("stores")){
            const auto original=identity_json(store.at("main"));if(original.device==file.device&&original.inode==file.inode)return true;
            for(const auto& aux:store.at("auxiliaries")){const auto found=identity_json(aux.at("identity"));if(found.device==file.device&&found.inode==file.inode)return true;}
        }return false;
    }
    void complete_root_census(root& r,deadline end)const{
        check(end);descriptor fresh(::openat(r.logical.value,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        if(id(metadata(fresh.value))!=identity_json(r.fact.at("logical")))resource_fail();
        DIR* directory=::fdopendir(fresh.release());if(!directory)resource_fail();
        struct close_directory {DIR* value;std::uint64_t creator;~close_directory(){if(creator==pid())::closedir(value);}} ending{directory,creator};
        std::set<std::string> actual,expected;for(const auto& value:r.fact.at("stores"))if(!expected.insert(text(value,"name",255)).second)resource_fail();
        std::size_t entries=0;for(;;){if(creator!=pid()||thread!=std::this_thread::get_id())resource_fail();bound(end);errno=0;auto* entry=::readdir(directory);if(!entry){if(errno)resource_fail();break;}
            const std::string name=entry->d_name;if(name=="."||name=="..")continue;if(++entries>8192)resource_fail();
            bool selected=false;for(const auto& n:r.fact.at("rule").at("names"))if(n.is_string()&&n.get<std::string>()==name)selected=true;
            for(const auto& s:r.fact.at("rule").at("suffixes"))if(s.is_string()){const auto suffix=s.get<std::string>();if(!suffix.empty()&&name.size()>suffix.size()&&name.ends_with(suffix))selected=true;}
            if(selected&&!actual.insert(name).second)resource_fail();
        }
        if(actual!=expected)resource_fail();check(end);
    }
    json role_fact(const old_role& role)const{
        return {{"image",{{"path",role.image.path},{"identity",identity_json(role.image.identity)},{"sha256",picosha2::bytes_to_hex_string(role.image.content)}}},
            {"pid",role.original.pid},{"auditToken",picosha2::bytes_to_hex_string(role.original.audit_token)},{"imageIdentity",identity_json(file_identity{role.original.executable_device,role.original.executable_inode})}};
    }
    old_role hold_role(const executable_fact& image,ordinary_launch::observed_incarnation original,deadline end){
        cold_fd_budget(end);auto verified=retained_installation_file::open_application_image(image,end);descriptor fd(::open(image.path.c_str(),O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        if(id(metadata(fd.value))!=image.identity)resource_fail();verified.verify(end);
        auto process=ordinary_launch::observed_process::reobserve(original,fd.value,end);const bool present=process.has_value();
        return {image,std::move(fd),original,std::move(process),present?std::string{}:"kernel-old-incarnation-retired"};
    }
    void capture_originals(deadline end){
        cold_fd_budget(end);check(end);if(!roles.empty())resource_fail();json expected;
        if(exists(installation.value,"retirement.expected.v1")){
            expected=parse(read_record(installation.value,"retirement.expected.v1",end));
            if(text(expected,"format")!="lattice-resource-retirement-expected-v1"||text(expected,"epoch",64)!=epoch||text(expected,"planSHA256",64)!=digest_string(plan_bytes)||!expected.at("roles").is_array()||expected.at("roles").size()>64)resource_fail();
            std::set<std::string> tokens;
            for(const auto& row:expected.at("roles")){
                auto image=executable(row.at("image"));const auto accepted=approved_old(image.path);if(!accepted||*accepted!=image)resource_fail();
                const auto p=number(row,"pid");if(p<=1||p>INT_MAX)resource_fail();ordinary_launch::observed_incarnation original;original.pid=p;
                const auto raw=token_bytes(text(row,"auditToken",64));std::copy(raw.begin(),raw.end(),original.audit_token.begin());
                const auto identity=identity_json(row.at("imageIdentity"));original.executable_device=identity.device;original.executable_inode=identity.inode;
                if(identity!=image.identity||!tokens.insert(text(row,"auditToken",64)).second)resource_fail();roles.push_back(hold_role(image,original,end));
            }
        }else{
            for(const auto p:processes(end)){
                const auto files=ordinary_launch::observed_process::inspect_files(p,end);if(!files)continue;
                const auto image=approved_old(image_path(p,end));
                if(!image){if(std::any_of(files->files.begin(),files->files.end(),[&](const auto& f){return original_file(f);}))resource_fail();continue;}
                if(roles.size()==64)resource_fail();cold_fd_budget(end);auto verified=retained_installation_file::open_application_image(*image,end);
                descriptor fd(::open(image->path.c_str(),O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));if(id(metadata(fd.value))!=image->identity)resource_fail();
                auto watched=ordinary_launch::observed_process::attach(p,fd.value,end);auto original=watched.incarnation(end);
                if(original.audit_token!=files->audit_token)resource_fail();verified.verify(end);roles.push_back({*image,std::move(fd),original,std::move(watched),{}});
            }
            json rows=json::array();for(const auto& role:roles)rows.push_back(role_fact(role));
            expected={{"format","lattice-resource-retirement-expected-v1"},{"epoch",epoch},{"planSHA256",digest_string(plan_bytes)},{"roles",std::move(rows)}};
            once(installation.value,"retirement.expected.v1",expected,end);
        }
        // A wider fresh scan refuses unknown holders and any new selected
        // incarnation. It is debt detection, never positive exit authority.
        for(const auto p:processes(end)){const auto files=ordinary_launch::observed_process::inspect_files(p,end);if(!files)continue;
            const bool known=std::any_of(roles.begin(),roles.end(),[&](const auto& r){return r.original.audit_token==files->audit_token;});
            if(!known&&(approved_old(image_path(p,end))||std::any_of(files->files.begin(),files->files.end(),[&](const auto& f){return original_file(f); })))resource_fail();}
    }
    void deny_originals(deadline end){
        cold_issuance_barrier(false,end);
        check(end);for(auto& r:roots){
            auto admission=regular_control(r.control.value,"root.entry.lock",identity_json(r.fact.at("entry")));lock(admission.value,LOCK_EX,end);
            complete_root_census(r,end);
            const auto expected=identity_json(r.fact.at("logical"));if(id(metadata(r.logical.value))!=expected)resource_fail();
            const auto old_mode=number(r.fact,"logicalMode");if(old_mode>07777||(old_mode&0022)||!(old_mode&0500))resource_fail();
            once(r.control.value,"denial.intent.v1",{{"format","lattice-resource-denial-v1"},{"epoch",epoch},{"logical",identity_json(expected)},{"mode",old_mode},{"stores",r.fact.at("stores")}},end);
            cold_cut(resource_cold_restart_test::cut::denial_intent,end);
            if(creator!=pid()||::fchmod(r.logical.value,0500)||::fsync(r.logical.value))resource_fail();
            cold_cut(resource_cold_restart_test::cut::directory_denied,end);
            complete_root_census(r,end); // No unplanned store may hide in a creation race.
            for(const auto& store:r.fact.at("stores")){
                auto issued=cold_open_issued(r,store,false,end);cold_check_corpus(r,store,issued,end);
                const auto name=text(store,"name",255);leaf(name);struct stat actual{};
                if(::fstatat(r.logical.value,name.c_str(),&actual,AT_SYMLINK_NOFOLLOW)||!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||actual.st_nlink!=1)resource_fail();
                if(id(actual)!=identity_json(store.at("main"))){
                    const auto intent=parse(read_record(issued.member.value,"main.move.v1",end));
                    if(intent!=cold_move_intent(issued,name,"main",identity_json(store.at("main")),static_cast<mode_t>(number(store,"mode")))||
                        identity_json(issued.ticket.at("stubs").at("main"))!=id(actual)||(actual.st_mode&07777)||actual.st_size)resource_fail();
                }
                if(::fchmodat(r.logical.value,name.c_str(),0000,AT_SYMLINK_NOFOLLOW))resource_fail();
                cold_cut(resource_cold_restart_test::cut::alias_denied,end);
                for(const auto& suffix:{"-wal","-shm","-journal"}){const auto aux=name+suffix;struct stat s{};
                    if(::fstatat(r.logical.value,aux.c_str(),&s,AT_SYMLINK_NOFOLLOW)==0){if(!S_ISREG(s.st_mode)||s.st_uid!=::geteuid()||s.st_nlink!=1||::fchmodat(r.logical.value,aux.c_str(),0000,AT_SYMLINK_NOFOLLOW))resource_fail();}
                    else if(errno!=ENOENT)resource_fail();}
            }
            if(::fsync(r.logical.value))resource_fail();check(end);
        }
        cold_cut(resource_cold_restart_test::cut::aliases_denied,end);
    }
    void actual_retirement(deadline end){
        for(auto& role:roles)if(role.process){check(end);role.process->request_termination(end);}
        for(auto& role:roles)if(role.process){check(end);const auto actual=role.process->wait_for_exit(end);
            if(actual.pid!=role.original.pid||actual.audit_token!=role.original.audit_token||actual.executable_device!=role.original.executable_device||actual.executable_inode!=role.original.executable_inode)resource_fail();role.actual_ending="note-exit";cold_cut(resource_cold_restart_test::cut::role_ended,end);}
        check(end);
        for(const auto p:processes(end)){const auto files=ordinary_launch::observed_process::inspect_files(p,end);if(!files)continue;
            if(approved_old(image_path(p,end))||std::any_of(files->files.begin(),files->files.end(),[&](const auto& file){return original_file(file);}))resource_fail();}
        json observations=json::array();for(const auto& role:roles){if(role.actual_ending!="note-exit"&&role.actual_ending!="kernel-old-incarnation-retired")resource_fail();auto fact=role_fact(role);fact["actualKernelEnding"]=role.actual_ending;observations.push_back(std::move(fact));}
        const auto expected=read_record(installation.value,"retirement.expected.v1",end);
        const json actual={{"format","lattice-resource-retirement-complete-v1"},{"epoch",epoch},{"expectedSHA256",digest_string(expected)},{"observations",std::move(observations)}};
        if(!exists(installation.value,"retirement.complete.v1"))publish_record(installation.value,"retirement.complete.v1",actual,creator,end);
        else{const auto first=parse(read_record(installation.value,"retirement.complete.v1",end));
            if(text(first,"epoch",64)!=epoch||text(first,"expectedSHA256",64)!=digest_string(expected)||first.at("observations").size()!=roles.size())resource_fail();
            for(std::size_t i=0;i<roles.size();++i){auto row=first.at("observations").at(i);const auto ending=text(row,"actualKernelEnding",64);row.erase("actualKernelEnding");
                if(row!=role_fact(roles[i])||(ending!="note-exit"&&ending!="kernel-old-incarnation-retired"))resource_fail();}}
    }
    file_identity move_file(root& r,const json& store,cold_issued& issued,const std::string& kind,
        file_identity expected,mode_t original_mode,deadline end){
        if(!retirement_ready)resource_fail();cold_check_corpus(r,store,issued,end);
        const auto name=text(store,"name",255)+(kind=="main"?"":"-"+kind),journal=kind+".move.v1",swapped=journal+".swap";
        leaf(name);leaf(journal);const auto intent=cold_move_intent(issued,name,kind,expected,original_mode);
        if(exists(issued.member.value,journal)){
            if(read_record(issued.member.value,journal,end)!=bytes(intent))resource_fail();
        }else{
            if(cold_locations(r,issued,name,kind,expected,false,end)!=cold_move_phase::in_vault)resource_fail();
            struct stat original{};if(!cold_stat(r.logical.value,name,original)||(original.st_mode&07777))resource_fail();
            cold_once(issued.member.value,journal,intent,end);
        }
        cold_cut(resource_cold_restart_test::cut::journal_durable,end);
        auto phase=cold_locations(r,issued,name,kind,expected,true,end);
        if(phase==cold_move_phase::in_vault){check(end);if(creator!=pid()||::renameatx_np(issued.vault.value,(kind+".stub").c_str(),issued.member.value,swapped.c_str(),RENAME_EXCL))resource_fail();
            cold_cut(resource_cold_restart_test::cut::stub_transferred,end);}
        // Successful rename followed by failed/crashed parent fsync is a
        // legitimate retry. Reestablish BOTH actual parent facts every time.
        if(creator!=pid()||::fsync(issued.vault.value)||::fsync(issued.member.value))resource_fail();
        cold_cut(resource_cold_restart_test::cut::transfer_synced,end);
        phase=cold_locations(r,issued,name,kind,expected,true,end);
        if(phase==cold_move_phase::in_pending){check(end);if(creator!=pid()||::renameatx_np(r.logical.value,name.c_str(),issued.member.value,swapped.c_str(),RENAME_SWAP))resource_fail();
            cold_cut(resource_cold_restart_test::cut::aliases_exchanged,end);}
        if(creator!=pid()||::fsync(r.logical.value)||::fsync(issued.member.value))resource_fail();
        cold_cut(resource_cold_restart_test::cut::exchange_synced,end);
        phase=cold_locations(r,issued,name,kind,expected,true,end);
        if(phase==cold_move_phase::exchanged){check(end);if(creator!=pid()||::renameatx_np(issued.member.value,swapped.c_str(),r.generation.value,name.c_str(),RENAME_EXCL))resource_fail();
            cold_cut(resource_cold_restart_test::cut::original_moved,end);}
        if(creator!=pid()||::fsync(issued.member.value)||::fsync(r.generation.value))resource_fail();
        cold_cut(resource_cold_restart_test::cut::original_move_synced,end);
        if(cold_locations(r,issued,name,kind,expected,true,end)!=cold_move_phase::complete||
            original_mode>07777||(original_mode&0022)||!(original_mode&0400))resource_fail();
        if(creator!=pid()||::fchmodat(r.generation.value,name.c_str(),original_mode,AT_SYMLINK_NOFOLLOW))resource_fail();
        cold_cut(resource_cold_restart_test::cut::original_mode_restored,end);
        // This is the known ORIGINAL in generation, never a refusal stub.
        descriptor original(::openat(r.generation.value,name.c_str(),O_RDONLY|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC));
        const auto actual=metadata(original.value);struct stat named{};
        if(id(actual)!=expected||!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||actual.st_nlink!=1||
            (actual.st_mode&07777)!=original_mode||::fstatat(r.generation.value,name.c_str(),&named,AT_SYMLINK_NOFOLLOW)||
            !same(actual,named)||creator!=pid()||::fsync(original.value)||::close(original.release())||::fsync(r.generation.value))resource_fail();
        cold_check_corpus(r,store,issued,end);return identity_json(issued.ticket.at("stubs").at(kind));
    }
    json migrate_root(root& r,deadline end){
        if(!retirement_ready)resource_fail();check(end);const auto old_mode=number(r.fact,"logicalMode");
        auto root_entry=regular_control(r.control.value,"root.entry.lock",identity_json(r.fact.at("entry")));lock(root_entry.value,LOCK_EX,end);
        cold_root_budget(r,0,end);
        for(const auto& store:r.fact.at("stores")){auto issued=cold_open_issued(r,store,false,end);cold_check_corpus(r,store,issued,end);}
        // Only the actual retained retirement + fully issued corpus can restore
        // directory mutation. Original legacy file aliases remain mode0000.
        if(creator!=pid()||::fchmod(r.logical.value,static_cast<mode_t>(old_mode))||::fsync(r.logical.value))resource_fail();
        cold_cut(resource_cold_restart_test::cut::directory_restored,end);
        for(const auto& store:r.fact.at("stores")){
            auto issued=cold_open_issued(r,store,false,end);cold_check_corpus(r,store,issued,end);
            const auto name=text(store,"name",255);const auto main_id=identity_json(store.at("main"));const auto mode=number(store,"mode");
            if(mode>07777||(mode&0022)||!(mode&0400))resource_fail();
            move_file(r,store,issued,"main",main_id,static_cast<mode_t>(mode),end);
            for(const auto* value:{"wal","shm","journal"}){const std::string kind=value,journal=kind+".move.v1",auxiliary=name+"-"+kind;struct stat actual{};
                if(exists(issued.member.value,journal)){const auto record=parse(read_record(issued.member.value,journal,end));
                    move_file(r,store,issued,kind,identity_json(record.at("original")),0600,end);}
                else if(cold_stat(r.logical.value,auxiliary,actual))move_file(r,store,issued,kind,id(actual),0600,end);
            }
            cold_check_corpus(r,store,issued,end);
            cold_once(issued.member.value,"member.v1",cold_member_record(r,store,issued),end);
            cold_cut(resource_cold_restart_test::cut::member_record,end);
            cold_once(issued.member.value,"state.v1",{{"phase","active"},{"epoch",epoch},{"main",identity_json(main_id)}},end);
            cold_cut(resource_cold_restart_test::cut::state_record,end);cold_check_corpus(r,store,issued,end);
        }
        if(::fsync(r.logical.value)||::fsync(r.generation.value)||::fsync(r.control.value))resource_fail();check(end);
        const json root_record={{"format","lattice-resource-root-v1"},{"epoch",epoch},{"logical",r.fact.at("logical")},{"generation",r.fact.at("generation")},{"control",r.fact.at("control")},{"entry",r.fact.at("entry")},{"rule",r.fact.at("rule")}};
        once(r.control.value,"root.v1",root_record,end);cold_cut(resource_cold_restart_test::cut::root_record,end);
        auto result=r.fact;result.erase("stores");result.erase("logicalMode");result["rootSHA256"]=digest_string(bytes(root_record));return result;
    }

#endif
};
retained_resource_cold_restart::retained_resource_cold_restart(implementation* value)noexcept:impl_(value){}
retained_resource_cold_restart::~retained_resource_cold_restart(){
#if defined(LATTICE_RESOURCE_POSIX) && defined(__APPLE__) && TARGET_OS_OSX
    if(impl_&&impl_->creator==pid())delete impl_;
#endif
}
retained_resource_cold_restart::retained_resource_cold_restart(retained_resource_cold_restart&& other)noexcept:impl_(std::exchange(other.impl_,nullptr)){}
void retained_resource_cold_restart::prepare_package(const std::string& package_path,deadline end){
#if defined(LATTICE_RESOURCE_POSIX) && defined(__APPLE__) && TARGET_OS_OSX
    const auto creator=pid();bound(end);const auto home=actual_home();auto package_directory=absolute_directory(package_path,true);
    const auto package_raw=read_record(package_directory.value,"resource-package.v1",end);const auto package=parse(package_raw);
    if(text(package,"format")!="lattice-resource-package-v1"||!package.at("roles").is_array()||package.at("roles").empty()||package.at("roles").size()>64||!package.at("oldImages").is_array()||package.at("oldImages").empty()||package.at("oldImages").size()>128)resource_fail();
    const auto host=executable(package.at("host"));if(host.path!=package_path+"/lattice-resource-launcher")resource_fail();loaded_image(::getpid(),host,end);host_core_code_current(host,end);
    // The explicit installer invocation selects this controlled package. A
    // normal application cannot register itself from its received wire bytes.
    validate_roles(package.at("roles"),host,end,true);
    for(const auto& image:package.at("oldImages")){auto held=retained_installation_file::open_application_image(executable(image),end);held.verify(end);}
    resource_continuous_root_preflight continuous_roots(package,home,end); // all declarations before effects
    continuous_roots.verify(end);
    auto ensure=[&](const std::string& relative,bool private_last){
        auto current=absolute_directory(home,false);std::size_t at=0;
        while(at<relative.size()){const auto slash=relative.find('/',at);auto name=relative.substr(at,slash==std::string::npos?relative.size()-at:slash-at);leaf(name);struct stat present{};
            if(::fstatat(current.value,name.c_str(),&present,AT_SYMLINK_NOFOLLOW)!=0){if(errno!=ENOENT||creator!=pid()||::mkdirat(current.value,name.c_str(),0700)||::fsync(current.value))resource_fail();}
            descriptor next(::openat(current.value,name.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));const auto actual=metadata(next.value);
            if(!S_ISDIR(actual.st_mode)||actual.st_uid!=::geteuid()||(actual.st_mode&0022)||(slash==std::string::npos&&private_last&&(actual.st_mode&07777)!=0700))resource_fail();
            current.~descriptor();new(&current)descriptor(std::move(next));if(slash==std::string::npos)break;at=slash+1;
        }return current;
    };
    auto installation=continuous_roots.size()?continuous_roots.retain_installation(end):ensure(".lattice-resource-installation",true);struct stat prior{};
    if(::fstatat(installation.value,"active.v1",&prior,AT_SYMLINK_NOFOLLOW)==0||errno!=ENOENT)resource_fail();
    auto exists=[&](int dir,const char* name){struct stat s{};if(::fstatat(dir,name,&s,AT_SYMLINK_NOFOLLOW)==0)return true;if(errno!=ENOENT)resource_fail();return false;};
    if(!exists(installation.value,"controller.lock"))create_control(installation.value,"controller.lock",creator,end);
    auto controller=regular_control(installation.value,"controller.lock");lock(controller.value,LOCK_EX,end);
    const json preparation={{"format","lattice-resource-preparation-v1"},{"uid",::geteuid()},{"home",home},{"packageSHA256",digest_string(package_raw)},{"host",package.at("host")}};
    if(exists(installation.value,"preparation.intent.v1")){if(read_record(installation.value,"preparation.intent.v1",end)!=bytes(preparation))resource_fail();}
    else publish_record(installation.value,"preparation.intent.v1",preparation,creator,end);
    if(exists(installation.value,"restart.v1")){const auto original=parse(read_record(installation.value,"restart.v1",end));if(text(original,"packageSHA256",64)!=digest_string(package_raw))resource_fail();return;}
    std::string epoch;
    if(exists(installation.value,"epoch.v1")){const auto record=parse(read_record(installation.value,"epoch.v1",end));if(text(record,"packageSHA256",64)!=digest_string(package_raw))resource_fail();epoch=text(record,"epoch",64);}
    else{if(continuous_roots.size())epoch=continuous_roots.epoch();else{std::array<std::uint8_t,32> nonce{};if(creator!=pid()||::getentropy(nonce.data(),nonce.size()))resource_fail();epoch=picosha2::bytes_to_hex_string(nonce);}
        publish_record(installation.value,"epoch.v1",{{"epoch",epoch},{"packageSHA256",digest_string(package_raw)}},creator,end);}
    if(continuous_roots.size()&&epoch!=continuous_roots.epoch())resource_fail();
    continuous_roots.realize(end); // actual retained no-follow parents/containers
    struct root_request {std::string logical;json rule;std::size_t continuous=SIZE_MAX;};
    std::vector<root_request> requests{
        {".claude",{{"names",json::array({"memory.sqlite"})},{"suffixes",json::array()}}},
        {".claude/sync",{{"names",json::array()},{"suffixes",json::array({".sqlite",".sqlite.revoked"})}}},
        {"Library/Application Support/Orbital/rooms",{{"names",json::array()},{"suffixes",json::array({".lattice"})}}},
        {"Library/Application Support/Orbital",{{"names",json::array({"cluster.lattice","templates.lattice"})},{"suffixes",json::array()}}}
    };
    for(std::size_t at=0;at<continuous_roots.size();++at)requests.push_back({continuous_roots.logical(at),
        {{"names",json::array({"store.sqlite"})},{"suffixes",json::array()}},at});
    if(requests.size()>16)resource_fail(); // complete prospective root set was checked before effects
    json roots=json::array();std::size_t index=0;
    for(const auto& request:requests){
        if(creator!=pid())resource_fail();bound(end);
        auto logical=request.continuous==SIZE_MAX?ensure(request.logical,false):continuous_roots.retain_logical(request.continuous,end);
        const auto generation_leaf=request.continuous==SIZE_MAX?std::to_string(index):"continuous-"+std::to_string(request.continuous)+".lattice-continuous";
        const auto generation_relative=".lattice-resource-installation/generation-"+epoch+"/"+generation_leaf;
        const auto control_relative=".lattice-resource-installation/control-"+epoch+"/"+std::to_string(index++);
        auto generation=request.continuous==SIZE_MAX?ensure(generation_relative,true):continuous_roots.retain_generation(request.continuous,end);
        auto control=request.continuous==SIZE_MAX?ensure(control_relative,true):continuous_roots.retain_control(request.continuous,end);
        if(!exists(control.value,"root.entry.lock"))create_control(control.value,"root.entry.lock",creator,end);
        auto entry=regular_control(control.value,"root.entry.lock");lock(entry.value,LOCK_EX,end);
        descriptor independent(::openat(logical.value,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));DIR* directory=::fdopendir(independent.release());if(!directory)resource_fail();
        struct close_directory {DIR* value;std::uint64_t creator;~close_directory(){if(creator==pid())::closedir(value);}} ending{directory,creator};
        json stores=json::array();std::size_t entries=0;
        for(;;){if(creator!=pid())resource_fail();bound(end);errno=0;auto* entry=::readdir(directory);if(!entry){if(errno)resource_fail();break;}
            const std::string name=entry->d_name;if(name=="."||name=="..")continue;
            if(request.continuous!=SIZE_MAX)resource_fail(); // occupied lineage is never a generic cold relocation
            if(++entries>8192)resource_fail();bool selected=false;
            for(const auto& n:request.rule.at("names"))if(n.get<std::string>()==name)selected=true;
            for(const auto& s:request.rule.at("suffixes")){const auto suffix=s.get<std::string>();if(name.size()>suffix.size()&&name.ends_with(suffix))selected=true;}if(!selected)continue;
            if(stores.size()==4096)resource_fail();struct stat actual{};if(::fstatat(logical.value,name.c_str(),&actual,AT_SYMLINK_NOFOLLOW)||!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||actual.st_nlink!=1||(actual.st_mode&0022)||!(actual.st_mode&0400))resource_fail();
            json auxiliaries=json::array();for(const auto* suffix:{"-wal","-shm","-journal"}){const auto aux=name+suffix;struct stat found{};
                if(::fstatat(logical.value,aux.c_str(),&found,AT_SYMLINK_NOFOLLOW)==0){if(!S_ISREG(found.st_mode)||found.st_uid!=::geteuid()||found.st_nlink!=1||(found.st_mode&0022))resource_fail();auxiliaries.push_back({{"name",aux},{"identity",identity_json(id(found))}});}
                else if(errno!=ENOENT)resource_fail();}
            stores.push_back({{"name",name},{"main",identity_json(id(actual))},{"mode",actual.st_mode&07777},{"auxiliaries",std::move(auxiliaries)}});
        }
        std::sort(stores.begin(),stores.end(),[](const auto& a,const auto& b){return a.at("name").template get<std::string>()<b.at("name").template get<std::string>();});
        const auto actual_logical=metadata(logical.value);
        roots.push_back({{"logicalPath",home+"/"+request.logical},{"logical",identity_json(id(actual_logical))},{"logicalMode",actual_logical.st_mode&07777},{"generationPath",home+"/"+generation_relative},{"generation",identity_json(id(metadata(generation.value)))},{"controlPath",home+"/"+control_relative},{"control",identity_json(id(metadata(control.value)))},{"entry",identity_json(id(metadata(entry.value)))},{"rule",request.rule},{"stores",std::move(stores)}});
    }
    const json plan={{"format","lattice-resource-restart-plan-v1"},{"uid",::geteuid()},{"home",home},{"installation",identity_json(id(metadata(installation.value)))},{"controller",identity_json(id(metadata(controller.value)))},{"epoch",epoch},{"packageSHA256",digest_string(package_raw)},{"host",package.at("host")},{"oldImages",package.at("oldImages")},{"roles",package.at("roles")},{"roots",std::move(roots)}};
    if(creator!=pid())resource_fail();publish_record(installation.value,"restart.v1",plan,creator,end);
#else
    (void)package_path;(void)end;resource_fail();
#endif
}
retained_resource_cold_restart retained_resource_cold_restart::open_staged(deadline end){
#if defined(LATTICE_RESOURCE_POSIX) && defined(__APPLE__) && TARGET_OS_OSX
    const auto creator=pid();auto* state=new implementation;if(creator!=pid())resource_fail();
    try{state->home=actual_home();state->directory=state->home+"/.lattice-resource-installation";
        auto directory=absolute_directory(state->directory,true);state->installation.~descriptor();new(&state->installation)descriptor(std::move(directory));state->directory_id=id(metadata(state->installation.value));
        state->plan_bytes=read_record(state->installation.value,"restart.v1",end);state->plan=parse(state->plan_bytes);const auto& v=state->plan;
        if(text(v,"format")!="lattice-resource-restart-plan-v1"||number(v,"uid")!=::geteuid()||text(v,"home")!=state->home||identity_json(v.at("installation"))!=state->directory_id||!v.at("roots").is_array()||v.at("roots").empty()||v.at("roots").size()>16||!v.at("oldImages").is_array()||v.at("oldImages").empty()||v.at("oldImages").size()>128||!v.at("roles").is_array()||v.at("roles").empty()||v.at("roles").size()>64)resource_fail();
        state->epoch=text(v,"epoch",64);validate_roles(v.at("roles"),executable(v.at("host")),end,true);auto controller=regular_control(state->installation.value,"controller.lock",identity_json(v.at("controller")));lock(controller.value,LOCK_EX,end);state->controller.~descriptor();new(&state->controller)descriptor(std::move(controller));
        std::set<std::string> unique;for(const auto& r:v.at("roots")){const auto logical=text(r,"logicalPath"),generation=text(r,"generationPath"),control=text(r,"controlPath");
            if(!unique.insert(logical).second||!unique.insert(generation).second||!unique.insert(control).second||!r.at("stores").is_array()||r.at("stores").size()>4096)resource_fail();
            auto a=absolute_directory(logical,false),b=absolute_directory(generation,true),c=absolute_directory(control,true);state->roots.push_back({std::move(a),std::move(b),std::move(c),r});}
        state->check(end);return retained_resource_cold_restart(state);
    }catch(...){if(creator==pid())delete state;throw;}
#else
    (void)end;resource_fail();
#endif
}
retained_resource_normal_host retained_resource_cold_restart::retire_and_activate(deadline end){
#if defined(LATTICE_RESOURCE_POSIX) && defined(__APPLE__) && TARGET_OS_OSX
    if(!impl_)resource_fail();auto& state=*impl_;state.check(end);if(state.started)resource_fail();state.started=true;
    if(state.cold_resume_active(end))return retained_resource_normal_host::open_installed(end);
    state.cold_issuance_barrier(true,end);
    state.capture_originals(end);state.deny_originals(end);state.actual_retirement(end);
    state.retirement_ready=true;state.cold_cut(resource_cold_restart_test::cut::retirement_durable,end);
    state.cold_issuance_barrier(false,end);
    json roots=json::array();for(auto& root:state.roots)roots.push_back(state.migrate_root(root,end));
    state.check(end);const json active={{"format","lattice-resource-installation-v1"},{"uid",::geteuid()},{"home",state.home},{"installation",identity_json(state.directory_id)},{"epoch",state.epoch},{"host",state.plan.at("host")},{"roles",state.plan.at("roles")},{"roots",std::move(roots)}};
    state.once(state.installation.value,"active.v1",active,end);state.cold_cut(resource_cold_restart_test::cut::active_record,end);
    return retained_resource_normal_host::open_installed(end);
#else
    (void)end;resource_fail();
#endif
}
