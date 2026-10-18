// Included only by the actual retained cold installer. Optional package
// declarations select exact fixed-leaf roots; no SQL or runtime registration.
#if defined(LATTICE_RESOURCE_POSIX) && defined(__APPLE__) && TARGET_OS_OSX
class resource_continuous_root_preflight final {
    const std::uint64_t creator_=pid();
    std::string home_;
    const json* package_=nullptr; // borrowed only through actual prepare_package return
    descriptor home_fd_,installation_,generation_parent_,control_parent_;
    std::string epoch_;
    struct parent final {
        std::string relative;
        std::vector<std::string> parts;
        std::array<descriptor,4> directories;
        std::size_t present=0;
        bool used=false;
    };
    struct selected final {
        std::string relative,name;
        std::size_t parent_index=0;
        descriptor directory,generation,control;
    };
    std::array<parent,2> parents_;
    std::vector<selected> selected_;
    struct statfs volume_{};
    static void directory_metadata(const struct stat& actual,bool strict){
        if(!S_ISDIR(actual.st_mode)||actual.st_uid!=::geteuid()||(actual.st_mode&0022)||
           (strict&&(actual.st_mode&07777)!=0700))resource_fail();
    }
    void current(deadline end)const {if(creator_!=pid())resource_fail();bound(end);}
    void domain(int fd)const {
        struct statfs actual{};
        if(::fstatfs(fd,&actual)||std::strncmp(actual.f_fstypename,"apfs",sizeof(actual.f_fstypename))||
           actual.f_fsid.val[0]!=volume_.f_fsid.val[0]||actual.f_fsid.val[1]!=volume_.f_fsid.val[1]||
           id(metadata(fd)).device!=id(metadata(home_fd_.value)).device)resource_fail();
    }
    void named(int parent_fd,const std::string& name,int fd,bool strict,deadline end)const {
        current(end);struct stat actual{};const auto held=metadata(fd);directory_metadata(held,strict);
        if(::fstatat(parent_fd,name.c_str(),&actual,AT_SYMLINK_NOFOLLOW)||!same(held,actual))resource_fail();
        domain(fd);current(end);
    }
    void absent(int parent_fd,const std::string& name,deadline end)const {
        current(end);struct stat actual{};
        if(::fstatat(parent_fd,name.c_str(),&actual,AT_SYMLINK_NOFOLLOW)==0||errno!=ENOENT)resource_fail();
    }
    void empty(int fd,deadline end)const {
        current(end);descriptor cursor(::openat(fd,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        if(!same(metadata(cursor.value),metadata(fd)))resource_fail();
        DIR* raw=::fdopendir(cursor.release());if(!raw)resource_fail();
        struct ending {DIR* value;std::uint64_t creator;~ending(){if(creator==pid())::closedir(value);}} done{raw,creator_};
        for(;;){current(end);errno=0;auto* entry=::readdir(raw);if(!entry){if(errno)resource_fail();break;}
            const std::string name=entry->d_name;if(name!="."&&name!="..")resource_fail();}
        current(end);
    }
    // Account for the entire prospective root set and retained preflight,
    // before opening the first declaration or making any filesystem effect.
    void descriptor_budget(const json& package,deadline end,bool reserve_all_roots=false)const {
        current(end);struct rlimit limit{};if(::getrlimit(RLIMIT_NOFILE,&limit))resource_fail();
        std::array<proc_fdinfo,1024> files{};
        const int n=::proc_pidinfo(::getpid(),PROC_PIDLISTFDS,0,files.data(),sizeof(files));
        if(n<=0||n%sizeof(proc_fdinfo)||static_cast<std::size_t>(n)>=sizeof(files))resource_fail();
        const auto count=static_cast<std::size_t>(n)/sizeof(proc_fdinfo);
        const auto capacity=std::min<std::uint64_t>(1024,static_cast<std::uint64_t>(limit.rlim_cur));
        const auto depth=[](const std::string& path){return static_cast<std::size_t>(std::count(path.begin(),path.end(),'/'));};
        std::size_t image_depth=depth(text(package.at("host"),"path"));
        for(const auto& image:package.at("oldImages"))image_depth=std::max(image_depth,depth(text(image,"path")));
        // Retain home, six fixed-parent components, installation, two epoch
        // parents, and each logical/generation/control root. The initial
        // reservation includes the FULL prospective root set. Later actual
        // retirement keeps its existing per-original fd census/caps; no
        // hypothetical old-role count tightens the old installation contract.
        const std::size_t preparation=1+6+1+2+3*selected_.size();
        const std::size_t cold_roots=3*(4+selected_.size())+2;
        std::size_t owned=0;
        const auto count_owned=[&](const descriptor& actual){if(actual.value>=0)++owned;};
        count_owned(home_fd_);count_owned(installation_);count_owned(generation_parent_);count_owned(control_parent_);
        for(const auto& parent:parents_)for(std::size_t at=0;at<parent.present;++at)count_owned(parent.directories[at]);
        for(const auto& selected:selected_){count_owned(selected.directory);count_owned(selected.generation);count_owned(selected.control);}
        if(owned>preparation)resource_fail();
        const auto remaining=reserve_all_roots?std::max(preparation,cold_roots):preparation-owned;
        const auto headroom=remaining+32+std::max(image_depth,depth(home_));
        if(capacity<headroom||count>capacity-headroom)resource_fail();
        for(std::size_t at=0;at<count;++at)if(files[at].proc_fd>=capacity-headroom)resource_fail();
    }
    int parent_fd(const parent& value)const {return value.present?value.directories[value.present-1].value:home_fd_.value;}
    void check_parent(const parent& value,deadline end)const {
        if(!value.used)return;int fd=home_fd_.value;
        for(std::size_t at=0;at<value.present;++at){named(fd,value.parts[at],value.directories[at].value,false,end);fd=value.directories[at].value;}
        if(value.present<value.parts.size())absent(fd,value.parts[value.present],end);
    }
    void make_directory(descriptor& target,int parent_fd,const std::string& name,bool strict,deadline end){
        current(end);absent(parent_fd,name,end);
        const auto parent=metadata(parent_fd);
        if((parent.st_mode&0700)!=0700)resource_fail();
        // EEXIST is a race, never authority to adopt an unobserved directory.
        if(creator_!=pid()||::mkdirat(parent_fd,name.c_str(),0700)||::fsync(parent_fd))resource_fail();
        descriptor actual(::openat(parent_fd,name.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        named(parent_fd,name,actual.value,strict,end);
        target.~descriptor();new(&target)descriptor(std::move(actual));
        if(creator_!=pid()||::fsync(target.value)||::fsync(parent_fd))resource_fail();
        named(parent_fd,name,target.value,strict,end);
    }
    std::string generation_name()const {return "generation-"+epoch_;}
    std::string control_name()const {return "control-"+epoch_;}
    static std::string generation_leaf(std::size_t at){return "continuous-"+std::to_string(at)+".lattice-continuous";}
    void existing(descriptor& target,int parent_fd,const std::string& name,bool must_be_empty,deadline end){
        current(end);struct stat present{};
        if(::fstatat(parent_fd,name.c_str(),&present,AT_SYMLINK_NOFOLLOW)){if(errno==ENOENT){if((metadata(parent_fd).st_mode&0700)!=0700)resource_fail();return;}resource_fail();}
        descriptor actual(::openat(parent_fd,name.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));named(parent_fd,name,actual.value,true,end);
        if(must_be_empty){if(::flock(actual.value,LOCK_EX|LOCK_NB))resource_fail();empty(actual.value,end);}
        target.~descriptor();new(&target)descriptor(std::move(actual));
    }
    void check_optional(int parent_fd,const std::string& name,const descriptor& actual,bool must_be_empty,deadline end)const {
        if(actual.value<0)absent(parent_fd,name,end);
        else {named(parent_fd,name,actual.value,true,end);if(must_be_empty)empty(actual.value,end);}
    }
    void check_physical(deadline end)const {
        check_optional(home_fd_.value,".lattice-resource-installation",installation_,false,end);
        if(installation_.value<0)return;
        check_optional(installation_.value,generation_name(),generation_parent_,false,end);
        check_optional(installation_.value,control_name(),control_parent_,false,end);
        for(std::size_t at=0;at<selected_.size();++at){const auto& selected=selected_[at];
            if(generation_parent_.value>=0)check_optional(generation_parent_.value,generation_leaf(at),selected.generation,true,end);
            else if(selected.generation.value>=0)resource_fail();
            if(control_parent_.value>=0)check_optional(control_parent_.value,std::to_string(4+at),selected.control,false,end);
            else if(selected.control.value>=0)resource_fail();}
    }
    void distinct_directories(deadline end)const {
        current(end);std::set<std::pair<std::uint64_t,std::uint64_t>> parents;
        const auto parent=[&](const descriptor& value){if(value.value>=0){const auto actual=id(metadata(value.value));parents.emplace(actual.device,actual.inode);}};
        parent(home_fd_);parent(installation_);parent(generation_parent_);parent(control_parent_);
        for(const auto& value:parents_)for(std::size_t at=0;at<value.present;++at)parent(value.directories[at]);
        auto roots=parents;
        for(const auto& value:selected_)for(const auto* actual:{&value.directory,&value.generation,&value.control})if(actual->value>=0){
            const auto identity=id(metadata(actual->value));if(!roots.emplace(identity.device,identity.inode).second)resource_fail();}
    }
    static json original_rule(std::size_t at){
        switch(at){
            case 0:return {{"names",json::array({"memory.sqlite"})},{"suffixes",json::array()}};
            case 1:return {{"names",json::array()},{"suffixes",json::array({".sqlite",".sqlite.revoked"})}};
            case 2:return {{"names",json::array()},{"suffixes",json::array({".lattice"})}};
            case 3:return {{"names",json::array({"cluster.lattice","templates.lattice"})},{"suffixes",json::array()}};
            default:resource_fail();
        }
    }
    json bounded_original_stores(std::size_t at,deadline end)const {
        const int fd=at==0?(parents_[0].present>=1?parents_[0].directories[0].value:-1):
            at==1?(parents_[0].present==2?parents_[0].directories[1].value:-1):
            at==2?(parents_[1].present==4?parents_[1].directories[3].value:-1):
                   (parents_[1].present>=3?parents_[1].directories[2].value:-1);
        json stores=json::array();if(fd<0)return stores;
        const auto parent=metadata(fd);if((parent.st_mode&0700)!=0700)resource_fail();
        const auto rule=original_rule(at);
        descriptor cursor(::openat(fd,".",O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));
        if(!same(metadata(cursor.value),parent))resource_fail();DIR* raw=::fdopendir(cursor.release());if(!raw)resource_fail();
        struct ending {DIR* value;std::uint64_t creator;~ending(){if(creator==pid())::closedir(value);}} done{raw,creator_};
        std::size_t entries=0;
        const auto maximum=identity_json(file_identity{UINT64_MAX,UINT64_MAX});
        for(;;){current(end);errno=0;auto* entry=::readdir(raw);if(!entry){if(errno)resource_fail();break;}
            const std::string name=entry->d_name;if(name=="."||name=="..")continue;if(++entries>8192)resource_fail();
            bool selected=false;for(const auto& value:rule.at("names"))if(value.get<std::string>()==name)selected=true;
            for(const auto& value:rule.at("suffixes")){const auto suffix=value.get<std::string>();if(name.size()>suffix.size()&&name.ends_with(suffix))selected=true;}
            if(!selected)continue;leaf(name);if(stores.size()==4096)resource_fail();struct stat actual{};
            if(::fstatat(fd,name.c_str(),&actual,AT_SYMLINK_NOFOLLOW)||!S_ISREG(actual.st_mode)||actual.st_uid!=::geteuid()||actual.st_nlink!=1||
               (actual.st_mode&0022)||!(actual.st_mode&0400))resource_fail();
            json auxiliaries=json::array();
            for(const auto* suffix:{"-wal","-shm","-journal"}){const auto auxiliary=name+suffix;struct stat found{};
                if(::fstatat(fd,auxiliary.c_str(),&found,AT_SYMLINK_NOFOLLOW)==0){if(!S_ISREG(found.st_mode)||found.st_uid!=::geteuid()||found.st_nlink!=1||(found.st_mode&0022))resource_fail();}
                else if(errno!=ENOENT)resource_fail();
                // All three possible siblings are charged, even when a live
                // old writer has not created one yet. No SQLite file is read.
                auxiliaries.push_back({{"name",auxiliary},{"identity",maximum}});}
            stores.push_back({{"name",name},{"main",maximum},{"mode",07777},{"auxiliaries",std::move(auxiliaries)}});
        }
        return stores;
    }
    void frame_budget(const json& package,deadline end)const {
        current(end);const auto maximum=identity_json(file_identity{UINT64_MAX,UINT64_MAX});
        const std::array<std::string,4> logicals{".claude",".claude/sync","Library/Application Support/Orbital/rooms","Library/Application Support/Orbital"};
        json roots=json::array(),active_roots=json::array();
        for(std::size_t at=0;at<4+selected_.size();++at){
            const bool continuous=at>=4;const auto relative=continuous?selected_[at-4].relative:logicals[at];
            const auto generation=home_+"/.lattice-resource-installation/"+generation_name()+"/"+(continuous?generation_leaf(at-4):std::to_string(at));
            const auto control=home_+"/.lattice-resource-installation/"+control_name()+"/"+std::to_string(at);
            const auto logical=home_+"/"+relative;
            if(logical.size()>4096||generation.size()>4096||control.size()>4096)resource_fail();
            const json rule=continuous?json{{"names",json::array({"store.sqlite"})},{"suffixes",json::array()}}:original_rule(at);
            const auto stores=continuous?json::array():bounded_original_stores(at,end);
            // These are finite DATA upper bounds, never prospective identities
            // consumed by resource admission. Actual identities come from fds.
            json root={{"logicalPath",logical},{"logical",maximum},{"logicalMode",07777},
                {"generationPath",generation},{"generation",maximum},{"controlPath",control},{"control",maximum},
                {"entry",maximum},{"rule",rule},{"stores",stores}};
            roots.push_back(root);root.erase("stores");root.erase("logicalMode");root["rootSHA256"]=std::string(64,'0');active_roots.push_back(std::move(root));
        }
        const json plan={{"format","lattice-resource-restart-plan-v1"},{"uid",::geteuid()},{"home",home_},
            {"installation",maximum},{"controller",maximum},{"epoch",epoch_},{"packageSHA256",std::string(64,'0')},
            {"host",package.at("host")},{"oldImages",package.at("oldImages")},{"roles",package.at("roles")},{"roots",std::move(roots)}};
        const json active={{"format","lattice-resource-installation-v1"},{"uid",::geteuid()},{"home",home_},
            {"installation",maximum},{"epoch",epoch_},{"host",package.at("host")},{"roles",package.at("roles")},{"roots",std::move(active_roots)}};
        // Existing canonical parser supplies BOTH frame-byte and total-event
        // bounds, before the first package directory or file effect.
        (void)parse(bytes(plan));(void)parse(bytes(active));current(end);
    }
    descriptor duplicate(const descriptor& actual,deadline end)const {
        current(end);descriptor result(::fcntl(actual.value,F_DUPFD_CLOEXEC,3));
        if(!same(metadata(result.value),metadata(actual.value)))resource_fail();current(end);return result;
    }
public:
    resource_continuous_root_preflight(const json& package,const std::string& home,deadline end):home_(home),package_(&package){
        parents_[0].relative=".claude/sync";parents_[0].parts={".claude","sync"};
        parents_[1].relative="Library/Application Support/Orbital/rooms";parents_[1].parts={"Library","Application Support","Orbital","rooms"};
        if(!package.contains("continuousContainers"))return; // old package formats unchanged
        const auto& declarations=package.at("continuousContainers");
        if(!declarations.is_array()||declarations.size()>12||4+declarations.size()>16)resource_fail();
        selected_.reserve(declarations.size());std::set<std::string> unique;
        // Parse ALL declarations and complete finite path/root bounds before
        // reading any candidate directory, and before ANY package effect.
        for(const auto& value:declarations){
            if(!value.is_string())resource_fail();const auto relative=value.get<std::string>();
            std::size_t selected_parent=parents_.size();std::string name;
            for(std::size_t at=0;at<parents_.size();++at){const auto prefix=parents_[at].relative+"/";
                if(relative.starts_with(prefix)){selected_parent=at;name=relative.substr(prefix.size());break;}}
            if(selected_parent==parents_.size())resource_fail();leaf(name);
            constexpr std::string_view suffix=".lattice-continuous";
            if(name.size()<=suffix.size()||!name.ends_with(suffix)||!unique.insert(relative).second||
               home_.size()+1+relative.size()>4096)resource_fail();
            const auto generation=home_+"/.lattice-resource-installation/generation-"+std::string(64,'0')+"/continuous-"+std::to_string(selected_.size())+".lattice-continuous";
            const auto control=home_+"/.lattice-resource-installation/control-"+std::string(64,'0')+"/"+std::to_string(4+selected_.size());
            if(generation.size()>4096||control.size()>4096)resource_fail();
            selected_.emplace_back();auto& added=selected_.back();added.relative=relative;added.name=std::move(name);added.parent_index=selected_parent;
            parents_[selected_parent].used=true;
        }
        if(selected_.empty())return;
        parents_[0].used=true;parents_[1].used=true; // whole prospective four ordinary roots
        descriptor_budget(package,end,true);
        auto home_directory=absolute_directory(home_,false);home_fd_.~descriptor();new(&home_fd_)descriptor(std::move(home_directory));
        if(::fstatfs(home_fd_.value,&volume_)||std::strncmp(volume_.f_fstypename,"apfs",sizeof(volume_.f_fstypename)))resource_fail();domain(home_fd_.value);
        // Retain the actual installation and all existing physical parents.
        // A declared occupied generation container cannot hide behind the
        // logical empty path or be discovered only after preparation writes.
        existing(installation_,home_fd_.value,".lattice-resource-installation",false,end);
        if(installation_.value>=0){
            struct stat active{};if(::fstatat(installation_.value,"active.v1",&active,AT_SYMLINK_NOFOLLOW)==0||errno!=ENOENT)resource_fail();
            struct stat present{};
            if(::fstatat(installation_.value,"epoch.v1",&present,AT_SYMLINK_NOFOLLOW)==0){
                const auto record=parse(read_record(installation_.value,"epoch.v1",end));
                if(text(record,"packageSHA256",64)!=digest_string(bytes(package)))resource_fail();epoch_=text(record,"epoch",64);
                if(epoch_.size()!=64||epoch_.find_first_not_of("0123456789abcdef")!=std::string::npos)resource_fail();
            }else if(errno!=ENOENT)resource_fail();
            for(const auto* record:{"preparation.intent.v1","restart.v1"}){
                if(::fstatat(installation_.value,record,&present,AT_SYMLINK_NOFOLLOW)==0){const auto prior=parse(read_record(installation_.value,record,end));
                    if(text(prior,"packageSHA256",64)!=digest_string(bytes(package)))resource_fail();}
                else if(errno!=ENOENT)resource_fail();}
        }
        if(epoch_.empty()){std::array<std::uint8_t,32> nonce{};current(end);
            if(::getentropy(nonce.data(),nonce.size()))resource_fail();epoch_=picosha2::bytes_to_hex_string(nonce);}
        if(installation_.value>=0){
            existing(generation_parent_,installation_.value,generation_name(),false,end);
            existing(control_parent_,installation_.value,control_name(),false,end);
            for(std::size_t at=0;at<selected_.size();++at){auto& selected=selected_[at];
                if(generation_parent_.value>=0)existing(selected.generation,generation_parent_.value,generation_leaf(at),true,end);
                if(control_parent_.value>=0)existing(selected.control,control_parent_.value,std::to_string(4+at),false,end);}
        }
        for(auto& parent:parents_){if(!parent.used)continue;
            for(const auto& part:parent.parts){const int fd=parent_fd(parent);struct stat present{};
                if(::fstatat(fd,part.c_str(),&present,AT_SYMLINK_NOFOLLOW)){if(errno==ENOENT){if((metadata(fd).st_mode&0700)!=0700)resource_fail();break;}resource_fail();}
                descriptor actual(::openat(fd,part.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));named(fd,part,actual.value,false,end);
                parent.directories[parent.present].~descriptor();new(&parent.directories[parent.present])descriptor(std::move(actual));++parent.present;
            }
        }
        std::set<std::pair<std::uint64_t,std::uint64_t>> identities;
        for(auto& selected:selected_){auto& parent=parents_[selected.parent_index];if(parent.present!=parent.parts.size())continue;
            const int fd=parent_fd(parent);struct stat present{};
            if(::fstatat(fd,selected.name.c_str(),&present,AT_SYMLINK_NOFOLLOW)){if(errno==ENOENT){if((metadata(fd).st_mode&0700)!=0700)resource_fail();continue;}resource_fail();}
            descriptor actual(::openat(fd,selected.name.c_str(),O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC));named(fd,selected.name,actual.value,true,end);
            const auto identity=id(metadata(actual.value));if(!identities.emplace(identity.device,identity.inode).second)resource_fail();
            if(::flock(actual.value,LOCK_EX|LOCK_NB))resource_fail();empty(actual.value,end);
            selected.directory.~descriptor();new(&selected.directory)descriptor(std::move(actual));
        }
        verify(end);frame_budget(package,end);
    }
    std::size_t size()const noexcept {return selected_.size();}
    const std::string& logical(std::size_t at)const {if(creator_!=pid()||at>=selected_.size())resource_fail();return selected_[at].relative;}
    void verify(deadline end)const {
        current(end);if(selected_.empty())return;
        descriptor_budget(*package_,end); // complete actual census remains live through effects
        actual_directory(home_fd_.value,home_,id(metadata(home_fd_.value)),false);domain(home_fd_.value);
        check_physical(end);distinct_directories(end);
        for(const auto& parent:parents_)check_parent(parent,end);
        for(const auto& selected:selected_){const auto& parent=parents_[selected.parent_index];
            if(parent.present!=parent.parts.size()){if(selected.directory.value>=0)resource_fail();continue;}
            if(selected.directory.value<0)absent(parent_fd(parent),selected.name,end);
            else {named(parent_fd(parent),selected.name,selected.directory.value,true,end);empty(selected.directory.value,end);}}
        current(end);
    }
    const std::string& epoch()const {if(creator_!=pid()||selected_.empty())resource_fail();return epoch_;}
    descriptor retain_installation(deadline end){
        verify(end);if(selected_.empty())resource_fail();
        if(installation_.value<0)make_directory(installation_,home_fd_.value,".lattice-resource-installation",true,end);
        verify(end);return duplicate(installation_,end);
    }
    void realize(deadline end){
        verify(end);if(selected_.empty())return;
        if(installation_.value<0)resource_fail();
        if(generation_parent_.value<0)make_directory(generation_parent_,installation_.value,generation_name(),true,end);
        if(control_parent_.value<0)make_directory(control_parent_,installation_.value,control_name(),true,end);
        for(std::size_t at=0;at<selected_.size();++at){auto& selected=selected_[at];
            if(selected.generation.value<0){make_directory(selected.generation,generation_parent_.value,generation_leaf(at),true,end);
                if(::flock(selected.generation.value,LOCK_EX|LOCK_NB))resource_fail();}
            if(selected.control.value<0)make_directory(selected.control,control_parent_.value,std::to_string(4+at),true,end);}

        for(auto& parent:parents_){if(!parent.used)continue;
            while(parent.present<parent.parts.size()){const int fd=parent_fd(parent);const auto at=parent.present;
                make_directory(parent.directories[at],fd,parent.parts[at],false,end);++parent.present;}}
        for(auto& selected:selected_){if(selected.directory.value>=0)continue;
            make_directory(selected.directory,parent_fd(parents_[selected.parent_index]),selected.name,true,end);
            if(::flock(selected.directory.value,LOCK_EX|LOCK_NB))resource_fail();}
        verify(end);
    }
    descriptor retain_logical(std::size_t at,deadline end)const {
        verify(end);if(at>=selected_.size()||selected_[at].directory.value<0)resource_fail();
        return duplicate(selected_[at].directory,end);
    }
    descriptor retain_generation(std::size_t at,deadline end)const {
        verify(end);if(at>=selected_.size()||selected_[at].generation.value<0)resource_fail();return duplicate(selected_[at].generation,end);
    }
    descriptor retain_control(std::size_t at,deadline end)const {
        verify(end);if(at>=selected_.size()||selected_[at].control.value<0)resource_fail();return duplicate(selected_[at].control,end);
    }
};
#endif
